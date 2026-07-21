"""Tests for GenericSQLConnector per-stream type-mapper dispatch.

The handler registers a stream_id → endpoint_ref index at startup; each
incoming SchemaMessage picks its mapper by scope. These tests lock in
that contract without the full gRPC/materialization stack.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from cdk.connection_runtime import ConnectionRuntime
from cdk.sql.exceptions import SchemaConfigurationError
from cdk.sql.generic import GenericSQLConnector
from cdk.type_map import TypeMapper
from cdk.type_map.rules import parse_rules


def _mapper(label: str) -> TypeMapper:
    return TypeMapper(
        label,
        parse_rules(
            [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
            source="<test>",
        ),
    )


def _runtime(
    *,
    connector_mapper: TypeMapper | None = None,
    connection_mapper: TypeMapper | None = None,
) -> ConnectionRuntime:
    return ConnectionRuntime(
        raw_config={},
        connection_id="dest-conn",
        connector_id="test-connector",
        connector_type="database",
        driver="postgresql",
        resolver=AsyncMock(),
        connector_type_mapper=connector_mapper,
        connection_type_mapper=connection_mapper,
    )


class TestEndpointRefDispatch:
    def test_pre_connect_raises(self):
        handler = GenericSQLConnector()
        handler.set_endpoint_refs(
            {
                "s1": {
                    "scope": "connector",
                    "connection_id": "pg",
                    "endpoint_id": "transfers",
                }
            }
        )
        with pytest.raises(RuntimeError, match="called before connect"):
            handler._type_mapper_for_stream("s1")

    def test_unknown_stream_id_raises(self):
        handler = GenericSQLConnector()
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        handler.set_endpoint_refs(
            {
                "s1": {
                    "scope": "connector",
                    "connection_id": "pg",
                    "endpoint_id": "transfers",
                }
            }
        )
        with pytest.raises(RuntimeError, match="no endpoint_ref registered"):
            handler._type_mapper_for_stream("unregistered-stream")

    def test_connector_scoped_uses_connector_mapper(self):
        handler = GenericSQLConnector()
        connector_map = _mapper("pg")
        handler._runtime = _runtime(
            connector_mapper=connector_map,
            connection_mapper=_mapper("connection:dest-conn"),
        )
        handler.set_endpoint_refs(
            {
                "s1": {
                    "scope": "connector",
                    "connection_id": "pg",
                    "endpoint_id": "transfers",
                }
            }
        )
        assert handler._type_mapper_for_stream("s1") is connector_map

    def test_connection_scoped_uses_connection_mapper(self):
        # Connection scope returns a composed mapper with the connection's slug.
        # The connection map is the primary (its rules take precedence).
        handler = GenericSQLConnector()
        handler._runtime = _runtime(
            connector_mapper=_mapper("pg"),
            connection_mapper=_mapper("connection:dest-conn"),
        )
        handler.set_endpoint_refs(
            {
                "s1": {
                    "scope": "connection",
                    "connection_id": "dest-conn",
                    "endpoint_id": "orders",
                }
            }
        )
        assert (
            handler._type_mapper_for_stream("s1").connector_slug
            == "connection:dest-conn"
        )

    def test_set_endpoint_refs_copies_mapping(self):
        """External mutations must not leak into the handler's state."""
        handler = GenericSQLConnector()
        source = {
            "s1": {
                "scope": "connector",
                "connection_id": "pg",
                "endpoint_id": "transfers",
            }
        }
        handler.set_endpoint_refs(source)
        source["s1"] = {
            "scope": "connector",
            "connection_id": "evil",
            "endpoint_id": "injected",
        }
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        # Original registration wins — set_endpoint_refs took a defensive copy.
        assert handler._endpoint_refs["s1"] == {
            "scope": "connector",
            "connection_id": "pg",
            "endpoint_id": "transfers",
        }


class TestColumnDefStrictness:
    """``_build_column_defs`` must refuse malformed payloads rather than
    silently dropping columns. Covers the sibling raise of the Arrow-side
    check in ``schema_contract``."""

    def test_unnamed_column_raises(self):
        from cdk.sql.generic import _StreamState

        handler = GenericSQLConnector()
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))

        state = _StreamState(
            schema_name="public",
            table_name="t",
            endpoint_document={
                "columns": [
                    {"arrow_type": "Int64"},  # missing 'name'
                    {"name": "valid", "arrow_type": "Int64"},
                ]
            },
        )
        with pytest.raises(SchemaConfigurationError, match="has no 'name' field"):
            handler._build_column_defs(state)

    def test_column_without_arrow_type_raises(self):
        from cdk.sql.generic import _StreamState

        handler = GenericSQLConnector()
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))

        state = _StreamState(
            schema_name="public",
            table_name="t",
            endpoint_document={
                "columns": [
                    # native_type alone is not enough: DDL consumes the
                    # stored arrow_type (the same declaration the schema
                    # contract casts with), never the read map.
                    {"name": "id", "native_type": "BIGINT"},
                ]
            },
        )
        with pytest.raises(SchemaConfigurationError, match="has no 'arrow_type'"):
            handler._build_column_defs(state)


class TestWriteBatchFatalOnTypeMapError:
    """Deterministic config and type-map errors in write_batch must not be retried."""

    @pytest.mark.asyncio
    async def test_missing_schema_contract_classified_as_fatal(self):
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock, MagicMock

        from cdk.sql.generic import GenericSQLConnector, _StreamState
        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = GenericSQLConnector()
        handler._engine = MagicMock()
        handler._connected = True
        handler._streams["s1"] = _StreamState(
            table=MagicMock(),
            schema_name="myschema",
            table_name="events",
            write_mode="insert",
            primary_keys=[],
            schema_contract=None,
        )

        @asynccontextmanager
        async def _fake_begin():
            yield AsyncMock()

        handler._engine.begin = _fake_begin

        import pyarrow as pa

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "adbc" in result.failure_summary
        assert "SchemaContract" in result.failure_summary

    @pytest.mark.asyncio
    async def test_type_map_error_classified_as_fatal(self):
        from contextlib import asynccontextmanager
        from unittest.mock import MagicMock

        from cdk.sql.generic import GenericSQLConnector, _StreamState
        from cdk.type_map import UnmappedTypeError
        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = GenericSQLConnector()
        # Preconditions: connected, schema configured. We don't actually hit
        # the DB because the insert raises before any SQL runs.
        contract_mock = MagicMock()
        contract_mock.to_db_records.return_value = [{"id": 1}]

        handler._engine = MagicMock()
        handler._connected = True
        handler._streams["s1"] = _StreamState(
            table=MagicMock(),
            write_mode="insert",
            primary_keys=[],
            schema_contract=contract_mock,
        )

        class _FakeTxnConn:
            async def run_sync(self, fn, *args):
                # AsyncConnection.run_sync hands the sync Connection to fn.
                return fn(MagicMock(), *args)

        @asynccontextmanager
        async def _fake_begin():
            yield _FakeTxnConn()

        handler._engine.begin = _fake_begin

        # The schema contract's prepare_records is called inside _insert_records;
        # route the UnmappedTypeError through that entry.
        def _raising_insert(_conn, _state, _records):
            raise UnmappedTypeError("pg", "forward", "MONEY")

        handler._insert_records = _raising_insert  # type: ignore[method-assign]

        import pyarrow as pa

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "type-map" in result.failure_summary

    @pytest.mark.asyncio
    async def test_adbc_only_missing_schema_contract_names_table(self):
        # The ADBC-only guard message must carry schema.table context so
        # the failure_summary is actionable in monitoring (issue #149).

        from cdk.sql.generic import GenericSQLConnector, _StreamState
        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = GenericSQLConnector()
        handler._connected = True
        handler._adbc_only = True
        handler._streams["s1"] = _StreamState(
            schema_name="myschema",
            table_name="events",
            write_mode="insert",
            primary_keys=[],
            schema_contract=None,
        )

        import pyarrow as pa

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "myschema.events" in result.failure_summary

    @pytest.mark.asyncio
    async def test_upsert_without_conflict_keys_classified_as_fatal(self):
        # End-to-end through write_batch: the _upsert_records raise must be
        # classified FATAL (deterministic — retrying an unkeyed upsert can
        # never heal), not retried forever or degraded.
        from contextlib import asynccontextmanager
        from unittest.mock import MagicMock

        from cdk.sql.generic import GenericSQLConnector, _StreamState
        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = GenericSQLConnector()
        contract_mock = MagicMock()
        contract_mock.to_db_records.return_value = [{"id": 1}]

        handler._engine = MagicMock()
        handler._connected = True
        handler._streams["s1"] = _StreamState(
            table=MagicMock(),
            write_mode="upsert",
            conflict_keys=[],
            schema_contract=contract_mock,
        )

        class _FakeTxnConn:
            async def run_sync(self, fn, *args):
                return fn(MagicMock(), *args)

        @asynccontextmanager
        async def _fake_begin():
            yield _FakeTxnConn()

        handler._engine.begin = _fake_begin

        import pyarrow as pa

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "write-config" in result.failure_summary


class TestUpsertFailsLoudWithoutConflictKeys:
    """Upsert with no conflict keys fails loud — the engine never silently
    degrades to INSERT (which would duplicate rows) and never derives a
    target from ``primary_keys`` (issue #254)."""

    @pytest.mark.asyncio
    async def test_upsert_without_conflict_keys_raises(self):
        from unittest.mock import MagicMock

        from cdk.sql.exceptions import SchemaConfigurationError
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        handler = GenericSQLConnector()
        handler._insert_records = MagicMock()  # type: ignore[method-assign]
        conn = MagicMock()
        state = _StreamState(
            table=MagicMock(),
            schema_name="public",
            table_name="events",
            write_mode="upsert",
            primary_keys=["id"],  # present, but must NOT be used as a fallback
            conflict_keys=[],
        )

        with pytest.raises(SchemaConfigurationError, match="no conflict_keys"):
            handler._upsert_records(conn, state, [{"id": 1}])

        handler._insert_records.assert_not_called()
        conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_upsert_with_conflict_keys_executes(self):
        from unittest.mock import MagicMock

        from cdk.sql.generic import GenericSQLConnector, _StreamState

        handler = GenericSQLConnector()
        handler.dialect = MagicMock()
        handler.dialect.build_sqlalchemy_upsert.return_value = MagicMock()
        state = _StreamState(
            table=MagicMock(),
            schema_name="public",
            table_name="events",
            write_mode="upsert",
            conflict_keys=["id"],
        )

        conn = MagicMock()
        handler._upsert_records(conn, state, [{"id": 1}])

        handler.dialect.build_sqlalchemy_upsert.assert_called_once()
        # The verbatim conflict target must reach the SQL builder unchanged.
        args, _ = handler.dialect.build_sqlalchemy_upsert.call_args
        assert args[2] == ["id"]
        conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_adbc_upsert_without_conflict_keys_raises(self):
        # ADBC twin of the SQLAlchemy raise: the same fail-loud semantics
        # must hold on the MERGE path (Snowflake/BigQuery), before any ingest.
        from unittest.mock import MagicMock

        from cdk.sql.exceptions import SchemaConfigurationError
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        handler = GenericSQLConnector()
        handler._adbc_only = True
        handler._merge_ingest_sync = MagicMock()  # type: ignore[method-assign]
        handler._adbc_only_ingest_sync = MagicMock()  # type: ignore[method-assign]
        contract = MagicMock()
        contract.cast_arrow_batch.return_value = MagicMock()
        state = _StreamState(
            schema_name="analytics",
            table_name="events",
            write_mode="upsert",
            conflict_keys=[],
            schema_contract=contract,
        )

        with pytest.raises(SchemaConfigurationError, match="no conflict_keys"):
            await handler._write_batch_adbc_only(
                state,
                "run-1",
                "s1",
                1,
                MagicMock(),
                truncate_now=False,
            )

        # Fail before any ingest/MERGE — no partial write.
        handler._merge_ingest_sync.assert_not_called()
        handler._adbc_only_ingest_sync.assert_not_called()


class TestEnsureTablesEngineNoneRaises:
    """A None engine during DDL is a violated invariant; silently skipping
    DDL would leave write_batch returning RETRYABLE_FAILURE forever
    (issue #150)."""

    @pytest.mark.asyncio
    async def test_engine_none_raises_adbc_configuration_error(self):
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        from cdk.adbc_registry import AdbcConfigurationError
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        handler = GenericSQLConnector()
        assert handler._engine is None
        state = _StreamState(
            schema_name="public",
            table_name="events",
            endpoint_document={"columns": [{"name": "id"}]},
        )

        with mock_patch.object(
            GenericSQLConnector, "_build_column_defs", return_value=[]
        ), mock_patch(
            "cdk.sql.generic.build_create_table_sql", return_value="CREATE ..."
        ):
            with pytest.raises(
                AdbcConfigurationError, match=r"connect\(\) must be called"
            ):
                await handler._ensure_tables_exist(state, MagicMock())


class TestWriteModeDispatch:
    """Mirror of ``test_build_schema_message_rejects_unknown_mode`` on
    the destination side: ``_get_write_mode`` must reject unmapped proto
    values rather than silently defaulting (e.g. when a future
    WRITE_MODE_MERGE is added)."""

    def test_known_modes(self):
        handler = GenericSQLConnector()
        assert handler._get_write_mode(1) == "insert"
        assert handler._get_write_mode(2) == "upsert"
        assert handler._get_write_mode(3) == "truncate_insert"

    def test_unknown_mode_raises(self):
        handler = GenericSQLConnector()
        with pytest.raises(
            SchemaConfigurationError, match="Unsupported proto write_mode"
        ):
            handler._get_write_mode(99)


class TestPrepareForSqlAlchemy:
    """``_prepare_for_sqlalchemy`` aligns the batch to the destination
    schema and materialises once. Json columns stay as their
    wire-format string so they bind directly into TEXT / JSONB columns
    without per-row coercion."""

    def test_json_column_kept_as_wire_string(self):
        import pyarrow as pa

        from cdk.schema_contract import SchemaContract
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        handler = GenericSQLConnector()
        contract = SchemaContract(
            {
                "columns": [
                    {
                        "name": "id",
                        "arrow_type": "Utf8",
                        "native_type": "TEXT",
                        "nullable": False,
                    },
                    {
                        "name": "metadata",
                        "arrow_type": "Json",
                        "native_type": "JSONB",
                        "nullable": True,
                    },
                ]
            }
        )
        state = _StreamState(schema_contract=contract)

        batch = pa.RecordBatch.from_pylist(
            [{"id": "r1", "metadata": '{"k": "v", "n": 1}'}],
            schema=contract.arrow_schema,
        )
        records = handler._prepare_for_sqlalchemy(state, batch)
        # The Json column bind value is the raw wire string -- PG
        # accepts it as JSONB text input; other dialects treat it as
        # TEXT. No per-row dict/list parsing happens here.
        assert records == [{"id": "r1", "metadata": '{"k": "v", "n": 1}'}]

    def test_null_json_column_passes_through(self):
        import pyarrow as pa

        from cdk.schema_contract import SchemaContract
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        handler = GenericSQLConnector()
        contract = SchemaContract(
            {
                "columns": [
                    {
                        "name": "metadata",
                        "arrow_type": "Json",
                        "native_type": "JSONB",
                        "nullable": True,
                    }
                ]
            }
        )
        state = _StreamState(schema_contract=contract)
        batch = pa.RecordBatch.from_pylist(
            [{"metadata": None}],
            schema=contract.arrow_schema,
        )
        records = handler._prepare_for_sqlalchemy(state, batch)
        assert records == [{"metadata": None}]

    def test_raises_when_schema_contract_is_none(self):
        import pyarrow as pa

        from cdk.adbc_registry import AdbcConfigurationError
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        handler = GenericSQLConnector()
        state = _StreamState(
            schema_name="public", table_name="events", schema_contract=None
        )
        batch = pa.RecordBatch.from_pylist([{"id": 1}])
        with pytest.raises(
            AdbcConfigurationError, match=r"public\.events.*SchemaContract"
        ):
            handler._prepare_for_sqlalchemy(state, batch)


class TestDDLLockSerialization:
    """``_ddl_lock`` must serialize concurrent _ensure_tables_exist calls
    so two streams sharing the handler do not race the database catalog
    (the failure that motivated commit 5bf2e00)."""

    @pytest.mark.asyncio
    async def test_ddl_lock_serializes_concurrent_table_creation(self, monkeypatch):
        import asyncio

        from cdk.sql import generic as generic_module
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        handler = GenericSQLConnector()
        # Pretend the engine is connected; we intercept the DDL build + the
        # reflection (run_sync) before any real SQL is dispatched.
        handler._engine = AsyncMock()

        # DDL rendering is irrelevant to the lock; stub it out so the
        # read-only test mapper (no write rules) doesn't raise before the
        # lock is even reached.
        monkeypatch.setattr(
            generic_module,
            "build_create_table_sql",
            lambda *a, **k: "CREATE TABLE t (id BIGINT)",
        )

        in_flight = 0
        max_concurrent = 0
        order: list[str] = []

        async def _fake_run_sync(_callable, *_args, **_kwargs):
            nonlocal in_flight, max_concurrent
            in_flight += 1
            max_concurrent = max(max_concurrent, in_flight)
            # Yield control to give a parallel coroutine a chance to enter
            # this critical section if the lock isn't holding.
            await asyncio.sleep(0.01)
            in_flight -= 1
            # Reflection returns the single bound target table.
            return object()

        # The handler's _ensure_tables_exist uses engine.begin() as an async
        # context manager; bypass with a coroutine returning a prepared ctx.
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _begin_ctx():
            conn = AsyncMock()
            conn.execute = AsyncMock()
            conn.run_sync = _fake_run_sync
            yield conn

        handler._engine.begin = _begin_ctx

        type_mapper = _mapper("pg")

        async def _drive(stream_id: str):
            state = _StreamState(
                schema_name="public",
                table_name=f"t_{stream_id}",
                endpoint_document={
                    "columns": [
                        {
                            "name": "id",
                            "native_type": "BIGINT",
                            "arrow_type": "Int64",
                            "nullable": False,
                        }
                    ],
                    "primary_keys": ["id"],
                    "database_object": {"name": f"t_{stream_id}", "schema": "public"},
                },
                primary_keys=["id"],
            )
            order.append(f"enter:{stream_id}")
            await handler._ensure_tables_exist(state, type_mapper)
            order.append(f"exit:{stream_id}")

        await asyncio.gather(_drive("a"), _drive("b"), _drive("c"))

        # If the lock works, run_sync is never executed concurrently —
        # max_concurrent across all three coroutines must be 1.
        assert max_concurrent == 1, (
            f"DDL lock did not serialize create_all calls "
            f"(max_concurrent={max_concurrent}, order={order})"
        )


class TestConfigureSchemaErrorPropagation:
    """``configure_schema`` no longer classifies exceptions itself: the
    deterministic, typed errors and any unexpected defect both propagate
    to the gRPC layer, which translates the former into a rejected
    SchemaAck and lets the latter fail the stream with its real type
    (issues #153 and #140)."""

    def _configured_handler(self) -> GenericSQLConnector:
        handler = GenericSQLConnector()
        handler._connected = True
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        handler.set_endpoint_refs(
            {
                "s1": {
                    "scope": "connector",
                    "connection_id": "pg",
                    "endpoint_id": "transfers",
                }
            }
        )
        handler.set_stream_endpoints(
            {
                "s1": {
                    "database_object": {"name": "events", "schema": "public"},
                    "columns": [{"name": "id", "native_type": "BIGINT"}],
                    "primary_keys": ["id"],
                }
            }
        )
        return handler

    @pytest.mark.asyncio
    async def test_unexpected_exception_propagates(self):
        """A defect inside DDL must surface as-is, not degrade into a
        ``return False`` the engine reads as a schema rejection."""
        from unittest.mock import patch

        from cdk.types import SchemaSpec, WriteMode

        handler = self._configured_handler()
        with patch.object(
            GenericSQLConnector,
            "_ensure_tables_exist",
            AsyncMock(side_effect=RuntimeError("DDL transaction deadlocked")),
        ):
            with pytest.raises(RuntimeError, match="deadlocked"):
                await handler.configure_schema(
                    SchemaSpec(
                        stream_id="s1",
                        version=1,
                        write_mode=WriteMode.WRITE_MODE_INSERT,
                        ack_timeout_seconds=30,
                    )
                )

    @pytest.mark.asyncio
    async def test_unknown_write_mode_raises_schema_configuration_error(self):
        """The intentional config-error signal keeps its typed form all
        the way out of ``configure_schema``."""
        from cdk.types import SchemaSpec

        handler = self._configured_handler()
        with pytest.raises(
            SchemaConfigurationError, match="Unsupported proto write_mode"
        ):
            await handler.configure_schema(
                SchemaSpec(
                    stream_id="s1",
                    version=1,
                    write_mode=99,
                    ack_timeout_seconds=30,
                )
            )
