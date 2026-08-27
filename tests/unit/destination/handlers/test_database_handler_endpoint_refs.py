"""Tests for GenericSQLConnector per-stream type-mapper dispatch.

The handler registers a stream_id → endpoint_ref index at startup; each
incoming SchemaMessage picks its mapper by scope. These tests lock in
that contract without the full gRPC/materialization stack.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from analitiq.contracts.endpoints import (
    DATABASE_ENDPOINT_SCHEMA_URL,
    DatabaseEndpointDoc,
)

from cdk.connection_runtime import ConnectionRuntime
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.exceptions import SchemaConfigurationError
from cdk.sql.generic import GenericSQLConnector, _StreamState
from cdk.type_map import TypeMapper
from cdk.type_map.rules import parse_rules

# A fixed, timezone-aware emit instant for write_batch/send_batch calls; the
# engine stamps this per batch (issue #353). Value is arbitrary for sinks
# that ignore it.
_EMITTED_AT = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)


def _endpoint_json(columns, *, table, schema="public", primary_keys=None):
    """A contract-valid database endpoint document as the engine sends it.

    ``set_stream_endpoints`` takes authored JSON, so the fixtures that go
    through that funnel keep the wire shape; the ones that reach into
    ``_StreamState`` directly parse it with :func:`_endpoint_doc`.
    """
    document = {
        "$schema": DATABASE_ENDPOINT_SCHEMA_URL,
        "endpoint_id": table,
        "database_object": {"name": table, "schema": schema},
        "columns": columns,
    }
    if primary_keys is not None:
        document["primary_keys"] = primary_keys
    return document


def _endpoint_doc(columns, **kwargs):
    """The parsed form of :func:`_endpoint_json`, for a hand-built state."""
    return DatabaseEndpointDoc.model_validate(_endpoint_json(columns, **kwargs))


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


class _StageRenderingDialect(SqlDialect):
    """The two stage-then-merge rendering hooks a write-role connector ships."""

    name = "staging"

    def stage_table_sql(self, stage, target, *, temp):
        return (
            f"CREATE TABLE {self.quote_table(stage)} AS SELECT * FROM "
            f"{self.quote_table(target)} WHERE FALSE"
        )

    def merge_statement_sql(self, stage, target, conflict_keys, columns):
        return "INSERT ... ON CONFLICT DO UPDATE"


_DECLARED_CAPS = SqlCapabilities.from_declaration(
    {
        "catalog": "none",
        "session_targeting": "per_statement",
        "merge_form": "insert_on_conflict",
        "bulk_load": {},
        "stage": {"scope": "temp", "schema": "target", "transactional_ddl": True},
    },
    source="<test>",
)


def _stage_capable(handler: GenericSQLConnector) -> GenericSQLConnector:
    """Give *handler* the declared capabilities + rendering dialect a
    migrated write-role connector carries after connect()."""
    handler.dialect_class = _StageRenderingDialect
    handler.dialect = handler.dialect_class(_DECLARED_CAPS)
    handler._capabilities = _DECLARED_CAPS
    return handler


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
    """A malformed column must be refused loudly rather than silently
    dropped from the DDL. The contract makes both shapes unrepresentable,
    so the document never parses -- but registration runs before the gRPC
    server exists, so it holds that failure against its own stream and the
    stream's schema handshake is where it is refused, naming both the
    stream id the engine needs and the field that broke.
    """

    async def _refusal(self, columns) -> str:
        from cdk.types import SchemaSpec, WriteMode

        handler = GenericSQLConnector()
        handler._connected = True
        handler.set_stream_endpoints({"s1": _endpoint_json(columns, table="t")})
        with pytest.raises(SchemaConfigurationError) as err:
            await handler.configure_schema(
                SchemaSpec(
                    stream_id="s1",
                    version=1,
                    write_mode=WriteMode.WRITE_MODE_INSERT,
                    ack_timeout_seconds=30,
                )
            )
        return str(err.value)

    @pytest.mark.asyncio
    async def test_unnamed_column_is_refused(self):
        """A column without a name never reaches DDL."""

        message = await self._refusal(
            [
                {"native_type": "BIGINT", "arrow_type": "Int64"},
                {"name": "valid", "native_type": "BIGINT", "arrow_type": "Int64"},
            ]
        )
        assert "s1" in message
        assert "name" in message

    @pytest.mark.asyncio
    async def test_column_without_arrow_type_is_refused(self):
        # native_type alone is not enough: DDL consumes the stored
        # arrow_type (the same declaration the schema contract casts
        # with), never the read map.
        message = await self._refusal([{"name": "id", "native_type": "BIGINT"}])
        assert "s1" in message
        assert "arrow_type" in message


class TestWriteBatchFatalOnTypeMapError:
    """Deterministic config and type-map errors in write_batch must not be retried."""

    @pytest.mark.asyncio
    async def test_missing_schema_contract_classified_as_fatal(self):
        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = _stage_capable(GenericSQLConnector())
        handler._backend = MagicMock()
        handler._connected = True
        handler._streams["s1"] = _StreamState(
            address=TableAddress(table="events", schema="myschema"),
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
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "adbc" in result.failure_summary
        assert "SchemaContract" in result.failure_summary

    @pytest.mark.asyncio
    async def test_type_map_error_classified_as_fatal(self):
        from cdk.type_map import UnmappedTypeError
        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = _stage_capable(GenericSQLConnector())
        # Preconditions: connected, schema configured. We don't actually hit
        # the DB because the Arrow-space cast raises before any SQL runs.
        contract_mock = MagicMock()
        contract_mock.cast_arrow_batch.side_effect = UnmappedTypeError(
            "pg", "forward", "MONEY"
        )

        handler._backend = MagicMock()
        handler._connected = True
        handler._streams["s1"] = _StreamState(
            write_mode="insert",
            primary_keys=[],
            schema_contract=contract_mock,
        )

        import pyarrow as pa

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "type-map" in result.failure_summary

    @pytest.mark.asyncio
    async def test_tls_verification_error_classified_as_fatal(self):
        # A pool connection opened for the write can fail the declared TLS
        # mode's post-connect check (SqlDialect.verify_tls_state) mid-run.
        # Retrying reconnects to the same downgraded endpoint, so the write
        # must classify fatal, not retryable (issue #376).

        from cdk.sql.exceptions import TlsVerificationError

        # CDK-native enums (int-identical to the wire enums), so this test
        # runs without the gRPC generated stack.
        from cdk.types import AckStatus, FailureCategory

        handler = _stage_capable(GenericSQLConnector())
        contract_mock = MagicMock()
        contract_mock.cast_arrow_batch.side_effect = lambda rb: rb

        handler._connected = True
        handler._streams["s1"] = _StreamState(
            write_mode="insert",
            primary_keys=["id"],
            schema_contract=contract_mock,
        )

        # The listener fires during pool connect when the backend's cycle
        # opens a connection; raising from execute_write models that
        # surface inside the same write_batch try block.
        handler._backend = MagicMock()
        handler._backend.execute_write = AsyncMock(
            side_effect=TlsVerificationError(
                "session is not encrypted under mode 'REQUIRED'"
            )
        )

        import pyarrow as pa

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=None,
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "tls" in result.failure_summary
        assert result.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT

    @pytest.mark.asyncio
    async def test_upsert_without_conflict_keys_classified_as_fatal(self):
        # End-to-end through write_batch: the plan builder's raise must be
        # classified FATAL (deterministic — retrying an unkeyed upsert can
        # never heal), not retried forever or degraded.

        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = _stage_capable(GenericSQLConnector())
        contract_mock = MagicMock()
        contract_mock.cast_arrow_batch.side_effect = lambda rb: rb

        handler._backend = MagicMock()
        handler._connected = True
        handler._streams["s1"] = _StreamState(
            write_mode="upsert",
            conflict_keys=[],
            schema_contract=contract_mock,
        )

        import pyarrow as pa

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "write-config" in result.failure_summary


class TestUpsertFailsLoudWithoutConflictKeys:
    """Upsert with no conflict keys fails loud — the engine never silently
    degrades to INSERT (which would duplicate rows) and never derives a
    target from ``primary_keys`` (issue #254)."""

    def test_upsert_without_conflict_keys_raises(self):
        from cdk.sql.write_plan import build_stage_write_plan

        class _RecordingDialect(_StageRenderingDialect):
            merge_calls: list = []

            def merge_statement_sql(self, stage, target, conflict_keys, columns):
                self.merge_calls.append((conflict_keys, columns))
                return "MERGE ..."

        dialect = _RecordingDialect()
        with pytest.raises(SchemaConfigurationError, match="no conflict_keys"):
            build_stage_write_plan(
                dialect,
                _DECLARED_CAPS,
                target=TableAddress(table="events", schema="public"),
                columns=("id", "name"),
                write_mode="upsert",
                conflict_keys=[],
                # primary keys present, but must NOT be used as a fallback
                identity=["id"],
                truncate_now=False,
                run_id="r1",
                stream_id="s1",
                batch_seq=1,
            )
        assert dialect.merge_calls == []

    def test_upsert_with_conflict_keys_renders_the_merge(self):
        from cdk.sql.write_plan import build_stage_write_plan

        calls: list = []

        class _RecordingDialect(_StageRenderingDialect):
            def merge_statement_sql(self, stage, target, conflict_keys, columns):
                calls.append((list(conflict_keys), list(columns)))
                return "MERGE ..."

        plan = build_stage_write_plan(
            _RecordingDialect(),
            _DECLARED_CAPS,
            target=TableAddress(table="events", schema="public"),
            columns=("id", "name"),
            write_mode="upsert",
            conflict_keys=["id"],
            identity=[],
            truncate_now=False,
            run_id="r1",
            stream_id="s1",
            batch_seq=1,
        )
        # The verbatim conflict target must reach the SQL renderer unchanged.
        assert calls == [(["id"], ["id", "name"])]
        assert plan.mode_sql == "MERGE ..."

    @pytest.mark.asyncio
    async def test_adbc_upsert_without_conflict_keys_fails_before_any_write(self):
        # ADBC twin of the SQLAlchemy classification test: the plan
        # builder's raise is the same code on both transports, and it must
        # fire before anything reaches the backend — no partial write.

        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = _stage_capable(GenericSQLConnector())
        handler._connected = True
        handler._adbc_only = True
        handler._backend = MagicMock()
        handler._backend.execute_write = AsyncMock()
        contract = MagicMock()
        contract.cast_arrow_batch.side_effect = lambda rb: rb
        handler._streams["s1"] = _StreamState(
            address=TableAddress(table="events", schema="analytics"),
            write_mode="upsert",
            conflict_keys=[],
            schema_contract=contract,
        )

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "no conflict_keys" in result.failure_summary
        handler._backend.execute_write.assert_not_called()


class TestEnsureTablesEngineNoneRaises:
    """A None engine during DDL is a violated invariant; silently skipping
    DDL would leave write_batch returning RETRYABLE_FAILURE forever
    (issue #150)."""

    @pytest.mark.asyncio
    async def test_engine_none_raises_adbc_configuration_error(self):
        from unittest.mock import patch as mock_patch

        from cdk.adbc_registry import AdbcConfigurationError

        handler = GenericSQLConnector()
        handler.dialect = handler.dialect_class()
        assert handler._engine is None
        state = _StreamState(
            address=TableAddress(table="events", schema="public"),
            endpoint_document=_endpoint_doc(
                [{"name": "id", "native_type": "BIGINT", "arrow_type": "Int64"}],
                table="events",
            ),
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


class TestPrepareWriteBatch:
    """``_prepare_write_batch`` aligns the batch to the destination
    schema in Arrow space; the backend materialises it at landing time.
    Json columns stay as their wire-format string so they bind directly
    into TEXT / JSONB columns without per-row coercion."""

    def test_json_column_kept_as_wire_string(self):
        import pyarrow as pa

        from cdk.schema_contract import SchemaContract

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
        prepared = handler._prepare_write_batch(state, batch)
        # The Json column bind value is the raw wire string -- PG
        # accepts it as JSONB text input; other dialects treat it as
        # TEXT. No per-row dict/list parsing happens here.
        assert prepared.to_pylist() == [{"id": "r1", "metadata": '{"k": "v", "n": 1}'}]

    def test_null_json_column_passes_through(self):
        import pyarrow as pa

        from cdk.schema_contract import SchemaContract

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
        prepared = handler._prepare_write_batch(state, batch)
        assert prepared.to_pylist() == [{"metadata": None}]

    def test_keyless_insert_attaches_and_dedups_record_hash(self):
        import pyarrow as pa

        from cdk.schema_contract import SchemaContract

        handler = GenericSQLConnector()
        contract = SchemaContract(
            {
                "columns": [
                    {
                        "name": "id",
                        "arrow_type": "Utf8",
                        "native_type": "TEXT",
                        "nullable": False,
                    }
                ]
            }
        )
        state = _StreamState(
            write_mode="insert", primary_keys=[], schema_contract=contract
        )
        batch = pa.RecordBatch.from_pylist(
            [{"id": "a"}, {"id": "a"}, {"id": "b"}],
            schema=contract.arrow_schema,
        )
        prepared = handler._prepare_write_batch(state, batch)
        # Byte-identical rows collapse to the first occurrence and every
        # surviving row carries the content-derived hash (issue #282).
        assert prepared.num_rows == 2
        assert "_record_hash" in prepared.schema.names

    def test_keyed_insert_collapses_duplicate_keys_first_wins(self):
        import pyarrow as pa

        from cdk.schema_contract import SchemaContract

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
                        "name": "v",
                        "arrow_type": "Utf8",
                        "native_type": "TEXT",
                        "nullable": True,
                    },
                ]
            }
        )
        state = _StreamState(
            write_mode="insert", primary_keys=["id"], schema_contract=contract
        )
        batch = pa.RecordBatch.from_pylist(
            [{"id": "k", "v": "first"}, {"id": "k", "v": "second"}],
            schema=contract.arrow_schema,
        )
        prepared = handler._prepare_write_batch(state, batch)
        assert prepared.to_pylist() == [{"id": "k", "v": "first"}]

    @pytest.mark.parametrize("mode", ["upsert", "truncate_insert"])
    def test_upsert_and_full_refresh_never_collapse(self, mode):
        # ADR §2: duplicate conflict_keys inside one source batch keep the
        # system's own loud failure for upsert (no collapse rule can be
        # correct), and deduping a full refresh would drop legitimate
        # duplicate rows. Only insert collapses.
        import pyarrow as pa

        from cdk.schema_contract import SchemaContract

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
                        "name": "v",
                        "arrow_type": "Utf8",
                        "native_type": "TEXT",
                        "nullable": True,
                    },
                ]
            }
        )
        state = _StreamState(
            write_mode=mode,  # type: ignore[arg-type]
            primary_keys=["id"],
            conflict_keys=["id"],
            schema_contract=contract,
        )
        batch = pa.RecordBatch.from_pylist(
            [{"id": "k", "v": "first"}, {"id": "k", "v": "second"}],
            schema=contract.arrow_schema,
        )
        prepared = handler._prepare_write_batch(state, batch)
        assert prepared.num_rows == 2

    def test_record_hash_digest_is_pinned(self):
        # Golden digest: the content-hash identity (issue #282) must stay
        # byte-stable across engine versions — a canonicalization change
        # here silently breaks dedup continuity against every existing
        # keyless target (historical rows re-insert once, no error).
        from datetime import date
        from decimal import Decimal

        import pyarrow as pa

        handler = GenericSQLConnector()
        state = _StreamState(write_mode="insert", primary_keys=[])
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([None], type=pa.large_string()),
                pa.array([Decimal("12.34")], type=pa.decimal128(10, 2)),
                pa.array(
                    # Zoneless on purpose: a naive wire timestamp must
                    # keep hashing identically forever.
                    [datetime(2026, 1, 2, 3, 4, 5, 123456)],  # noqa: DTZ001
                    type=pa.timestamp("us"),
                ),
                pa.array([date(2026, 1, 2)], type=pa.date32()),
                pa.array(['{"k": 1}'], type=pa.large_string()),
            ],
            names=["id", "amount", "created", "day", "meta"],
        )
        hashed = handler._attach_record_hash_to_batch(batch, state)
        digest = hashed.column("_record_hash")[0].as_py()
        assert digest == (
            "fd19ff71ae81aa690cd02028f9aee4c21654e009b4c6c1d7ddad614d6fc2ec76"
        )

    def test_raises_when_schema_contract_is_none(self):
        import pyarrow as pa

        from cdk.adbc_registry import AdbcConfigurationError

        handler = GenericSQLConnector()
        state = _StreamState(
            address=TableAddress(table="events", schema="public"),
            schema_contract=None,
        )
        batch = pa.RecordBatch.from_pylist([{"id": 1}])
        with pytest.raises(
            AdbcConfigurationError, match=r"public\.events.*SchemaContract"
        ):
            handler._prepare_write_batch(state, batch)


class TestDDLLockSerialization:
    """``_ddl_lock`` must serialize concurrent _ensure_tables_exist calls
    so two streams sharing the handler do not race the database catalog
    (the failure that motivated commit 5bf2e00)."""

    @pytest.mark.asyncio
    async def test_ddl_lock_serializes_concurrent_table_creation(self, monkeypatch):
        """The DDL lock serializes concurrent table creation."""
        import asyncio

        from cdk.sql import generic as generic_module

        handler = GenericSQLConnector()
        handler.dialect = handler.dialect_class()

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

        class _SlowBackend:
            """Backend stand-in: records DDL concurrency under the lock."""

            async def run_ddl(self, statements):
                nonlocal in_flight, max_concurrent
                in_flight += 1
                max_concurrent = max(max_concurrent, in_flight)
                # Yield control to give a parallel coroutine a chance to
                # enter this critical section if the lock isn't holding.
                await asyncio.sleep(0.01)
                in_flight -= 1

            async def target_columns(self, target):
                return ("id",)

        handler._backend = _SlowBackend()

        type_mapper = _mapper("pg")

        async def _drive(stream_id: str):
            """Configure one stream end-to-end against the shared handler."""
            state = _StreamState(
                address=TableAddress(table=f"t_{stream_id}", schema="public"),
                endpoint_document=_endpoint_doc(
                    [
                        {
                            "name": "id",
                            "native_type": "BIGINT",
                            "arrow_type": "Int64",
                            "nullable": False,
                        }
                    ],
                    table=f"t_{stream_id}",
                    primary_keys=["id"],
                ),
                primary_keys=["id"],
            )
            order.append(f"enter:{stream_id}")
            await handler._ensure_tables_exist(state, type_mapper)
            order.append(f"exit:{stream_id}")

        await asyncio.gather(_drive("a"), _drive("b"), _drive("c"))

        # If the lock works, run_ddl is never executed concurrently —
        # max_concurrent across all three coroutines must be 1.
        assert max_concurrent == 1, (
            f"DDL lock did not serialize concurrent DDL "
            f"(max_concurrent={max_concurrent}, order={order})"
        )


class TestConfigureSchemaErrorPropagation:
    """``configure_schema`` no longer classifies exceptions itself: the
    deterministic, typed errors and any unexpected defect both propagate
    to the gRPC layer, which translates the former into a rejected
    SchemaAck and lets the latter fail the stream with its real type
    (issues #153 and #140)."""

    def _configured_handler(self) -> GenericSQLConnector:
        handler = _stage_capable(GenericSQLConnector())
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
                "s1": _endpoint_json(
                    [{"name": "id", "native_type": "BIGINT", "arrow_type": "Int64"}],
                    table="events",
                    primary_keys=["id"],
                )
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


class TestAMalformedEndpointDocumentRejectsOneStream:
    """``set_stream_endpoints`` runs from the worker entry point, before the
    gRPC server is constructed. A document that cannot satisfy the contract
    is therefore held against its own stream and reported at the handshake:
    raising at registration would exit the process, and the engine would see
    a dead worker instead of one rejected stream."""

    def _handler(self) -> GenericSQLConnector:
        handler = _stage_capable(GenericSQLConnector())
        handler._connected = True
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        handler.set_endpoint_refs(
            {
                sid: {
                    "scope": "connector",
                    "connection_id": "pg",
                    "endpoint_id": "events",
                }
                for sid in ("bad", "good")
            }
        )
        handler.set_stream_endpoints(
            {
                # No database_object: the contract requires one, so this
                # document never becomes a DatabaseEndpointDoc.
                "bad": {
                    "$schema": DATABASE_ENDPOINT_SCHEMA_URL,
                    "endpoint_id": "events",
                    "columns": [],
                },
                "good": _endpoint_json(
                    [{"name": "id", "native_type": "BIGINT", "arrow_type": "Int64"}],
                    table="events",
                    primary_keys=["id"],
                ),
            }
        )
        return handler

    @pytest.mark.asyncio
    async def test_the_handshake_carries_the_parse_failure(self):
        from cdk.types import SchemaSpec, WriteMode

        with pytest.raises(SchemaConfigurationError) as caught:
            await self._handler().configure_schema(
                SchemaSpec(
                    stream_id="bad",
                    version=1,
                    write_mode=WriteMode.WRITE_MODE_INSERT,
                    ack_timeout_seconds=30,
                )
            )
        # The servicer turns SchemaConfigurationError into a rejected ack
        # for this stream alone. The message must be the parse failure, not
        # the "you never registered it" text the same missing entry would
        # otherwise produce.
        assert "DatabaseEndpointDoc" in str(caught.value)
        assert "call set_stream_endpoints" not in str(caught.value)

    @pytest.mark.asyncio
    async def test_every_other_stream_on_the_handler_still_configures(self):
        from unittest.mock import patch

        from cdk.types import SchemaSpec, WriteMode

        handler = self._handler()
        with patch.object(GenericSQLConnector, "_ensure_tables_exist", AsyncMock()):
            assert await handler.configure_schema(
                SchemaSpec(
                    stream_id="good",
                    version=1,
                    write_mode=WriteMode.WRITE_MODE_INSERT,
                    ack_timeout_seconds=30,
                )
            )
