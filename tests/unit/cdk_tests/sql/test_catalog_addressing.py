"""Catalog addressing through the engine paths (issue #348).

The dialect owns one ``catalog.schema.table`` composer: intent resolves once
into a ``TableAddress`` (normalization + capability gate) and every surface —
destination DDL, ADBC ingest, stage-MERGE, TRUNCATE, the source read, the
control-plane ``create_table`` and discovery — consumes that address through a
sink. These tests pin the two behaviors the composer exists to guarantee:

* a catalog on a system that cannot address one fails loud at the boundary
  it entered through (``configure_schema`` / ``read_batches`` / discovery /
  ``create_table``) — never a silently-dropped catalog (issue #330) and never
  broken cross-database SQL on an ANSI system (issue #343);
* on a connector declaring ``sql_capabilities.catalog: full``, the same
  address reaches every sink identically — three-part quoted SQL,
  catalog-scoped metadata paths, and ``catalog_name`` ingest targeting
  (issues #336/#337/#339/#340).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.exceptions import (
    CatalogAddressingError,
    ReadError,
    SchemaConfigurationError,
)
from cdk.sql.generic import GenericSQLConnector
from cdk.types import SchemaSpec, WriteMode

from .conftest import caps_block

STREAM = "s1"

ENDPOINT_DOC = {
    "database_object": {"name": "events", "schema": "ds", "catalog": "proj"},
    "columns": [
        {
            "name": "id",
            "native_type": "BIGINT",
            "arrow_type": "Int64",
            "nullable": False,
        }
    ],
    "primary_keys": ["id"],
}


class _CatalogAdbcDialect(SqlDialect):
    """An ADBC dialect bound to a full-catalog, merge-capable declaration."""

    name = "cataloged"
    capabilities = SqlCapabilities.from_declaration(caps_block(catalog="full"))

    def render_column_type(self, canonical, type_mapper, *, params=None):
        return {
            "Int64": "INTEGER",
            "Utf8": "STRING",
            "Timestamp(MICROSECOND, UTC)": "TIMESTAMPTZ",
        }[canonical]

    def adbc_stage_table_sql(self, stage_qualified, target_qualified):
        return (
            f"CREATE TABLE {stage_qualified} AS SELECT * FROM "
            f"{target_qualified} WHERE FALSE"
        )


class _CatalogConnector(GenericSQLConnector):
    dialect_class = _CatalogAdbcDialect


def _schema_spec() -> SchemaSpec:
    return SchemaSpec(
        stream_id=STREAM,
        version=1,
        write_mode=WriteMode.WRITE_MODE_INSERT,
        ack_timeout_seconds=30,
    )


def _handler(cls=GenericSQLConnector, *, adbc_only: bool):
    handler = cls()
    handler._connected = True
    handler._adbc_only = adbc_only
    # Mirror _bind_capabilities: the facade and the dialect carry the same
    # declaration object (the dialect fixture's class attribute stands in
    # for a bound connector declaration; None for the undeclared base).
    handler._capabilities = handler.dialect.capabilities
    if not adbc_only:
        handler._engine = MagicMock()
    handler.set_stream_endpoints({STREAM: ENDPOINT_DOC})
    handler.set_endpoint_refs(
        {STREAM: {"scope": "connector", "connection_id": "c", "endpoint_id": "e"}}
    )
    runtime = MagicMock()
    runtime.type_mapper_for.return_value = MagicMock()
    handler._runtime = runtime
    return handler


class TestConfigureSchemaGate:
    @pytest.mark.asyncio
    async def test_catalog_with_no_declaration_rejects_the_schema(self):
        # No declared sql_capabilities means the catalog fact is unknown;
        # the failure is a deterministic authoring error naming the missing
        # declaration, which the gRPC layer turns into a rejected SchemaAck
        # — not a silently-dropped catalog and not broken SQL.
        handler = _handler(adbc_only=True)
        with pytest.raises(SchemaConfigurationError, match="sql_capabilities.catalog"):
            await handler.configure_schema(_schema_spec())

    @pytest.mark.asyncio
    async def test_catalog_on_sqlalchemy_transport_rejects_the_schema(self):
        # Reflection-based DML cannot cross catalogs; a supporting dialect on
        # the SA transport still refuses rather than writing to whatever the
        # two-part reflected name resolves to.
        handler = _handler(_CatalogConnector, adbc_only=False)
        with pytest.raises(SchemaConfigurationError, match="SQLAlchemy write"):
            await handler.configure_schema(_schema_spec())

    @pytest.mark.asyncio
    async def test_catalog_write_on_read_only_declaration_rejects_the_schema(self):
        # 'read' declares discovery/read addressing only: the address door
        # passes, but an ADBC write/DDL target in another catalog needs
        # 'full' — refused before any DDL runs.
        handler = _handler(_CatalogConnector, adbc_only=True)
        read_caps = SqlCapabilities.from_declaration(caps_block(catalog="read"))
        handler._capabilities = read_caps
        handler.dialect.capabilities = read_caps
        executed: list[str] = []
        with patch.object(
            handler, "_execute_adbc_ddl_sync", side_effect=executed.extend
        ):
            with pytest.raises(SchemaConfigurationError, match="require 'full'"):
                await handler.configure_schema(_schema_spec())
        assert executed == []

    @pytest.mark.asyncio
    async def test_catalog_reaches_every_ddl_statement_on_adbc(self):
        handler = _handler(_CatalogConnector, adbc_only=True)
        # The fixture binds the dialect's full-catalog declaration onto the
        # handler, so this success path exercises the write-side 'full'
        # requirement, not a skipped gate.
        assert handler._capabilities is not None
        assert handler._capabilities.catalog == "full"
        executed: list[str] = []
        with patch.object(
            handler, "_execute_adbc_ddl_sync", side_effect=executed.extend
        ):
            assert await handler.configure_schema(_schema_spec()) is True

        state = handler._streams[STREAM]
        assert state.address == TableAddress(
            table="events", schema="ds", catalog="proj"
        )
        create_schema = [s for s in executed if s.startswith("CREATE SCHEMA")]
        assert create_schema == ['CREATE SCHEMA IF NOT EXISTS "proj"."ds"']
        create_table = [s for s in executed if s.startswith("CREATE TABLE")]
        assert len(create_table) == 1
        assert '"proj"."ds"."events"' in create_table[0]


class TestConfigureSchemaAddress:
    @pytest.mark.asyncio
    async def test_sqlalchemy_schema_fallback_lands_in_the_address(self):
        # A catalog-free SA stream with no declared schema resolves the
        # explicit "public" fallback into the stored address — not an
        # unqualified name the connection's search path would resolve.
        handler = _handler(adbc_only=False)
        doc = dict(ENDPOINT_DOC)
        doc["database_object"] = {"name": "events"}
        handler.set_stream_endpoints({STREAM: doc})
        with patch.object(handler, "_ensure_tables_exist", new=AsyncMock()):
            assert await handler.configure_schema(_schema_spec()) is True
        assert handler._streams[STREAM].address == TableAddress(
            table="events", schema="public"
        )


def _checkpoint() -> AsyncMock:
    cp = AsyncMock()
    cp.get_cursor = AsyncMock(return_value=None)
    cp.save_cursor = AsyncMock()
    return cp


async def _drain(connector, runtime, config, checkpoint):
    out = []
    async for batch in connector.read_batches(
        runtime, config, checkpoint=checkpoint, stream_name="s", batch_size=2
    ):
        out.append(batch)
    return out


class _FakeRuntime:
    def __init__(self, *, is_adbc: bool, engine=None, declared=None):
        self.is_adbc = is_adbc
        self.is_sync_sqlalchemy = False
        self.driver = "postgresql"
        self.connector_id = "postgresql"
        self.declared_sql_capabilities = declared
        self.engine = engine
        self.type_mapper_for = MagicMock()
        self.close = AsyncMock()


class TestReadPathGate:
    @pytest.mark.asyncio
    async def test_catalog_with_no_declaration_fails_before_extraction(self):
        connector = GenericSQLConnector()
        config = {"endpoint_document": ENDPOINT_DOC, "stream_source": {}}
        with pytest.raises(ReadError, match="sql_capabilities.catalog"):
            await _drain(connector, _FakeRuntime(is_adbc=True), config, _checkpoint())


class TestReadPathCatalog:
    """The read path must emit the catalog it was given — on both
    transports. A dropped ``catalog_name`` silently reads the session
    catalog: the exact silent-drop class this PR closes (#330)."""

    def _config(self):
        return {
            "endpoint_document": ENDPOINT_DOC,
            "stream_source": {},
        }

    @pytest.mark.asyncio
    async def test_sqlalchemy_read_compiles_catalog_qualified_from(self):
        runtime = _FakeRuntime(
            is_adbc=False, engine=object(), declared=caps_block(catalog="full")
        )
        executed: list[str] = []

        class _RecordingConn:
            def exec_driver_sql(self, sql, params=None):
                executed.append(sql)
                return []

            async def run_sync(self, fn, *args):
                return fn(self, *args)

        class _AcquireCM:
            async def __aenter__(self):
                return _RecordingConn()

            async def __aexit__(self, *exc):
                return False

        connector = _CatalogConnector()
        with (
            patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()),
            patch("cdk.sql.generic.acquire_connection", return_value=_AcquireCM()),
            patch("cdk.sql.generic.SchemaContract") as sc,
        ):
            sc.return_value.from_pylist.side_effect = lambda rows: rows
            await _drain(connector, runtime, self._config(), _checkpoint())

        assert "FROM proj.ds.events" in executed[0]

    @pytest.mark.asyncio
    async def test_adbc_read_passes_under_read_only_declaration(self):
        # 'read' is exactly the declaration a read-only cross-catalog
        # connector carries; the source read must not be refused under it.
        runtime = _FakeRuntime(is_adbc=True, declared=caps_block(catalog="read"))
        calls: list[str] = []

        class _Reader:
            async def fetch_page(self, sql, params=()):
                calls.append(sql)
                return []

        class _CM:
            async def __aenter__(self):
                return _Reader()

            async def __aexit__(self, *exc):
                return False

        connector = _CatalogConnector()
        with (
            patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()),
            patch("cdk.sql.generic.open_adbc_reader", return_value=_CM()),
            patch("cdk.sql.generic.SchemaContract") as sc,
        ):
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            await _drain(connector, runtime, self._config(), _checkpoint())

        assert 'FROM "proj"."ds"."events"' in calls[0]

    @pytest.mark.asyncio
    async def test_adbc_read_compiles_catalog_qualified_from(self):
        runtime = _FakeRuntime(is_adbc=True, declared=caps_block(catalog="full"))
        calls: list[str] = []

        class _Reader:
            async def fetch_page(self, sql, params=()):
                calls.append(sql)
                return []

        class _CM:
            async def __aenter__(self):
                return _Reader()

            async def __aexit__(self, *exc):
                return False

        connector = _CatalogConnector()
        with (
            patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()),
            patch("cdk.sql.generic.open_adbc_reader", return_value=_CM()),
            patch("cdk.sql.generic.SchemaContract") as sc,
        ):
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            await _drain(connector, runtime, self._config(), _checkpoint())

        assert 'FROM "proj"."ds"."events"' in calls[0]


class _CapturingCursor:
    def __init__(self, executed: list[str], ingests: list[dict]):
        self._executed = executed
        self._ingests = ingests

    def execute(self, sql, *args):
        self._executed.append(sql)

    def adbc_ingest(self, table, batch, mode, **kwargs):
        self._ingests.append({"table": table, "mode": mode, **kwargs})

    def close(self):
        """No-op: the fake owns no resources."""


class _CapturingConn:
    def __init__(self):
        self.executed: list[str] = []
        self.ingests: list[dict] = []

    def cursor(self):
        return _CapturingCursor(self.executed, self.ingests)

    def commit(self):
        """No-op: statements are captured, not transacted."""


class TestAdbcWriteSinks:
    """One address, every sink: ingest kwargs, TRUNCATE, stage + MERGE."""

    def _connected(self):
        handler = _CatalogConnector()
        handler._adbc_only = True
        conn = _CapturingConn()
        handler._adbc_conn = conn
        return handler, conn

    def _address(self, handler):
        return handler.dialect.table_address("orders", schema="ds", catalog="proj")

    def test_plain_ingest_targets_catalog_and_schema(self):
        handler, conn = self._connected()
        handler._adbc_only_ingest_sync(
            pa.record_batch([pa.array([1])], names=["id"]),
            self._address(handler),
        )
        assert conn.ingests == [
            {
                "table": "orders",
                "mode": "append",
                "db_schema_name": "ds",
                "catalog_name": "proj",
            }
        ]

    def test_truncate_renders_three_part_name(self):
        handler, conn = self._connected()
        handler._adbc_truncate_sync(self._address(handler))
        assert conn.executed == ['TRUNCATE TABLE "proj"."ds"."orders"']

    def test_merge_ingest_uses_the_same_address_everywhere(self):
        handler, conn = self._connected()
        handler._merge_ingest_sync(
            pa.record_batch([pa.array([1]), pa.array(["a"])], names=["id", "v"]),
            self._address(handler),
            ["id", "v"],
            ["id"],
            "btok",
        )
        stage = '"proj"."ds"."_analitiq_stage_orders_btok"'
        target = '"proj"."ds"."orders"'
        merges = [s for s in conn.executed if s.startswith("MERGE INTO")]
        assert len(merges) == 1
        assert f"MERGE INTO {target} t USING {stage} s" in merges[0]
        assert f"DROP TABLE IF EXISTS {stage}" in conn.executed[0]
        creates = [s for s in conn.executed if s.startswith("CREATE TABLE")]
        assert len(creates) == 1
        assert stage in creates[0] and target in creates[0]
        # The stage ingest targets the stage through the same address sink.
        assert conn.ingests == [
            {
                "table": "_analitiq_stage_orders_btok",
                "mode": "append",
                "db_schema_name": "ds",
                "catalog_name": "proj",
            }
        ]


class TestControlPlaneGate:
    """The standalone helpers bind the runtime's declaration themselves
    (the same rule the facade applies), so control-plane calls enforce
    the declared catalog fact without any manual pre-binding."""

    @pytest.mark.asyncio
    async def test_create_table_refuses_catalog_with_no_declaration(self):
        from cdk.contract import ColumnDef
        from cdk.sql.ddl import create_table

        with pytest.raises(CatalogAddressingError, match="sql_capabilities.catalog"):
            await create_table(
                _FakeRuntime(is_adbc=True),
                "ds",
                "t",
                [ColumnDef("id", "Int64")],
                [],
                dialect=SqlDialect(),
                catalog="proj",
            )

    @pytest.mark.asyncio
    async def test_create_table_refuses_catalog_on_read_only_declaration(self):
        # 'read' declares discovery/read addressing only; DDL across
        # catalogs needs 'full'. The declaration comes from the runtime —
        # the helper binds it, the caller never pre-binds the dialect.
        from cdk.contract import ColumnDef
        from cdk.sql.ddl import create_table

        with pytest.raises(CatalogAddressingError, match="requires 'full'"):
            await create_table(
                _FakeRuntime(is_adbc=True, declared=caps_block(catalog="read")),
                "ds",
                "t",
                [ColumnDef("id", "Int64")],
                [],
                dialect=SqlDialect(),
                catalog="proj",
            )

    @pytest.mark.asyncio
    async def test_discovery_refuses_catalog_with_no_declaration(self):
        from cdk.sql.discovery import list_columns, list_schemas, list_tables

        runtime = _FakeRuntime(is_adbc=True)
        for call in (
            lambda: list_schemas(runtime, dialect=SqlDialect(), catalog="proj"),
            lambda: list_tables(runtime, "ds", dialect=SqlDialect(), catalog="proj"),
            lambda: list_columns(
                runtime, "ds", "t", dialect=SqlDialect(), catalog="proj"
            ),
        ):
            with pytest.raises(CatalogAddressingError):
                await call()
