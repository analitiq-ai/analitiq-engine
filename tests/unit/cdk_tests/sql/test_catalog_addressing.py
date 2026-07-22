"""Catalog addressing through the engine paths (issue #348).

The dialect owns one ``catalog.schema.table`` composer: intent resolves once
into a ``TableAddress`` (normalization + capability gate) and every surface —
destination DDL, ADBC ingest, stage-MERGE, TRUNCATE, the source read, the
control-plane ``create_table`` and discovery — consumes that address through a
sink. These tests pin the two behaviors the composer exists to guarantee:

* a catalog on a dialect that cannot address one fails loud at the boundary
  it entered through (``configure_schema`` / ``read_batches`` / discovery /
  ``create_table``) — never a silently-dropped catalog (issue #330) and never
  broken cross-database SQL on an ANSI system (issue #343);
* on a dialect that declares ``supports_catalog_addressing``, the same address
  reaches every sink identically — three-part quoted SQL, catalog-scoped
  metadata paths, and ``catalog_name`` ingest targeting (issues #336/#337/
  #339/#340).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.exceptions import (
    CatalogAddressingError,
    ReadError,
    SchemaConfigurationError,
)
from cdk.sql.generic import GenericSQLConnector
from cdk.types import SchemaSpec, WriteMode

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
    """An ADBC dialect whose system addresses catalogs per statement."""

    name = "cataloged"
    supports_catalog_addressing = True
    supports_upsert_adbc = True

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
    async def test_catalog_on_unsupporting_dialect_rejects_the_schema(self):
        # The base (ANSI) dialect cannot address a catalog; the failure is a
        # deterministic authoring error the gRPC layer turns into a rejected
        # SchemaAck — not a silently-dropped catalog and not broken SQL.
        handler = _handler(adbc_only=True)
        with pytest.raises(SchemaConfigurationError, match="default catalog"):
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
    async def test_catalog_reaches_every_ddl_statement_on_adbc(self):
        handler = _handler(_CatalogConnector, adbc_only=True)
        executed: list[str] = []
        with patch.object(
            handler, "_execute_adbc_ddl_sync", side_effect=executed.extend
        ):
            assert await handler.configure_schema(_schema_spec()) is True

        state = handler._streams[STREAM]
        assert state.address == TableAddress(
            table="events", schema="ds", catalog="proj"
        )
        create_schema = next(s for s in executed if s.startswith("CREATE SCHEMA"))
        assert create_schema == 'CREATE SCHEMA IF NOT EXISTS "proj"."ds"'
        create_table = next(s for s in executed if s.startswith("CREATE TABLE"))
        assert '"proj"."ds"."events"' in create_table


class TestReadPathGate:
    @pytest.mark.asyncio
    async def test_catalog_on_unsupporting_dialect_fails_before_extraction(self):
        connector = GenericSQLConnector()
        config = {"endpoint_document": ENDPOINT_DOC, "stream_source": {}}
        checkpoint = AsyncMock()
        with pytest.raises(ReadError, match="default catalog"):
            async for _ in connector.read_batches(
                MagicMock(), config, checkpoint=checkpoint, stream_name="s"
            ):
                pass  # pragma: no cover - the gate fires before any batch


class _CapturingCursor:
    def __init__(self, executed: list[str], ingests: list[dict]):
        self._executed = executed
        self._ingests = ingests

    def execute(self, sql, *args):
        self._executed.append(sql)

    def adbc_ingest(self, table, batch, mode, **kwargs):
        self._ingests.append({"table": table, "mode": mode, **kwargs})

    def close(self):
        pass


class _CapturingConn:
    def __init__(self):
        self.executed: list[str] = []
        self.ingests: list[dict] = []

    def cursor(self):
        return _CapturingCursor(self.executed, self.ingests)

    def commit(self):
        pass


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
        merge = next(s for s in conn.executed if s.startswith("MERGE INTO"))
        assert f"MERGE INTO {target} t USING {stage} s" in merge
        assert f"DROP TABLE IF EXISTS {stage}" in conn.executed[0]
        create = next(s for s in conn.executed if s.startswith("CREATE TABLE"))
        assert stage in create and target in create
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
    @pytest.mark.asyncio
    async def test_create_table_refuses_catalog_on_unsupporting_dialect(self):
        from cdk.contract import ColumnDef
        from cdk.sql.ddl import create_table

        with pytest.raises(CatalogAddressingError, match="default catalog"):
            await create_table(
                MagicMock(),
                "ds",
                "t",
                [ColumnDef("id", "Int64")],
                [],
                dialect=SqlDialect(),
                catalog="proj",
            )

    @pytest.mark.asyncio
    async def test_discovery_refuses_catalog_on_unsupporting_dialect(self):
        from cdk.sql.discovery import list_columns, list_schemas, list_tables

        runtime = MagicMock()
        for call in (
            lambda: list_schemas(runtime, dialect=SqlDialect(), catalog="proj"),
            lambda: list_tables(runtime, "ds", dialect=SqlDialect(), catalog="proj"),
            lambda: list_columns(
                runtime, "ds", "t", dialect=SqlDialect(), catalog="proj"
            ),
        ):
            with pytest.raises(CatalogAddressingError):
                await call()
