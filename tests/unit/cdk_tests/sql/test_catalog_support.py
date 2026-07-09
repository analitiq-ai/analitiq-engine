"""Tests for database_object.catalog threading through the SQL stack.

Covers issue #330: catalog was accepted by the contract but never consumed.
Verifies catalog propagates through _StreamState, quote_qualified, DDL,
discovery queries, QueryConfig, and the configure_schema call path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cdk.query_builder import QueryBuilder, QueryConfig
from cdk.sql.dialects import SqlDialect
from cdk.sql.discovery import list_columns, list_tables
from cdk.type_map import TypeMapper
from cdk.type_map.rules import parse_rules

from .conftest import FakeAdbcRuntime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _route(rows_by_view):
    def responder(sql, params):
        lowered = sql.lower()
        if "key_column_usage" in lowered:
            return rows_by_view.get("pks", [])
        if "information_schema.columns" in lowered:
            return rows_by_view.get("columns", [])
        if "information_schema.tables" in lowered:
            return rows_by_view.get("tables", [])
        if "information_schema.schemata" in lowered:
            return rows_by_view.get("schemas", [])
        return []

    return responder


class _StubMapper:
    """Minimal TypeMapper that maps any native type to Utf8."""

    def to_arrow_type(self, native):
        return "Utf8"

    def to_native_type(self, canonical, *, params=None):
        return "TEXT"


def _make_handler():
    """Return a connected GenericSQLConnector stub wired for configure_schema."""
    from cdk.sql.generic import GenericSQLConnector

    handler = GenericSQLConnector()
    handler._connected = True
    handler._engine = MagicMock()
    handler._runtime = MagicMock()
    handler._runtime.type_mapper_for = MagicMock(
        return_value=TypeMapper(
            "pg",
            parse_rules(
                [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
                source="<t>",
            ),
        ),
    )
    handler._ensure_tables_exist = AsyncMock()
    return handler


# ---------------------------------------------------------------------------
# quote_qualified with catalog
# ---------------------------------------------------------------------------


class TestQuoteQualifiedCatalog:
    def test_three_part_ansi(self):
        d = SqlDialect()
        assert d.quote_qualified("public", "orders", catalog="my_db") == (
            '"my_db"."public"."orders"'
        )

    def test_catalog_without_schema(self):
        d = SqlDialect()
        assert d.quote_qualified("", "orders", catalog="my_db") == (
            '"my_db"."orders"'
        )

    def test_no_catalog_unchanged(self):
        d = SqlDialect()
        assert d.quote_qualified("public", "orders") == '"public"."orders"'

    def test_catalog_is_quoted(self):
        d = SqlDialect()
        result = d.quote_qualified("public", "orders", catalog="my-db")
        assert '"my-db"' in result


# ---------------------------------------------------------------------------
# adbc_ingest_schema_kwargs with catalog
# ---------------------------------------------------------------------------


class TestAdbcIngestSchemaKwargs:
    def test_schema_only_no_catalog(self):
        d = SqlDialect()
        kwargs = d.adbc_ingest_schema_kwargs("public")
        assert kwargs == {"db_schema_name": "public"}
        assert "catalog_name" not in kwargs

    def test_schema_and_catalog(self):
        d = SqlDialect()
        kwargs = d.adbc_ingest_schema_kwargs("public", catalog_name="my_db")
        assert kwargs["db_schema_name"] == "public"
        assert kwargs["catalog_name"] == "my_db"

    def test_empty_schema_no_db_schema_name(self):
        d = SqlDialect()
        kwargs = d.adbc_ingest_schema_kwargs("", catalog_name="my_db")
        assert "db_schema_name" not in kwargs
        assert kwargs["catalog_name"] == "my_db"


# ---------------------------------------------------------------------------
# configure_schema populates _StreamState.catalog_name
# ---------------------------------------------------------------------------


class TestConfigureSchemaCatalogWiring:
    @pytest.mark.asyncio
    async def test_catalog_stored_in_stream_state(self):
        from src.grpc.generated.analitiq.v1 import SchemaMessage, WriteMode

        handler = _make_handler()
        handler._endpoint_refs = {
            "s1": {"scope": "connector", "connection_id": "pg", "endpoint_id": "x"},
        }
        handler._stream_endpoints = {
            "s1": {
                "database_object": {
                    "name": "orders",
                    "schema": "analytics",
                    "catalog": "my_project",
                },
                "columns": [
                    {
                        "name": "id",
                        "native_type": "BIGINT",
                        "arrow_type": "Int64",
                        "nullable": False,
                    }
                ],
                "primary_keys": ["id"],
            },
        }

        msg = SchemaMessage(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_INSERT,
        )
        ok = await handler.configure_schema(msg)

        assert ok is True
        state = handler._streams["s1"]
        assert state.catalog_name == "my_project"
        assert state.schema_name == "analytics"
        assert state.table_name == "orders"

    @pytest.mark.asyncio
    async def test_no_catalog_gives_empty_string(self):
        from src.grpc.generated.analitiq.v1 import SchemaMessage, WriteMode

        handler = _make_handler()
        handler._endpoint_refs = {
            "s1": {"scope": "connector", "connection_id": "pg", "endpoint_id": "x"},
        }
        handler._stream_endpoints = {
            "s1": {
                "database_object": {"name": "orders", "schema": "public"},
                "columns": [
                    {
                        "name": "id",
                        "native_type": "BIGINT",
                        "arrow_type": "Int64",
                        "nullable": False,
                    }
                ],
                "primary_keys": ["id"],
            },
        }

        msg = SchemaMessage(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_INSERT,
        )
        await handler.configure_schema(msg)

        assert handler._streams["s1"].catalog_name == ""


# ---------------------------------------------------------------------------
# Discovery catalog filtering via list_tables / list_columns
# ---------------------------------------------------------------------------


class TestDiscoveryCatalogFilter:
    @pytest.mark.asyncio
    async def test_list_tables_catalog_param_appended(self):
        runtime = FakeAdbcRuntime(
            "ansi",
            responder=_route({"tables": [{"table_name": "orders"}]}),
        )
        result = await list_tables(
            runtime, "analytics", dialect=SqlDialect(), catalog="my_project"
        )
        assert result == ["orders"]
        _sql, params = runtime.connections[-1].executed[-1]
        assert params[0] == "analytics"
        assert params[1] == "my_project"

    @pytest.mark.asyncio
    async def test_list_tables_without_catalog_omits_catalog_param(self):
        runtime = FakeAdbcRuntime(
            "ansi",
            responder=_route({"tables": [{"table_name": "orders"}]}),
        )
        await list_tables(runtime, "public", dialect=SqlDialect())
        _sql, params = runtime.connections[-1].executed[-1]
        assert params == ["public"]

    @pytest.mark.asyncio
    async def test_list_columns_catalog_param_in_both_queries(self):
        runtime = FakeAdbcRuntime(
            "ansi",
            mapper=_StubMapper(),
            responder=_route(
                {
                    "pks": [{"column_name": "id"}],
                    "columns": [
                        {
                            "column_name": "id",
                            "data_type": "text",
                            "is_nullable": "NO",
                        }
                    ],
                }
            ),
        )
        await list_columns(
            runtime, "public", "orders", dialect=SqlDialect(), catalog="my_db"
        )
        for _sql, params in runtime.connections[-1].executed:
            assert params[-1] == "my_db"


# ---------------------------------------------------------------------------
# QueryConfig.catalog_name in QueryBuilder
# ---------------------------------------------------------------------------


class TestQueryBuilderCatalog:
    def test_catalog_and_schema_compose_dotted_from(self):
        builder = QueryBuilder("postgresql")
        sql, _ = builder.build_select_query(
            QueryConfig(
                schema_name="analytics",
                table_name="orders",
                catalog_name="my_project",
                columns=["id"],
            )
        )
        assert "my_project.analytics" in sql

    def test_no_catalog_no_dotted_prefix(self):
        builder = QueryBuilder("postgresql")
        sql, _ = builder.build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="orders",
                columns=["id"],
            )
        )
        assert "my_project" not in sql
        assert "public" in sql


# ---------------------------------------------------------------------------
# _ensure_tables_via_adbc emits catalog-qualified CREATE SCHEMA
# ---------------------------------------------------------------------------


class TestEnsureTablesViaAdbcCatalog:
    @pytest.mark.asyncio
    async def test_schema_ddl_includes_catalog_when_set(self):
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        executed: list[str] = []

        class _FakeCursor:
            def execute(self, sql, *args):
                executed.append(sql)

            def close(self):
                pass

        class _FakeConn:
            def cursor(self):
                return _FakeCursor()

            def commit(self):
                pass

        handler = GenericSQLConnector()
        handler._adbc_only = True
        handler._adbc_conn = _FakeConn()

        state = _StreamState(
            schema_name="analytics",
            table_name="orders",
            catalog_name="my_project",
        )
        ddl = 'CREATE TABLE IF NOT EXISTS "my_project"."analytics"."orders" (id INTEGER)'
        await handler._ensure_tables_via_adbc(state, [ddl])

        schema_stmts = [s for s in executed if "CREATE SCHEMA" in s]
        assert schema_stmts, "expected a CREATE SCHEMA statement"
        assert schema_stmts[0] == 'CREATE SCHEMA IF NOT EXISTS "my_project"."analytics"'

    @pytest.mark.asyncio
    async def test_schema_ddl_suppressed_when_schema_is_implicit_default_with_catalog(self):
        from cdk.sql.dialects import SqlDialect
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        class _ImplicitPublicDialect(SqlDialect):
            def schema_is_implicit_default(self, schema_name: str) -> bool:
                return (not schema_name) or schema_name.upper() == "PUBLIC"

        class _ImplicitConnector(GenericSQLConnector):
            dialect_class = _ImplicitPublicDialect

        executed: list[str] = []

        class _FakeCursor:
            def execute(self, sql, *args):
                executed.append(sql)

            def close(self):
                pass

        class _FakeConn:
            def cursor(self):
                return _FakeCursor()

            def commit(self):
                pass

        handler = _ImplicitConnector()
        handler._adbc_only = True
        handler._adbc_conn = _FakeConn()

        state = _StreamState(
            schema_name="public",
            table_name="orders",
            catalog_name="my_project",
        )
        ddl = 'CREATE TABLE IF NOT EXISTS "my_project"."public"."orders" (id INTEGER)'
        await handler._ensure_tables_via_adbc(state, [ddl])

        schema_stmts = [s for s in executed if "CREATE SCHEMA" in s]
        assert not schema_stmts, "CREATE SCHEMA should be suppressed when schema is implicit default"

    @pytest.mark.asyncio
    async def test_schema_ddl_without_catalog_unchanged(self):
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        executed: list[str] = []

        class _FakeCursor:
            def execute(self, sql, *args):
                executed.append(sql)

            def close(self):
                pass

        class _FakeConn:
            def cursor(self):
                return _FakeCursor()

            def commit(self):
                pass

        handler = GenericSQLConnector()
        handler._adbc_only = True
        handler._adbc_conn = _FakeConn()

        state = _StreamState(schema_name="analytics", table_name="orders")
        ddl = 'CREATE TABLE IF NOT EXISTS "analytics"."orders" (id INTEGER)'
        await handler._ensure_tables_via_adbc(state, [ddl])

        schema_stmts = [s for s in executed if "CREATE SCHEMA" in s]
        assert schema_stmts, "expected a CREATE SCHEMA statement"
        assert "my_project" not in schema_stmts[0]
        assert schema_stmts[0] == 'CREATE SCHEMA IF NOT EXISTS "analytics"'
