"""Tests for database_object.catalog threading through the SQL stack.

Covers issue #330: catalog was accepted by the contract but never consumed.
Verifies catalog propagates through _StreamState, quote_qualified, DDL,
discovery queries, QueryConfig, and the configure_schema call path.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cdk.contract import ColumnDef
from cdk.query_builder import QueryBuilder, QueryConfig
from cdk.sql.ddl import build_create_table_sql
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

    @staticmethod
    def to_arrow_type(native):
        return "Utf8"

    @staticmethod
    def to_native_type(canonical, *, params=None):
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
    @staticmethod
    def test_three_part_ansi():
        d = SqlDialect()
        assert d.quote_qualified("public", "orders", catalog="my_db") == (
            '"my_db"."public"."orders"'
        )

    @staticmethod
    def test_catalog_without_schema():
        d = SqlDialect()
        assert d.quote_qualified("", "orders", catalog="my_db") == ('"my_db"."orders"')

    @staticmethod
    def test_no_catalog_unchanged():
        d = SqlDialect()
        assert d.quote_qualified("public", "orders") == '"public"."orders"'

    @staticmethod
    def test_catalog_is_quoted():
        d = SqlDialect()
        result = d.quote_qualified("public", "orders", catalog="my-db")
        assert '"my-db"' in result


# ---------------------------------------------------------------------------
# adbc_ingest_schema_kwargs with catalog
# ---------------------------------------------------------------------------


class TestAdbcIngestSchemaKwargs:
    @staticmethod
    def test_schema_only_no_catalog():
        d = SqlDialect()
        kwargs = d.adbc_ingest_schema_kwargs("public")
        assert kwargs == {"db_schema_name": "public"}
        assert "catalog_name" not in kwargs

    @staticmethod
    def test_schema_and_catalog():
        d = SqlDialect()
        kwargs = d.adbc_ingest_schema_kwargs("public", catalog_name="my_db")
        assert kwargs["db_schema_name"] == "public"
        assert kwargs["catalog_name"] == "my_db"

    @staticmethod
    def test_empty_schema_no_db_schema_name():
        d = SqlDialect()
        kwargs = d.adbc_ingest_schema_kwargs("", catalog_name="my_db")
        assert "db_schema_name" not in kwargs
        assert kwargs["catalog_name"] == "my_db"


# ---------------------------------------------------------------------------
# configure_schema populates _StreamState.catalog_name
# ---------------------------------------------------------------------------


class TestConfigureSchemaCatalogWiring:
    @staticmethod
    @pytest.mark.asyncio
    async def test_catalog_stored_in_stream_state():
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

    @staticmethod
    @pytest.mark.asyncio
    async def test_no_catalog_gives_empty_string():
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
    @staticmethod
    @pytest.mark.asyncio
    async def test_list_tables_catalog_param_appended():
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

    @staticmethod
    @pytest.mark.asyncio
    async def test_list_tables_without_catalog_omits_catalog_param():
        runtime = FakeAdbcRuntime(
            "ansi",
            responder=_route({"tables": [{"table_name": "orders"}]}),
        )
        await list_tables(runtime, "public", dialect=SqlDialect())
        _sql, params = runtime.connections[-1].executed[-1]
        assert params == ["public"]

    @staticmethod
    @pytest.mark.asyncio
    async def test_list_columns_catalog_param_in_both_queries():
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
    @staticmethod
    def test_catalog_and_schema_compose_dotted_from():
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

    @staticmethod
    def test_no_catalog_no_dotted_prefix():
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
    @staticmethod
    @pytest.mark.asyncio
    async def test_schema_ddl_includes_catalog_when_set():
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        executed: list[str] = []
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = lambda sql, *args: executed.append(sql)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        handler = GenericSQLConnector()
        handler._adbc_only = True
        handler._adbc_conn = mock_conn

        state = _StreamState(
            schema_name="analytics",
            table_name="orders",
            catalog_name="my_project",
        )
        ddl = (
            'CREATE TABLE IF NOT EXISTS "my_project"."analytics"."orders" (id INTEGER)'
        )
        await handler._ensure_tables_via_adbc(state, [ddl])

        schema_stmts = [s for s in executed if "CREATE SCHEMA" in s]
        assert schema_stmts, "expected a CREATE SCHEMA statement"
        assert schema_stmts[0] == 'CREATE SCHEMA IF NOT EXISTS "my_project"."analytics"'

    @staticmethod
    @pytest.mark.asyncio
    async def test_schema_ddl_suppressed_when_schema_is_implicit_default_with_catalog():
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        class _ImplicitPublicDialect(SqlDialect):
            def schema_is_implicit_default(self, schema_name: str) -> bool:
                return (not schema_name) or schema_name.upper() == "PUBLIC"

        class _ImplicitConnector(GenericSQLConnector):
            dialect_class = _ImplicitPublicDialect

        executed: list[str] = []
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = lambda sql, *args: executed.append(sql)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        handler = _ImplicitConnector()
        handler._adbc_only = True
        handler._adbc_conn = mock_conn

        state = _StreamState(
            schema_name="public",
            table_name="orders",
            catalog_name="my_project",
        )
        ddl = 'CREATE TABLE IF NOT EXISTS "my_project"."public"."orders" (id INTEGER)'
        await handler._ensure_tables_via_adbc(state, [ddl])

        schema_stmts = [s for s in executed if "CREATE SCHEMA" in s]
        assert (
            not schema_stmts
        ), "CREATE SCHEMA should be suppressed when schema is implicit default"

    @staticmethod
    @pytest.mark.asyncio
    async def test_schema_ddl_without_catalog_unchanged():
        from cdk.sql.generic import GenericSQLConnector, _StreamState

        executed: list[str] = []
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = lambda sql, *args: executed.append(sql)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        handler = GenericSQLConnector()
        handler._adbc_only = True
        handler._adbc_conn = mock_conn

        state = _StreamState(schema_name="analytics", table_name="orders")
        ddl = 'CREATE TABLE IF NOT EXISTS "analytics"."orders" (id INTEGER)'
        await handler._ensure_tables_via_adbc(state, [ddl])

        schema_stmts = [s for s in executed if "CREATE SCHEMA" in s]
        assert schema_stmts, "expected a CREATE SCHEMA statement"
        assert "my_project" not in schema_stmts[0]
        assert schema_stmts[0] == 'CREATE SCHEMA IF NOT EXISTS "analytics"'


# ---------------------------------------------------------------------------
# Read-path quoting: catalog/schema components quoted per dialect
# ---------------------------------------------------------------------------


class TestQueryBuilderCatalogQuoting:
    @staticmethod
    def test_quote_identifiers_quotes_each_component():
        builder = QueryBuilder("postgresql", quote_identifiers=True)
        sql, _ = builder.build_select_query(
            QueryConfig(
                schema_name="Analytics",
                table_name="Orders",
                catalog_name="my-project",
                columns=["id"],
            )
        )
        # Hyphenated catalog and mixed-case schema each quoted; dot unquoted.
        assert '"my-project"."Analytics"' in sql

    @staticmethod
    def test_no_quote_identifiers_leaves_dotted_raw():
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
        assert '"my_project"' not in sql


# ---------------------------------------------------------------------------
# primary_keys_query scopes the join to the catalog
# ---------------------------------------------------------------------------


class TestPrimaryKeysQueryCatalog:
    @staticmethod
    def test_join_scopes_constraint_and_table_catalog():
        sql, params = SqlDialect().primary_keys_query(
            "public", "orders", catalog="my_db"
        )
        assert "tc.constraint_catalog = kcu.constraint_catalog" in sql
        assert "tc.constraint_schema = kcu.constraint_schema" in sql
        assert "tc.table_catalog = ?" in sql
        assert params[-1] == "my_db"


# ---------------------------------------------------------------------------
# Discovery catalog filter is normalized like the DDL/name path
# ---------------------------------------------------------------------------


class _UpperDialect(SqlDialect):
    def normalize_schema(self, schema: str) -> str:
        return schema.upper()


class TestDiscoveryCatalogFilterNormalized:
    @staticmethod
    @pytest.mark.asyncio
    async def test_catalog_filter_folded_for_folding_dialect():
        runtime = FakeAdbcRuntime(
            "ansi",
            responder=_route({"tables": [{"table_name": "orders"}]}),
        )
        await list_tables(
            runtime, "analytics", dialect=_UpperDialect(), catalog="my_db"
        )
        _sql, params = runtime.connections[-1].executed[-1]
        assert params[0] == "ANALYTICS"
        assert params[1] == "MY_DB"


# ---------------------------------------------------------------------------
# Fail loud when a catalog is set but the dialect cannot target it on ingest
# ---------------------------------------------------------------------------


class _NoTargetDialect(SqlDialect):
    """A Snowflake-like dialect that suppresses per-statement ingest kwargs."""

    def adbc_ingest_schema_kwargs(self, schema_name, *, catalog_name=""):
        return {}


class TestAdbcIngestTargetKwargs:
    @staticmethod
    def test_base_threads_catalog():
        from cdk.sql.generic import GenericSQLConnector

        kwargs = GenericSQLConnector()._adbc_ingest_target_kwargs("analytics", "my_db")
        assert kwargs["db_schema_name"] == "analytics"
        assert kwargs["catalog_name"] == "my_db"

    @staticmethod
    def test_catalog_is_normalized():
        """Normalizing dialects apply the same case-folding to catalog as schema."""
        from cdk.sql.generic import GenericSQLConnector

        class _UpperNormalizingDialect(SqlDialect):
            def normalize_schema(self, s):
                return s.upper()

        class _C(GenericSQLConnector):
            dialect_class = _UpperNormalizingDialect

        kwargs = _C()._adbc_ingest_target_kwargs("analytics", "my_db")
        assert kwargs["db_schema_name"] == "ANALYTICS"
        assert kwargs["catalog_name"] == "MY_DB"

    @staticmethod
    def test_no_catalog_never_raises():
        from cdk.sql.generic import GenericSQLConnector

        class _C(GenericSQLConnector):
            dialect_class = _NoTargetDialect

        assert _C()._adbc_ingest_target_kwargs("analytics") == {}

    @staticmethod
    def test_raises_when_dialect_drops_a_requested_catalog():
        from cdk.adbc_registry import AdbcConfigurationError
        from cdk.sql.generic import GenericSQLConnector

        class _C(GenericSQLConnector):
            dialect_class = _NoTargetDialect

        with pytest.raises(AdbcConfigurationError, match="my_db"):
            _C()._adbc_ingest_target_kwargs("analytics", "my_db")

    @staticmethod
    def test_truncate_then_ingest_validates_before_truncating():
        """Validation must raise before _adbc_truncate_sync is called."""
        from unittest.mock import patch

        import pyarrow as pa

        from cdk.adbc_registry import AdbcConfigurationError
        from cdk.sql.generic import GenericSQLConnector

        class _C(GenericSQLConnector):
            dialect_class = _NoTargetDialect

        connector = _C()
        batch = pa.RecordBatch.from_pydict({"x": [1]})
        with patch.object(connector, "_adbc_truncate_sync") as mock_trunc:
            with pytest.raises(AdbcConfigurationError, match="my_db"):
                connector._truncate_then_ingest_sync(
                    batch, "analytics", "orders", "my_db"
                )
            mock_trunc.assert_not_called()


# ---------------------------------------------------------------------------
# sqlalchemy_pre_ddl is catalog-aware
# ---------------------------------------------------------------------------


class TestSqlalchemyPreDdlCatalog:
    @staticmethod
    def test_base_accepts_catalog_kwarg():
        assert SqlDialect().sqlalchemy_pre_ddl("public", catalog_name="db") == []

    @staticmethod
    def test_override_can_qualify_schema_with_catalog():
        class _D(SqlDialect):
            def sqlalchemy_pre_ddl(self, schema_name, *, catalog_name=""):
                if catalog_name:
                    return [
                        f'CREATE SCHEMA IF NOT EXISTS "{catalog_name}".'
                        f'"{schema_name}"'
                    ]
                return [f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"']

        assert _D().sqlalchemy_pre_ddl("analytics", catalog_name="proj") == [
            'CREATE SCHEMA IF NOT EXISTS "proj"."analytics"'
        ]


# ---------------------------------------------------------------------------
# _run_ddl_and_reflect composes catalog+schema with per-component quoting
# ---------------------------------------------------------------------------


class TestRunDdlAndReflectCatalogQuoting:
    @staticmethod
    def test_catalog_and_schema_produce_per_component_quoted_reflect_schema(
        monkeypatch,
    ):
        """SQLAlchemy reflection schema uses quoted catalog + schema separately."""
        import cdk.sql.generic as gen

        class _UpperNormalizingDialect(SqlDialect):
            def normalize_schema(self, s: str) -> str:
                return s.upper()

        class _C(gen.GenericSQLConnector):
            dialect_class = _UpperNormalizingDialect

        captured: dict = {}

        def _fake_table(name, meta, *, autoload_with, schema):
            captured["schema"] = schema
            return MagicMock()

        monkeypatch.setattr(gen, "Table", _fake_table)
        conn = MagicMock()
        conn.execute.return_value = None

        state = gen._StreamState(
            schema_name="my-schema",
            table_name="orders",
            catalog_name="my-catalog",
        )
        _C()._run_ddl_and_reflect(conn, state, "CREATE TABLE orders (id INT)")

        schema_arg = str(captured["schema"])
        assert '"MY-CATALOG"' in schema_arg
        assert '"MY-SCHEMA"' in schema_arg
        assert "my-catalog" not in schema_arg
        assert "my-schema" not in schema_arg

    @staticmethod
    def test_catalog_only_uses_quoted_catalog(monkeypatch):
        import cdk.sql.generic as gen

        captured: dict = {}

        def _fake_table(name, meta, *, autoload_with, schema):
            captured["schema"] = schema
            return MagicMock()

        monkeypatch.setattr(gen, "Table", _fake_table)
        conn = MagicMock()

        state = gen._StreamState(
            schema_name="",
            table_name="orders",
            catalog_name="my-catalog",
        )
        gen.GenericSQLConnector()._run_ddl_and_reflect(
            conn, state, "CREATE TABLE orders (id INT)"
        )

        schema_arg = str(captured["schema"])
        assert '"my-catalog"' in schema_arg
        assert "my-catalog" not in schema_arg.replace('"my-catalog"', "")


# ---------------------------------------------------------------------------
# Control-plane capability surface threads catalog to the discovery/DDL helpers
# ---------------------------------------------------------------------------


class TestCapabilityCatalogSurface:
    @staticmethod
    @pytest.mark.asyncio
    async def test_list_tables_delegator_threads_catalog(monkeypatch):
        import cdk.sql.generic as gen

        seen: dict[str, str] = {}

        async def _fake(runtime, schema, *, dialect, catalog=""):
            seen["catalog"] = catalog
            return []

        monkeypatch.setattr(gen, "_sql_list_tables", _fake)
        await gen.GenericSQLConnector().list_tables(
            MagicMock(), "public", catalog="my_db"
        )
        assert seen["catalog"] == "my_db"

    @staticmethod
    @pytest.mark.asyncio
    async def test_create_table_delegator_threads_catalog(monkeypatch):
        import cdk.sql.generic as gen

        seen: dict[str, str] = {}

        async def _fake(runtime, schema, table, columns, pks, *, dialect, catalog=""):
            seen["catalog"] = catalog

        monkeypatch.setattr(gen, "_sql_create_table", _fake)
        await gen.GenericSQLConnector().create_table(
            MagicMock(), "public", "orders", [], [], catalog="my_db"
        )
        assert seen["catalog"] == "my_db"


# ---------------------------------------------------------------------------
# build_create_table_sql emits catalog-qualified identifier
# ---------------------------------------------------------------------------


class TestBuildCreateTableSqlCatalog:
    @staticmethod
    def test_three_part_identifier_when_catalog_set():
        mapper = _StubMapper()
        col = ColumnDef(name="id", canonical_type="Int64", nullable=False)
        sql = build_create_table_sql(
            SqlDialect(), mapper, "analytics", "orders", [col], ["id"], catalog="my_db"
        )
        assert '"my_db"."analytics"."orders"' in sql

    @staticmethod
    def test_two_part_identifier_when_no_catalog():
        mapper = _StubMapper()
        col = ColumnDef(name="id", canonical_type="Int64", nullable=False)
        sql = build_create_table_sql(
            SqlDialect(), mapper, "public", "orders", [col], ["id"]
        )
        assert '"public"."orders"' in sql
        assert "my_db" not in sql
