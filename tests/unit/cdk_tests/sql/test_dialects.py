"""The ANSI-neutral ``SqlDialect`` base and its hook surface.

The CDK ships only this base; per-system dialects (postgres/mysql/snowflake/
bigquery) live in connector packages. These tests pin the base's portable
machinery (ANSI quoting, schema normalization, PK clause, INFORMATION_SCHEMA
query shapes) and assert that every operation with no portable form raises
``UnsupportedDialectOperationError`` naming the operation and pointing at the
connector package. Small fixture subclasses exercise non-default behavior
(backtick quoting, NOT ENFORCED PKs, system-schema filtering) the way a
connector package's dialect would, without recreating any vendor's dialect.
"""

from __future__ import annotations

import pytest

from cdk.sql.dialects import SqlDialect
from cdk.sql.exceptions import UnsupportedDialectOperationError

# --- fixture dialects (stand in for connector-package dialects) -------------


class _BacktickDialect(SqlDialect):
    """A dialect that quotes with backticks (as MySQL/BigQuery would)."""

    name = "backtick"
    quote_char = "`"


class _NotEnforcedDialect(SqlDialect):
    """A dialect whose PK clause carries ``NOT ENFORCED`` (as BigQuery would)."""

    name = "not_enforced"
    quote_char = "`"
    pk_not_enforced = True


class _UpperNormalizingDialect(SqlDialect):
    """A dialect that folds schema names upper-case (as Snowflake would)."""

    name = "upper"

    def normalize_schema(self, schema: str) -> str:
        return schema.upper()


class _SystemSchemaDialect(SqlDialect):
    """A dialect that hides catalog/internal schemas from list_schemas."""

    name = "sysfilter"
    system_schemas = ("information_schema", "pg_catalog")


# --- identifier quoting ------------------------------------------------------


class TestQuoting:
    def test_ansi_double_quote_and_escaping(self):
        d = SqlDialect()
        assert d.quote_ident("orders") == '"orders"'
        # An embedded double quote is doubled, not stripped (injection-safe).
        assert d.quote_ident('we"ird') == '"we""ird"'

    def test_backtick_quote(self):
        assert _BacktickDialect().quote_ident("orders") == "`orders`"

    def test_backtick_dialect_rejects_embedded_backtick(self):
        with pytest.raises(ValueError, match="backtick"):
            _BacktickDialect().quote_ident("we`ird")

    def test_qualified_name(self):
        assert SqlDialect().quote_qualified("public", "orders") == ('"public"."orders"')
        assert _BacktickDialect().quote_qualified("ds", "orders") == ("`ds`.`orders`")

    def test_qualified_name_without_schema(self):
        assert SqlDialect().quote_qualified("", "orders") == '"orders"'

    def test_qualified_name_composes_normalize_then_quote(self):
        # quote_qualified normalizes the schema before quoting it; the table
        # name is quoted verbatim.
        assert _UpperNormalizingDialect().quote_qualified("public", "orders") == (
            '"PUBLIC"."orders"'
        )

    def test_catalog_produces_three_part_name(self):
        assert SqlDialect().quote_qualified(
            "public", "orders", catalog="my_db"
        ) == '"my_db"."public"."orders"'

    def test_catalog_without_schema_produces_two_part_name(self):
        assert SqlDialect().quote_qualified(
            "", "orders", catalog="my_db"
        ) == '"my_db"."orders"'

    def test_catalog_with_backtick_dialect(self):
        assert _BacktickDialect().quote_qualified(
            "ds", "orders", catalog="proj"
        ) == "`proj`.`ds`.`orders`"

    def test_catalog_normalizes_schema(self):
        assert _UpperNormalizingDialect().quote_qualified(
            "public", "orders", catalog="MY_DB"
        ) == '"MY_DB"."PUBLIC"."orders"'


class TestNormalizeSchema:
    def test_base_is_identity(self):
        d = SqlDialect()
        assert d.normalize_schema("public") == "public"
        assert d.normalize_schema("Analytics") == "Analytics"

    def test_subclass_hook_applies(self):
        assert _UpperNormalizingDialect().normalize_schema("public") == "PUBLIC"


# --- primary key clause ------------------------------------------------------


class TestPrimaryKeyClause:
    def test_bare_clause(self):
        assert SqlDialect().pk_clause(["id"]) == 'PRIMARY KEY ("id")'
        assert SqlDialect().pk_clause(["a", "b"]) == 'PRIMARY KEY ("a", "b")'

    def test_not_enforced_clause(self):
        assert _NotEnforcedDialect().pk_clause(["id"]) == (
            "PRIMARY KEY (`id`) NOT ENFORCED"
        )


# --- discovery query shapes --------------------------------------------------


class TestDiscoveryQueries:
    def test_schemas_query_no_filter(self):
        sql, params = SqlDialect().schemas_query()
        assert sql == (
            "SELECT schema_name FROM information_schema.schemata "
            "ORDER BY schema_name"
        )
        assert params == []

    def test_schemas_query_filters_system_schemas(self):
        sql, params = _SystemSchemaDialect().schemas_query()
        assert "information_schema.schemata" in sql
        assert "WHERE schema_name NOT IN (?, ?)" in sql
        assert "ORDER BY schema_name" in sql
        assert params == ["information_schema", "pg_catalog"]
        assert sql.count("?") == len(params)

    def test_tables_query(self):
        sql, params = SqlDialect().tables_query("public")
        assert sql == (
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? ORDER BY table_name"
        )
        assert params == ["public"]

    def test_tables_query_normalizes_schema(self):
        _, params = _UpperNormalizingDialect().tables_query("public")
        assert params == ["PUBLIC"]

    def test_columns_query(self):
        sql, params = SqlDialect().columns_query("public", "orders")
        assert "information_schema.columns" in sql
        assert "ORDER BY ordinal_position" in sql
        assert params == ["public", "orders"]
        assert sql.count("?") == 2

    def test_columns_query_normalizes_schema(self):
        _, params = _UpperNormalizingDialect().columns_query("public", "ORDERS")
        assert params == ["PUBLIC", "ORDERS"]

    def test_primary_keys_query(self):
        sql, params = SqlDialect().primary_keys_query("public", "orders")
        assert "PRIMARY KEY" in sql
        assert "key_column_usage" in sql
        assert params == ["public", "orders"]
        assert sql.count("?") == 2

    def test_primary_keys_query_normalizes_schema(self):
        _, params = _UpperNormalizingDialect().primary_keys_query("public", "orders")
        assert params == ["PUBLIC", "orders"]

    def test_tables_query_with_catalog_adds_filter(self):
        sql, params = SqlDialect().tables_query("public", catalog="my_db")
        assert "table_catalog = ?" in sql
        assert params == ["public", "my_db"]
        assert sql.count("?") == 2

    def test_tables_query_without_catalog_has_no_catalog_filter(self):
        sql, params = SqlDialect().tables_query("public")
        assert "table_catalog" not in sql
        assert params == ["public"]

    def test_columns_query_with_catalog_adds_filter(self):
        sql, params = SqlDialect().columns_query("public", "orders", catalog="my_db")
        assert "table_catalog = ?" in sql
        assert params == ["public", "orders", "my_db"]
        assert sql.count("?") == 3

    def test_columns_query_without_catalog_has_no_catalog_filter(self):
        sql, params = SqlDialect().columns_query("public", "orders")
        assert "table_catalog" not in sql
        assert params == ["public", "orders"]

    def test_primary_keys_query_with_catalog_adds_filter(self):
        sql, params = SqlDialect().primary_keys_query("public", "orders", catalog="my_db")
        assert "table_catalog = ?" in sql
        assert params == ["public", "orders", "my_db"]
        assert sql.count("?") == 3

    def test_primary_keys_query_without_catalog_has_no_catalog_filter(self):
        sql, params = SqlDialect().primary_keys_query("public", "orders")
        assert "table_catalog" not in sql
        assert params == ["public", "orders"]


# --- schema semantics --------------------------------------------------------


class TestSchemaIsImplicitDefault:
    def test_empty_is_implicit(self):
        assert SqlDialect().schema_is_implicit_default("") is True

    def test_non_empty_is_not_implicit(self):
        assert SqlDialect().schema_is_implicit_default("public") is False
        assert SqlDialect().schema_is_implicit_default("analytics") is False


# --- pre-DDL -----------------------------------------------------------------


class TestPreDdl:
    def test_base_declares_no_pre_ddl(self):
        assert SqlDialect().sqlalchemy_pre_ddl("analytics") == []


# --- paged reads --------------------------------------------------------------


class TestPagingOrderFallback:
    def test_base_declares_no_fallback(self):
        """ANSI SQL accepts OFFSET without ORDER BY, so the neutral base
        injects no ordering; systems that refuse it (T-SQL) override in
        their connector package."""
        assert SqlDialect().paging_order_fallback() is None

    def test_subclass_hook_applies(self):
        class _NoOpOrderDialect(SqlDialect):
            name = "tsqlish"

            def paging_order_fallback(self):
                return "(SELECT NULL)"

        assert _NoOpOrderDialect().paging_order_fallback() == "(SELECT NULL)"


# --- column type rendering (the single write surface) ------------------------


class _RecordingMapper:
    """Stub TypeMapper recording the to_native_type call and returning a
    sentinel, so the delegation (and params pass-through) is observable."""

    def __init__(self, result="SENTINEL"):
        self.result = result
        self.calls = []

    def to_native_type(self, canonical, *, params=None):
        self.calls.append((canonical, params))
        return self.result


class TestRenderColumnType:
    def test_base_delegates_to_type_mapper(self):
        mapper = _RecordingMapper("NUMERIC(38, 9)")
        out = SqlDialect().render_column_type("Decimal128(38, 9)", mapper)
        assert out == "NUMERIC(38, 9)"
        assert mapper.calls == [("Decimal128(38, 9)", None)]

    def test_base_passes_params_through(self):
        mapper = _RecordingMapper("VARCHAR(255)")
        out = SqlDialect().render_column_type("Utf8", mapper, params={"length": 255})
        assert out == "VARCHAR(255)"
        assert mapper.calls == [("Utf8", {"length": 255})]


# --- unsupported hooks raise loudly ------------------------------------------


class TestUnsupportedHooks:
    """Every operation with no portable form raises a loud, named error."""

    @pytest.mark.parametrize(
        "operation,call",
        [
            (
                "build_sqlalchemy_upsert",
                lambda d: d.build_sqlalchemy_upsert(object(), [], ["id"]),
            ),
            (
                "build_tls_connect_arg",
                lambda d: d.build_tls_connect_arg("verify-ca", None),
            ),
            (
                "adbc_stage_table_sql",
                lambda d: d.adbc_stage_table_sql("stage", "target"),
            ),
        ],
    )
    def test_hook_raises_unsupported(self, operation, call):
        d = SqlDialect()
        with pytest.raises(UnsupportedDialectOperationError) as exc:
            call(d)
        # The error names the operation and the active dialect ...
        assert exc.value.operation == operation
        assert exc.value.dialect == d.name
        message = str(exc.value)
        assert operation in message
        # ... and points the operator at the connector package.
        assert "connector package" in message

    def test_base_does_not_support_upsert_flags(self):
        d = SqlDialect()
        assert d.supports_upsert_sqlalchemy is False
        assert d.supports_upsert_adbc is False


class TestAdbcIngestSchemaKwargs:
    """The base targets the schema explicitly via the standard ADBC option;
    drivers without per-statement ingest targeting override to return none."""

    def test_base_targets_normalized_schema(self):
        assert SqlDialect().adbc_ingest_schema_kwargs("public") == {
            "db_schema_name": "public"
        }

    def test_base_normalizes_before_targeting(self):
        assert _UpperNormalizingDialect().adbc_ingest_schema_kwargs("public") == {
            "db_schema_name": "PUBLIC"
        }

    def test_empty_schema_targets_nothing(self):
        assert SqlDialect().adbc_ingest_schema_kwargs("") == {}
