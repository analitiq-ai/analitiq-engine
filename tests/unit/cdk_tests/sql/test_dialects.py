"""The ANSI-neutral ``SqlDialect`` base and its hook surface.

The CDK ships only this base; per-system dialects (postgres/mysql/snowflake/
bigquery) live in connector packages. These tests pin the base's portable
machinery (ANSI quoting, the ``TableAddress`` composer with its one-shot
identifier normalization and catalog capability gate, PK clause,
INFORMATION_SCHEMA query shapes) and assert that every operation with no
portable form raises ``UnsupportedDialectOperationError`` naming the operation
and pointing at the connector package. Small fixture subclasses exercise
non-default behavior (backtick quoting, NOT ENFORCED PKs, system-schema
filtering, catalog addressing) the way a connector package's dialect would,
without recreating any vendor's dialect.
"""

from __future__ import annotations

import pytest

from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.exceptions import CatalogAddressingError, UnsupportedDialectOperationError

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
    """A dialect that folds identifiers upper-case (as a folding system would)."""

    name = "upper"

    def normalize_ident(self, name: str) -> str:
        return name.upper()


class _SystemSchemaDialect(SqlDialect):
    """A dialect that hides catalog/internal schemas from list_schemas."""

    name = "sysfilter"
    system_schemas = ("information_schema", "pg_catalog")


class _CatalogDialect(SqlDialect):
    """A dialect whose system addresses catalogs per statement (as DuckDB or
    BigQuery would)."""

    name = "cataloged"
    supports_catalog_addressing = True


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
        d = SqlDialect()
        assert d.quote_table(d.table_address("orders", schema="public")) == (
            '"public"."orders"'
        )
        b = _BacktickDialect()
        assert b.quote_table(b.table_address("orders", schema="ds")) == (
            "`ds`.`orders`"
        )

    def test_qualified_name_without_schema(self):
        d = SqlDialect()
        assert d.quote_table(d.table_address("orders")) == '"orders"'

    def test_qualified_name_composes_normalize_then_quote(self):
        # table_address normalizes every component once at construction;
        # quote_table only quotes — schema and table fold identically.
        d = _UpperNormalizingDialect()
        assert d.quote_table(d.table_address("orders", schema="public")) == (
            '"PUBLIC"."ORDERS"'
        )

    def test_three_part_name_with_catalog(self):
        d = _CatalogDialect()
        address = d.table_address("orders", schema="ds", catalog="proj")
        assert d.quote_table(address) == '"proj"."ds"."orders"'

    def test_quote_schema_with_and_without_catalog(self):
        d = _CatalogDialect()
        assert d.quote_schema(d.table_address("t", schema="ds")) == '"ds"'
        assert (
            d.quote_schema(d.table_address("t", schema="ds", catalog="proj"))
            == '"proj"."ds"'
        )


class TestTableAddress:
    def test_base_normalize_is_identity(self):
        d = SqlDialect()
        assert d.normalize_ident("public") == "public"
        assert d.normalize_ident("Analytics") == "Analytics"

    def test_normalization_applies_to_every_component(self):
        # One rule, applied once, to catalog AND schema AND table — no
        # consumer can diverge afterwards (issue #348 item 3).
        class _UpperCatalogDialect(_UpperNormalizingDialect):
            supports_catalog_addressing = True

        address = _UpperCatalogDialect().table_address(
            "orders", schema="public", catalog="analytics_db"
        )
        assert address == TableAddress(
            table="ORDERS", schema="PUBLIC", catalog="ANALYTICS_DB"
        )

    def test_str_is_dotted_display_form(self):
        assert str(TableAddress(table="t", schema="s", catalog="c")) == "c.s.t"
        assert str(TableAddress(table="t", schema="s")) == "s.t"
        assert str(TableAddress(table="t")) == "t"

    def test_catalog_on_unsupporting_dialect_fails_loud(self):
        # ANSI SQL has no portable cross-catalog form; the base refuses a
        # catalog before any SQL is composed instead of compiling a
        # statement the system would misread (issues #343 / #338).
        with pytest.raises(CatalogAddressingError, match="default catalog"):
            SqlDialect().table_address("t", schema="s", catalog="other_db")

    def test_catalog_without_schema_fails_loud(self):
        with pytest.raises(CatalogAddressingError, match="requires an explicit schema"):
            _CatalogDialect().table_address("t", catalog="proj")

    def test_no_catalog_is_always_accepted(self):
        assert SqlDialect().table_address("t", schema="s") == TableAddress(
            table="t", schema="s"
        )


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

    def test_columns_query_normalizes_uniformly(self):
        # Schema AND table bind values fold through the same normalize_ident
        # rule (issue #336 — one rule, every component, every path).
        _, params = _UpperNormalizingDialect().columns_query("public", "orders")
        assert params == ["PUBLIC", "ORDERS"]

    def test_primary_keys_query(self):
        sql, params = SqlDialect().primary_keys_query("public", "orders")
        assert "PRIMARY KEY" in sql
        assert "key_column_usage" in sql
        assert params == ["public", "orders"]
        assert sql.count("?") == 2

    def test_primary_keys_query_normalizes_uniformly(self):
        _, params = _UpperNormalizingDialect().primary_keys_query("public", "orders")
        assert params == ["PUBLIC", "ORDERS"]


class TestCatalogScopedDiscoveryQueries:
    """Discovery honors catalog symmetrically: all four queries scope to it
    on a catalog-addressing dialect, and all four refuse it on any other
    (issue #348 item 4 / issue #337)."""

    def test_schemas_query_scopes_to_catalog(self):
        sql, params = _CatalogDialect().schemas_query("proj")
        assert '"proj".information_schema.schemata' in sql
        assert "catalog_name = ?" in sql
        assert params == ["proj"]

    def test_tables_query_scopes_to_catalog(self):
        sql, params = _CatalogDialect().tables_query("ds", "proj")
        assert '"proj".information_schema.tables' in sql
        assert "table_catalog = ?" in sql
        assert params == ["ds", "proj"]
        assert sql.count("?") == len(params)

    def test_columns_query_scopes_to_catalog(self):
        sql, params = _CatalogDialect().columns_query("ds", "orders", "proj")
        assert '"proj".information_schema.columns' in sql
        assert "table_catalog = ?" in sql
        assert params == ["ds", "orders", "proj"]
        assert sql.count("?") == len(params)

    def test_primary_keys_query_scopes_to_catalog(self):
        sql, params = _CatalogDialect().primary_keys_query("ds", "orders", "proj")
        assert '"proj".information_schema.table_constraints' in sql
        assert '"proj".information_schema.key_column_usage' in sql
        assert "tc.table_catalog = ?" in sql
        assert params == ["ds", "orders", "proj"]
        assert sql.count("?") == len(params)

    def test_catalog_params_are_normalized(self):
        class _UpperCatalogDialect(_UpperNormalizingDialect):
            supports_catalog_addressing = True

        sql, params = _UpperCatalogDialect().tables_query("ds", "proj")
        assert '"PROJ".information_schema.tables' in sql
        assert params == ["DS", "PROJ"]

    @pytest.mark.parametrize(
        "query",
        [
            lambda d: d.schemas_query("other"),
            lambda d: d.tables_query("s", "other"),
            lambda d: d.columns_query("s", "t", "other"),
            lambda d: d.primary_keys_query("s", "t", "other"),
        ],
    )
    def test_every_query_refuses_catalog_on_unsupporting_dialect(self, query):
        with pytest.raises(CatalogAddressingError):
            query(SqlDialect())

    def test_empty_catalog_keeps_the_plain_shape(self):
        # No catalog on a catalog-addressing dialect compiles the exact
        # session-local shape the base emits.
        assert _CatalogDialect().tables_query("s") == SqlDialect().tables_query("s")


class TestInformationSchemaRef:
    """The single hook composing the metadata path (issue #348 item 2 /
    issue #339 — a dataset-scoped system overrides it in its package)."""

    def test_base_is_session_local(self):
        assert SqlDialect().information_schema_ref("tables") == (
            "information_schema.tables"
        )

    def test_catalog_prefix_on_addressing_dialect(self):
        assert _CatalogDialect().information_schema_ref("tables", catalog="proj") == (
            '"proj".information_schema.tables'
        )

    def test_schema_scoped_override_receives_the_schema(self):
        # A dialect whose metadata views are schema-scoped (BigQuery's
        # project.dataset.INFORMATION_SCHEMA) composes the full path from
        # the same hook — the base signature carries what it needs.
        class _DatasetScopedDialect(_CatalogDialect):
            def information_schema_ref(self, view, *, catalog="", schema=""):
                self._check_catalog(catalog)
                parts = [p for p in (catalog, schema) if p]
                prefix = ".".join(self.quote_ident(p) for p in parts)
                view_ref = f"INFORMATION_SCHEMA.{view.upper()}"
                return f"{prefix}.{view_ref}" if prefix else view_ref

        sql, _ = _DatasetScopedDialect().tables_query("ds", "proj")
        assert '"proj"."ds".INFORMATION_SCHEMA.TABLES' in sql


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


class TestAdbcIngestKwargs:
    """The base targets schema (and catalog, when addressable) explicitly via
    the standard ADBC options; drivers without per-statement ingest targeting
    override to return none."""

    def test_base_targets_address_schema(self):
        d = SqlDialect()
        assert d.adbc_ingest_kwargs(d.table_address("t", schema="public")) == {
            "db_schema_name": "public"
        }

    def test_address_normalization_reaches_the_kwargs(self):
        d = _UpperNormalizingDialect()
        assert d.adbc_ingest_kwargs(d.table_address("t", schema="public")) == {
            "db_schema_name": "PUBLIC"
        }

    def test_empty_schema_targets_nothing(self):
        d = SqlDialect()
        assert d.adbc_ingest_kwargs(d.table_address("t")) == {}

    def test_catalog_targets_the_catalog_option(self):
        d = _CatalogDialect()
        address = d.table_address("t", schema="ds", catalog="proj")
        assert d.adbc_ingest_kwargs(address) == {
            "db_schema_name": "ds",
            "catalog_name": "proj",
        }
