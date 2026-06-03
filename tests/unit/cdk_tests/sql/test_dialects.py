"""Dialect strategy: quoting, schema normalization, PK clause, and the exact
INFORMATION_SCHEMA query shapes per dialect.

These are pure string builders, so the queries are pinned verbatim — a drift in
a dialect's SQL (a renamed catalog view, a dropped filter) would otherwise ship
silently, since the warehouses themselves are not exercised here.
"""

from __future__ import annotations

import pytest

from cdk.sql.dialects import (
    SUPPORTED_DIALECTS,
    BigQueryDialect,
    MySQLDialect,
    PostgresDialect,
    SnowflakeDialect,
    get_dialect,
)
from cdk.sql.exceptions import UnsupportedDialectError


class TestRegistry:
    @pytest.mark.parametrize(
        "driver,cls",
        [
            ("postgresql", PostgresDialect),
            ("postgres", PostgresDialect),
            ("redshift", PostgresDialect),  # libpq-compatible, rides postgres
            ("mysql", MySQLDialect),
            ("mariadb", MySQLDialect),
            ("snowflake", SnowflakeDialect),
            ("bigquery", BigQueryDialect),
        ],
    )
    def test_driver_resolves_to_dialect(self, driver, cls):
        assert isinstance(get_dialect(driver), cls)

    def test_driver_is_case_insensitive(self):
        assert isinstance(get_dialect("PostgreSQL"), PostgresDialect)

    @pytest.mark.parametrize("driver", [None, "", "oracle", "clickhouse", "duckdb"])
    def test_unsupported_driver_raises(self, driver):
        with pytest.raises(UnsupportedDialectError) as exc:
            get_dialect(driver)
        assert exc.value.driver == driver
        assert exc.value.supported == SUPPORTED_DIALECTS


class TestQuoting:
    def test_ansi_double_quote_and_escaping(self):
        d = PostgresDialect()
        assert d.quote_ident("orders") == '"orders"'
        # An embedded double quote is doubled, not stripped (injection-safe).
        assert d.quote_ident('we"ird') == '"we""ird"'

    def test_backtick_quote_for_mysql_and_bigquery(self):
        assert MySQLDialect().quote_ident("orders") == "`orders`"
        assert BigQueryDialect().quote_ident("orders") == "`orders`"

    def test_backtick_dialect_rejects_embedded_backtick(self):
        with pytest.raises(ValueError, match="backtick"):
            BigQueryDialect().quote_ident("we`ird")

    def test_qualified_name(self):
        assert PostgresDialect().quote_qualified("public", "orders") == (
            '"public"."orders"'
        )
        assert BigQueryDialect().quote_qualified("ds", "orders") == "`ds`.`orders`"

    def test_qualified_name_without_schema(self):
        assert PostgresDialect().quote_qualified("", "orders") == '"orders"'

    def test_snowflake_folds_public_to_uppercase(self):
        # Unquoted ``public`` resolves to PUBLIC on Snowflake; quoting the
        # lowercase form would miss the real schema.
        assert SnowflakeDialect().quote_qualified("public", "orders") == (
            '"PUBLIC"."orders"'
        )
        # A non-default schema is preserved verbatim.
        assert SnowflakeDialect().quote_qualified("Analytics", "t") == (
            '"Analytics"."t"'
        )

    def test_postgres_does_not_fold_public(self):
        assert PostgresDialect().normalize_schema("public") == "public"


class TestPrimaryKeyClause:
    def test_bare_clause(self):
        assert PostgresDialect().pk_clause(["id"]) == 'PRIMARY KEY ("id")'
        assert PostgresDialect().pk_clause(["a", "b"]) == 'PRIMARY KEY ("a", "b")'

    def test_bigquery_requires_not_enforced(self):
        assert BigQueryDialect().pk_clause(["id"]) == "PRIMARY KEY (`id`) NOT ENFORCED"


class TestPostgresQueries:
    def test_schemas_query_excludes_catalog_and_temp(self):
        sql, params = PostgresDialect().schemas_query()
        assert "information_schema.schemata" in sql
        assert "pg_temp_%" in sql and "pg_toast_temp_%" in sql
        assert params == ["information_schema", "pg_catalog", "pg_toast"]
        assert sql.count("?") == len(params)

    def test_tables_query(self):
        sql, params = PostgresDialect().tables_query("public")
        assert sql == (
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? ORDER BY table_name"
        )
        assert params == ["public"]

    def test_columns_query(self):
        sql, params = PostgresDialect().columns_query("public", "orders")
        assert "information_schema.columns" in sql
        assert "ORDER BY ordinal_position" in sql
        assert params == ["public", "orders"]
        assert sql.count("?") == 2

    def test_primary_keys_query(self):
        sql, params = PostgresDialect().primary_keys_query("public", "orders")
        assert "PRIMARY KEY" in sql
        assert "key_column_usage" in sql
        assert params == ["public", "orders"]


class TestSnowflakeQueries:
    def test_schemas_query_excludes_information_schema(self):
        sql, params = SnowflakeDialect().schemas_query()
        assert params == ["INFORMATION_SCHEMA"]
        assert sql.count("?") == 1

    def test_tables_query_normalizes_public(self):
        _, params = SnowflakeDialect().tables_query("public")
        assert params == ["PUBLIC"]

    def test_columns_query_normalizes_public(self):
        _, params = SnowflakeDialect().columns_query("public", "ORDERS")
        assert params == ["PUBLIC", "ORDERS"]


class TestBigQueryQueries:
    def test_schemas_query_is_unqualified(self):
        sql, params = BigQueryDialect().schemas_query()
        assert sql == (
            "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA "
            "ORDER BY schema_name"
        )
        assert params == []

    def test_tables_query_qualifies_dataset_in_from(self):
        # Dataset is a structural identifier (backtick-quoted), not a bind.
        sql, params = BigQueryDialect().tables_query("ds")
        assert sql == (
            "SELECT table_name FROM `ds`.INFORMATION_SCHEMA.TABLES "
            "ORDER BY table_name"
        )
        assert params == []

    def test_columns_query_qualifies_dataset_table_is_bind(self):
        sql, params = BigQueryDialect().columns_query("ds", "orders")
        assert "`ds`.INFORMATION_SCHEMA.COLUMNS" in sql
        assert "WHERE table_name = ?" in sql
        assert params == ["orders"]

    def test_primary_keys_query_qualifies_dataset(self):
        sql, params = BigQueryDialect().primary_keys_query("ds", "orders")
        assert "`ds`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS" in sql
        assert "`ds`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE" in sql
        assert params == ["orders"]
