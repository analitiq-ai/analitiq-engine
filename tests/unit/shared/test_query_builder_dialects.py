"""Unit tests for QueryBuilder dialect expansion and paging fix.

Exercises:

* Built-in dialect resolution (postgresql, mysql, mssql, sqlite).
* Third-party dialect lookup falls through ``importlib`` with a clear
  error when the package is missing.
* Pushing ``limit`` / ``offset`` into ``QueryConfig`` produces
  dialect-correct paging SQL (PG ``LIMIT ... OFFSET ...`` vs MSSQL
  ``OFFSET ... ROWS FETCH NEXT ... ROWS ONLY``).
* The asyncpg positional-param conversion handles the extra bind
  values SQLAlchemy emits for ``.limit().offset()``.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.shared.query_builder import (
    Filter,
    QueryBuilder,
    QueryConfig,
    _get_sqlalchemy_dialect,
)


class TestBuiltinDialectResolution:
    @pytest.mark.parametrize("name", ["postgresql", "postgres", "POSTGRES"])
    def test_postgresql(self, name):
        d = _get_sqlalchemy_dialect(name)
        assert d.name == "postgresql"

    @pytest.mark.parametrize("name", ["mysql", "mariadb", "MariaDB"])
    def test_mysql_family(self, name):
        d = _get_sqlalchemy_dialect(name)
        assert d.name == "mysql"

    def test_mssql(self):
        d = _get_sqlalchemy_dialect("mssql")
        assert d.name == "mssql"

    def test_sqlite(self):
        d = _get_sqlalchemy_dialect("sqlite")
        assert d.name == "sqlite"

    def test_unknown_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported dialect"):
            _get_sqlalchemy_dialect("nonexistent")


class TestLazyDialectLoading:
    def test_missing_third_party_package_raises_actionable_import_error(self):
        # Patch importlib so the lookup acts as if the package isn't
        # installed even when it actually is (or vice versa).
        with patch("src.shared.query_builder.importlib.import_module") as imp:
            imp.side_effect = ImportError("not installed")
            with pytest.raises(ImportError, match="snowflake"):
                _get_sqlalchemy_dialect("snowflake")


class TestPaging:
    def test_postgresql_limit_offset_compiles_to_positional(self):
        builder = QueryBuilder("postgresql")
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="events",
                columns=["id", "name"],
                filters=[Filter(field="id", op="gt", value=10)],
                limit=100,
                offset=200,
            )
        )
        assert "LIMIT" in sql.upper()
        assert "OFFSET" in sql.upper()
        # asyncpg positional placeholders: $1 (filter), $2 (limit), $3 (offset).
        # The exact assignment depends on SA ordering -- guard via count.
        assert sql.count("$") == 3
        assert params == [10, 100, 200]

    def test_mssql_compiles_dialect_correct_paging(self):
        """MSSQL no longer goes through the broken ``f"... LIMIT {N} OFFSET {M}"``
        string concat. With limit/offset pushed into ``QueryConfig`` the
        SA dialect emits T-SQL-valid pagination -- either ``OFFSET ROWS
        FETCH NEXT ... ROWS ONLY`` (SQL Server 2012+) or a
        ``ROW_NUMBER() OVER (...)`` subquery (older compat). Both are
        valid; what we guard against is the previous bug of MySQL/PG
        ``LIMIT N OFFSET M`` syntax leaking into a SQL Server query.
        """
        builder = QueryBuilder("mssql")
        sql, _ = builder.build_select_query(
            QueryConfig(
                schema_name="dbo",
                table_name="events",
                columns=["id"],
                limit=50,
                offset=100,
                cursor_field="id",
                cursor_value=0,
            )
        )
        upper = sql.upper()
        # Either modern OFFSET/FETCH or legacy ROW_NUMBER pagination is fine.
        assert any(token in upper for token in ("FETCH NEXT", "FETCH FIRST", "ROW_NUMBER"))
        # The naive ``LIMIT N OFFSET M`` form is what we were producing
        # before; assert it does NOT appear (T-SQL doesn't accept it).
        assert "LIMIT 50" not in upper
        assert "LIMIT :" not in upper

    def test_mssql_paging_without_cursor_injects_order_by(self):
        """Full-refresh on MSSQL has no cursor / order_by, but T-SQL
        refuses OFFSET without ORDER BY. The builder must inject
        ``ORDER BY (SELECT NULL)`` (the documented no-op order) so the
        compile succeeds.
        """
        builder = QueryBuilder("mssql")
        sql, _ = builder.build_select_query(
            QueryConfig(
                schema_name="dbo",
                table_name="events",
                columns=["id"],
                limit=50,
                offset=100,
            )
        )
        upper = sql.upper()
        # Either OFFSET/FETCH or ROW_NUMBER, plus the synthetic ORDER BY.
        assert "ORDER BY" in upper
        assert "(SELECT NULL)" in upper or "SELECT NULL" in upper

    def test_postgres_paging_without_cursor_omits_order_by(self):
        """The synthetic ``ORDER BY (SELECT NULL)`` is MSSQL-only.
        Non-MSSQL dialects accept OFFSET without ORDER BY and shouldn't
        get the no-op order silently appended.
        """
        builder = QueryBuilder("postgresql")
        sql, _ = builder.build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="events",
                columns=["id"],
                limit=50,
                offset=100,
            )
        )
        assert "ORDER BY" not in sql.upper()

    def test_mysql_uses_limit_paging(self):
        """MySQL SA dialect emits ``LIMIT offset, count`` (equivalent to
        ``LIMIT count OFFSET offset`` — the comma form is what mysql/
        mariadb actually accept). What we guard against is the broken
        previous form where Postgres-style ``OFFSET`` made it through."""
        builder = QueryBuilder("mysql")
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="events",
                columns=["id"],
                limit=10,
                offset=20,
            )
        )
        assert "LIMIT" in sql.upper()
        # Both limit and offset values appear in the bound params.
        assert 10 in params and 20 in params

    def test_sqlite_uses_limit_offset(self):
        builder = QueryBuilder("sqlite")
        sql, _ = builder.build_select_query(
            QueryConfig(
                schema_name=None,
                table_name="events",
                columns=["id"],
                limit=10,
                offset=20,
            )
        )
        upper = sql.upper()
        assert "LIMIT" in upper
        assert "OFFSET" in upper


class TestMssqlParamstyle:
    """MSSQL via aioodbc / pyodbc consumes ``?`` (qmark) placeholders.
    ``exec_driver_sql`` bypasses SA's bind translation, so the
    builder must compile with qmark directly."""

    def test_mssql_emits_qmark_placeholders(self):
        builder = QueryBuilder("mssql")
        sql, _ = builder.build_select_query(
            QueryConfig(
                schema_name="dbo",
                table_name="events",
                columns=["id"],
                filters=[Filter(field="id", op="gt", value=10)],
                cursor_field="id",
                cursor_value=0,
                limit=100,
                offset=200,
            )
        )
        # No named (``:foo``) placeholders should leak through.
        assert ":param" not in sql
        assert "?" in sql

    def test_mssql_positional_repetition_preserved(self):
        """SA's ROW_NUMBER pagination shape on MSSQL binds the same
        offset value twice (``mssql_rn > ? AND mssql_rn <= ? + ?``).
        The previous ``list(compiled.params.values())`` would silently
        drop the repeat. Using ``positiontup`` keeps the right number
        of positional values for the driver to bind.
        """
        builder = QueryBuilder("mssql")
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name="dbo",
                table_name="events",
                columns=["id"],
                limit=100,
                offset=200,
            )
        )
        # The qmark count must match the positional values count
        # the driver will receive -- a mismatch raises at bind time.
        assert sql.count("?") == len(params)


class TestPositionalParamConversion:
    def test_filter_and_paging_params_preserve_order(self):
        builder = QueryBuilder("postgresql")
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="events",
                columns=["id", "name"],
                filters=[
                    Filter(field="id", op="gte", value=1),
                    Filter(field="name", op="eq", value="alpha"),
                ],
                cursor_field="updated_at",
                cursor_value="2024-01-01",
                cursor_mode="exclusive",
                limit=100,
                offset=50,
            )
        )
        # 4 placeholders: id, name, cursor, limit, offset = 5
        # (cursor counts as one bound param via the cursor condition).
        # Exact count: 2 filters + cursor + limit + offset = 5.
        assert sql.count("$") == 5
        assert len(params) == 5
        # Filter & cursor values come first, paging values last.
        assert params[:3] == [1, "alpha", "2024-01-01"]
        assert params[-2:] == [100, 50]

    def test_no_filters_only_paging(self):
        builder = QueryBuilder("postgresql")
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="events",
                columns=["id"],
                limit=10,
                offset=0,
            )
        )
        assert sql.count("$") == 2
        assert params == [10, 0]
