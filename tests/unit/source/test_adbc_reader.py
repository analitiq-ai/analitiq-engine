"""ADBC-only source reader.

The ADBC reader path doesn't go through QueryBuilder (which depends on
sync SA dialect compilation). ``_build_select_sql`` is the minimal
ANSI rendering helper that the reader uses for every page. Tests
freeze:

* identifier quoting (Snowflake uppercases unquoted names — the
  reader's contract is that everything stays in the engine's
  declared case).
* cursor mode semantics (inclusive vs exclusive picks ``>=`` vs ``>``).
* paging clauses (LIMIT/OFFSET as positional ints, not parameters).
* the explicit refusal to build for an empty column list.

``AdbcReadPlan.__post_init__`` also validates cursor_mode so a
typo can't silently demote ``inclusive`` to the default.
"""

from __future__ import annotations

import pytest

from src.source.drivers.adbc_reader import AdbcReadPlan, _build_select_sql


class TestBuildSelectSql:
    def test_basic_select(self):
        plan = AdbcReadPlan(
            table_name="orders",
            columns=["id", "status"],
            schema_name="public",
        )
        sql, params = _build_select_sql(plan)
        assert sql == 'SELECT "id", "status" FROM "public"."orders"'
        assert params == ()

    def test_unqualified_when_no_schema(self):
        plan = AdbcReadPlan(table_name="orders", columns=["id"])
        sql, params = _build_select_sql(plan)
        assert sql == 'SELECT "id" FROM "orders"'
        assert params == ()

    def test_quote_escaping(self):
        plan = AdbcReadPlan(
            table_name='ord"ers', columns=['id"col'], schema_name='pu"blic',
        )
        sql, _ = _build_select_sql(plan)
        assert '"pu""blic"."ord""ers"' in sql
        assert '"id""col"' in sql

    def test_cursor_inclusive(self):
        plan = AdbcReadPlan(
            table_name="orders",
            columns=["id"],
            cursor_field="id",
            cursor_value=100,
            cursor_mode="inclusive",
        )
        sql, params = _build_select_sql(plan)
        assert 'WHERE "id" >= ?' in sql
        assert 'ORDER BY "id" ASC' in sql
        assert params == (100,)

    def test_cursor_exclusive(self):
        plan = AdbcReadPlan(
            table_name="orders",
            columns=["id"],
            cursor_field="id",
            cursor_value=100,
            cursor_mode="exclusive",
        )
        sql, params = _build_select_sql(plan)
        assert 'WHERE "id" > ?' in sql
        assert params == (100,)

    def test_cursor_field_without_value_no_where(self):
        plan = AdbcReadPlan(
            table_name="orders",
            columns=["id"],
            cursor_field="id",
            cursor_value=None,
        )
        sql, params = _build_select_sql(plan)
        assert "WHERE" not in sql
        # ORDER BY still applies so paging stays deterministic
        assert 'ORDER BY "id" ASC' in sql
        assert params == ()

    def test_limit_offset_inlined(self):
        plan = AdbcReadPlan(
            table_name="orders",
            columns=["id"],
            limit=100,
            offset=200,
        )
        sql, _ = _build_select_sql(plan)
        assert "LIMIT 100" in sql
        assert "OFFSET 200" in sql

    def test_empty_columns_rejected(self):
        with pytest.raises(ValueError, match="columns must not be empty"):
            _build_select_sql(
                AdbcReadPlan(table_name="orders", columns=[]),
            )


class TestAdbcReadPlanValidation:
    def test_invalid_cursor_mode_rejected(self):
        with pytest.raises(ValueError, match="cursor_mode"):
            AdbcReadPlan(
                table_name="orders",
                columns=["id"],
                cursor_mode="inclusiv",  # typo
            )

    def test_default_cursor_mode_inclusive(self):
        plan = AdbcReadPlan(table_name="orders", columns=["id"])
        assert plan.cursor_mode == "inclusive"
