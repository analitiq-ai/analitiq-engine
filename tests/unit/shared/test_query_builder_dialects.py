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

from cdk.query_builder import (
    Filter,
    QueryBuilder,
    QueryConfig,
    _get_sqlalchemy_dialect,
    _positional_params,
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
        with patch("cdk.query_builder.importlib.import_module") as imp:
            imp.side_effect = ImportError("not installed")
            with pytest.raises(ImportError, match="snowflake") as exc_info:
                _get_sqlalchemy_dialect("snowflake")
        # The message names the missing package but does not suggest a
        # poetry extra — extras naming lives in pyproject and would rot
        # here silently (issue #90).
        assert "poetry install -E" not in str(exc_info.value)


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

    def test_mssql_paging_without_ordering_and_without_fallback_fails_loudly(self):
        """The builder no longer invents an MSSQL ordering. Without a
        ``paging_order_fallback`` hook, T-SQL's OFFSET-requires-ORDER-BY
        rule surfaces as SQLAlchemy's ``CompileError`` instead of a
        silently injected ``(SELECT NULL)``; the mssql connector package
        supplies the fallback via its ``SqlDialect`` subclass.
        """
        from sqlalchemy.exc import CompileError

        builder = QueryBuilder("mssql")
        with pytest.raises(CompileError, match="order_by"):
            builder.build_select_query(
                QueryConfig(
                    schema_name="dbo",
                    table_name="events",
                    columns=["id"],
                    limit=50,
                    offset=100,
                )
            )

    def test_paging_order_fallback_hook_injects_expression(self):
        """A connector-supplied fallback hook provides the ordering for
        paged queries that declare none (the mssql connector returns
        ``(SELECT NULL)``, Microsoft's documented no-op order)."""
        builder = QueryBuilder(
            "mssql", paging_order_fallback=lambda: "(SELECT NULL)"
        )
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
        assert "ORDER BY" in upper
        assert "(SELECT NULL)" in upper

    def test_paging_order_fallback_not_consulted_when_ordering_declared(self):
        """The hook fires only when a paged query lacks an ordering, so a
        connector hook that raises to demand an explicit ordering never
        affects cursor-ordered or order_by reads."""

        def raising_fallback():
            raise AssertionError("fallback consulted despite declared ordering")

        builder = QueryBuilder("mssql", paging_order_fallback=raising_fallback)
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
        assert "ORDER BY" in sql.upper()

    def test_paging_order_fallback_returning_none_injects_nothing(self):
        """A hook returning ``None`` (the ANSI ``SqlDialect`` base) leaves
        the query unordered — matching the no-hook behavior."""
        builder = QueryBuilder("postgresql", paging_order_fallback=lambda: None)
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

    def test_postgres_paging_without_cursor_omits_order_by(self):
        """Dialects that accept OFFSET without ORDER BY get no ordering
        silently appended when no fallback hook is configured."""
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

        The fallback hook stands in for the mssql connector dialect so
        the unordered paged query compiles at all.
        """
        builder = QueryBuilder(
            "mssql", paging_order_fallback=lambda: "(SELECT NULL)"
        )
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


class TestNamedParamstyleReturnsDict:
    """Dialects whose SA compiler uses named/pyformat paramstyle
    (Snowflake, BigQuery, etc.) have ``compiled.positiontup is None``.
    Iterating None would TypeError, so the builder must return a
    dict the caller can pass straight to the driver."""

    def test_named_paramstyle_returns_dict(self, monkeypatch):
        from sqlalchemy.dialects import sqlite

        builder = QueryBuilder("sqlite")
        # Force named paramstyle to simulate Snowflake / BigQuery
        # behaviour (which we don't have installed locally).
        builder._sa_dialect = sqlite.dialect(paramstyle="named")
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name=None,
                table_name="events",
                columns=["id"],
                filters=[Filter(field="id", op="gt", value=10)],
            )
        )
        assert isinstance(params, dict)
        # The bind name SA picked for the filter shows up in the SQL
        # AND in the params dict; their pairing is the contract.
        assert any(v == 10 for v in params.values())
        assert ":" in sql  # named placeholder

    def test_positional_paramstyle_returns_list(self):
        builder = QueryBuilder("postgresql")
        _, params = builder.build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="events",
                columns=["id"],
                filters=[Filter(field="id", op="gt", value=10)],
            )
        )
        assert isinstance(params, list)


class TestPositionalParamMapping:
    """``_positional_params`` underlies the BigQuery type-suffix fix.

    The real BigQuery dialect tags each ``positiontup`` entry with its bind
    type (``status_1:STRING``) while ``compiled.params`` is keyed by the bare
    name. The real-dialect tests that exercise this are ``importorskip``-guarded
    (BigQuery package absent in CI), so these unit tests pin the mapping logic
    directly with synthetic input — no optional package required.
    """

    def test_bigquery_type_suffix_falls_back_to_bare_name(self):
        # positiontup entries carry ``:TYPE``; params keyed by bare name.
        positiontup = ["id_1:INT64", "name_2:STRING"]
        bind_params = {"id_1": 7, "name_2": "alpha"}
        assert _positional_params(positiontup, bind_params) == [7, "alpha"]

    def test_untagged_names_used_directly(self):
        # Non-BigQuery positional dialects: names match params verbatim.
        positiontup = ["id_1", "cursor_2", "param_3"]
        bind_params = {"id_1": 1, "cursor_2": "2024-01-01", "param_3": 100}
        assert _positional_params(positiontup, bind_params) == [
            1,
            "2024-01-01",
            100,
        ]

    def test_repeated_name_preserves_order_and_count(self):
        # MSSQL ROW_NUMBER pagination reuses a bind name; iterating
        # positiontup (not the dict) keeps the repeat.
        positiontup = ["param_1", "param_2", "param_1"]
        bind_params = {"param_1": 10, "param_2": 20}
        assert _positional_params(positiontup, bind_params) == [10, 20, 10]

    def test_missing_bind_name_raises_keyerror(self):
        # A name absent under both the tagged and bare form is a real error,
        # not silently dropped.
        with pytest.raises(KeyError):
            _positional_params(["ghost_1:STRING"], {"id_1": 1})


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


class TestAdbcQmarkMode:
    """The ADBC-only source compiles through QueryBuilder with
    ``paramstyle="qmark"`` + ``quote_identifiers`` + ``inline_paging`` so
    one builder serves both transports. Postgres is the only ADBC dialect
    installed in the base env, so these tests freeze the knobs against it;
    the Snowflake/BigQuery dialects are pinned separately in
    :class:`TestAdbcQmarkModeRealDialects` (skipped where their optional
    packages are absent).
    """

    def _adbc_builder(self):
        return QueryBuilder(
            "postgresql",
            paramstyle="qmark",
            quote_identifiers=True,
            inline_paging=True,
        )

    def test_qmark_placeholders_no_dollar_or_named(self):
        sql, params = self._adbc_builder().build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="orders",
                columns=["id"],
                filters=[Filter(field="id", op="gt", value=10)],
            )
        )
        assert "?" in sql
        assert "$" not in sql  # not asyncpg numeric_dollar
        assert ":param" not in sql  # not named
        assert isinstance(params, list)
        assert params == [10]

    def test_postgres_qmark_has_no_asyncpg_bind_casts(self):
        # The asyncpg dialect renders ``?::INTEGER`` casts; the ADBC libpq
        # driver wants bare ``?``. Forcing qmark must resolve the plain PG
        # dialect, so no inline ``::TYPE`` casts leak into the SQL.
        sql, _ = self._adbc_builder().build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="orders",
                columns=["id"],
                filters=[Filter(field="id", op="gte", value=5)],
            )
        )
        assert "::" not in sql

    def test_identifiers_quoted(self):
        sql, _ = self._adbc_builder().build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="orders",
                columns=["id", "status"],
            )
        )
        assert '"public"."orders"' in sql
        assert '"id"' in sql and '"status"' in sql

    def test_limit_offset_inlined_not_bound(self):
        sql, params = self._adbc_builder().build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="orders",
                columns=["id"],
                filters=[Filter(field="id", op="gt", value=1)],
                limit=100,
                offset=200,
            )
        )
        assert "LIMIT 100" in sql
        assert "OFFSET 200" in sql
        # Only the filter value is bound; paging is literal.
        assert params == [1]

    def test_filters_and_cursor_compose_one_where(self):
        sql, params = self._adbc_builder().build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="orders",
                columns=["id"],
                filters=[
                    Filter(field="status", op="eq", value="active"),
                    Filter(field="amount", op="gte", value=10),
                ],
                cursor_field="updated_at",
                cursor_value="2024-01-01",
                cursor_mode="inclusive",
                order_by="updated_at",
                limit=50,
                offset=0,
            )
        )
        assert sql.count("WHERE") == 1
        assert '"status" = ?' in sql
        assert '"amount" >= ?' in sql
        assert '"updated_at" >= ?' in sql
        assert 'ORDER BY "updated_at"' in sql
        # Filters first (declaration order), cursor last.
        assert params == ["active", 10, "2024-01-01"]

    def test_in_filter_expands_to_qmark_list(self):
        # render_postcompile turns the expanding IN bind into individual
        # ``?`` placeholders so the raw cursor.execute gets a flat param
        # list rather than an unexpanded POSTCOMPILE marker.
        sql, params = self._adbc_builder().build_select_query(
            QueryConfig(
                schema_name=None,
                table_name="orders",
                columns=["id"],
                filters=[Filter(field="id", op="in", value=[1, 2, 3])],
            )
        )
        assert "POSTCOMPILE" not in sql
        assert "IN (?, ?, ?)" in sql
        assert params == [1, 2, 3]


class TestAdbcQmarkModeRealDialects:
    """Compile-only checks against the actual Snowflake / BigQuery
    dialects, skipped where the optional packages are not installed.

    They freeze the two contract points the Postgres-only tests cannot:
    (1) forcing ``paramstyle="qmark"`` yields *positional* params -- a
    dict would make the ADBC reader bind parameter names, which the source
    rejects with a ReadError; and (2) each dialect's quote character
    matches what the destination handler emits (backticks for BigQuery,
    double quotes for Snowflake) so reads and writes resolve the same
    physical object.
    """

    def test_bigquery_qmark_positional_and_backtick_quoted(self):
        pytest.importorskip("sqlalchemy_bigquery")
        builder = QueryBuilder(
            "bigquery",
            paramstyle="qmark",
            quote_identifiers=True,
            inline_paging=True,
        )
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name="analytics",
                table_name="orders",
                columns=["id", "status"],
                filters=[Filter(field="status", op="eq", value="active")],
                cursor_field="updated_at",
                cursor_value="2024-01-01",
                cursor_mode="inclusive",
                order_by="updated_at",
                limit=50,
                offset=0,
            )
        )
        # qmark -> positional list (filter value, then cursor value); a
        # dict here would mean the dialect ignored the forced paramstyle.
        assert isinstance(params, list)
        assert params == ["active", "2024-01-01"]
        assert "?" in sql and "$" not in sql
        # BigQuery quotes with backticks (matches the destination handler);
        # paging is inlined, proven by params carrying no 50/0.
        assert "`id`" in sql and "`status`" in sql
        assert '"id"' not in sql
        assert "LIMIT" in sql.upper()

    def test_snowflake_qmark_positional_and_double_quoted(self):
        pytest.importorskip("snowflake.sqlalchemy")
        builder = QueryBuilder(
            "snowflake",
            paramstyle="qmark",
            quote_identifiers=True,
            inline_paging=True,
        )
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name="PUBLIC",
                table_name="orders",
                columns=["id", "status"],
                filters=[Filter(field="status", op="eq", value="active")],
                limit=25,
                offset=0,
            )
        )
        assert isinstance(params, list)
        assert params == ["active"]
        assert "?" in sql and "$" not in sql
        # Snowflake quotes with double quotes (matches the destination);
        # never backticks.
        assert '"orders"' in sql and '"id"' in sql
        assert "`" not in sql
        assert "LIMIT" in sql.upper()


class TestFilterFailLoud:
    """A declared filter that compiles away silently widens the result
    set, so an unmapped operator must raise instead of being skipped."""

    def test_unknown_filter_operator_raises(self):
        builder = QueryBuilder("postgresql")
        with pytest.raises(ValueError, match="Unknown filter operator 'regex'"):
            builder.build_select_query(
                QueryConfig(
                    table_name="events",
                    filters=[Filter(field="name", op="regex", value="^a")],
                )
            )

    def test_known_operators_still_compile(self):
        builder = QueryBuilder("postgresql")
        sql, params = builder.build_select_query(
            QueryConfig(
                table_name="events",
                filters=[
                    Filter(field="id", op="gte", value=1),
                    Filter(field="deleted_at", op="is_null"),
                ],
            )
        )
        assert "WHERE" in sql.upper()
        assert params == [1]


class TestRedshiftDriverFlavour:
    """Redshift must resolve the ``redshift_connector`` flavour of the
    sqlalchemy-redshift dialect. The registry default is psycopg2-shaped
    and compiles named ``%(name)s`` params, which redshift_connector
    rejects at execute time with "Only %s and %% are supported in the
    query" — positional params are the regression signal here."""

    def test_redshift_compiles_positional_params(self):
        pytest.importorskip("sqlalchemy_redshift")
        builder = QueryBuilder("redshift")
        sql, params = builder.build_select_query(
            QueryConfig(
                schema_name="public",
                table_name="orders",
                columns=["id", "status"],
                filters=[Filter(field="status", op="eq", value="active")],
                limit=25,
                offset=0,
            )
        )
        # A dict here means the default (psycopg2-flavoured) dialect was
        # loaded; redshift_connector binds positionally. (The redshift
        # compiler renders LIMIT/OFFSET as literals, so only the filter
        # value is bound.)
        assert isinstance(params, list)
        assert params == ["active"]
        assert "%(" not in sql
        assert "%s" in sql
        assert "LIMIT" in sql.upper()
