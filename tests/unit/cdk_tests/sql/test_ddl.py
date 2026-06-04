"""Standalone create_table: DDL rendering + execution.

These exercise the *generic* DDL assembly — column rendering through the
connector write-type-map, the NOT NULL-on-PK rule, the PRIMARY KEY clause, the
IF NOT EXISTS toggle, and the error paths — driven through the ANSI-neutral
``SqlDialect`` base. Quoting and PK-clause variants (backtick, NOT ENFORCED,
schema folding) are supplied by small fixture dialects standing in for the
connector packages, where the real per-system dialects live. The native type
text (TIMESTAMPTZ, NUMBER, ...) still comes from the *real* connector
write-type-maps, so the canonical->native round-trip is exercised end to end.
"""

from __future__ import annotations

import pytest

from cdk.contract import ColumnDef
from cdk.sql.ddl import build_create_table_sql, create_table
from cdk.sql.dialects import SqlDialect
from cdk.sql.exceptions import CreateTableError
from cdk.type_map import TypeMapper
from cdk.type_map.exceptions import InvalidTypeMapError, UnmappedTypeError
from cdk.type_map.rules import parse_rules

from .conftest import FakeAdbcRuntime


class _StubMapper:
    """Minimal canonical->native lookup for dialect-only assertions."""

    def __init__(self, mapping):
        self._mapping = mapping

    def to_native_type(self, canonical, *, params=None):
        return self._mapping[canonical]


class _BacktickNotEnforcedDialect(SqlDialect):
    """Backtick quoting + NOT ENFORCED PK clause (as a BigQuery dialect would)."""

    name = "backtick"
    quote_char = "`"
    pk_not_enforced = True


class TestBuildAnsi:
    def test_full_ddl_round_trip(self, pg_mapper):
        # Native type text comes from the real postgres write-type-map; quoting
        # and the PK clause come from the ANSI base dialect.
        columns = [
            ColumnDef("id", "Int64", nullable=False, primary_key=True),
            ColumnDef("name", "Utf8"),
            ColumnDef("amount", "Decimal128(38, 9)"),
            ColumnDef("created", "Timestamp(MICROSECOND, UTC)"),
        ]
        ddl = build_create_table_sql(
            SqlDialect(), pg_mapper, "public", "orders", columns, ["id"]
        )
        assert ddl == (
            'CREATE TABLE IF NOT EXISTS "public"."orders" (\n'
            '  "id" BIGINT NOT NULL,\n'
            '  "name" TEXT,\n'
            '  "amount" NUMERIC(38, 9),\n'
            '  "created" TIMESTAMPTZ,\n'
            '  PRIMARY KEY ("id")\n'
            ")"
        )

    def test_pk_column_is_not_null_even_if_declared_nullable(self, pg_mapper):
        columns = [ColumnDef("id", "Int64", nullable=True, primary_key=True)]
        ddl = build_create_table_sql(
            SqlDialect(), pg_mapper, "public", "t", columns, ["id"]
        )
        assert '"id" BIGINT NOT NULL' in ddl

    def test_no_primary_key_omits_clause(self, pg_mapper):
        columns = [ColumnDef("note", "Utf8")]
        ddl = build_create_table_sql(
            SqlDialect(), pg_mapper, "public", "t", columns, []
        )
        assert "PRIMARY KEY" not in ddl
        assert '"note" TEXT' in ddl

    def test_if_not_exists_toggle(self, pg_mapper):
        columns = [ColumnDef("id", "Int64")]
        ddl = build_create_table_sql(
            SqlDialect(), pg_mapper, "", "t", columns, [],
            if_not_exists=False,
        )
        assert ddl.startswith('CREATE TABLE "t" (')


class TestBuildBacktickNotEnforced:
    def test_quoting_and_not_enforced_pk(self):
        # A fixture dialect supplies backtick quoting + NOT ENFORCED, the way a
        # BigQuery connector package's dialect would.
        mapper = _StubMapper({"Int64": "INT64", "Utf8": "STRING"})
        columns = [
            ColumnDef("id", "Int64", primary_key=True),
            ColumnDef("region", "Utf8", primary_key=True),
            ColumnDef("name", "Utf8"),
        ]
        ddl = build_create_table_sql(
            _BacktickNotEnforcedDialect(), mapper, "ds", "orders", columns,
            ["id", "region"],
        )
        assert ddl == (
            "CREATE TABLE IF NOT EXISTS `ds`.`orders` (\n"
            "  `id` INT64 NOT NULL,\n"
            "  `region` STRING NOT NULL,\n"
            "  `name` STRING,\n"
            "  PRIMARY KEY (`id`, `region`) NOT ENFORCED\n"
            ")"
        )


class TestBuildErrors:
    def test_empty_columns_raises(self, pg_mapper):
        with pytest.raises(CreateTableError, match="at least one column"):
            build_create_table_sql(
                SqlDialect(), pg_mapper, "public", "t", [], []
            )

    def test_primary_key_not_in_columns_raises(self, pg_mapper):
        columns = [ColumnDef("id", "Int64")]
        with pytest.raises(CreateTableError, match="not in the column list"):
            build_create_table_sql(
                SqlDialect(), pg_mapper, "public", "t", columns, ["nope"]
            )

    def test_unmapped_canonical_raises_chaining_typemap_error(self, pg_mapper):
        # A canonical the write-map cannot render to forces the reverse miss.
        columns = [ColumnDef("weird", "LargeList")]
        with pytest.raises(CreateTableError) as exc:
            build_create_table_sql(
                SqlDialect(), pg_mapper, "public", "t", columns, []
            )
        assert "weird" in str(exc.value)
        assert isinstance(exc.value.__cause__, UnmappedTypeError)

    def test_no_write_map_raises_chaining_invalid_type_map(self):
        # A connector that ships only a read type-map (no type-map-write.json)
        # cannot render native DDL types, so to_native_type raises through
        # dialect.render_column_type and create_table fails loudly.
        read_only_mapper = TypeMapper(
            "read-only",
            parse_rules(
                [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
                source="<test>",
            ),
        )
        columns = [ColumnDef("id", "Int64")]
        with pytest.raises(CreateTableError, match="type-map-write rule") as exc:
            build_create_table_sql(
                SqlDialect(), read_only_mapper, "db", "t", columns, []
            )
        assert isinstance(exc.value.__cause__, InvalidTypeMapError)


class TestCreateTableExecution:
    @pytest.mark.asyncio
    async def test_builds_and_executes_ddl(self, pg_mapper):
        runtime = FakeAdbcRuntime("postgresql", mapper=pg_mapper)
        columns = [
            ColumnDef("id", "Int64", nullable=False, primary_key=True),
            ColumnDef("name", "Utf8"),
        ]
        await create_table(
            runtime, "public", "orders", columns, ["id"], dialect=SqlDialect()
        )

        expected = build_create_table_sql(
            SqlDialect(), pg_mapper, "public", "orders", columns, ["id"]
        )
        conn = runtime.connections[-1]
        assert [s for s, _ in conn.executed] == [expected]
        assert conn.commits == 1
        assert conn.closed is True

    @pytest.mark.asyncio
    async def test_explicit_type_mapper_overrides_runtime_default(self):
        # Runtime has no mapper; passing one explicitly must be used instead.
        runtime = FakeAdbcRuntime("bigquery", mapper=None)
        mapper = _StubMapper({"Int64": "INT64"})
        await create_table(
            runtime, "ds", "t", [ColumnDef("id", "Int64")], [],
            dialect=_BacktickNotEnforcedDialect(), type_mapper=mapper,
        )
        executed = runtime.connections[-1].executed[-1][0]
        assert "`ds`.`t`" in executed
        assert "`id` INT64" in executed
