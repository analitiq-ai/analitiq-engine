"""Declared system limits (issue #401): grammar, chunk math, identifier cap.

``sql_capabilities.limits`` is the additive member of the capability block:
undeclared caps are ``None`` and never refuse anything; declared caps drive
the executemany chunk size (``StageWritePlan.rows_per_statement``), the
stage-name identifier budget, and the DDL identifier check.
"""

from __future__ import annotations

import pytest

from cdk.contract import ColumnDef
from cdk.sql.backend import iter_landing_chunks
from cdk.sql.capabilities import SqlCapabilities, SqlCapabilitiesError, SqlLimits
from cdk.sql.ddl import build_create_table_sql
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.exceptions import CreateTableError, SchemaConfigurationError
from cdk.sql.write_plan import (
    build_stage_write_plan,
    identifier_budget,
    rows_per_statement,
)

from .conftest import caps_block


class _StagingDialect(SqlDialect):
    name = "staging"

    def stage_table_sql(self, stage, target, *, temp):
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return f"{keyword} {self.quote_table(stage)} LIKE {self.quote_table(target)}"

    def merge_statement_sql(self, stage, target, conflict_keys, columns):
        return f"MERGE INTO {self.quote_table(target)} USING {self.quote_table(stage)}"


def _caps(limits: dict | None = None, **kwargs) -> SqlCapabilities:
    block = caps_block(**kwargs)
    if limits is not None:
        block["limits"] = limits
    return SqlCapabilities.from_declaration(block)


def _plan(caps: SqlCapabilities, columns=("id", "v")):
    return build_stage_write_plan(
        _StagingDialect(),
        caps,
        target=TableAddress(table="events", schema="public"),
        columns=columns,
        write_mode="insert",
        conflict_keys=[],
        identity=["id"],
        truncate_now=False,
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
    )


class TestLimitsGrammar:
    def test_undeclared_limits_are_none(self):
        caps = _caps()
        assert caps.limits == SqlLimits(max_bind_params=None, max_identifier_len=None)

    def test_declared_limits_parse(self):
        caps = _caps(limits={"max_bind_params": 2100, "max_identifier_len": 63})
        assert caps.limits.max_bind_params == 2100
        assert caps.limits.max_identifier_len == 63

    def test_partial_limits_are_legal(self):
        caps = _caps(limits={"max_bind_params": 999})
        assert caps.limits.max_bind_params == 999
        assert caps.limits.max_identifier_len is None

    def test_unknown_limits_field_fails(self):
        with pytest.raises(SqlCapabilitiesError, match="unknown fields"):
            _caps(limits={"max_bind_parms": 2100})

    @pytest.mark.parametrize("value", [0, -5, "2100", 2.5, True])
    def test_non_positive_or_non_int_limit_fails(self, value):
        with pytest.raises(SqlCapabilitiesError, match="positive integer"):
            _caps(limits={"max_bind_params": value})

    def test_non_object_limits_fails(self):
        with pytest.raises(SqlCapabilitiesError, match="must be an object"):
            _caps(limits=2100)


class TestRowsPerStatement:
    def test_undeclared_cap_means_no_chunking(self):
        assert _plan(_caps()).rows_per_statement is None

    def test_floor_division_by_column_count(self):
        caps = _caps(limits={"max_bind_params": 2100})
        plan = _plan(caps, columns=("a", "b", "c"))
        assert plan.rows_per_statement == 700

    def test_remainder_is_floored(self):
        caps = _caps(limits={"max_bind_params": 7})
        plan = _plan(caps, columns=("a", "b", "c"))
        assert plan.rows_per_statement == 2

    def test_cap_below_one_row_refuses(self):
        caps = _caps(limits={"max_bind_params": 2})
        with pytest.raises(SchemaConfigurationError, match="cannot hold one row"):
            _plan(caps, columns=("a", "b", "c"))

    def test_helper_matches_plan_field(self):
        caps = _caps(limits={"max_bind_params": 10})
        target = TableAddress(table="events")
        assert rows_per_statement(caps, ["a", "b"], target=target) == 5


class TestIterLandingChunks:
    def test_no_cap_yields_whole_batch(self):
        rows = [1, 2, 3]
        assert list(iter_landing_chunks(rows, None)) == [rows]

    def test_chunks_never_exceed_the_cap(self):
        rows = list(range(10))
        chunks = list(iter_landing_chunks(rows, 4))
        assert chunks == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]
        assert sum(len(c) for c in chunks) == len(rows)

    def test_exact_multiple_has_no_empty_tail(self):
        chunks = list(iter_landing_chunks(list(range(8)), 4))
        assert [len(c) for c in chunks] == [4, 4]


class TestIdentifierBudget:
    def test_declared_cap_overrides_dialect_default(self):
        dialect = _StagingDialect()
        caps = _caps(limits={"max_identifier_len": 30})
        assert identifier_budget(caps, dialect) == 30

    def test_undeclared_cap_keeps_dialect_default(self):
        dialect = _StagingDialect()
        assert identifier_budget(_caps(), dialect) == dialect.max_identifier_length
        assert identifier_budget(None, dialect) == dialect.max_identifier_length

    def test_stage_name_respects_declared_budget(self):
        caps = _caps(limits={"max_identifier_len": 40})
        plan = _plan(caps)
        assert len(plan.stage.table.encode()) <= 40

    def test_declared_budget_too_tight_for_the_token_refuses(self):
        # The hash token is never truncated; a declared cap that cannot
        # hold it is an authoring error, same rule as the dialect default.
        caps = _caps(limits={"max_identifier_len": 10})
        with pytest.raises(SchemaConfigurationError, match="identifier budget"):
            _plan(caps)


class TestDdlIdentifierCap:
    def _columns(self, names):
        return [
            ColumnDef(name=name, canonical_type="Utf8", nullable=True) for name in names
        ]

    def _mapper(self):
        class _Mapper:
            def to_native_type(self, canonical, params=None):
                return "TEXT"

        return _Mapper()

    def test_over_cap_column_refuses(self):
        dialect = _StagingDialect()
        dialect.capabilities = _caps(limits={"max_identifier_len": 8})
        with pytest.raises(CreateTableError, match="max_identifier_len"):
            build_create_table_sql(
                dialect,
                self._mapper(),
                TableAddress(table="events"),
                self._columns(["way_too_long_column"]),
                [],
            )

    def test_over_cap_table_refuses(self):
        dialect = _StagingDialect()
        dialect.capabilities = _caps(limits={"max_identifier_len": 4})
        with pytest.raises(CreateTableError, match="table identifier"):
            build_create_table_sql(
                dialect,
                self._mapper(),
                TableAddress(table="events"),
                self._columns(["id"]),
                [],
            )

    def test_undeclared_cap_keeps_current_behavior(self):
        dialect = _StagingDialect()
        dialect.capabilities = _caps()
        ddl = build_create_table_sql(
            dialect,
            self._mapper(),
            TableAddress(table="events"),
            self._columns(["a_perfectly_reasonable_column_name_that_is_long"]),
            [],
        )
        assert "CREATE TABLE" in ddl

    def test_budget_counts_bytes_not_characters(self):
        dialect = _StagingDialect()
        dialect.capabilities = _caps(limits={"max_identifier_len": 8})
        # Six characters, twelve UTF-8 bytes.
        with pytest.raises(CreateTableError, match="bytes"):
            build_create_table_sql(
                dialect,
                self._mapper(),
                TableAddress(table="events"),
                self._columns(["éééééé"]),
                [],
            )
