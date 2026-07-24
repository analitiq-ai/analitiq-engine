"""Rendering matches declaration; refusals fire (ADR sql-write-path-v2 s.10).

Every assertion drives the connector's real dialect and declared
capabilities through the same plan builder the write path uses
(:func:`~cdk.sql.write_plan.build_stage_write_plan`), so what the suite
certifies is exactly what a customer batch executes.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

import pytest

from cdk.conformance.skips import (
    require_dialect,
    require_stage_rendering,
    require_write_role,
)
from cdk.conformance.target import ConformanceTarget
from cdk.sql.backend import StageWritePlan
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.exceptions import CatalogAddressingError, SchemaConfigurationError
from cdk.sql.generic import GenericSQLConnector
from cdk.sql.write_plan import build_stage_write_plan

_TEMP_TOKEN = re.compile(r"\bTEMP(ORARY)?\b")
_TRUNCATE_TOKEN = re.compile(r"\bTRUNCATE\b")
_MERGE_TOKEN = re.compile(r"\bMERGE\b")
_ON_CONFLICT_TOKEN = re.compile(r"\bON\s+CONFLICT\b")
_ON_DUPLICATE_KEY_TOKEN = re.compile(r"\bON\s+DUPLICATE\s+KEY\s+UPDATE\b")
_WHEN_MATCHED_TOKEN = re.compile(r"\bWHEN\s+MATCHED\b")

#: Which statement token certifies each declared merge form.
_MERGE_FORM_TOKENS = {
    "merge": _MERGE_TOKEN,
    "insert_on_conflict": _ON_CONFLICT_TOKEN,
    "insert_on_duplicate_key": _ON_DUPLICATE_KEY_TOKEN,
}


def _target_address(dialect: SqlDialect) -> TableAddress:
    return dialect.table_address("conformance_target", schema="conformance")


def _plan(
    dialect: SqlDialect,
    caps: SqlCapabilities,
    *,
    mode: str,
    columns: Sequence[str] = ("id", "val", "seq"),
    conflict_keys: Sequence[str] = (),
    identity: Sequence[str] = ("id",),
    batch_seq: int = 1,
    truncate_now: bool = False,
) -> StageWritePlan:
    """One batch write's plan, built exactly as the facade builds it."""
    return build_stage_write_plan(
        dialect,
        caps,
        target=_target_address(dialect),
        columns=tuple(columns),
        write_mode=mode,
        conflict_keys=list(conflict_keys),
        identity=list(identity),
        truncate_now=truncate_now,
        run_id="conformance-run",
        stream_id="conformance-stream",
        batch_seq=batch_seq,
    )


def _require_merge_rendering(
    target: ConformanceTarget,
) -> tuple[SqlDialect, SqlCapabilities]:
    """Gate for upsert-rendering tests: a declared, implemented merge form."""
    dialect, caps = require_stage_rendering(target)
    if caps.merge_form == "none":
        pytest.skip("connector declares merge_form 'none'; no upsert to render")
    if (
        type(dialect).merge_statement_sql is SqlDialect.merge_statement_sql
    ):  # pragma: no cover - reported by the declaration check
        pytest.skip(
            "merge_statement_sql not implemented; reported by the "
            "declaration-consistency check"
        )
    return dialect, caps


def test_stage_ddl_matches_declared_scope(
    conformance_target: ConformanceTarget,
) -> None:
    """Temporary-table DDL iff the declared stage scope is ``temp``."""
    dialect, caps = require_stage_rendering(conformance_target)
    plan = _plan(dialect, caps, mode="insert")
    rendered = plan.create_stage_sql.upper()
    if caps.stage.scope == "temp":
        assert _TEMP_TOKEN.search(rendered), (
            f"sql_capabilities.stage.scope is 'temp' but the stage DDL "
            f"renders no TEMPORARY form: {plan.create_stage_sql!r}"
        )
    else:
        assert not _TEMP_TOKEN.search(rendered), (
            f"sql_capabilities.stage.scope is 'real' but the stage DDL "
            f"renders a TEMPORARY form: {plan.create_stage_sql!r}"
        )


def test_stage_ddl_names_the_stage(conformance_target: ConformanceTarget) -> None:
    """The DDL creates exactly the stage the rest of the cycle targets."""
    dialect, caps = require_stage_rendering(conformance_target)
    plan = _plan(dialect, caps, mode="insert")
    assert dialect.quote_table(plan.stage) in plan.create_stage_sql, (
        f"stage DDL does not reference the quoted stage address "
        f"{dialect.quote_table(plan.stage)}: {plan.create_stage_sql!r}"
    )


def test_stage_name_fits_the_identifier_budget(
    conformance_target: ConformanceTarget,
) -> None:
    """No target name can push the stage name past the dialect budget."""
    dialect, caps = require_stage_rendering(conformance_target)
    long_target = dialect.table_address("conformance_" + "x" * 80, schema="conformance")
    plan = build_stage_write_plan(
        dialect,
        caps,
        target=long_target,
        columns=("id",),
        write_mode="insert",
        conflict_keys=[],
        identity=["id"],
        truncate_now=False,
        run_id="conformance-run",
        stream_id="conformance-stream",
        batch_seq=1,
    )
    budget = dialect.max_identifier_length
    assert len(plan.stage.table.encode()) <= budget, (
        f"stage name {plan.stage.table!r} exceeds the dialect's "
        f"max_identifier_length {budget}"
    )


def test_merge_statement_matches_declared_form(
    conformance_target: ConformanceTarget,
) -> None:
    """Declared-but-wrong fails: the statement carries the declared form."""
    dialect, caps = _require_merge_rendering(conformance_target)
    plan = _plan(dialect, caps, mode="upsert", conflict_keys=("id",))
    rendered = plan.mode_sql.upper()
    expected = _MERGE_FORM_TOKENS[caps.merge_form]
    assert expected.search(rendered), (
        f"connector declares merge_form {caps.merge_form!r} but the "
        f"rendered upsert statement carries no such form: "
        f"{plan.mode_sql!r}"
    )
    for form, token in _MERGE_FORM_TOKENS.items():
        if form != caps.merge_form:
            assert not token.search(rendered), (
                f"connector declares merge_form {caps.merge_form!r} but "
                f"the rendered upsert statement carries the {form!r} "
                f"form: {plan.mode_sql!r}"
            )


def test_merge_statement_references_stage_target_and_keys(
    conformance_target: ConformanceTarget,
) -> None:
    """The statement moves the stage into the target on the stream keys."""
    dialect, caps = _require_merge_rendering(conformance_target)
    plan = _plan(dialect, caps, mode="upsert", conflict_keys=("id",))
    for label, reference in (
        ("stage", dialect.quote_table(plan.stage)),
        ("target", dialect.quote_table(plan.target)),
        ("conflict key", dialect.quote_ident("id")),
    ):
        assert reference in plan.mode_sql, (
            f"the rendered upsert statement does not reference the "
            f"{label} ({reference}): {plan.mode_sql!r}"
        )


def test_merge_with_only_key_columns_degrades_to_insert_only(
    conformance_target: ConformanceTarget,
) -> None:
    """All-keys upsert renders the insert-only degradation, not an error."""
    dialect, caps = _require_merge_rendering(conformance_target)
    plan = _plan(
        dialect,
        caps,
        mode="upsert",
        columns=("id",),
        conflict_keys=("id",),
    )
    rendered = plan.mode_sql.upper()
    if caps.merge_form == "insert_on_conflict":
        assert "DO NOTHING" in rendered, (
            f"all landed columns are conflict keys; the ON CONFLICT form "
            f"must degrade to DO NOTHING: {plan.mode_sql!r}"
        )
    if caps.merge_form == "merge":
        assert not _WHEN_MATCHED_TOKEN.search(rendered), (
            f"all landed columns are conflict keys; the MERGE form must "
            f"render no WHEN MATCHED clause: {plan.mode_sql!r}"
        )


def test_insert_anti_join_matches_on_the_declared_identity(
    conformance_target: ConformanceTarget,
) -> None:
    """Keyed insert anti-joins on the primary key, quoted by the dialect."""
    dialect, caps = require_stage_rendering(conformance_target)
    plan = _plan(dialect, caps, mode="insert", identity=("id",))
    assert "NOT EXISTS" in plan.mode_sql.upper()
    assert dialect.quote_ident("id") in plan.mode_sql


def test_keyless_insert_anti_joins_on_the_record_hash(
    conformance_target: ConformanceTarget,
) -> None:
    """A keyless stream's identity is the engine-managed record hash."""
    dialect, caps = require_stage_rendering(conformance_target)
    hash_column = GenericSQLConnector.RECORD_HASH_COLUMN
    plan = _plan(
        dialect,
        caps,
        mode="insert",
        columns=("val", hash_column),
        identity=(hash_column,),
    )
    assert dialect.quote_ident(hash_column) in plan.mode_sql


def test_empty_table_statement_is_delete_shaped(
    conformance_target: ConformanceTarget,
) -> None:
    """The full-refresh emptying statement is never ``TRUNCATE``.

    ``TRUNCATE`` implicitly commits on several systems and would
    break the declared single-transaction stage cycle.
    """
    dialect = require_dialect(conformance_target)
    require_write_role(conformance_target)
    statement = dialect.empty_table_sql(_target_address(dialect))
    assert not _TRUNCATE_TOKEN.search(statement.upper()), (
        f"empty_table_sql renders TRUNCATE, which implicitly commits on "
        f"several systems: {statement!r}"
    )
    assert dialect.quote_table(_target_address(dialect)) in statement


def test_identical_batches_build_identical_plans(
    conformance_target: ConformanceTarget,
) -> None:
    """Same (run, stream, seq) -> byte-identical plan, so retries self-heal."""
    dialect, caps = require_stage_rendering(conformance_target)
    first = _plan(dialect, caps, mode="insert", batch_seq=7)
    second = _plan(dialect, caps, mode="insert", batch_seq=7)
    assert first == second


def test_distinct_batches_get_distinct_stages(
    conformance_target: ConformanceTarget,
) -> None:
    """Different batches must never share a stage table name."""
    dialect, caps = require_stage_rendering(conformance_target)
    first = _plan(dialect, caps, mode="insert", batch_seq=1)
    second = _plan(dialect, caps, mode="insert", batch_seq=2)
    assert first.stage.table != second.stage.table


def test_upsert_without_conflict_keys_refuses(
    conformance_target: ConformanceTarget,
) -> None:
    """No silent downgrade to INSERT for a keyless upsert stream."""
    dialect, caps = require_stage_rendering(conformance_target)
    with pytest.raises(SchemaConfigurationError):
        _plan(dialect, caps, mode="upsert", conflict_keys=())


def test_catalog_refused_when_undeclared(conformance_target: ConformanceTarget) -> None:
    """A dialect with no declaration refuses catalog addressing."""
    dialect = require_dialect(conformance_target)
    unbound = type(dialect)()
    assert unbound.capabilities is None
    with pytest.raises(CatalogAddressingError):
        unbound.table_address("t", schema="s", catalog="other_catalog")


def test_catalog_gate_follows_the_declaration(
    conformance_target: ConformanceTarget,
) -> None:
    """Declared 'none' refuses; declared 'read'/'full' addresses."""
    dialect = require_dialect(conformance_target)
    caps = conformance_target.declared_capabilities
    if caps is None:
        pytest.skip(
            "sql_capabilities undeclared; reported by the "
            "declaration-consistency check"
        )
    if caps.catalog == "none":
        with pytest.raises(CatalogAddressingError):
            dialect.table_address("t", schema="s", catalog="other_catalog")
    else:
        address = dialect.table_address("t", schema="s", catalog="other_catalog")
        assert address.catalog


def test_session_init_statements_are_a_list_of_sql_strings(
    conformance_target: ConformanceTarget,
) -> None:
    """``session_init_sql`` returns statements the CDK can execute."""
    dialect = require_dialect(conformance_target)
    statements = dialect.session_init_sql()
    assert isinstance(statements, list)
    assert all(isinstance(s, str) and s.strip() for s in statements)
