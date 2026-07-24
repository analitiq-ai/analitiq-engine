"""Build :class:`~cdk.sql.backend.StageWritePlan`s for the write primitive.

The facade decides semantics (mode, identity, truncate gating) and calls
:func:`build_stage_write_plan` to turn them into the complete plan a
transport backend executes. Everything here is pure rendering over the
dialect's quoting and hooks:

* the deterministic stage name — ``_analitiq_stage_b<sha16>_<target>``,
  hash first so no identifier budget can ever cut it off; the target tail
  is readability only and truncates freely,
* the ANSI statements the CDK renders itself (the set-based anti-join
  insert, the plain append, the ``DROP TABLE IF EXISTS``), and
* delegation to the dialect for the statements that have per-system forms
  (``stage_table_sql``, ``merge_statement_sql``, ``empty_table_sql``).
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Sequence
from dataclasses import replace

from .backend import StageWritePlan
from .capabilities import SqlCapabilities
from .dialects import SqlDialect, TableAddress
from .exceptions import SchemaConfigurationError

logger = logging.getLogger(__name__)

_STAGE_PREFIX = "_analitiq_stage_"

WriteModeName = str  # "insert" | "upsert" | "truncate_insert" (facade-owned)


def stage_table_name(
    target_table: str,
    *,
    run_id: str,
    stream_id: str,
    batch_seq: int,
    max_identifier_length: int,
) -> str:
    """Compose the deterministic per-batch stage name.

    ``sha256(run_id|stream_id|batch_seq)[:16]`` is the uniqueness token —
    fixed-width and collision-resistant, so a retry of the same batch
    computes the same name and the pre-flight drop finds exactly its own
    leftovers. The token comes *before* the target tail: the tail is
    readability only and is truncated to the dialect's identifier budget,
    so no target name can ever push the token past the budget and collapse
    distinct stages into one name.
    """
    token = (
        "b"
        + hashlib.sha256(f"{run_id}|{stream_id}|{batch_seq}".encode()).hexdigest()[:16]
    )
    head = f"{_STAGE_PREFIX}{token}"
    if max_identifier_length < len(head):
        # The token is never truncated — a cut token collapses distinct
        # stages into one name, exactly the defect the hash-first grammar
        # exists to prevent. A budget this tight is a dialect authoring
        # error; refuse it loudly.
        raise SchemaConfigurationError(
            f"dialect identifier budget {max_identifier_length} cannot hold "
            f"the {len(head)}-byte stage-name prefix and uniqueness token; "
            f"fix the dialect's max_identifier_length"
        )
    tail_budget = max_identifier_length - len(head) - 1
    if tail_budget <= 0 or not target_table:
        return head
    return f"{head}_{target_table[:tail_budget]}"


def _stage_address(
    caps: SqlCapabilities,
    target: TableAddress,
    stage_name: str,
) -> TableAddress:
    """Place the stage per the declared scope and schema rule.

    A temp-scope stage carries no schema or catalog: it lives in the
    session's namespace, which is also what lets the mode statement see it
    on the same connection. A real-scope stage lands in the target's
    schema, or in the declared dedicated schema. The engine-generated name
    is used verbatim (no re-normalization), so the quoted DDL and the
    landing statements stay the same string.
    """
    if caps.stage.scope == "temp":
        return TableAddress(table=stage_name)
    if caps.stage.schema == "dedicated":
        # dedicated_schema is validated non-empty for this placement.
        return TableAddress(
            table=stage_name,
            schema=str(caps.stage.dedicated_schema),
            catalog=target.catalog,
        )
    return replace(target, table=stage_name)


def render_anti_join_insert_sql(
    dialect: SqlDialect,
    stage: TableAddress,
    target: TableAddress,
    columns: Sequence[str],
    identity: Sequence[str],
) -> str:
    """One set-based ``INSERT … SELECT … WHERE NOT EXISTS`` from the stage.

    Plain ANSI over the dialect's quoting; runs on every backend. The
    identity match is the contract primary key or the synthetic
    ``_record_hash`` — both NOT NULL, so plain equality is the correct
    match (no null-key semantics to special-case).
    """
    if not identity:
        raise SchemaConfigurationError(
            f"insert into {target} has no identity columns for the "
            f"anti-join; the facade must pass the contract primary key or "
            f"the synthetic record-hash column"
        )
    quoted_cols = [dialect.quote_ident(c) for c in columns]
    col_list = ", ".join(quoted_cols)
    select_list = ", ".join(f"s.{c}" for c in quoted_cols)
    match = " AND ".join(
        f"t.{dialect.quote_ident(c)} = s.{dialect.quote_ident(c)}" for c in identity
    )
    # Composed exclusively of dialect-quoted identifiers; values never
    # enter this text (they were landed via bound parameters).
    return (
        f"INSERT INTO {dialect.quote_table(target)} ({col_list}) "  # nosec B608
        f"SELECT {select_list} FROM {dialect.quote_table(stage)} s "
        f"WHERE NOT EXISTS (SELECT 1 FROM {dialect.quote_table(target)} t "
        f"WHERE {match})"
    )


def render_append_sql(
    dialect: SqlDialect,
    stage: TableAddress,
    target: TableAddress,
    columns: Sequence[str],
) -> str:
    """Plain ``INSERT INTO target SELECT … FROM stage`` (truncate_insert).

    No identity dedup by design: deduping a full refresh would collapse
    legitimate duplicate rows.
    """
    quoted_cols = [dialect.quote_ident(c) for c in columns]
    col_list = ", ".join(quoted_cols)
    # Dialect-quoted identifiers only; see render_anti_join_insert_sql.
    return (
        f"INSERT INTO {dialect.quote_table(target)} ({col_list}) "  # nosec B608
        f"SELECT {col_list} FROM {dialect.quote_table(stage)}"
    )


def build_stage_write_plan(
    dialect: SqlDialect,
    caps: SqlCapabilities,
    *,
    target: TableAddress,
    columns: Sequence[str],
    write_mode: WriteModeName,
    conflict_keys: Sequence[str],
    identity: Sequence[str],
    truncate_now: bool,
    run_id: str,
    stream_id: str,
    batch_seq: int,
) -> StageWritePlan:
    """Assemble the complete plan for one batch write.

    The facade has already enforced the refusals (declared capabilities,
    non-empty ``conflict_keys`` for upsert, dialect hook presence); this
    builder renders. ``columns`` is the landing order — the prepared
    batch's schema names, identity included.
    """
    stage_name = stage_table_name(
        target.table,
        run_id=run_id,
        stream_id=stream_id,
        batch_seq=batch_seq,
        max_identifier_length=dialect.max_identifier_length,
    )
    stage = _stage_address(caps, target, stage_name)
    temp = caps.stage.scope == "temp"

    if write_mode == "upsert":
        if not conflict_keys:
            raise SchemaConfigurationError(
                f"upsert requested for {target} but the stream carries no "
                f"conflict_keys; refusing to fall back to plain INSERT "
                f"(would silently duplicate rows)"
            )
        if not [c for c in columns if c not in set(conflict_keys)]:
            logger.warning(
                "upsert into %s has no non-key columns to update "
                "(all landed columns are conflict keys); the merge "
                "statement only inserts new rows. Consider "
                "write_mode='insert' for clarity.",
                target,
            )
        mode_sql = dialect.merge_statement_sql(
            stage, target, list(conflict_keys), list(columns)
        )
    elif write_mode == "insert":
        mode_sql = render_anti_join_insert_sql(
            dialect, stage, target, columns, identity
        )
    else:
        mode_sql = render_append_sql(dialect, stage, target, columns)

    return StageWritePlan(
        stage=stage,
        target=target,
        scope=caps.stage.scope,  # type: ignore[arg-type]
        transactional=caps.stage.transactional_ddl,
        create_stage_sql=dialect.stage_table_sql(stage, target, temp=temp),
        truncate_sql=dialect.empty_table_sql(target) if truncate_now else None,
        mode_sql=mode_sql,
        drop_stage_sql=f"DROP TABLE IF EXISTS {dialect.quote_table(stage)}",
        columns=tuple(columns),
    )
