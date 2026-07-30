"""The stage-then-merge cycle, owned once above both SQL transports.

Every SQL write is stage-then-merge (spec sql-write-path section 2): land
the batch in a stage table, then run exactly one mode statement from stage
to target. :class:`StageCycle` owns that step order and everything hanging
off it — the pre-flight drop, the bulk-land dispatch with its fallback,
land verification, the drop after success or failure, the poisoning rule,
and the honest orphan log — so no transport carries a second copy to
drift from. A transport supplies mechanisms, never rules.

The cycle is the **sync body only**. It is handed a :class:`StageConnection`
by a transport backend that has already put itself on a worker thread (or
inside ``AsyncConnection.run_sync``), and each backend keeps its own async
wrapper: the two transports hold different locks under cancellation and one
discipline cannot cover both.

Two rules the cycle owns outright:

* **Poisoning is path-scoped.** On the non-transactional path a failed step
  leaves unknown session state and a possibly-committed stage, so the
  connection is discarded (``invalidate``) and the next batch reopens. On
  the transactional path the rollback *is* the recovery — a rejected row
  says nothing about the connection, and discarding a healthy pooled
  connection on every constraint violation is what no connection pool
  does. The single exception is a rollback that itself fails: that is the
  transactional path's connection-level evidence, and the connection is
  discarded on it.
* **The backends render no SQL.** The write plan carries every
  dialect-specific write statement; the generic ANSI probes this module
  renders (``SELECT 1``, ``SELECT COUNT(*)``, the zero-row column probe,
  the qmark landing ``INSERT``) differ per system only in identifier
  quoting, so they are rendered here through the dialect and the backends
  only execute them.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

from cdk.adbc_registry import AdbcConfigurationError

from .backend import StageWritePlan
from .dialects import SqlDialect, TableAddress

if TYPE_CHECKING:
    import pyarrow as pa

    from cdk.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)

#: The liveness probe both transports execute for ``health_check``.
LIVENESS_PROBE_SQL = "SELECT 1"


def render_row_count_sql(dialect: SqlDialect, address: TableAddress) -> str:
    """Render the aggregate that verifies what a landing actually landed."""
    # Dialect-quoted identifiers only; no user values.
    return f"SELECT COUNT(*) FROM {dialect.quote_table(address)}"  # nosec B608


def render_column_probe_sql(dialect: SqlDialect, address: TableAddress) -> str:
    """Render the zero-row SELECT a transport reads column names from."""
    # Dialect-quoted identifiers only; no user values.
    return f"SELECT * FROM {dialect.quote_table(address)} WHERE 1=0"  # nosec B608


def render_landing_insert_sql(
    dialect: SqlDialect, stage: TableAddress, columns: Sequence[str]
) -> str:
    """Render the qmark ``INSERT`` an executemany landing binds rows to."""
    cols = ", ".join(dialect.quote_ident(c) for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    # Dialect-quoted identifiers only; values bind via qmark params.
    return (
        f"INSERT INTO {dialect.quote_table(stage)} "  # nosec B608
        f"({cols}) VALUES ({placeholders})"
    )


def _stage_leftover_note(plan: StageWritePlan) -> str:
    """Word the honest, scope-accurate consequence of an undropped stage."""
    if plan.scope == "temp":
        return (
            "the session-temp stage is invisible to other sessions, is "
            "cleared by a retry's pre-flight drop on this session, and "
            "dies with the connection"
        )
    return (
        "a retried batch clears it via the pre-flight drop, but after a "
        "fatal failure or exhausted retries it needs a manual drop or a "
        "table-expiration policy"
    )


class BulkLandOutcome(Enum):
    """What the connector's declared bulk mechanism did with a batch."""

    #: No bulk mechanism is declared for this transport, so nothing ran.
    UNDECLARED = "undeclared"
    #: The declared mechanism claims it landed the whole batch.
    LANDED = "landed"
    #: The declared mechanism ran and refused this batch.
    DECLINED = "declined"


def offer_bulk_land(
    dialect: SqlDialect,
    conn: Any,
    plan: StageWritePlan,
    batch: pa.RecordBatch,
    *,
    runtime: ConnectionRuntime | None,
    declared: bool,
) -> BulkLandOutcome:
    """Offer the batch to the declared bulk mechanism; report what happened.

    The one call into the untrusted hook, so "only a declared mechanism is
    ever consulted" is stated once for every transport. *conn* is the
    backend's own native connection object, which is what the hook's
    contract hands the dialect, and *runtime* is the second half of that
    contract — a declared mechanism reached without one means the backend
    is landing before it connected, which is refused here rather than
    handed to connector code as a ``None``. What follows a decline — the
    fallback landing, the verification — is the cycle's rule, not a
    transport's.
    """
    if not declared:
        return BulkLandOutcome.UNDECLARED
    if runtime is None:
        raise RuntimeError(
            f"dialect {dialect.name!r} declares a bulk land mechanism but the "
            f"backend reached it with no connection runtime; execute_write() "
            f"ran before connect()"
        )
    if dialect.bulk_land(conn, plan.stage, batch, runtime=runtime):
        return BulkLandOutcome.LANDED
    return BulkLandOutcome.DECLINED


class StageConnection(Protocol):
    """One live connection, expressed as the steps a stage cycle takes.

    A transport backend implements this over its own handle: a SQLAlchemy
    sync ``Connection``, or a cached ADBC DBAPI connection. Nothing here
    decides *what* runs — the cycle does — and nothing here renders SQL.
    """

    def run_statement(self, sql: str, *, commit: bool) -> None:
        """Execute *sql*, committing it as its own unit when *commit*."""

    def offer_bulk_land(
        self, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> BulkLandOutcome:
        """Offer *batch* to the connector's declared bulk mechanism.

        Implemented by handing this transport's native connection to
        :func:`offer_bulk_land`; a transport with no declared mechanism
        reports :attr:`BulkLandOutcome.UNDECLARED` and lands nothing.
        """

    def land_batch(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Land *batch* into the stage by this transport's own mechanism.

        The CDK's own landing — ``executemany`` for SQLAlchemy,
        ``adbc_ingest`` or ``executemany`` for ADBC — which is both the
        undeclared path and the fallback the cycle runs when the declared
        mechanism declines.
        """

    def stage_row_count(self, plan: StageWritePlan) -> int:
        """Report the rows the stage actually holds.

        Read back whenever the untrusted hook ran, so a mechanism that
        landed into the wrong place — or partially, then declined —
        cannot ack a full batch.
        """

    def commit(self) -> None:
        """Commit the current unit (the landing has no statement of its own)."""

    def rollback(self) -> None:
        """Undo the current unit.

        Both the recovery on the transactional path and what clears an
        aborted transaction before a cleanup drop can run.
        """

    def invalidate(self) -> None:
        """Discard this connection so no later batch inherits it."""


class StageCycle:
    """The one stage-then-merge step order, executed over any transport."""

    def __init__(self, dialect: SqlDialect) -> None:
        self._dialect = dialect

    def run(
        self, conn: StageConnection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Run one full stage cycle for *batch* on *conn*.

        Transactional: every step — stage DDL, landing, the optional
        target-emptying DELETE, the mode statement, the drop — shares the
        batch transaction, so an interrupted batch leaves nothing, not
        even a stage.

        Non-transactional: per-step commits. Safety comes from the
        primitive, not atomicity — the pre-flight drop clears this same
        batch's leftover stage from a prior attempt (deterministic names
        make retries self-healing), the mode statement is idempotent on
        identity for insert/upsert, and a failed step ends with a
        best-effort drop and a discarded connection.
        """
        if plan.transactional:
            self._run_in_one_transaction(conn, plan, batch)
            return
        self._run_stepwise(conn, plan, batch)

    # ---- the two transaction shapes ------------------------------------

    def _run_in_one_transaction(
        self, conn: StageConnection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        try:
            self._land_and_apply(conn, plan, batch, commit_each=False)
            # The drop joins the transaction, so no pre-flight drop is
            # needed: a failed prior attempt rolled its stage back with it.
            conn.run_statement(plan.drop_stage_sql, commit=False)
            conn.commit()
        except Exception:
            # The rollback is the recovery: nothing committed, so nothing
            # is left to clean up and the connection stays healthy. A
            # constraint violation says nothing about the connection, and
            # discarding a pooled one per rejected row is what no pool
            # does — unless the rollback itself fails, which is where the
            # connection is discarded.
            self._rollback_after_failure(conn, plan)
            raise

    def _run_stepwise(
        self, conn: StageConnection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        try:
            conn.run_statement(plan.drop_stage_sql, commit=True)
            self._land_and_apply(conn, plan, batch, commit_each=True)
        except Exception:
            # Failure path: best-effort drop, then the connection is
            # discarded — a failed step here leaves unknown session state
            # and a possibly-committed stage, which the next batch must
            # not inherit. The batch still fails with its own error.
            self._drop_stage_after_failure(conn, plan)
            self._discard(conn)
            raise
        except BaseException:
            # Cancellation or interpreter shutdown: no further database
            # I/O on a deadline that already expired — a cleanup DROP
            # would block on exactly the condition that caused the
            # cancellation, past the ack budget, while holding the write
            # lock. Log honestly and let a retry's pre-flight drop (or
            # the session's end, for temp scope) clear the stage.
            logger.warning(
                "stage cycle for %s interrupted before cleanup; %s",
                self._dialect.quote_table(plan.stage),
                _stage_leftover_note(plan),
            )
            raise
        self._drop_stage_after_success(conn, plan)

    def _land_and_apply(
        self,
        conn: StageConnection,
        plan: StageWritePlan,
        batch: pa.RecordBatch,
        *,
        commit_each: bool,
    ) -> None:
        """Create the stage, land the batch, apply the one mode statement."""
        conn.run_statement(plan.create_stage_sql, commit=commit_each)
        self._land(conn, plan, batch)
        if commit_each:
            conn.commit()
        if plan.truncate_sql is not None:
            # The emptying statement shares the batch transaction on the
            # transactional path, so a failed first batch rolls it back;
            # committed as its own step otherwise, where a failure before
            # the append heals on retry (the same first-batch plan
            # re-runs it).
            conn.run_statement(plan.truncate_sql, commit=commit_each)
        conn.run_statement(plan.mode_sql, commit=commit_each)

    def _land(
        self, conn: StageConnection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Dispatch the landing, then refuse a stage that is not the batch's.

        One rule for every transport: the declared bulk mechanism gets
        the batch first, a decline falls back to the transport's own
        landing (logged INFO — a speed downgrade is visible, never
        silent), and whenever untrusted connector code ran the stage's
        real row count is read back and checked. Its claim — "landed" on
        a ``True`` return, "landed nothing" on a decline — is verified
        before the mode statement runs: a mechanism that landed into the
        wrong place (or partially, then declined) would otherwise let a
        wrong stage ack a full batch. A mismatch is a connector defect,
        refused loudly before any target mutation.

        Only the CDK's own mechanisms skip the read-back: they land the
        batch they were handed, so the batch is its own count.
        """
        outcome = conn.offer_bulk_land(plan, batch)
        if outcome is BulkLandOutcome.UNDECLARED:
            conn.land_batch(plan, batch)
            return
        if outcome is BulkLandOutcome.DECLINED:
            logger.info(
                "dialect %s declined the declared bulk land for %s; "
                "landing via executemany",
                self._dialect.name,
                self._dialect.quote_table(plan.stage),
            )
            conn.land_batch(plan, batch)
        landed = conn.stage_row_count(plan)
        if landed != batch.num_rows:
            raise AdbcConfigurationError(
                f"dialect {self._dialect.name!r} bulk_land ran for "
                f"{plan.stage} but the stage holds {landed} rows, "
                f"expected {batch.num_rows}; the declared bulk mechanism did "
                f"not land this batch cleanly — fix the connector."
            )

    # ---- cleanup, poisoning, and the honest orphan record ---------------

    def _rollback_after_failure(
        self, conn: StageConnection, plan: StageWritePlan
    ) -> None:
        """Undo the batch transaction; discard a connection that cannot.

        A rollback that succeeds is the whole recovery, and the batch
        still fails with its own error. A rollback that *fails* is this
        path's connection-level evidence — the one signal here that the
        connection itself is broken rather than the batch rejected — and
        it is what a pool acts on too (SQLAlchemy invalidates when the
        return-to-pool rollback raises). It cannot fire on a constraint
        violation, which rolls back cleanly, so the discard costs nothing
        the path-scoped rule protects; without it a transport that owns
        its own cached handle would serve a dead connection, holding an
        open transaction, to every later batch of the run.
        """
        try:
            conn.rollback()
        except Exception:
            logger.warning(
                "rollback of the batch transaction for %s failed; the "
                "connection is discarded so the next batch does not "
                "inherit its open transaction",
                self._dialect.quote_table(plan.stage),
                exc_info=True,
            )
            self._discard(conn)

    def _drop_stage_after_failure(
        self, conn: StageConnection, plan: StageWritePlan
    ) -> None:
        """Best-effort stage drop after a failed step (non-transactional).

        Never raises: the batch must fail with the step's own error, not
        with whatever the cleanup ran into — the retry verdict belongs to
        the write failure. Each log tells only what actually happened: a
        failed rollback means the drop was never attempted (and the stage
        may not even exist if the create step was what failed).
        """
        try:
            conn.rollback()
        except Exception:
            logger.warning(
                "rollback after a failed batch also failed; the stage drop "
                "for %s was not attempted — %s",
                self._dialect.quote_table(plan.stage),
                _stage_leftover_note(plan),
                exc_info=True,
            )
            return
        try:
            conn.run_statement(plan.drop_stage_sql, commit=True)
        except Exception:
            logger.warning(
                "stage table %s could not be dropped after a failed batch; %s",
                self._dialect.quote_table(plan.stage),
                _stage_leftover_note(plan),
                exc_info=True,
            )

    def _drop_stage_after_success(
        self, conn: StageConnection, plan: StageWritePlan
    ) -> None:
        """Drop the stage after a committed mode statement (non-transactional).

        Attempted twice; when both fail the connection is discarded (a
        failed DROP means a possibly-dead connection the next batch must
        not inherit) and the log tells the truth: for real scope the named
        stage table is orphaned until dropped manually or expired; a
        temp-scope stage dies with the discarded session.
        """
        first_exc = self._try_drop(conn, plan)
        if first_exc is None:
            return
        # INFO, not DEBUG, with the cause: a connection failing its first
        # DROP on every batch needs a footprint at the default log level
        # before it escalates to an orphan, and the cause (lock vs network
        # vs permissions) is the actionable part.
        logger.info(
            "post-merge drop of stage %s failed (attempt 1/2); retrying",
            self._dialect.quote_table(plan.stage),
            exc_info=first_exc,
        )
        try:
            # A failed first attempt can leave the connection in an
            # aborted transaction; clear it or the retry fails on the
            # transaction state instead of the drop. Logged apart from
            # the drop errors so neither masks the other.
            conn.rollback()
        except Exception:
            logger.warning(
                "rollback before the stage-drop retry for %s failed",
                self._dialect.quote_table(plan.stage),
                exc_info=True,
            )
        second_exc = self._try_drop(conn, plan)
        if second_exc is None:
            logger.info(
                "post-merge drop of stage %s succeeded on the second attempt",
                self._dialect.quote_table(plan.stage),
            )
            return
        if plan.scope == "real":
            orphan_note = (
                "it is orphaned — a full copy of this batch — until dropped "
                "manually or removed by a table-expiration policy"
            )
        else:
            orphan_note = "the session-temp stage dies with the discarded connection"
        # The honest orphan record comes before the discard: a failing
        # invalidate() must never erase the only log naming the leftover
        # table. The batch stays acked either way — the mode statement
        # committed.
        logger.warning(
            "stage table %s could not be dropped after a successful mode "
            "statement (two attempts). The batch is acked and never "
            "retried, so no automatic cleanup reaches this table: %s. The "
            "connection is discarded so the next batch does not inherit it",
            self._dialect.quote_table(plan.stage),
            orphan_note,
            exc_info=second_exc,
        )
        self._discard(conn)

    @staticmethod
    def _try_drop(conn: StageConnection, plan: StageWritePlan) -> Exception | None:
        """One drop attempt, returning the failure instead of raising."""
        try:
            conn.run_statement(plan.drop_stage_sql, commit=True)
        except Exception as exc:
            return exc
        return None

    @staticmethod
    def _discard(conn: StageConnection) -> None:
        """Poison the connection; never mask the failure that caused it."""
        try:
            conn.invalidate()
        except Exception:
            logger.warning(
                "could not discard the connection after a failed stage "
                "step; the pool may hand it out again",
                exc_info=True,
            )
