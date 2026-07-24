"""Transport backends for the stage-then-merge write primitive.

Every SQL write is stage-then-merge (ADR sql-write-path-v2): land the batch
in a stage table, then run exactly one mode statement from stage to target.
The facade (``GenericSQLConnector``) owns the semantics — write modes,
identity and duplicate rules, refusals, retry verdicts, timeouts — and
expresses one batch write as a :class:`StageWritePlan`. A
:class:`TransportBackend` executes plans; it owns connections, cursors, and
commit calls, and holds no write-mode logic.

``SqlAlchemyBackend`` is the SQLAlchemy transport (issue #388). It serves
both engine flavors — async engine, and sync engine on a worker thread —
through one shared sync-``Connection`` cycle body, so DML semantics cannot
fork between them. The ADBC transport is
:class:`~cdk.sql.adbc_backend.AdbcBackend` (issue #389), executing the
same plans over a direct ADBC DBAPI connection.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from cdk.adbc_registry import AdbcConfigurationError

from .dialects import SqlDialect, TableAddress

if TYPE_CHECKING:
    import pyarrow as pa

    from cdk.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)

StageScope = Literal["temp", "real"]

_Row = TypeVar("_Row")


@dataclass(frozen=True)
class StageWritePlan:
    """Everything a backend needs to execute one batch write.

    Built by the facade: addresses from the dialect's ``TableAddress``
    factory, SQL text from the dialect's rendering hooks, scope and
    transaction shape from the connector's declared capabilities. The
    backend never renders SQL of its own — a plan is complete.
    """

    stage: TableAddress
    target: TableAddress
    scope: StageScope
    transactional: bool
    create_stage_sql: str
    #: Set on a truncate_insert read's first batch only: the dialect's
    #: target-emptying statement (``empty_table_sql``), run after landing
    #: and before the mode statement.
    truncate_sql: str | None
    #: The one mode statement from stage to target (anti-join insert,
    #: merge-form upsert, or plain append).
    mode_sql: str
    drop_stage_sql: str
    #: Landing column order, identity columns included.
    columns: tuple[str, ...]
    #: Rows one landing statement may carry under the connector's declared
    #: ``sql_capabilities.limits.max_bind_params``
    #: (``floor(cap / column_count)``, issue #401); ``None`` when no cap is
    #: declared — the executemany landing then sends the whole batch in one
    #: statement, as before.
    rows_per_statement: int | None = None


def iter_landing_chunks(
    rows: Sequence[_Row], per_statement: int | None
) -> Iterator[Sequence[_Row]]:
    """Split landed rows so no statement exceeds the declared bind cap.

    The one chunking rule both transport backends apply in their
    executemany landing (``StageWritePlan.rows_per_statement``, issue
    #401); ``None`` (no declared cap) yields the whole batch unchanged.
    """
    if per_statement is None:
        yield rows
        return
    for start in range(0, len(rows), per_statement):
        yield rows[start : start + per_statement]


def stage_leftover_note(plan: StageWritePlan) -> str:
    """Word the honest, scope-accurate consequence of an undropped stage.

    Shared by both backends' failure-path logs so the same leftover has
    the same documented consequence whichever transport left it.
    """
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


class TransportBackend(ABC):
    """Executes :class:`StageWritePlan`s; owns connections and commits.

    Holds no write-mode logic: which statement runs is the plan's, decided
    by the facade. Success of :meth:`execute_write` is returning without
    raising — it deliberately returns nothing, because database rowcounts
    lie about the batch (an idempotent replay's anti-join affects 0 rows;
    MySQL upserts count 2 per updated row); the facade reports
    ``records_written`` from the batch's own row count.
    """

    @abstractmethod
    async def connect(self, runtime: ConnectionRuntime) -> None:
        """Take ownership of the runtime's transport for this backend."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Release backend-held transport state.

        The ``ConnectionRuntime`` itself (engine disposal) stays owned by
        the facade's lifecycle, which materialized it.
        """

    @abstractmethod
    async def run_ddl(self, statements: Sequence[str]) -> None:
        """Run *statements* committed together (one transaction)."""

    @abstractmethod
    async def target_columns(self, target: TableAddress) -> tuple[str, ...]:
        """Return *target*'s column names as the database reports them.

        Called by the facade after DDL: the names serve its readiness
        checks (the engine-managed ``_record_hash`` column, one rule for
        every transport). A backend may also cache what it learns here
        for the landing step's own use.
        """

    @abstractmethod
    async def health_check(self) -> None:
        """Probe transport liveness; success is returning without raising."""

    @abstractmethod
    async def execute_write(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Create the stage, land *batch*, run the plan's statements, drop.

        One transaction spanning every step when ``plan.transactional``;
        stepwise commits with pre-flight drop and self-healing retry
        semantics when not. At most one stage cycle runs at a time per
        backend — the whole cycle holds the backend's write lock, so a
        retry can only ever meet a completed or abandoned stage, never a
        live one.
        """


class SqlAlchemyBackend(TransportBackend):
    """Stage-then-merge over a SQLAlchemy engine (async or sync flavor).

    Both flavors run one shared sync-``Connection`` body: the async engine
    enters it through ``AsyncConnection.run_sync``, the sync engine runs it
    directly on a worker thread. The backend reflects each target table
    once (after the facade's DDL) so the stage landing binds with the
    column types the database actually created — the same binding rule the
    pre-split write path used.
    """

    def __init__(self, dialect: SqlDialect) -> None:
        self._dialect = dialect
        self._runtime: ConnectionRuntime | None = None
        self._engine: AsyncEngine | None = None
        self._sync_engine: Engine | None = None
        # Reflected target tables, keyed by address. Populated by
        # target_columns() at configure time; execute_write requires the
        # entry (fail loud on a write whose target was never prepared).
        self._targets: dict[TableAddress, Table] = {}
        # Whether the connector declares a native bulk-load mechanism.
        # Only a declared mechanism ever calls the dialect's bulk_land
        # hook; a bare override without a declaration is never called.
        self._bulk_declared: bool = False
        # One stage cycle at a time per backend: the pre-flight drop of a
        # retry must never race a still-running prior attempt's cycle.
        self._write_lock: asyncio.Lock = asyncio.Lock()

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """Wire the backend to the runtime's already-materialized engine."""
        self._runtime = runtime
        self._engine = None
        self._sync_engine = None
        self._targets.clear()
        if runtime.is_sync_sqlalchemy:
            self._sync_engine = runtime.sync_engine
        else:
            self._engine = runtime.engine
        caps = getattr(self._dialect, "capabilities", None)
        self._bulk_declared = caps is not None and caps.bulk_load != "none"

    async def disconnect(self) -> None:
        """Drop backend-held refs; the facade disposes the runtime."""
        self._engine = None
        self._sync_engine = None
        self._runtime = None
        self._targets.clear()

    async def run_ddl(self, statements: Sequence[str]) -> None:
        """Run *statements* in one transaction on the active flavor."""
        stmts = list(statements)
        if not stmts:
            return
        if self._sync_engine is not None:
            await asyncio.to_thread(self._run_ddl_sync, stmts)
            return
        async with self._require_engine().begin() as conn:
            for stmt in stmts:
                await conn.exec_driver_sql(stmt)

    async def health_check(self) -> None:
        """Probe liveness with ``SELECT 1`` on the active engine flavor."""
        if self._sync_engine is not None:
            await asyncio.to_thread(self._health_check_sync)
            return
        async with self._require_engine().connect() as conn:
            await conn.exec_driver_sql("SELECT 1")

    def _health_check_sync(self) -> None:
        with self._require_sync_engine().connect() as conn:
            conn.exec_driver_sql("SELECT 1")

    async def target_columns(self, target: TableAddress) -> tuple[str, ...]:
        """Reflect *target* (cached) and return its column names.

        Called by the facade after DDL: the reflected table carries the
        column types the database reports, which the stage landing binds
        with; the names serve the facade's readiness checks.
        """
        if self._sync_engine is not None:
            table = await asyncio.to_thread(self._reflect_on_sync_engine, target)
        else:
            async with self._require_engine().connect() as conn:
                table = await conn.run_sync(self._reflect, target)
        self._targets[target] = table
        return tuple(c.name for c in table.columns)

    async def execute_write(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Run one full stage cycle for *batch* under the write lock."""
        async with self._write_lock:
            if self._sync_engine is not None:
                # The worker thread cannot be cancelled in-band, so a
                # cancellation of this await must not release the write
                # lock while the thread is still landing or merging — a
                # retry's pre-flight drop would then meet a live stage.
                # Shield the thread future and, on cancellation, hold the
                # lock until the thread finishes or abandons its cycle
                # (the section-6 mutual-exclusion rule), then re-raise.
                future = asyncio.ensure_future(
                    asyncio.to_thread(self._execute_write_sync, plan, batch)
                )
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    try:
                        await future
                    except Exception:
                        # The thread's own failure; the cancellation is
                        # what the caller sees.
                        logger.warning(
                            "sync stage cycle for %s failed while its "
                            "caller was already cancelled",
                            self._dialect.quote_table(plan.stage),
                            exc_info=True,
                        )
                    raise
                return
            # The async flavor cancels in-band: SQLAlchemy's greenlet
            # bridge delivers the cancellation at the next await point
            # and the connection context rolls back, so the cycle is
            # genuinely over when the lock releases.
            async with self._require_engine().connect() as conn:
                await conn.run_sync(self._run_stage_cycle, plan, batch)

    # ---- shared sync-Connection bodies --------------------------------

    def _execute_write_sync(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Sync-flavor entry: same cycle body, worker thread."""
        with self._require_sync_engine().connect() as conn:
            self._run_stage_cycle(conn, plan, batch)

    def _run_ddl_sync(self, statements: list[str]) -> None:
        with self._require_sync_engine().begin() as conn:
            for stmt in statements:
                conn.exec_driver_sql(stmt)

    def _reflect_on_sync_engine(self, target: TableAddress) -> Table:
        with self._require_sync_engine().connect() as conn:
            return self._reflect(conn, target)

    @staticmethod
    def _reflect(conn: Connection, target: TableAddress) -> Table:
        return Table(
            target.table,
            MetaData(),
            autoload_with=conn,
            schema=target.schema or None,
        )

    def _run_stage_cycle(
        self, conn: Connection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Run the one cycle body both engine flavors execute.

        Transactional: every step — stage DDL, landing, the optional
        target-emptying DELETE, the mode statement, the drop — shares the
        batch transaction, so an interrupted batch leaves nothing, not
        even a stage.

        Non-transactional: per-step commits. Safety comes from the
        primitive, not atomicity — the pre-flight drop clears this same
        batch's leftover stage from a prior attempt on this session
        (deterministic names make retries self-healing), the mode
        statement is idempotent on identity for insert/upsert, and a
        failed cycle ends with a best-effort drop plus the poisoning
        rules below.
        """
        if plan.transactional:
            with conn.begin():
                self._run_cycle_steps(conn, plan, batch, preflight_drop=False)
            return

        self._exec_commit(conn, plan.drop_stage_sql)
        try:
            self._run_cycle_steps(conn, plan, batch, preflight_drop=True)
        except Exception:
            # Failure path: best-effort drop, then the batch fails with
            # its own error.
            self._drop_stage_after_failure(conn, plan)
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
                stage_leftover_note(plan),
            )
            raise
        self._drop_stage_after_success(conn, plan)

    def _drop_stage_after_failure(self, conn: Connection, plan: StageWritePlan) -> None:
        """Best-effort stage drop after a failed cycle step (non-transactional).

        Never raises: the batch must fail with the step's own error, not
        with whatever the cleanup ran into — the retry verdict belongs to
        the write failure. Each log tells only what actually happened:
        a failed rollback means the drop was never attempted (and the
        stage may not even exist if the create step was what failed).
        """
        try:
            conn.rollback()
        except Exception:
            logger.warning(
                "rollback after a failed batch also failed; the stage drop "
                "for %s was not attempted — %s",
                self._dialect.quote_table(plan.stage),
                stage_leftover_note(plan),
                exc_info=True,
            )
            return
        try:
            self._exec_commit(conn, plan.drop_stage_sql)
        except Exception:
            logger.warning(
                "stage table %s could not be dropped after a failed batch; " "%s",
                self._dialect.quote_table(plan.stage),
                stage_leftover_note(plan),
                exc_info=True,
            )

    def _run_cycle_steps(
        self,
        conn: Connection,
        plan: StageWritePlan,
        batch: pa.RecordBatch,
        *,
        preflight_drop: bool,
    ) -> None:
        """Create stage, land, truncate when planned, run the mode statement.

        On the transactional path the caller wraps this in the batch
        transaction and appends the drop inside it; on the
        non-transactional path each step commits as its own unit and the
        caller owns the drops.
        """
        commit_each = not plan.transactional
        self._exec(conn, plan.create_stage_sql, commit=commit_each)
        self._land(conn, plan, batch)
        if commit_each:
            conn.commit()
        if plan.truncate_sql is not None:
            # The emptying statement shares the batch transaction on the
            # transactional path, so a failed first batch rolls it back;
            # committed as its own step otherwise, where a failure before
            # the append heals on retry (the same first-batch plan
            # re-runs it).
            self._exec(conn, plan.truncate_sql, commit=commit_each)
        self._exec(conn, plan.mode_sql, commit=commit_each)
        if not preflight_drop:
            # Transactional path: the drop joins the transaction.
            self._exec(conn, plan.drop_stage_sql, commit=False)

    def _land(
        self, conn: Connection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Land the batch into the stage: declared bulk mechanism first.

        The bulk hook is a pure speed slot — stage contents are identical
        either way — and it is only consulted when the connector declares
        a mechanism. A declined bulk land falls back to executemany with
        an INFO log: a speed downgrade is visible, never silent.
        """
        if self._bulk_declared:
            if self._runtime is None:
                raise RuntimeError(
                    "SqlAlchemyBackend.execute_write() called before connect()"
                )
            if self._dialect.bulk_land(conn, plan.stage, batch, runtime=self._runtime):
                self._verify_bulk_land(conn, plan, batch.num_rows)
                return
            logger.info(
                "dialect %s declined the declared bulk land for %s; landing "
                "via executemany",
                self._dialect.name,
                self._dialect.quote_table(plan.stage),
            )
            self._executemany_land(conn, plan, batch)
            # A decline must mean "landed nothing": untrusted connector
            # code touched the stage before declining, so the fallback's
            # result is verified like a claimed land — a partial landing
            # under the False return would otherwise silently duplicate
            # rows in the stage.
            self._verify_bulk_land(conn, plan, batch.num_rows)
            return
        self._executemany_land(conn, plan, batch)

    def _executemany_land(
        self, conn: Connection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Land via executemany ``INSERT`` in plan column order (the default).

        Chunked by the plan's ``rows_per_statement`` so no statement exceeds
        the connector's declared bind-parameter cap (issue #401); an
        undeclared cap lands the whole batch in one statement.
        """
        stage_table = self._stage_table(conn, plan)
        records: list[dict[str, Any]] = batch.to_pylist()
        for chunk in iter_landing_chunks(records, plan.rows_per_statement):
            conn.execute(stage_table.insert(), list(chunk))

    def _verify_bulk_land(
        self, conn: Connection, plan: StageWritePlan, expected_rows: int
    ) -> None:
        """Refuse a stage whose row count is not the batch's after bulk_land ran.

        ``bulk_land`` is untrusted connector code, and its claim —
        "landed" on ``True``, "landed nothing" on a decline — would
        otherwise go unverified before the mode statement runs: a
        mechanism that landed into the wrong place (or partially, then
        declined) would let a wrong stage ack a full batch. One cheap
        aggregate closes that: a mismatch is a connector defect, refused
        loudly before any target mutation.
        """
        # Dialect-quoted identifiers only; no user values.
        stage_ref = self._dialect.quote_table(plan.stage)
        count = conn.exec_driver_sql(
            f"SELECT COUNT(*) FROM {stage_ref}"  # nosec B608
        ).scalar_one()
        if count != expected_rows:
            raise AdbcConfigurationError(
                f"dialect {self._dialect.name!r} bulk_land ran for "
                f"{plan.stage} but the stage holds {count} rows, "
                f"expected {expected_rows}; the declared bulk mechanism did "
                f"not land this batch cleanly — fix the connector."
            )

    def _stage_table(self, conn: Connection, plan: StageWritePlan) -> Table:
        """Build a lightweight stage ``Table`` with the target's column types.

        Built per batch from the reflected target so executemany binds each
        landed column exactly as a direct target insert would have — the
        stage table itself is never reflected (it was created moments ago
        in this same cycle). The target reflection is normally cached at
        configure time (``target_columns``); after a reconnect that
        rebuilt the backend it is re-reflected here on the live cycle
        connection, so a still-configured stream never wedges on a stale
        cache.
        """
        target = self._targets.get(plan.target)
        if target is None:
            target = self._reflect(conn, plan.target)
            self._targets[plan.target] = target
        missing = [name for name in plan.columns if name not in target.c]
        if missing:
            raise RuntimeError(
                f"write plan for {plan.target} lands columns {missing} the "
                f"reflected target does not have"
            )
        return Table(
            plan.stage.table,
            MetaData(),
            *(Column(name, target.c[name].type) for name in plan.columns),
            schema=plan.stage.schema or None,
        )

    def _drop_stage_after_success(self, conn: Connection, plan: StageWritePlan) -> None:
        """Drop the stage after a committed mode statement (non-transactional).

        Attempted twice; when both fail the connection is invalidated (a
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
        # The honest orphan record comes before the invalidation: a
        # failing invalidate() must never erase the only log naming the
        # leftover table. The batch stays acked either way — the mode
        # statement committed.
        logger.warning(
            "stage table %s could not be dropped after a successful mode "
            "statement (two attempts). The batch is acked and never "
            "retried, so no automatic cleanup reaches this table: %s. The "
            "connection is discarded so the next batch does not inherit it",
            self._dialect.quote_table(plan.stage),
            orphan_note,
            exc_info=second_exc,
        )
        try:
            conn.invalidate()
        except Exception:
            logger.warning(
                "could not invalidate the connection after the failed stage "
                "drops; the pool may hand it out again",
                exc_info=True,
            )

    def _try_drop(self, conn: Connection, plan: StageWritePlan) -> Exception | None:
        """One drop attempt, returning the failure instead of raising."""
        try:
            self._exec_commit(conn, plan.drop_stage_sql)
        except Exception as exc:
            return exc
        return None

    # ---- small execution helpers ---------------------------------------

    @staticmethod
    def _exec(conn: Connection, sql: str, *, commit: bool) -> None:
        conn.exec_driver_sql(sql)
        if commit:
            conn.commit()

    @staticmethod
    def _exec_commit(conn: Connection, sql: str) -> None:
        conn.exec_driver_sql(sql)
        conn.commit()

    def _require_engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError(
                "async SQLAlchemy engine not available; this backend is not "
                "on the async-SQLAlchemy transport path"
            )
        return self._engine

    def _require_sync_engine(self) -> Engine:
        if self._sync_engine is None:
            raise RuntimeError(
                "sync SQLAlchemy engine not available; this backend is not "
                "on the sync-SQLAlchemy transport path"
            )
        return self._sync_engine
