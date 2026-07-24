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
fork between them. The ADBC transport joins this interface in #389.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from .dialects import SqlDialect, TableAddress

if TYPE_CHECKING:
    import pyarrow as pa

    from cdk.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)

StageScope = Literal["temp", "real"]


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
                await asyncio.to_thread(self._execute_write_sync, plan, batch)
                return
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
        except BaseException:
            # Failure path: best-effort drop, then the batch fails with
            # its own error. A failed drop here is expected on a dead
            # connection; the next attempt's pre-flight drop heals it.
            try:
                conn.rollback()
                self._exec_commit(conn, plan.drop_stage_sql)
            except Exception:
                logger.warning(
                    "stage table %s left behind after a failed batch; a "
                    "retried batch clears it via the pre-flight drop, but "
                    "after a fatal failure or exhausted retries it needs a "
                    "manual drop",
                    self._dialect.quote_table(plan.stage),
                    exc_info=True,
                )
            raise
        self._drop_stage_after_success(conn, plan)

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
                return
            logger.info(
                "dialect %s declined the declared bulk land for %s; landing "
                "via executemany",
                self._dialect.name,
                self._dialect.quote_table(plan.stage),
            )
        stage_table = self._stage_table(plan)
        records: list[dict[str, Any]] = batch.to_pylist()
        conn.execute(stage_table.insert(), records)

    def _stage_table(self, plan: StageWritePlan) -> Table:
        """Build a lightweight stage ``Table`` with the target's column types.

        Built per batch from the reflected target so executemany binds each
        landed column exactly as a direct target insert would have — the
        stage table itself is never reflected (it was created moments ago
        in this same cycle).
        """
        target = self._targets.get(plan.target)
        if target is None:
            raise RuntimeError(
                f"no reflected table for write target {plan.target}; "
                f"target_columns() must run at configure time before writes"
            )
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
        last_exc: Exception | None = None
        for attempt in (1, 2):
            try:
                if last_exc is not None:
                    # A failed first attempt can leave the connection in
                    # an aborted transaction; clear it or the retry fails
                    # on the transaction state instead of the drop.
                    conn.rollback()
                self._exec_commit(conn, plan.drop_stage_sql)
            except Exception as exc:
                last_exc = exc
                logger.debug(
                    "post-merge drop of stage %s failed (attempt %d/2)",
                    self._dialect.quote_table(plan.stage),
                    attempt,
                    exc_info=True,
                )
                continue
            if last_exc is not None:
                # Surface the recovery at INFO: a connection failing its
                # first DROP on every batch would otherwise have no
                # footprint at the default log level until it escalates
                # to an orphan.
                logger.info(
                    "post-merge drop of stage %s succeeded on the second " "attempt",
                    self._dialect.quote_table(plan.stage),
                )
            return
        conn.invalidate()
        if plan.scope == "real":
            orphan_note = (
                "it is orphaned — a full copy of this batch — until dropped "
                "manually or removed by a table-expiration policy"
            )
        else:
            orphan_note = "the session-temp stage dies with the discarded connection"
        logger.warning(
            "stage table %s could not be dropped after a successful mode "
            "statement (two attempts). The batch is acked and never "
            "retried, so no automatic cleanup reaches this table: %s. The "
            "connection was invalidated and the pool opens a fresh one",
            self._dialect.quote_table(plan.stage),
            orphan_note,
            exc_info=last_exc,
        )

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
