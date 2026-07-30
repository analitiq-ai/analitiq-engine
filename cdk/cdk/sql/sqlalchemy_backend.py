"""SQLAlchemy transport backend for the stage-then-merge write primitive.

``SqlAlchemyBackend`` executes :class:`~cdk.sql.backend.StageWritePlan`s
over a SQLAlchemy engine, serving both flavors — async engine, and sync
engine on a worker thread — through one shared sync-``Connection`` body, so
DML semantics cannot fork between them. The step order itself is
:class:`~cdk.sql.stage_cycle.StageCycle`, shared with the ADBC transport;
what lives here is connection acquisition, the executemany landing, and the
cancellation discipline of this backend's write lock.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from .backend import StageWritePlan, TransportBackend, iter_landing_chunks
from .dialects import SqlDialect, TableAddress
from .stage_cycle import (
    LIVENESS_PROBE_SQL,
    StageCycle,
    consult_bulk_land,
    render_row_count_sql,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from cdk.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)


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
        self._cycle = StageCycle(dialect)
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
        self._bulk_declared = (
            caps is not None and caps.bulk_mechanism("sqlalchemy") is not None
        )

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
        """Probe liveness with the shared ANSI probe on the active flavor."""
        if self._sync_engine is not None:
            await asyncio.to_thread(self._health_check_sync)
            return
        async with self._require_engine().connect() as conn:
            await conn.exec_driver_sql(LIVENESS_PROBE_SQL)

    def _health_check_sync(self) -> None:
        with self._require_sync_engine().connect() as conn:
            conn.exec_driver_sql(LIVENESS_PROBE_SQL)

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

    def _run_stage_cycle(
        self, conn: Connection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Hand the shared cycle this flavor's live sync ``Connection``."""
        self._cycle.run(_SqlAlchemyStageConnection(self, conn), plan, batch)

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

    # ---- landing ------------------------------------------------------

    def _land(
        self, conn: Connection, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> int:
        """Land the batch into the stage: declared bulk mechanism first.

        Returns the rows the cycle verifies against the batch. A landing
        that consulted the dialect's untrusted ``bulk_land`` hook reports
        what the stage actually holds — on a claimed land and on a
        decline alike, since a connector that touched the stage before
        declining would otherwise duplicate rows under the executemany
        fallback. The plain executemany landing is the CDK's own and
        reports the batch it just bound.
        """
        if self._bulk_declared:
            if self._runtime is None:
                raise RuntimeError(
                    "SqlAlchemyBackend.execute_write() called before connect()"
                )
            if not consult_bulk_land(
                self._dialect, conn, plan, batch, runtime=self._runtime
            ):
                self._executemany_land(conn, plan, batch)
            return self._stage_row_count(conn, plan)
        self._executemany_land(conn, plan, batch)
        return int(batch.num_rows)

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

    def _stage_row_count(self, conn: Connection, plan: StageWritePlan) -> int:
        count = conn.exec_driver_sql(
            render_row_count_sql(self._dialect, plan.stage)
        ).scalar_one()
        return int(count)

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


class _SqlAlchemyStageConnection:
    """One live sync ``Connection``, as the stage cycle's steps.

    SQLAlchemy's ``Connection`` autobegins on the first statement, so the
    transactional path's steps join one transaction and the closing
    :meth:`commit` ends it; on the non-transactional path each step
    commits its own unit.
    """

    def __init__(self, backend: SqlAlchemyBackend, conn: Connection) -> None:
        self._backend = backend
        self._conn = conn

    def run_statement(self, sql: str, *, commit: bool) -> None:
        """Execute *sql*, committing it as its own unit when *commit*."""
        self._conn.exec_driver_sql(sql)
        if commit:
            self.commit()

    def land_rows(self, plan: StageWritePlan, batch: pa.RecordBatch) -> int:
        """Land *batch* by the declared mechanism; report the staged rows."""
        return self._backend._land(self._conn, plan, batch)

    def commit(self) -> None:
        """Commit the connection's current transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Roll the connection's current transaction back."""
        self._conn.rollback()

    def invalidate(self) -> None:
        """Discard the pooled DBAPI connection behind this ``Connection``."""
        self._conn.invalidate()
