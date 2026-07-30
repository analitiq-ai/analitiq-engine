"""ADBC transport backend for the stage-then-merge write primitive.

``AdbcBackend`` executes :class:`~cdk.sql.backend.StageWritePlan`s over a
direct ADBC DBAPI connection (issue #389, spec sql-write-path). It owns
everything transport-mechanical the facade must never touch: the cached
connection, its locks, reopen/poison handling after failures, the
session-schema invariant probe (issue #377), and this transport's landing
mechanisms. It holds no write-mode logic — which statement runs is the
plan's, decided by the facade — and no step order of its own: the cycle is
:class:`~cdk.sql.stage_cycle.StageCycle`, shared with the SQLAlchemy
transport.

Landing into the stage is by this family's declared mechanism
(``sql_capabilities.bulk_load.adbc``): ``"adbc_ingest"`` uses
``cursor.adbc_ingest`` (Arrow straight through — the sanctioned home of
what used to be an ADBC-private code path), any other declared mechanism
is offered to the dialect's ``bulk_land`` hook, and an undeclared family
lands via executemany ``INSERT``. Those are mechanisms only: what to do
with a decline, and when to verify what landed, is the cycle's one rule.
Stage contents are identical whichever mechanism lands them.

PEP-249 reports ``threadsafety = 1`` for every ADBC driver we ship —
"threads may share the module, but not connections" — so one operation
lock serializes ALL cursor work on the cached connection. The whole stage
cycle runs on a worker thread holding that lock, which is also what makes
retries safe against a still-running prior attempt: the thread itself
releases the lock, so a cancelled ``await`` leaves the cycle running to
completion and the retry's cycle blocks until it finishes (the ADR §6
mutual-exclusion rule) — no asyncio shielding needed.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, NoReturn

from cdk.adbc_registry import AdbcConfigurationError
from cdk.declarations import ErrorMap, error_map_for

from ._adbc_utils import (
    _close_cursor_quietly,
    _is_fatal_adbc_error,
    _reclassify_as_fatal,
)
from .backend import StageWritePlan, TransportBackend, iter_landing_chunks
from .capabilities import undeclared_capability_error
from .dialects import SqlDialect, TableAddress
from .stage_cycle import (
    LIVENESS_PROBE_SQL,
    BulkLandOutcome,
    StageCycle,
    offer_bulk_land,
    render_column_probe_sql,
    render_landing_insert_sql,
    render_row_count_sql,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from cdk.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)


class AdbcBackend(TransportBackend):
    """Stage-then-merge over a cached ADBC DBAPI connection.

    Every operation outside the stage cycle is poison-aware: a failure
    closes the cached handle (a possibly-dead connection must not serve
    the next batch) and the next operation reopens through the runtime,
    so writes are self-healing. Inside the cycle, discarding the handle
    is the cycle's own path-scoped rule, satisfied here by the same
    poison. Deterministic PEP-249 failure classes are reclassified as
    :class:`AdbcConfigurationError` at this boundary so the facade's ack
    ladder marks them fatal instead of retrying forever — except a
    write-cycle failure claimed by the connector's declared ``error_map``
    (issue #401), which propagates raw so the ladder derives the declared
    verdict instead.
    """

    def __init__(self, dialect: SqlDialect) -> None:
        self._dialect = dialect
        self._cycle = StageCycle(dialect)
        self._runtime: ConnectionRuntime | None = None
        # Cached ADBC DBAPI connection, opened eagerly in connect() so a
        # bad credential fails there, not on the first batch. Nulled on
        # any failure under _conn_lock so the next operation reopens
        # instead of reusing a poisoned handle.
        self._conn: Any = None
        # Guards mutations of ``self._conn`` from worker threads: two
        # concurrent failures must not double-close one DBAPI handle
        # (libpq segfault risk) and two concurrent reopens must not leak
        # a second connection. Sync because the protected sections run
        # off the event loop.
        self._conn_lock: threading.Lock = threading.Lock()
        # Serializes ALL cursor operations on the cached connection
        # (PEP-249 threadsafety=1). Also the backend's write lock: the
        # whole stage cycle holds it, so at most one cycle runs at a
        # time and a retry only ever meets a completed or abandoned
        # stage, never a live one.
        self._op_lock: threading.Lock = threading.Lock()
        # Session schema reported by the live connection, probed lazily
        # the first time a session-default dialect needs the issue-#377
        # invariant checked. Valid only for the current ``_conn``: every
        # site that drops the connection resets both fields. ``_known``
        # distinguishes "not probed yet" from a session that
        # legitimately has no schema selected (None).
        self._session_schema: str | None = None
        self._session_schema_known: bool = False
        # This transport family's declared landing mechanism
        # (``sql_capabilities.bulk_load.adbc``), read off the dialect
        # binding at connect(). Only a declared mechanism is ever used;
        # "none" (no adbc entry declared) lands via executemany.
        self._bulk_load: str = "none"
        # The connector's declared error taxonomy (issue #401), parsed at
        # connect(). A declared fact claims a driver failure before the
        # PEP-249 class-name heuristic runs — the raw exception then
        # propagates to the facade's ack ladder, where the declared
        # category derives the verdict.
        self._error_map: ErrorMap | None = None

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """Open the ADBC connection eagerly through *runtime*."""
        self._runtime = runtime
        caps = getattr(self._dialect, "capabilities", None)
        self._bulk_load = (
            caps.bulk_mechanism("adbc") or "none" if caps is not None else "none"
        )
        self._error_map = error_map_for(runtime)
        conn = await asyncio.to_thread(runtime.open_adbc_connection)
        with self._conn_lock:
            self._conn = conn
            self._session_schema = None
            self._session_schema_known = False

    def _reraise_driver_error(self, exc: Exception, *, write_cycle: bool) -> NoReturn:
        """Reraise a driver failure, promoting to fatal only when unclaimed.

        Resolution order per issue #401, on the write cycle only: a declared
        ``error_map`` fact claims the failure first — the raw exception then
        propagates so the facade's ack ladder derives the verdict from the
        declared category. On the DDL / readiness-probe path
        (``write_cycle=False``) nothing downstream consumes a declared
        category — the schema handshake rejects on
        :class:`AdbcConfigurationError` — so the fatal promotion always
        applies there; skipping it would turn a clean schema rejection into
        a raw RPC failure. An exception the PEP-249 class-name heuristic
        does not claim reraises raw either way.
        """
        if (
            write_cycle
            and self._error_map is not None
            and self._error_map.match_exception(exc)
        ):
            raise exc
        if _is_fatal_adbc_error(exc):
            raise _reclassify_as_fatal(exc) from exc
        raise exc

    async def disconnect(self) -> None:
        """Close the cached connection; the facade releases the runtime."""
        with self._conn_lock:
            conn = self._conn
            self._conn = None
            self._session_schema = None
            self._session_schema_known = False
        self._runtime = None
        if conn is not None:
            await asyncio.to_thread(conn.close)

    async def run_ddl(self, statements: Sequence[str]) -> None:
        """Run *statements* through one cursor, committed together."""
        stmts = list(statements)
        if not stmts:
            return
        await asyncio.to_thread(self._run_ddl_sync, stmts)

    async def target_columns(self, target: TableAddress) -> tuple[str, ...]:
        """Probe *target*'s column names via a zero-row SELECT.

        The shared zero-row probe + ``cursor.description`` is portable
        across ADBC drivers (this transport has no reflection layer) and
        doubles as a post-DDL readiness check: a target that
        IF-NOT-EXISTS skipped but is not selectable (or lacks an
        engine-managed column) surfaces at handshake, not on the first
        batch.
        """
        return await asyncio.to_thread(self._target_columns_sync, target)

    async def health_check(self) -> None:
        """Probe liveness; self-heals a poisoned cached connection.

        Without the reopen, a poisoned cache would fail this probe until
        the next write repopulated it — liveness would lag actual
        reachability by one batch interval.
        """
        await asyncio.to_thread(self._health_check_sync)

    async def execute_write(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Run one full stage cycle for *batch* on a worker thread.

        The thread cannot be cancelled in-band: it finishes or abandons
        its cycle under the op lock (the ADR §6 mutual-exclusion rule),
        so a cancelled ``await`` needs no shielding for correctness — but
        the abandoned attempt's own failure must still reach the log
        with its stage context instead of vanishing into asyncio's
        never-retrieved bucket.
        """
        future = asyncio.ensure_future(
            asyncio.to_thread(self._execute_write_sync, plan, batch)
        )
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            stage_ref = self._dialect.quote_table(plan.stage)

            def _log_abandoned_failure(done: asyncio.Future[None]) -> None:
                exc = None if done.cancelled() else done.exception()
                if exc is not None:
                    logger.warning(
                        "ADBC stage cycle for %s failed while its caller "
                        "was already cancelled",
                        stage_ref,
                        exc_info=exc,
                    )

            future.add_done_callback(_log_abandoned_failure)
            raise

    # ---- sync bodies (worker thread, op lock) --------------------------

    def _execute_write_sync(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Hand the shared cycle this transport's connection, under the lock."""
        with self._op_lock:
            conn = self._require_conn_sync()
            stage_conn = _AdbcStageConnection(
                conn,
                dialect=self._dialect,
                runtime=self._runtime,
                bulk_load=self._bulk_load,
                check_session_schema=self._check_session_schema_sync,
                discard=self._poison_sync,
            )
            try:
                self._cycle.run(stage_conn, plan, batch)
            except Exception as exc:
                self._reraise_driver_error(exc, write_cycle=True)

    # ---- the ingest-mode precondition ----------------------------------

    def _check_session_schema_sync(
        self,
        conn: Any,
        address: TableAddress,
        ingest_kwargs: Mapping[str, Any],
    ) -> None:
        """Guard bare-name ingest against a session/target schema mismatch.

        The declared ``sql_capabilities.session_targeting`` is the
        authority for which regime a bare-name operation is in. When the
        ingest kwargs carry no ``db_schema_name`` while the address has
        a schema, ``adbc_ingest`` resolves the bare table name against
        the connection's session schema; the invariant *session schema
        == target schema* (issue #377) is then checked for a declared
        ``session_default`` system, refused as undeclared when no
        declaration exists, and reported as a connector defect for a
        declared ``per_statement`` system whose dialect failed to
        target the statement. A temp-scope stage carries no schema and
        never reaches the probe.

        The guard is a precondition of *this transport's* ingest mode,
        not a cycle step, so it runs here, immediately before the only
        bare-name statement of the cycle.

        The ``session_default`` probe runs
        :meth:`SqlDialect.adbc_session_schema_sql` once per connection
        (cached; reset whenever the connection is dropped) and compares
        the dialect-normalized result against ``address.schema``.
        """
        if "db_schema_name" in ingest_kwargs or not address.schema:
            return
        caps = getattr(self._dialect, "capabilities", None)
        if caps is None:
            raise AdbcConfigurationError(
                str(
                    undeclared_capability_error(
                        "session_targeting",
                        need=f"a bare-name ADBC operation against {address} "
                        f"must know whether the session or the statement "
                        f"selects the schema",
                    )
                )
            )
        if caps.session_targeting == "per_statement":
            raise AdbcConfigurationError(
                f"connector declares sql_capabilities.session_targeting "
                f"'per_statement', but dialect {self._dialect.name!r} "
                f"returned no db_schema_name targeting kwarg for {address} "
                f"— the declaration and the dialect's adbc_ingest_kwargs "
                f"disagree; fix the connector."
            )
        if not self._session_schema_known:
            cursor = conn.cursor()
            try:
                cursor.execute(self._dialect.adbc_session_schema_sql())
                row = cursor.fetchone()
            finally:
                _close_cursor_quietly(cursor)
            raw = row[0] if row else None
            self._session_schema = self._dialect.normalize_ident(raw) if raw else None
            self._session_schema_known = True
        if self._session_schema != address.schema:
            raise AdbcConfigurationError(
                f"dialect {self._dialect.name!r} does not support per-statement "
                f"ingest targeting, so adbc_ingest resolves bare table names "
                f"against the connection's session schema "
                f"({self._session_schema!r}"
                f"{'' if self._session_schema else ' — no schema selected'}), "
                f"but this write targets schema {address.schema!r}. Refusing to "
                f"ingest into the wrong schema; align the connection's schema "
                f"with the stream's target schema."
            )

    # ---- non-cycle operations ------------------------------------------

    def _run_ddl_sync(self, statements: list[str]) -> None:
        with self._op_lock:
            conn = self._require_conn_sync()
            try:
                cursor = conn.cursor()
                try:
                    for stmt in statements:
                        cursor.execute(stmt)
                    conn.commit()
                finally:
                    _close_cursor_quietly(cursor)
            except Exception as exc:
                self._poison_sync()
                self._reraise_driver_error(exc, write_cycle=False)

    def _target_columns_sync(self, target: TableAddress) -> tuple[str, ...]:
        sql = render_column_probe_sql(self._dialect, target)
        with self._op_lock:
            conn = self._require_conn_sync()
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql)
                    description = cursor.description
                    if description is None:
                        # PEP-249 leaves description None only for
                        # non-queries; None after a SELECT is a driver
                        # defect. Treating it as zero columns would
                        # misdiagnose the target (a bogus "missing
                        # _record_hash" refusal, or a vacuous readiness
                        # pass) — refuse naming the real culprit.
                        raise AdbcConfigurationError(
                            f"ADBC driver returned no result description "
                            f"for the readiness probe of {target}; cannot "
                            f"report the target's columns"
                        )
                finally:
                    _close_cursor_quietly(cursor)
            except Exception as exc:
                self._poison_sync()
                self._reraise_driver_error(exc, write_cycle=False)
        return tuple(col[0] for col in description)

    def _health_check_sync(self) -> None:
        with self._op_lock:
            conn = self._require_conn_sync()
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(LIVENESS_PROBE_SQL)
                    cursor.fetchone()
                finally:
                    _close_cursor_quietly(cursor)
            except Exception:
                self._poison_sync()
                raise

    # ---- connection lifecycle ------------------------------------------

    def _require_conn_sync(self) -> Any:
        """Return the cached connection, reopening a poisoned one on demand.

        The lock guards a check-then-act race: two threads could both
        observe a dropped connection and each open a new one, leaking
        one. The reopen is what makes every operation self-healing after
        a poison.
        """
        with self._conn_lock:
            if self._conn is not None:
                return self._conn
            if self._runtime is None:
                raise AdbcConfigurationError("Runtime not available for ADBC reconnect")
            self._conn = self._runtime.open_adbc_connection()
            return self._conn

    def _poison_sync(self) -> None:
        """Drop and close the cached connection after a failure.

        The next operation re-opens via the runtime. Close runs outside
        the lock so a slow libpq close path doesn't block other threads
        waiting to reopen; the lock ensures only one thread runs the
        close, preventing double-free on libpq handles.
        """
        with self._conn_lock:
            conn = self._conn
            self._conn = None
            self._session_schema = None
            self._session_schema_known = False
        if conn is not None:
            try:
                conn.close()
            except Exception:
                # A failing close on a warehouse ADBC handle is a
                # server-side resource leak (session, libpq fd, gRPC
                # context) operators may need to act on.
                logger.warning(
                    "Discarded poisoned ADBC connection; close failed — "
                    "potential server-side resource leak",
                    exc_info=True,
                )


class _AdbcStageConnection:
    """One cached ADBC connection, as the stage cycle's steps.

    ADBC DBAPI connections carry no transaction object: a statement runs
    on a fresh cursor and :meth:`commit` delimits the unit, which is what
    lets the same cycle body express both the one-transaction and the
    per-step-commit shape.

    It owns this transport's landing outright and holds no handle back
    into the backend. Two things outlive one cycle and therefore stay the
    backend's — the session-schema probe cache, and the cached handle a
    discard drops — so each arrives as a named function to call, not as
    the backend itself.
    """

    def __init__(
        self,
        conn: Any,
        *,
        dialect: SqlDialect,
        runtime: ConnectionRuntime | None,
        bulk_load: str,
        check_session_schema: Callable[[Any, TableAddress, Mapping[str, Any]], None],
        discard: Callable[[], None],
    ) -> None:
        self._conn = conn
        self._dialect = dialect
        self._runtime = runtime
        self._bulk_load = bulk_load
        self._check_session_schema = check_session_schema
        self._discard = discard

    def run_statement(self, sql: str, *, commit: bool) -> None:
        """Execute *sql* on its own cursor, committing when asked."""
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
        finally:
            _close_cursor_quietly(cursor)
        if commit:
            self.commit()

    def offer_bulk_land(
        self, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> BulkLandOutcome:
        """Offer *batch* to the connector's declared bulk mechanism.

        ``adbc_ingest`` is the CDK's own landing rather than connector
        code, so it never reaches the hook: it is what :meth:`land_batch`
        does, and this reports the family as undeclared.
        """
        return offer_bulk_land(
            self._dialect,
            self._conn,
            plan,
            batch,
            runtime=self._runtime,
            declared=self._bulk_load not in ("none", "adbc_ingest"),
        )

    def land_batch(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Land *batch* by this transport's own mechanism.

        ``adbc_ingest`` sends Arrow straight to the driver, targeted by
        the dialect's ingest kwargs and guarded by the issue-#377
        session-schema invariant that bare-name ingest depends on.
        Everything else lands via executemany ``INSERT`` in plan column
        order, chunked by the plan's ``rows_per_statement`` so no
        statement exceeds the connector's declared bind-parameter cap
        (issue #401); an undeclared cap lands the whole batch in one
        statement.
        """
        if self._bulk_load == "adbc_ingest":
            self._ingest(plan, batch)
            return
        sql = render_landing_insert_sql(self._dialect, plan.stage, plan.columns)
        rows = [tuple(row[c] for c in plan.columns) for row in batch.to_pylist()]
        cursor = self._conn.cursor()
        try:
            for chunk in iter_landing_chunks(rows, plan.rows_per_statement):
                cursor.executemany(sql, list(chunk))
        finally:
            _close_cursor_quietly(cursor)

    def _ingest(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        ingest_kwargs = self._dialect.adbc_ingest_kwargs(plan.stage)
        self._check_session_schema(self._conn, plan.stage, ingest_kwargs)
        cursor = self._conn.cursor()
        try:
            cursor.adbc_ingest(plan.stage.table, batch, mode="append", **ingest_kwargs)
        finally:
            _close_cursor_quietly(cursor)

    def stage_row_count(self, plan: StageWritePlan) -> int:
        """Read what the stage actually holds, for the cycle to verify."""
        cursor = self._conn.cursor()
        try:
            cursor.execute(render_row_count_sql(self._dialect, plan.stage))
            row = cursor.fetchone()
        finally:
            _close_cursor_quietly(cursor)
        if row is None or row[0] is None:
            # An aggregate always returns one row; nothing back is a
            # driver defect, and guessing a count here would let an
            # unverified stage ack a full batch.
            raise AdbcConfigurationError(
                f"ADBC driver returned no row for the row-count probe of "
                f"stage {plan.stage}; cannot verify what the declared bulk "
                f"mechanism landed"
            )
        return int(row[0])

    def commit(self) -> None:
        """Commit the statements run since the last commit or rollback."""
        self._conn.commit()

    def rollback(self) -> None:
        """Discard the statements run since the last commit or rollback."""
        self._conn.rollback()

    def invalidate(self) -> None:
        """Close and drop the cached handle; the next operation reopens."""
        self._discard()
