"""ADBC transport backend for the stage-then-merge write primitive.

``AdbcBackend`` executes :class:`~cdk.sql.backend.StageWritePlan`s over a
direct ADBC DBAPI connection (issue #389, ADR sql-write-path-v2). It owns
everything transport-mechanical the facade must never touch: the cached
connection, its locks, reopen/poison handling after failures, the
session-schema invariant probe (issue #377), and the stage-cycle commit
shape. It holds no write-mode logic — which statement runs is the plan's,
decided by the facade.

Landing into the stage is by this family's declared mechanism
(``sql_capabilities.bulk_load.adbc``): ``"adbc_ingest"`` uses
``cursor.adbc_ingest`` (Arrow straight through — the sanctioned home of
what used to be an ADBC-private code path), any other declared mechanism
consults the dialect's ``bulk_land`` hook (executemany fallback when
declined, logged INFO), and an undeclared family lands via executemany
``INSERT``. Stage contents are identical whichever mechanism lands them.

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
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from cdk.adbc_registry import AdbcConfigurationError

from ._adbc_utils import (
    _close_cursor_quietly,
    _is_fatal_adbc_error,
    _reclassify_as_fatal,
)
from .backend import StageWritePlan, TransportBackend, stage_leftover_note
from .capabilities import undeclared_capability_error
from .dialects import SqlDialect, TableAddress

if TYPE_CHECKING:
    import pyarrow as pa

    from cdk.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)


class AdbcBackend(TransportBackend):
    """Stage-then-merge over a cached ADBC DBAPI connection.

    Every operation is poison-aware: a failure closes the cached handle
    (a possibly-dead connection must not serve the next batch) and the
    next operation reopens through the runtime, so writes are
    self-healing. Deterministic PEP-249 failure classes are reclassified
    as :class:`AdbcConfigurationError` at this boundary so the facade's
    ack ladder marks them fatal instead of retrying forever.
    """

    def __init__(self, dialect: SqlDialect) -> None:
        self._dialect = dialect
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

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """Open the ADBC connection eagerly through *runtime*."""
        self._runtime = runtime
        caps = getattr(self._dialect, "capabilities", None)
        self._bulk_load = (
            caps.bulk_mechanism("adbc") or "none" if caps is not None else "none"
        )
        conn = await asyncio.to_thread(runtime.open_adbc_connection)
        with self._conn_lock:
            self._conn = conn
            self._session_schema = None
            self._session_schema_known = False

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

        ``SELECT * … WHERE 1=0`` + ``cursor.description`` is portable
        across ADBC drivers and doubles as a post-DDL readiness check:
        a target that IF-NOT-EXISTS skipped but is not selectable (or
        lacks an engine-managed column) surfaces at handshake, not on
        the first batch.
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
        with self._op_lock:
            conn = self._require_conn_sync()
            if self._bulk_load == "adbc_ingest":
                # The issue-#377 invariant guard runs before any stage
                # DDL: a bare-name ingest on a session-default system
                # must find the session schema equal to the stage's
                # schema, and a mismatch refuses before anything is
                # created rather than after.
                try:
                    self._check_session_schema_sync(
                        conn, plan.stage, self._dialect.adbc_ingest_kwargs(plan.stage)
                    )
                except Exception as exc:
                    self._poison_sync()
                    if _is_fatal_adbc_error(exc):
                        raise _reclassify_as_fatal(exc) from exc
                    raise
            if plan.transactional:
                self._run_transactional_cycle(conn, plan, batch)
            else:
                self._run_stepwise_cycle(conn, plan, batch)

    def _run_transactional_cycle(
        self, conn: Any, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Every step in one transaction: an interrupted batch leaves nothing.

        The drop joins the transaction, so no pre-flight drop is needed —
        a failed prior attempt rolled its stage back with it. On failure
        the handle is poisoned (closing it discards the open
        transaction server-side) and the next operation reopens.
        """
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(plan.create_stage_sql)
                self._land(conn, cursor, plan, batch)
                if plan.truncate_sql is not None:
                    cursor.execute(plan.truncate_sql)
                cursor.execute(plan.mode_sql)
                cursor.execute(plan.drop_stage_sql)
                conn.commit()
            finally:
                _close_cursor_quietly(cursor)
        except Exception as exc:
            self._poison_sync()
            if _is_fatal_adbc_error(exc):
                raise _reclassify_as_fatal(exc) from exc
            raise

    def _run_stepwise_cycle(
        self, conn: Any, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Per-step commits with the ADR §6 self-healing and poisoning rules.

        Pre-flight ``DROP TABLE IF EXISTS`` clears this same batch's
        leftover stage from a prior attempt (deterministic names make
        retries self-healing); a failed cycle ends with a best-effort
        drop and the batch's own error; a committed mode statement is
        followed by the twice-tried drop with the honest orphan log.
        """
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(plan.drop_stage_sql)
                conn.commit()
                cursor.execute(plan.create_stage_sql)
                conn.commit()
                self._land(conn, cursor, plan, batch)
                conn.commit()
                if plan.truncate_sql is not None:
                    # Committed as its own step: a failure before the
                    # mode statement heals on retry — the same
                    # first-batch plan re-runs the emptying statement.
                    cursor.execute(plan.truncate_sql)
                    conn.commit()
                cursor.execute(plan.mode_sql)
                conn.commit()
            finally:
                _close_cursor_quietly(cursor)
        except Exception as exc:
            # Failure path: best-effort drop, then the batch fails with
            # its own error. The handle is poisoned either way — a
            # failed step means a possibly-dead connection the next
            # batch must not inherit.
            self._drop_stage_after_failure(conn, plan)
            self._poison_sync()
            if _is_fatal_adbc_error(exc):
                raise _reclassify_as_fatal(exc) from exc
            raise
        self._drop_stage_after_success(conn, plan)

    def _land(
        self, conn: Any, cursor: Any, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Land *batch* into the stage by the declared mechanism.

        ``adbc_ingest`` is this backend's own declared mechanism — Arrow
        goes straight to the driver, targeted by the dialect's ingest
        kwargs. Any other declared mechanism is the dialect's
        ``bulk_land`` hook (a pure speed slot; declined falls back to
        executemany with an INFO log — a speed downgrade is visible,
        never silent). Undeclared lands via executemany ``INSERT``.
        """
        if self._bulk_load == "adbc_ingest":
            cursor.adbc_ingest(
                plan.stage.table,
                batch,
                mode="append",
                **self._dialect.adbc_ingest_kwargs(plan.stage),
            )
            return
        if self._bulk_load != "none":
            if self._runtime is None:
                raise RuntimeError(
                    "AdbcBackend.execute_write() called before connect()"
                )
            if self._dialect.bulk_land(conn, plan.stage, batch, runtime=self._runtime):
                self._verify_bulk_land(cursor, plan, batch.num_rows)
                return
            logger.info(
                "dialect %s declined the declared bulk land for %s; landing "
                "via executemany",
                self._dialect.name,
                self._dialect.quote_table(plan.stage),
            )
            self._executemany_land(cursor, plan, batch)
            # A decline must mean "landed nothing": untrusted connector
            # code touched the stage before declining, so the fallback's
            # result is verified like a claimed land — a partial landing
            # under the False return would otherwise silently duplicate
            # rows in the stage.
            self._verify_bulk_land(cursor, plan, batch.num_rows)
            return
        self._executemany_land(cursor, plan, batch)

    def _executemany_land(
        self, cursor: Any, plan: StageWritePlan, batch: pa.RecordBatch
    ) -> None:
        """Land via one executemany ``INSERT`` in plan column order (the default)."""
        cols = ", ".join(self._dialect.quote_ident(c) for c in plan.columns)
        placeholders = ", ".join("?" for _ in plan.columns)
        # Dialect-quoted identifiers only; values bind via qmark params.
        sql = (
            f"INSERT INTO {self._dialect.quote_table(plan.stage)} "  # nosec B608
            f"({cols}) VALUES ({placeholders})"
        )
        rows = batch.to_pylist()
        cursor.executemany(sql, [tuple(row[c] for c in plan.columns) for row in rows])

    def _verify_bulk_land(
        self, cursor: Any, plan: StageWritePlan, expected_rows: int
    ) -> None:
        """Refuse a stage whose row count is not the batch's after bulk_land ran.

        ``bulk_land`` is untrusted connector code; one cheap aggregate
        closes the gap where its claim — "landed" on ``True``, "landed
        nothing" on a decline — would otherwise go unverified before the
        mode statement runs.
        """
        # Dialect-quoted identifiers only; no user values.
        stage_ref = self._dialect.quote_table(plan.stage)
        cursor.execute(f"SELECT COUNT(*) FROM {stage_ref}")  # nosec B608
        row = cursor.fetchone()
        count = row[0] if row else None
        if count != expected_rows:
            raise AdbcConfigurationError(
                f"dialect {self._dialect.name!r} bulk_land ran for "
                f"{plan.stage} but the stage holds {count} rows, "
                f"expected {expected_rows}; the declared bulk mechanism did "
                f"not land this batch cleanly — fix the connector."
            )

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

    def _drop_stage_after_failure(self, conn: Any, plan: StageWritePlan) -> None:
        """Best-effort stage drop after a failed cycle step (stepwise path).

        Never raises: the batch must fail with the step's own error, not
        with whatever the cleanup ran into. The rollback clears an
        aborted transaction that would otherwise fail the drop on its
        transaction state; a failed rollback means the drop was never
        attempted (and the stage may not even exist if the create step
        was what failed).
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
            cursor = conn.cursor()
            try:
                cursor.execute(plan.drop_stage_sql)
                conn.commit()
            finally:
                _close_cursor_quietly(cursor)
        except Exception:
            logger.warning(
                "stage table %s could not be dropped after a failed batch; %s",
                self._dialect.quote_table(plan.stage),
                stage_leftover_note(plan),
                exc_info=True,
            )

    def _drop_stage_after_success(self, conn: Any, plan: StageWritePlan) -> None:
        """Drop the stage after a committed mode statement (stepwise path).

        Attempted twice; when both fail the connection is poisoned (a
        failed DROP means a possibly-dead connection the next batch must
        not inherit) and the log tells the truth: for real scope the
        named stage table is orphaned until dropped manually or expired;
        a temp-scope stage dies with the discarded session.
        """
        first_exc = self._try_drop_sync(conn, plan)
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
            conn.rollback()
        except Exception:
            logger.warning(
                "rollback before the stage-drop retry for %s failed",
                self._dialect.quote_table(plan.stage),
                exc_info=True,
            )
        second_exc = self._try_drop_sync(conn, plan)
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
        # The honest orphan record comes before the poison: a failing
        # close must never erase the only log naming the leftover table.
        # The batch stays acked either way — the mode statement committed.
        logger.warning(
            "stage table %s could not be dropped after a successful mode "
            "statement (two attempts). The batch is acked and never "
            "retried, so no automatic cleanup reaches this table: %s. The "
            "connection is discarded so the next batch does not inherit it",
            self._dialect.quote_table(plan.stage),
            orphan_note,
            exc_info=second_exc,
        )
        self._poison_sync()

    @staticmethod
    def _try_drop_sync(conn: Any, plan: StageWritePlan) -> Exception | None:
        """One drop attempt, returning the failure instead of raising."""
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(plan.drop_stage_sql)
                conn.commit()
            finally:
                _close_cursor_quietly(cursor)
        except Exception as exc:
            return exc
        return None

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
                if _is_fatal_adbc_error(exc):
                    raise _reclassify_as_fatal(exc) from exc
                raise

    def _target_columns_sync(self, target: TableAddress) -> tuple[str, ...]:
        # Dialect-quoted identifiers only; no user values.
        sql = (
            f"SELECT * FROM {self._dialect.quote_table(target)} WHERE 1=0"  # nosec B608
        )
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
                if _is_fatal_adbc_error(exc):
                    raise _reclassify_as_fatal(exc) from exc
                raise
        return tuple(col[0] for col in description)

    def _health_check_sync(self) -> None:
        with self._op_lock:
            conn = self._require_conn_sync()
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT 1")
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
