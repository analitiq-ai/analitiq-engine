"""The stage cycle's own rules, certified once for both transports.

``StageCycle`` owns the step order, the cleanup, the poisoning rule and
the honest orphan log that the two transport backends used to carry a copy
of each. These tests drive the cycle through its own interface — a real
``StageConnection`` running actual SQL against a real SQLite database, not
mocks of a connection surface — so a rule proven here holds on every
transport that satisfies the protocol.

One test per settled decision, plus the cleanup rules the two per-backend
suites used to assert twice:

* the poisoning rule is path-scoped (non-transactional only);
* the generic ANSI probes are rendered above the transport seam, through
  the dialect's identifier quoting, and the backends only execute them;
* ``target_columns`` stays a backend member — each transport reads it its
  own way, outside any write cycle.
"""

from __future__ import annotations

import logging
import sqlite3
from uuid import uuid4

import pyarrow as pa
import pytest

from cdk.adbc_registry import AdbcConfigurationError
from cdk.sql.backend import TransportBackend
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.stage_cycle import (
    LIVENESS_PROBE_SQL,
    StageCycle,
    render_column_probe_sql,
    render_landing_insert_sql,
    render_row_count_sql,
)
from cdk.sql.write_plan import build_stage_write_plan

from .conftest import caps_block

TARGET = TableAddress(table="events")
TARGET_DDL = "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)"
CYCLE_LOGGER = "cdk.sql.stage_cycle"


class _SqliteDialect(SqlDialect):
    """The two rendering hooks the primitive needs, spoken by SQLite."""

    name = "sqlite_cycle"

    def stage_table_sql(self, stage, target, *, temp):
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return (
            f"{keyword} {self.quote_table(stage)} AS "
            f"SELECT * FROM {self.quote_table(target)} WHERE 0"
        )

    def merge_statement_sql(self, stage, target, conflict_keys, columns):
        col_list = ", ".join(self.quote_ident(c) for c in columns)
        keys = ", ".join(self.quote_ident(c) for c in conflict_keys)
        return (
            f"INSERT INTO {self.quote_table(target)} ({col_list}) "
            f"SELECT {col_list} FROM {self.quote_table(stage)} "
            f"ON CONFLICT ({keys}) DO NOTHING"
        )


class _BacktickDialect(_SqliteDialect):
    """A system whose identifier quote is not the ANSI double quote."""

    name = "backtick"
    quote_char = "`"


class _StatementFailure(Exception):
    """What a driver raises when a statement cannot run."""


class _SqliteStageConnection:
    """A real ``StageConnection`` over a real SQLite connection.

    Everything a transport backend does for the cycle, done for real:
    statements execute, commits and rollbacks are the database's own, the
    executemany landing binds the shared renderer's ``INSERT``. What it
    adds is observability — the statements it was handed, and how often
    the cycle committed, rolled back, or discarded it — plus a hook to
    fail a chosen statement the way a driver would.
    """

    def __init__(self, raw: sqlite3.Connection, dialect: SqlDialect, *, fail_hook=None):
        self._raw = raw
        self._dialect = dialect
        # Called with each statement before it runs; raises to inject the
        # driver failure a test is about.
        self._fail_hook = fail_hook
        self._in_txn = False
        #: A landing may be told to report a count that is not the
        #: batch's, the way an untrusted bulk mechanism can.
        self.land_report: int | None = None
        self.statements: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self.invalidations = 0

    # ---- the StageConnection protocol ---------------------------------

    def run_statement(self, sql: str, *, commit: bool) -> None:
        self._execute(sql)
        if commit:
            self.commit()

    def land_rows(self, plan, batch: pa.RecordBatch) -> int:
        sql = render_landing_insert_sql(self._dialect, plan.stage, plan.columns)
        rows = [tuple(row[c] for c in plan.columns) for row in batch.to_pylist()]
        self.statements.append(sql)
        self._begin()
        self._raw.executemany(sql, rows)
        if self.land_report is not None:
            return self.land_report
        return self._staged_rows(plan)

    def commit(self) -> None:
        self.commits += 1
        if self._in_txn:
            self._raw.commit()
            self._in_txn = False

    def rollback(self) -> None:
        self.rollbacks += 1
        if self._in_txn:
            self._raw.rollback()
            self._in_txn = False

    def invalidate(self) -> None:
        self.invalidations += 1
        # Closing discards the connection and any transaction on it,
        # which is what every transport's poison amounts to.
        self._in_txn = False
        self._raw.close()

    # ---- the transport mechanics behind it ----------------------------

    def _staged_rows(self, plan) -> int:
        """Read what the stage holds, with the probe rendered above the seam."""
        sql = render_row_count_sql(self._dialect, plan.stage)
        self.statements.append(sql)
        return int(self._raw.execute(sql).fetchone()[0])

    def _execute(self, sql: str) -> None:
        self.statements.append(sql)
        if self._fail_hook is not None:
            self._fail_hook(sql)
        self._begin()
        self._raw.execute(sql)

    def _begin(self) -> None:
        # Explicit transactions: sqlite3's autocommit default would make
        # rollback a no-op and the transactional shape a fiction.
        if not self._in_txn:
            self._raw.execute("BEGIN")
            self._in_txn = True


class _Database:
    """A shared-cache in-memory database that outlives a discarded connection."""

    def __init__(self) -> None:
        self._uri = f"file:cycle_{uuid4().hex}?mode=memory&cache=shared"
        self._anchor = self._connect()
        self._anchor.execute(TARGET_DDL)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(
            self._uri, uri=True, check_same_thread=False, isolation_level=None
        )

    def connection(self, dialect: SqlDialect, **kwargs) -> _SqliteStageConnection:
        return _SqliteStageConnection(self._connect(), dialect, **kwargs)

    def rows(self) -> list[tuple]:
        return sorted(self._anchor.execute("SELECT id, name FROM events").fetchall())

    def table_names(self) -> set[str]:
        return {
            row[0]
            for row in self._anchor.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    def close(self) -> None:
        self._anchor.close()


@pytest.fixture
def db():
    database = _Database()
    yield database
    database.close()


def _caps(*, transactional: bool, scope: str = "real") -> SqlCapabilities:
    return SqlCapabilities.from_declaration(
        caps_block(
            merge_form="insert_on_conflict",
            stage_scope=scope,
            transactional_ddl=transactional,
        )
    )


def _plan(dialect, caps, *, batch_seq: int = 1, write_mode: str = "insert"):
    return build_stage_write_plan(
        dialect,
        caps,
        target=TARGET,
        columns=("id", "name"),
        write_mode=write_mode,
        conflict_keys=[],
        identity=["id"],
        truncate_now=False,
        run_id="r1",
        stream_id="s1",
        batch_seq=batch_seq,
    )


def _batch(rows=None) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows or [{"id": 1, "name": "a"}])


def _stage_leftovers(db: _Database) -> set[str]:
    return {name for name in db.table_names() if name.startswith("_analitiq_stage_")}


def _fail_statement(sql: str):
    """Fail *sql* the first time it runs, the way a driver would."""
    left = {"n": 1}

    def hook(statement: str) -> None:
        if statement == sql and left["n"] > 0:
            left["n"] -= 1
            raise _StatementFailure(f"driver refused: {statement}")

    return hook


def _fail_the_drops_after_the_mode_statement(plan, times: int):
    """Fail only the post-merge drops, never the pre-flight one."""
    state = {"merged": False, "left": times}

    def hook(statement: str) -> None:
        if statement == plan.mode_sql:
            state["merged"] = True
        elif state["merged"] and statement == plan.drop_stage_sql and state["left"] > 0:
            state["left"] -= 1
            raise _StatementFailure("driver refused: DROP")

    return hook


class TestPathScopedPoisoning:
    """Decision 2.1: every failed step of the non-transactional path
    discards the connection; no step of the transactional path does."""

    def test_a_failed_step_poisons_only_the_non_transactional_path(self, db):
        dialect = _SqliteDialect()
        results = {}
        for transactional in (True, False):
            caps = _caps(transactional=transactional)
            plan = _plan(dialect, caps, batch_seq=1 if transactional else 2)
            # The mode statement fails the way a driver fails one.
            conn = db.connection(dialect, fail_hook=_fail_statement(plan.mode_sql))
            with pytest.raises(_StatementFailure):
                StageCycle(dialect).run(conn, plan, _batch())
            results[transactional] = conn

        transactional_conn = results[True]
        stepwise_conn = results[False]

        # Non-transactional: unknown session state and a stage that may
        # have committed, so the handle is discarded after a best-effort
        # drop — the next batch must not inherit it.
        assert stepwise_conn.invalidations == 1
        assert stepwise_conn.rollbacks >= 1

        # Transactional: the rollback IS the recovery. Discarding a
        # healthy pooled connection per rejected batch is what no pool
        # does, so the handle survives.
        assert transactional_conn.invalidations == 0
        assert transactional_conn.rollbacks == 1

        # Either way the failure is honest and nothing is left behind.
        assert db.rows() == []
        assert _stage_leftovers(db) == set()

    def test_the_twice_failed_success_drop_poisons_too(self, db, caplog):
        # Still the non-transactional rule: a DROP that will not run
        # means a possibly-dead connection, even though the batch is
        # acked — the mode statement committed.
        dialect = _SqliteDialect()
        caps = _caps(transactional=False)
        plan = _plan(dialect, caps)
        conn = db.connection(
            dialect, fail_hook=_fail_the_drops_after_the_mode_statement(plan, 2)
        )
        with caplog.at_level(logging.INFO, logger=CYCLE_LOGGER):
            StageCycle(dialect).run(conn, plan, _batch())
        assert db.rows() == [(1, "a")]
        assert conn.invalidations == 1
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert plan.stage.table in warnings[0]
        assert "orphaned" in warnings[0]


class TestGenericProbesRenderedAboveTheSeam:
    """Decision 2.2: the four generic ANSI statements are rendered once
    above the transport seam, through the dialect's identifier quoting;
    a backend executes them and renders nothing of its own."""

    def test_the_probes_carry_the_dialect_quoting_and_the_cycle_hands_them_down(
        self, db
    ):
        dialect = _BacktickDialect()
        stage = TableAddress(table="stg")
        assert LIVENESS_PROBE_SQL == "SELECT 1"
        assert render_row_count_sql(dialect, stage) == "SELECT COUNT(*) FROM `stg`"
        assert (
            render_column_probe_sql(dialect, stage) == "SELECT * FROM `stg` WHERE 1=0"
        )
        assert render_landing_insert_sql(dialect, stage, ("id", "name")) == (
            "INSERT INTO `stg` (`id`, `name`) VALUES (?, ?)"
        )

        # And a full cycle over a real database runs on those renderings
        # alone: the landing INSERT and the verification aggregate the
        # connection executed are the rendered strings, not text a
        # backend composed.
        caps = _caps(transactional=False)
        plan = _plan(dialect, caps)
        conn = db.connection(dialect)
        StageCycle(dialect).run(conn, plan, _batch())
        assert db.rows() == [(1, "a")]
        assert render_landing_insert_sql(dialect, plan.stage, plan.columns) in (
            conn.statements
        )
        assert render_row_count_sql(dialect, plan.stage) in conn.statements

    def test_a_landing_that_does_not_match_the_batch_is_refused(self, db):
        # What the verification exists for: the count the landing reports
        # is checked against the batch before the mode statement runs, so
        # an untrusted mechanism cannot ack rows it never landed.
        dialect = _SqliteDialect()
        caps = _caps(transactional=False)
        plan = _plan(dialect, caps)
        conn = db.connection(dialect)
        conn.land_report = 0
        with pytest.raises(AdbcConfigurationError, match="did not land"):
            StageCycle(dialect).run(conn, plan, _batch())
        assert db.rows() == []
        assert _stage_leftovers(db) == set()


class TestBackendSurface:
    """Decision 2.3: the transport backend keeps six members — the cycle
    took the seven duplicated sync bodies, not ``target_columns``, which
    every transport reads its own way and outside any write cycle."""

    def test_transport_backend_declares_the_six_members(self):
        assert TransportBackend.__abstractmethods__ == frozenset(
            {
                "connect",
                "disconnect",
                "run_ddl",
                "target_columns",
                "health_check",
                "execute_write",
            }
        )


class TestCleanupRules:
    """The cleanup and orphan-log rules, asserted once for both
    transports instead of once per backend."""

    def test_a_failed_step_drops_the_stage_best_effort(self, db):
        dialect = _SqliteDialect()
        caps = _caps(transactional=False)
        plan = _plan(dialect, caps)
        conn = db.connection(dialect, fail_hook=_fail_statement(plan.mode_sql))
        with pytest.raises(_StatementFailure):
            StageCycle(dialect).run(conn, plan, _batch())
        # The batch fails with the step's own error and the stage is gone.
        assert _stage_leftovers(db) == set()

    def test_a_failed_cleanup_drop_logs_the_scope_accurate_consequence(self, db, caplog):
        dialect = _SqliteDialect()
        caps = _caps(transactional=False)
        plan = _plan(dialect, caps)
        def fail_the_mode_then_the_cleanup_drop(statement: str) -> None:
            if statement == plan.mode_sql:
                raise _StatementFailure("driver refused: the mode statement")
            if statement == plan.drop_stage_sql and plan.mode_sql in conn.statements:
                raise _StatementFailure("driver refused: the cleanup drop")

        conn = db.connection(dialect, fail_hook=fail_the_mode_then_the_cleanup_drop)
        with (
            caplog.at_level(logging.WARNING, logger=CYCLE_LOGGER),
            pytest.raises(_StatementFailure, match="the mode statement"),
        ):
            StageCycle(dialect).run(conn, plan, _batch())
        message = "\n".join(
            r.getMessage()
            for r in caplog.records
            if "could not be dropped after a failed batch" in r.getMessage()
        )
        assert "pre-flight drop" in message
        assert "manual drop" in message

    def test_a_second_drop_attempt_recovers_without_poisoning(self, db, caplog):
        dialect = _SqliteDialect()
        caps = _caps(transactional=False)
        plan = _plan(dialect, caps)
        conn = db.connection(
            dialect, fail_hook=_fail_the_drops_after_the_mode_statement(plan, 1)
        )
        with caplog.at_level(logging.INFO, logger=CYCLE_LOGGER):
            StageCycle(dialect).run(conn, plan, _batch())
        messages = [r.getMessage() for r in caplog.records]
        assert any("attempt 1/2" in m for m in messages)
        assert any("succeeded on the second attempt" in m for m in messages)
        assert conn.invalidations == 0
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert _stage_leftovers(db) == set()

    def test_a_temp_scope_orphan_note_names_the_session(self, db, caplog):
        dialect = _SqliteDialect()
        caps = _caps(transactional=False, scope="temp")
        plan = _plan(dialect, caps)
        conn = db.connection(
            dialect, fail_hook=_fail_the_drops_after_the_mode_statement(plan, 2)
        )
        with caplog.at_level(logging.WARNING, logger=CYCLE_LOGGER):
            StageCycle(dialect).run(conn, plan, _batch())
        warnings = [r.getMessage() for r in caplog.records]
        assert len(warnings) == 1
        assert "dies with the discarded connection" in warnings[0]
        assert "orphaned" not in warnings[0]
