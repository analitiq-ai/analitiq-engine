"""AdbcBackend: stage-cycle mechanics over a fake ADBC DBAPI connection.

The backend executes the facade's ``StageWritePlan``s; these tests pin
its transport mechanics (issue #389, ADR sql-write-path-v2 §6-§7):

* the declared transaction shape — one transaction spanning every step,
  or per-step commits with the pre-flight drop;
* landing by declared mechanism — ``adbc_ingest``, a dialect
  ``bulk_land`` hook (verified, with the visible executemany fallback),
  or executemany;
* the issue-#377 session-schema guard at the backend's ingest site;
* the issue-#379 cleanup and poisoning rules on both sides of the mode
  statement;
* reopen-after-poison self-healing for every operation.
"""

from __future__ import annotations

import logging

import pyarrow as pa
import pytest

from cdk.adbc_registry import AdbcConfigurationError
from cdk.sql import adbc_backend as backend_module
from cdk.sql.adbc_backend import AdbcBackend
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect
from cdk.sql.write_plan import build_stage_write_plan

from .conftest import caps_block


# Bare PEP-249 class names: the classifier matches on type(exc).__name__.
class ProgrammingError(Exception):
    pass


class OperationalError(Exception):
    pass


class _StageDialect(SqlDialect):
    """Per-statement targeting dialect with the two rendering hooks."""

    name = "stagefix"

    def stage_table_sql(self, stage, target, *, temp):
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return (
            f"{keyword} {self.quote_table(stage)} AS SELECT * FROM "
            f"{self.quote_table(target)} WHERE FALSE"
        )

    def merge_statement_sql(self, stage, target, conflict_keys, columns):
        on = " AND ".join(
            f"t.{self.quote_ident(k)} = s.{self.quote_ident(k)}" for k in conflict_keys
        )
        return (
            f"MERGE INTO {self.quote_table(target)} t "
            f"USING {self.quote_table(stage)} s ON {on}"
        )


class _SessionDefaultDialect(_StageDialect):
    """Opts out of per-statement ingest targeting (stands in for Snowflake)."""

    name = "sessionfix"

    def adbc_ingest_kwargs(self, address):
        return {}


class _FoldingSessionDefaultDialect(_SessionDefaultDialect):
    """Case-folding session-default system (uppercase like Snowflake)."""

    name = "foldingfix"

    def normalize_ident(self, name: str) -> str:
        return name.upper()


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *args):
        self._conn.statements.append(sql)
        self._conn.last_sql = sql
        if sql == self._conn.probe_sql:
            self._conn.probe_count += 1
            if self._conn.probe_error is not None:
                raise self._conn.probe_error
            return
        hook = self._conn.execute_hook
        if hook is not None:
            hook(sql)

    def executemany(self, sql, params):
        self._conn.statements.append(sql)
        self._conn.last_sql = sql
        self._conn.executemany_params.append(list(params))

    def adbc_ingest(self, table, batch, mode, **kwargs):
        self._conn.ingests.append({"table": table, "mode": mode, "kwargs": kwargs})

    def fetchone(self):
        if self._conn.last_sql == self._conn.probe_sql:
            return None if self._conn.no_row else (self._conn.session_schema,)
        if self._conn.fetch_value is not None:
            return (self._conn.fetch_value,)
        return None

    @property
    def description(self):
        return self._conn.description

    def close(self):
        pass


class _FakeConn:
    """Programmable ADBC DBAPI fake.

    ``execute_hook(sql)`` may raise to inject a statement failure;
    ``commit_hook()`` likewise for commits. The session-schema probe is
    matched by exact SQL and counted, so the once-per-connection cache
    is observable.
    """

    def __init__(
        self,
        *,
        probe_sql=None,
        session_schema=None,
        probe_error=None,
        no_row=False,
        execute_hook=None,
        commit_hook=None,
        description=(),
        fetch_value=None,
    ):
        self.probe_sql = probe_sql
        self.session_schema = session_schema
        self.probe_error = probe_error
        self.no_row = no_row
        self.execute_hook = execute_hook
        self.commit_hook = commit_hook
        self.description = description
        self.fetch_value = fetch_value
        self.statements: list[str] = []
        self.ingests: list[dict] = []
        self.executemany_params: list[list] = []
        self.probe_count = 0
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.last_sql = ""

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        if self.commit_hook is not None:
            self.commit_hook()
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class _FakeRuntime:
    """Hands out fake connections; records how many were opened."""

    def __init__(self, conns):
        self._conns = list(conns)
        self.opened = 0

    def open_adbc_connection(self):
        self.opened += 1
        return self._conns.pop(0)


def _caps(**overrides) -> SqlCapabilities:
    return SqlCapabilities.from_declaration(caps_block(**overrides))


def _plan(
    dialect,
    caps,
    *,
    write_mode="insert",
    conflict_keys=(),
    truncate_now=False,
    schema="public",
    batch_seq=1,
):
    return build_stage_write_plan(
        dialect,
        caps,
        target=dialect.table_address("orders", schema=schema),
        columns=("id", "v"),
        write_mode=write_mode,
        conflict_keys=list(conflict_keys),
        identity=["id"],
        truncate_now=truncate_now,
        run_id="r1",
        stream_id="s1",
        batch_seq=batch_seq,
    )


def _batch():
    return pa.RecordBatch.from_pydict({"id": [1, 2], "v": ["a", "b"]})


def _backend(dialect, conn, *, bulk_load="adbc_ingest", runtime=None):
    backend = AdbcBackend(dialect)
    backend._conn = conn
    backend._bulk_load = bulk_load
    backend._runtime = runtime
    return backend


class TestStepwiseCycle:
    """``transactional_ddl: false``: per-step commits with the pre-flight
    drop; deterministic names make retries self-healing."""

    def _run(self, *, truncate_now=False, bulk_load="adbc_ingest"):
        dialect = _StageDialect()
        caps = _caps(bulk_load=bulk_load, stage_scope="real", transactional_ddl=False)
        dialect.capabilities = caps
        plan = _plan(
            dialect,
            caps,
            write_mode="truncate_insert" if truncate_now else "insert",
            truncate_now=truncate_now,
        )
        conn = _FakeConn()
        _backend(dialect, conn, bulk_load=bulk_load)._execute_write_sync(plan, _batch())
        return plan, conn

    def test_step_order_with_preflight_drop(self):
        plan, conn = self._run()
        assert conn.statements[0] == plan.drop_stage_sql  # pre-flight
        assert conn.statements[1] == plan.create_stage_sql
        assert conn.statements[2] == plan.mode_sql
        assert conn.statements[3] == plan.drop_stage_sql  # success drop
        assert conn.ingests[0]["table"] == plan.stage.table
        # drop, create, land, mode, drop — each its own commit.
        assert conn.commits == 5

    def test_truncate_runs_between_landing_and_mode_statement(self):
        plan, conn = self._run(truncate_now=True)
        assert plan.truncate_sql is not None
        truncate_at = conn.statements.index(plan.truncate_sql)
        mode_at = conn.statements.index(plan.mode_sql)
        assert truncate_at < mode_at
        assert conn.commits == 6

    def test_mode_failure_drops_stage_best_effort_and_poisons(self, caplog):
        dialect = _StageDialect()
        caps = _caps(stage_scope="real", transactional_ddl=False)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)

        def fail_mode(sql):
            if sql == plan.mode_sql:
                raise OperationalError("connection reset")

        conn = _FakeConn(execute_hook=fail_mode)
        backend = _backend(dialect, conn)
        with pytest.raises(OperationalError):
            backend._execute_write_sync(plan, _batch())
        # Best-effort drop ran after the rollback, then the poison.
        assert conn.rollbacks == 1
        assert conn.statements[-1] == plan.drop_stage_sql
        assert conn.closed
        assert backend._conn is None

    def test_fatal_failure_is_reclassified(self):
        dialect = _StageDialect()
        caps = _caps(stage_scope="real", transactional_ddl=False)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)

        def fail_mode(sql):
            if sql == plan.mode_sql:
                raise ProgrammingError("syntax error")

        conn = _FakeConn(execute_hook=fail_mode)
        with pytest.raises(AdbcConfigurationError, match="ProgrammingError"):
            _backend(dialect, conn)._execute_write_sync(plan, _batch())

    def test_failed_cleanup_drop_logs_the_honest_note(self, caplog):
        dialect = _StageDialect()
        caps = _caps(stage_scope="real", transactional_ddl=False)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        state = {"failed": False}

        def fail_mode_then_drop(sql):
            if sql == plan.mode_sql:
                state["failed"] = True
                raise OperationalError("connection reset")
            if state["failed"] and sql == plan.drop_stage_sql:
                raise OperationalError("still down")

        conn = _FakeConn(execute_hook=fail_mode_then_drop)
        with caplog.at_level(logging.WARNING, logger=backend_module.logger.name):
            with pytest.raises(OperationalError, match="connection reset"):
                _backend(dialect, conn)._execute_write_sync(plan, _batch())
        warnings = [
            r
            for r in caplog.records
            if "could not be dropped after a failed batch" in r.getMessage()
        ]
        assert len(warnings) == 1
        # Honest, scope-accurate consequence: cleanup-by-retry only while
        # retries remain; a real-scope orphan needs manual action after.
        message = warnings[0].getMessage()
        assert "pre-flight drop" in message
        assert "manual drop" in message


class TestTransactionalCycle:
    """``transactional_ddl: true``: every step in one transaction — no
    pre-flight drop, one commit, nothing left behind on failure."""

    def test_single_commit_and_no_preflight_drop(self):
        dialect = _StageDialect()
        caps = _caps(stage_scope="temp", transactional_ddl=True)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn()
        _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.statements[0] == plan.create_stage_sql
        assert conn.statements[-1] == plan.drop_stage_sql
        assert conn.statements.count(plan.drop_stage_sql) == 1  # in-txn only
        assert conn.commits == 1

    def test_failure_poisons_without_a_cleanup_drop(self):
        # The open transaction dies with the closed connection; no
        # stepwise cleanup runs because nothing was committed.
        dialect = _StageDialect()
        caps = _caps(stage_scope="temp", transactional_ddl=True)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)

        def fail_mode(sql):
            if sql == plan.mode_sql:
                raise OperationalError("connection reset")

        conn = _FakeConn(execute_hook=fail_mode)
        backend = _backend(dialect, conn)
        with pytest.raises(OperationalError):
            backend._execute_write_sync(plan, _batch())
        assert conn.commits == 0
        assert conn.closed
        assert backend._conn is None


class TestLandingMechanisms:
    def test_declared_adbc_ingest_lands_arrow_with_targeting_kwargs(self):
        dialect = _StageDialect()
        caps = _caps(bulk_load="adbc_ingest", stage_scope="real")
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn()
        _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.ingests == [
            {
                "table": plan.stage.table,
                "mode": "append",
                "kwargs": {"db_schema_name": "public"},
            }
        ]
        assert conn.executemany_params == []

    def test_undeclared_mechanism_lands_via_executemany(self):
        dialect = _StageDialect()
        caps = _caps(bulk_load="none", stage_scope="real")
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn()
        _backend(dialect, conn, bulk_load="none")._execute_write_sync(plan, _batch())
        assert conn.ingests == []
        insert = next(s for s in conn.statements if s.startswith("INSERT INTO"))
        assert dialect.quote_table(plan.stage) in insert
        assert insert.endswith("VALUES (?, ?)")
        assert conn.executemany_params == [[(1, "a"), (2, "b")]]

    def test_declared_dialect_mechanism_calls_bulk_land_and_verifies(self):
        calls = []

        class _BulkDialect(_StageDialect):
            def bulk_land(self, conn, stage, batch, *, runtime):
                calls.append((stage, batch.num_rows, runtime))
                return True

        dialect = _BulkDialect()
        caps = _caps(bulk_load="load_job", stage_scope="real")
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn(fetch_value=2)  # COUNT(*) matches the batch
        runtime = _FakeRuntime([])
        _backend(
            dialect, conn, bulk_load="load_job", runtime=runtime
        )._execute_write_sync(plan, _batch())
        assert calls == [(plan.stage, 2, runtime)]
        counts = [s for s in conn.statements if s.startswith("SELECT COUNT(*)")]
        assert len(counts) == 1
        assert conn.ingests == []
        assert conn.executemany_params == []

    def test_bulk_land_count_mismatch_refuses(self):
        class _LyingBulkDialect(_StageDialect):
            def bulk_land(self, conn, stage, batch, *, runtime):
                return True  # claims landed; the stage stays empty

        dialect = _LyingBulkDialect()
        caps = _caps(bulk_load="load_job", stage_scope="real")
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn(fetch_value=0)
        with pytest.raises(AdbcConfigurationError, match="did .*not land|not land"):
            _backend(
                dialect, conn, bulk_load="load_job", runtime=_FakeRuntime([])
            )._execute_write_sync(plan, _batch())

    def test_declined_bulk_land_falls_back_to_executemany(self, caplog):
        class _DecliningBulkDialect(_StageDialect):
            def bulk_land(self, conn, stage, batch, *, runtime):
                return False

        dialect = _DecliningBulkDialect()
        caps = _caps(bulk_load="load_job", stage_scope="real")
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn()
        with caplog.at_level(logging.INFO, logger=backend_module.logger.name):
            _backend(
                dialect, conn, bulk_load="load_job", runtime=_FakeRuntime([])
            )._execute_write_sync(plan, _batch())
        assert conn.executemany_params == [[(1, "a"), (2, "b")]]
        assert any(
            "declined the declared bulk land" in r.message for r in caplog.records
        )


class TestSessionSchemaGuard:
    """Issue #377 at the backend's ingest site: when a dialect opts out of
    per-statement ingest targeting, bare-name ingest resolves against the
    connection's session schema. The invariant *session schema == stage
    schema* is checked before any stage DDL, so a mismatch leaves nothing
    behind."""

    def _setup(self, session_schema, *, dialect_cls=_SessionDefaultDialect, **caps_kw):
        dialect = dialect_cls()
        caps = _caps(session_targeting="session_default", stage_scope="real", **caps_kw)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn(
            probe_sql=dialect.adbc_session_schema_sql(),
            session_schema=session_schema,
        )
        return dialect, plan, conn

    def test_matching_session_schema_ingests_bare_name(self):
        dialect, plan, conn = self._setup("public")
        _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.probe_count == 1
        assert conn.ingests[0]["kwargs"] == {}
        assert conn.ingests[0]["table"] == plan.stage.table

    def test_probe_runs_once_per_connection(self):
        dialect, plan, conn = self._setup("public")
        backend = _backend(dialect, conn)
        backend._execute_write_sync(plan, _batch())
        backend._execute_write_sync(plan, _batch())
        assert conn.probe_count == 1
        assert len(conn.ingests) == 2

    def test_mismatch_refuses_before_any_stage_ddl(self):
        dialect, plan, conn = self._setup("other")
        backend = _backend(dialect, conn)
        with pytest.raises(AdbcConfigurationError) as exc:
            backend._execute_write_sync(plan, _batch())
        assert "'other'" in str(exc.value)
        assert "'public'" in str(exc.value)
        assert conn.ingests == []
        assert not any("CREATE" in s for s in conn.statements)
        # Poisoned like today: the next operation reopens.
        assert conn.closed
        assert backend._conn is None

    def test_no_session_schema_refuses(self):
        dialect, plan, conn = self._setup(None)
        with pytest.raises(AdbcConfigurationError, match="no schema selected"):
            _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.ingests == []

    def test_probe_returning_no_row_is_no_schema(self):
        dialect, plan, conn = self._setup(None)
        conn.no_row = True
        with pytest.raises(AdbcConfigurationError, match="no schema selected"):
            _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.ingests == []

    def test_session_value_is_dialect_normalized(self):
        # CURRENT_SCHEMA() reporting a spelling that needs folding still
        # matches the normalized address.
        dialect, plan, conn = self._setup(
            "public", dialect_cls=_FoldingSessionDefaultDialect
        )
        _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert len(conn.ingests) == 1

    def test_poison_resets_the_probe_cache(self):
        dialect, plan, conn = self._setup("public")
        backend = _backend(dialect, conn)
        backend._execute_write_sync(plan, _batch())
        assert backend._session_schema_known
        backend._poison_sync()
        assert not backend._session_schema_known
        assert backend._session_schema is None
        assert conn.closed
        # A replacement connection with a diverged session schema is
        # re-probed and refused — the cache never outlives its connection.
        replacement = _FakeConn(
            probe_sql=dialect.adbc_session_schema_sql(), session_schema="other"
        )
        backend._conn = replacement
        with pytest.raises(AdbcConfigurationError):
            backend._execute_write_sync(plan, _batch())

    def test_targeting_dialect_never_probes(self):
        dialect = _StageDialect()
        caps = _caps(stage_scope="real")
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn(probe_sql=dialect.adbc_session_schema_sql())
        _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.probe_count == 0
        assert conn.ingests[0]["kwargs"] == {"db_schema_name": "public"}

    def test_temp_scope_stage_skips_the_probe(self):
        # A temp-scope stage carries no schema: the session default IS the
        # intent, so the guard stays out of the way on an opt-out dialect.
        dialect = _SessionDefaultDialect()
        caps = _caps(
            session_targeting="session_default",
            stage_scope="temp",
            transactional_ddl=False,
        )
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        assert plan.stage.schema == ""
        conn = _FakeConn(probe_sql=dialect.adbc_session_schema_sql())
        _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.probe_count == 0
        assert len(conn.ingests) == 1

    def test_undeclared_session_targeting_refuses_bare_name(self):
        dialect = _SessionDefaultDialect()
        caps = _caps(session_targeting="session_default", stage_scope="real")
        plan = _plan(dialect, caps)
        dialect.capabilities = None  # undeclared at the binding
        conn = _FakeConn(probe_sql=dialect.adbc_session_schema_sql())
        with pytest.raises(
            AdbcConfigurationError, match="sql_capabilities.session_targeting"
        ):
            _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.probe_count == 0
        assert conn.ingests == []

    def test_per_statement_declaration_with_bare_name_is_a_connector_defect(self):
        dialect = _SessionDefaultDialect()  # returns no targeting kwargs
        caps = _caps(session_targeting="per_statement", stage_scope="real")
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        conn = _FakeConn(probe_sql=dialect.adbc_session_schema_sql())
        with pytest.raises(AdbcConfigurationError, match="disagree"):
            _backend(dialect, conn)._execute_write_sync(plan, _batch())
        assert conn.probe_count == 0
        assert conn.ingests == []

    def test_probe_fatal_driver_error_reclassified(self):
        dialect, plan, conn = self._setup("public")
        conn.probe_error = ProgrammingError("unknown function CURRENT_SCHEMA")
        backend = _backend(dialect, conn)
        with pytest.raises(AdbcConfigurationError, match="ProgrammingError"):
            backend._execute_write_sync(plan, _batch())
        assert conn.ingests == []
        assert conn.closed
        assert not backend._session_schema_known  # retry re-probes

    def test_probe_transient_error_propagates_and_poisons(self):
        dialect, plan, conn = self._setup("public")
        conn.probe_error = OperationalError("connection reset")
        backend = _backend(dialect, conn)
        with pytest.raises(OperationalError):
            backend._execute_write_sync(plan, _batch())
        assert conn.ingests == []
        assert conn.closed
        assert not backend._session_schema_known


class TestSuccessPathDropRules:
    """Stage-DROP failure handling after a committed mode statement
    (issue #379, generalized): retry once, then poison with the honest
    orphan log — the batch stays acked either way."""

    def _run(self, *, drop_failures=0, drop_error=None, commit_failures=0, caplog=None):
        dialect = _StageDialect()
        caps = _caps(stage_scope="real", transactional_ddl=False)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        state = {"mode_seen": False, "drop_failures": drop_failures}

        def hook(sql):
            if sql == plan.mode_sql:
                state["mode_seen"] = True
            elif sql == plan.drop_stage_sql and state["mode_seen"]:
                if state["drop_failures"] > 0:
                    state["drop_failures"] -= 1
                    raise drop_error or OperationalError("reset during DROP")

        commit_state = {"left": commit_failures}

        def commit_hook():
            if (
                state["mode_seen"]
                and conn.last_sql == plan.drop_stage_sql
                and commit_state["left"] > 0
            ):
                commit_state["left"] -= 1
                raise OperationalError("reset during DROP commit")

        conn = _FakeConn(execute_hook=hook, commit_hook=commit_hook)
        backend = _backend(dialect, conn)
        backend._execute_write_sync(plan, _batch())
        return plan, conn, backend

    @staticmethod
    def _post_mode_drops(plan, conn):
        mode_at = conn.statements.index(plan.mode_sql)
        return [s for s in conn.statements[mode_at + 1 :] if s == plan.drop_stage_sql]

    def test_single_drop_success_no_retry_no_poison(self, caplog):
        with caplog.at_level(logging.INFO, logger=backend_module.logger.name):
            plan, conn, backend = self._run()
        assert len(self._post_mode_drops(plan, conn)) == 1
        assert not conn.closed
        assert backend._conn is conn
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_drop_retry_second_attempt_succeeds_without_poison(self, caplog):
        with caplog.at_level(logging.INFO, logger=backend_module.logger.name):
            plan, conn, backend = self._run(drop_failures=1)
        assert len(self._post_mode_drops(plan, conn)) == 2
        assert not conn.closed
        assert backend._conn is conn
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        # Both the failed attempt and the recovery leave an INFO footprint.
        infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert any("attempt 1/2" in m for m in infos)
        assert any("second attempt" in m for m in infos)
        # The retry rolled back the aborted transaction first.
        assert conn.rollbacks == 1

    def test_persistent_drop_failure_poisons_and_logs_orphan(self, caplog):
        with caplog.at_level(logging.INFO, logger=backend_module.logger.name):
            plan, conn, backend = self._run(drop_failures=2)  # must NOT raise
        assert len(self._post_mode_drops(plan, conn)) == 2
        assert conn.closed
        assert backend._conn is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        # Honest: names the table, says it is orphaned, promises nothing.
        assert plan.stage.table in message
        assert "orphaned" in message
        assert "never retried" in message
        assert warnings[0].exc_info is not None
        assert isinstance(warnings[0].exc_info[1], OperationalError)

    def test_fatal_named_drop_failure_still_swallowed_and_poisons(self, caplog):
        # A fatal-classified name (ProgrammingError) on the success-path
        # DROP must not be reclassified or raised: the mode statement
        # committed, so the batch acks SUCCESS regardless.
        with caplog.at_level(logging.WARNING, logger=backend_module.logger.name):
            plan, conn, backend = self._run(
                drop_failures=2, drop_error=ProgrammingError("permission denied")
            )
        assert conn.closed
        assert backend._conn is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert isinstance(warnings[0].exc_info[1], ProgrammingError)

    def test_drop_commit_failure_counts_as_failed_drop(self, caplog):
        # A dropped stage means a committed DROP: an executed DROP whose
        # commit is lost must take the same retry-poison-warn path.
        with caplog.at_level(logging.WARNING, logger=backend_module.logger.name):
            plan, conn, backend = self._run(commit_failures=2)
        assert len(self._post_mode_drops(plan, conn)) == 2
        assert conn.closed
        assert backend._conn is None
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1

    def test_temp_scope_orphan_note_names_the_session(self, caplog):
        dialect = _StageDialect()
        caps = _caps(stage_scope="temp", transactional_ddl=False)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)
        state = {"mode_seen": False}

        def hook(sql):
            if sql == plan.mode_sql:
                state["mode_seen"] = True
            elif sql == plan.drop_stage_sql and state["mode_seen"]:
                raise OperationalError("reset during DROP")

        conn = _FakeConn(execute_hook=hook)
        with caplog.at_level(logging.WARNING, logger=backend_module.logger.name):
            _backend(dialect, conn)._execute_write_sync(plan, _batch())
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "dies with the discarded connection" in warnings[0].getMessage()


class TestConnectionLifecycle:
    @pytest.mark.asyncio
    async def test_connect_opens_eagerly_and_reads_the_bulk_declaration(self):
        dialect = _StageDialect()
        dialect.capabilities = _caps(bulk_load="adbc_ingest")
        conn = _FakeConn()
        runtime = _FakeRuntime([conn])
        backend = AdbcBackend(dialect)
        await backend.connect(runtime)
        assert runtime.opened == 1
        assert backend._conn is conn
        assert backend._bulk_load == "adbc_ingest"

    @pytest.mark.asyncio
    async def test_disconnect_closes_and_resets_probe_state(self):
        dialect = _StageDialect()
        conn = _FakeConn()
        backend = _backend(dialect, conn)
        backend._session_schema = "public"
        backend._session_schema_known = True
        await backend.disconnect()
        assert conn.closed
        assert backend._conn is None
        assert not backend._session_schema_known

    def test_poisoned_connection_reopens_on_next_operation(self):
        dialect = _StageDialect()
        caps = _caps(stage_scope="real", transactional_ddl=False)
        dialect.capabilities = caps
        plan = _plan(dialect, caps)

        first = _FakeConn(
            execute_hook=lambda sql: (_ for _ in ()).throw(OperationalError("down"))
        )
        second = _FakeConn()
        runtime = _FakeRuntime([second])
        backend = _backend(dialect, first, runtime=runtime)
        with pytest.raises(OperationalError):
            backend._execute_write_sync(plan, _batch())
        assert first.closed
        # Self-healing: the next cycle opens and uses a fresh connection.
        backend._execute_write_sync(plan, _batch())
        assert runtime.opened == 1
        assert second.ingests[0]["table"] == plan.stage.table

    def test_no_connection_and_no_runtime_fails_loud(self):
        backend = AdbcBackend(_StageDialect())
        with pytest.raises(AdbcConfigurationError, match="Runtime not available"):
            backend._require_conn_sync()


class TestDdlAndProbes:
    @pytest.mark.asyncio
    async def test_run_ddl_executes_all_statements_with_one_commit(self):
        conn = _FakeConn()
        backend = _backend(_StageDialect(), conn)
        await backend.run_ddl(["CREATE SCHEMA s", "CREATE TABLE t (id INT)"])
        assert conn.statements == ["CREATE SCHEMA s", "CREATE TABLE t (id INT)"]
        assert conn.commits == 1

    @pytest.mark.asyncio
    async def test_run_ddl_reclassifies_fatal_and_poisons(self):
        conn = _FakeConn(
            execute_hook=lambda sql: (_ for _ in ()).throw(ProgrammingError("bad ddl"))
        )
        backend = _backend(_StageDialect(), conn)
        with pytest.raises(AdbcConfigurationError, match="ProgrammingError"):
            await backend.run_ddl(["CREATE TABLE t (id INT)"])
        assert conn.closed
        assert backend._conn is None

    @pytest.mark.asyncio
    async def test_target_columns_reads_the_cursor_description(self):
        dialect = _StageDialect()
        conn = _FakeConn(description=(("id", None), ("_record_hash", None)))
        backend = _backend(dialect, conn)
        cols = await backend.target_columns(
            dialect.table_address("orders", schema="public")
        )
        assert cols == ("id", "_record_hash")
        probe = conn.statements[0]
        assert probe == 'SELECT * FROM "public"."orders" WHERE 1=0'

    @pytest.mark.asyncio
    async def test_health_check_probes_and_poisons_on_failure(self):
        conn = _FakeConn()
        backend = _backend(_StageDialect(), conn)
        await backend.health_check()
        assert conn.statements == ["SELECT 1"]

        sick = _FakeConn(
            execute_hook=lambda sql: (_ for _ in ()).throw(OperationalError("down"))
        )
        backend._conn = sick
        with pytest.raises(OperationalError):
            await backend.health_check()
        assert sick.closed
        assert backend._conn is None
