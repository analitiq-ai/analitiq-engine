"""Transport-agnostic execution: qmark<->named rewrite, ADBC + SQLAlchemy paths.

The ADBC path is exercised with the in-memory fake DBAPI. The SQLAlchemy path is
covered two ways: a fake async engine that always runs in CI (proving the
named-bind rewrite reaches the driver and the dict-row shaping), and a real
``aiosqlite`` engine when the driver is present (skipped otherwise). The live
postgres validation of the SA path is documented in the PR.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from cdk.sql._adbc_utils import _close_cursor_quietly
from cdk.sql.exceptions import CreateTableError, DiscoveryError
from cdk.sql.execution import _qmark_to_named, execute_ddl, fetch_rows

from .conftest import FakeAdbcRuntime, FakeSaRuntime


def _runtime(*, responder=None, fail_execute=None, fail_close=None) -> FakeAdbcRuntime:
    return FakeAdbcRuntime(
        "postgresql",
        mapper=None,
        responder=responder,
        fail_execute=fail_execute,
        fail_close=fail_close,
    )


def _close_warnings(caplog) -> int:
    """Count the cursor-close WARNINGs the shared helper emitted."""
    return sum(
        1
        for record in caplog.records
        if record.levelno == logging.WARNING
        and "cursor close failed" in record.getMessage()
    )


class TestQmarkRewrite:
    def test_rewrites_each_placeholder_in_order(self):
        sql, binds = _qmark_to_named(
            "SELECT * FROM t WHERE a = ? AND b = ?", ["x", "y"]
        )
        assert sql == "SELECT * FROM t WHERE a = :_p0 AND b = :_p1"
        assert binds == {"_p0": "x", "_p1": "y"}

    def test_no_placeholders(self):
        sql, binds = _qmark_to_named("SELECT 1", [])
        assert sql == "SELECT 1"
        assert binds == {}

    def test_count_mismatch_raises(self):
        with pytest.raises(DiscoveryError, match="count mismatch"):
            _qmark_to_named("WHERE a = ? AND b = ?", ["only-one"])


class TestAdbcFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_rows_and_passes_params(self):
        rows = [{"schema_name": "public"}, {"schema_name": "analytics"}]
        runtime = _runtime(
            responder=lambda sql, params: rows if params == ["arg"] else []
        )
        out = await fetch_rows(
            runtime, "SELECT schema_name FROM x WHERE y = ?", ["arg"]
        )
        assert out == rows
        # One connection opened, the statement ran with the params, conn closed.
        assert len(runtime.connections) == 1
        conn = runtime.connections[0]
        assert conn.executed == [("SELECT schema_name FROM x WHERE y = ?", ["arg"])]
        assert conn.closed is True

    @pytest.mark.asyncio
    async def test_fetch_without_params(self):
        runtime = _runtime(responder=lambda sql, params: [{"n": 1}])
        out = await fetch_rows(runtime, "SELECT 1 AS n", [])
        assert out == [{"n": 1}]
        assert runtime.connections[0].executed == [("SELECT 1 AS n", [])]


class TestAdbcDdl:
    @pytest.mark.asyncio
    async def test_runs_statements_and_commits(self):
        runtime = _runtime()
        await execute_ddl(runtime, ["CREATE TABLE a (x INT)", "CREATE TABLE b (y INT)"])
        conn = runtime.connections[0]
        assert [s for s, _ in conn.executed] == [
            "CREATE TABLE a (x INT)",
            "CREATE TABLE b (y INT)",
        ]
        assert conn.commits == 1
        assert conn.closed is True

    @pytest.mark.asyncio
    async def test_single_string_statement(self):
        runtime = _runtime()
        await execute_ddl(runtime, "CREATE TABLE a (x INT)")
        assert runtime.connections[0].executed == [("CREATE TABLE a (x INT)", [])]

    @pytest.mark.asyncio
    async def test_empty_statements_is_noop(self):
        runtime = _runtime()
        await execute_ddl(runtime, [])
        assert runtime.connections == []


class TestSqlAlchemyFakePath:
    """Fake async engine — always-on CI coverage of the SA execution branch."""

    @pytest.mark.asyncio
    async def test_fetch_rewrites_to_named_binds_and_shapes_dict_rows(self):
        runtime = FakeSaRuntime(rows=[{"schema_name": "public"}])
        out = await fetch_rows(
            runtime, "SELECT schema_name FROM s WHERE a = ? AND b = ?", ["x", "y"]
        )
        # Dict rows (proves result.mappings().all() shaping, not positional .all()).
        assert out == [{"schema_name": "public"}]
        # The ? placeholders reached the driver as named binds.
        clause, binds = runtime.engine.executed[-1]
        assert ":_p0" in clause and ":_p1" in clause
        assert binds == {"_p0": "x", "_p1": "y"}

    @pytest.mark.asyncio
    async def test_ddl_runs_each_statement_via_begin(self):
        runtime = FakeSaRuntime()
        await execute_ddl(runtime, ["CREATE TABLE a (x INT)", "CREATE TABLE b (y INT)"])
        assert [s for s, _ in runtime.engine.executed] == [
            "CREATE TABLE a (x INT)",
            "CREATE TABLE b (y INT)",
        ]

    @pytest.mark.asyncio
    async def test_empty_statements_is_noop(self):
        runtime = FakeSaRuntime()
        await execute_ddl(runtime, [])
        assert runtime.engine.executed == []


class TestSqlAlchemyRealDriver:
    """Real aiosqlite engine — proves the rewrite against an actual driver."""

    @pytest.mark.asyncio
    async def test_fetch_and_ddl_round_trip(self):
        pytest.importorskip("aiosqlite")
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        runtime = SimpleNamespace(
            is_adbc=False, is_sync_sqlalchemy=False, engine=engine
        )
        try:
            await execute_ddl(runtime, "CREATE TABLE t (id INTEGER, label TEXT)")
            # A bound parameter survives the ? -> :_p0 rewrite into the driver.
            created = await fetch_rows(
                runtime,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                ["t"],
            )
            assert created == [{"name": "t"}]

            rows = await fetch_rows(runtime, "SELECT ? AS a, ? AS b", [1, "two"])
            assert rows == [{"a": 1, "b": "two"}]
        finally:
            await engine.dispose()


class TestSyncEngineRealDriver:
    """Real sync sqlite engine — the sync-only-driver branch (issue #224)
    runs the same qmark rewrite and DDL batching off the event loop."""

    @pytest.mark.asyncio
    async def test_fetch_and_ddl_round_trip(self):
        from sqlalchemy import create_engine
        from sqlalchemy.pool import StaticPool

        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        runtime = SimpleNamespace(
            is_adbc=False, is_sync_sqlalchemy=True, sync_engine=engine
        )
        try:
            await execute_ddl(runtime, "CREATE TABLE t (id INTEGER, label TEXT)")
            created = await fetch_rows(
                runtime,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                ["t"],
            )
            assert created == [{"name": "t"}]

            rows = await fetch_rows(runtime, "SELECT ? AS a, ? AS b", [1, "two"])
            assert rows == [{"a": 1, "b": "two"}]
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_ddl_failure_becomes_create_table_error(self):
        from sqlalchemy import create_engine

        engine = create_engine("sqlite://")
        runtime = SimpleNamespace(
            is_adbc=False, is_sync_sqlalchemy=True, sync_engine=engine
        )
        try:
            with pytest.raises(CreateTableError, match="DDL execution failed"):
                await execute_ddl(runtime, "CREATE GIBBERISH")
        finally:
            engine.dispose()


class TestErrorWrapping:
    """Transport/driver failures surface as the CDK error types (chained)."""

    @pytest.mark.asyncio
    async def test_adbc_fetch_failure_becomes_discovery_error(self):
        boom = RuntimeError("permission denied for relation")
        runtime = _runtime(fail_execute=boom)
        with pytest.raises(DiscoveryError) as exc:
            await fetch_rows(runtime, "SELECT 1 FROM information_schema.tables", [])
        assert exc.value.__cause__ is boom
        assert runtime.connections[0].closed is True

    @pytest.mark.asyncio
    async def test_sa_fetch_failure_becomes_discovery_error(self):
        boom = RuntimeError("syntax error")
        runtime = FakeSaRuntime(fail=boom)
        with pytest.raises(DiscoveryError) as exc:
            await fetch_rows(runtime, "SELECT 1", [])
        assert exc.value.__cause__ is boom

    @pytest.mark.asyncio
    async def test_adbc_ddl_failure_rolls_back_and_wraps(self):
        boom = RuntimeError("relation already exists")
        runtime = _runtime(fail_execute=boom)
        with pytest.raises(CreateTableError) as exc:
            await execute_ddl(runtime, "CREATE TABLE t (x INT)")
        assert exc.value.__cause__ is boom
        conn = runtime.connections[0]
        # Partial batch rolled back, nothing committed, connection closed.
        assert conn.rollbacks == 1
        assert conn.commits == 0
        assert conn.closed is True

    @pytest.mark.asyncio
    async def test_sa_ddl_failure_becomes_create_table_error(self):
        boom = RuntimeError("insufficient privilege")
        runtime = FakeSaRuntime(fail=boom)
        with pytest.raises(CreateTableError) as exc:
            await execute_ddl(runtime, "CREATE TABLE t (x INT)")
        assert exc.value.__cause__ is boom

    @pytest.mark.asyncio
    async def test_placeholder_mismatch_passes_through_unwrapped(self):
        # A SqlIntrospectionError from the bind rewrite is not re-wrapped.
        runtime = FakeSaRuntime()
        with pytest.raises(DiscoveryError, match="count mismatch"):
            await fetch_rows(runtime, "WHERE a = ? AND b = ?", ["only-one"])


class TestCursorCloseDoesNotMask:
    """A failing ``cursor.close()`` in the finally must not hide the real error.

    Both ADBC helpers close their cursor in a ``finally``. If the body already
    raised, a close that also raises would replace it -- the caller would see
    "cursor close failed" instead of the query/auth/syntax failure. The close
    is swallowed (logged) so the body's exception stays in flight, and a close
    that fails on the success path never breaks an otherwise-good run.
    """

    @pytest.mark.asyncio
    async def test_fetch_close_failure_preserves_execute_error(self, caplog):
        execute_boom = RuntimeError("permission denied for relation")
        close_boom = RuntimeError("cursor already invalidated")
        runtime = _runtime(fail_execute=execute_boom, fail_close=close_boom)
        with caplog.at_level(logging.WARNING, logger="cdk.sql._adbc_utils"):
            with pytest.raises(DiscoveryError) as exc:
                await fetch_rows(runtime, "SELECT 1 FROM information_schema.tables", [])
        # The original execute failure is chained, not the close failure.
        assert exc.value.__cause__ is execute_boom
        conn = runtime.connections[0]
        assert conn.cursor_close_attempts == 1
        assert conn.closed is True
        # The swallowed close is surfaced at WARNING, not silently dropped.
        assert _close_warnings(caplog) == 1

    @pytest.mark.asyncio
    async def test_fetch_close_failure_on_success_returns_rows(self):
        close_boom = RuntimeError("cursor already invalidated")
        runtime = _runtime(
            responder=lambda sql, params: [{"n": 1}], fail_close=close_boom
        )
        out = await fetch_rows(runtime, "SELECT 1 AS n", [])
        assert out == [{"n": 1}]
        conn = runtime.connections[0]
        assert conn.cursor_close_attempts == 1
        assert conn.closed is True

    @pytest.mark.asyncio
    async def test_ddl_close_failure_preserves_execute_error(self, caplog):
        execute_boom = RuntimeError("relation already exists")
        close_boom = RuntimeError("cursor already invalidated")
        runtime = _runtime(fail_execute=execute_boom, fail_close=close_boom)
        with caplog.at_level(logging.WARNING, logger="cdk.sql._adbc_utils"):
            with pytest.raises(CreateTableError) as exc:
                await execute_ddl(runtime, "CREATE TABLE t (x INT)")
        # The execute failure survives the close failure, and rollback still ran.
        assert exc.value.__cause__ is execute_boom
        conn = runtime.connections[0]
        assert conn.cursor_close_attempts == 1
        assert conn.rollbacks == 1
        assert conn.commits == 0
        assert conn.closed is True
        # The swallowed close is surfaced at WARNING, not silently dropped.
        assert _close_warnings(caplog) == 1

    @pytest.mark.asyncio
    async def test_ddl_close_failure_on_success_still_commits(self):
        close_boom = RuntimeError("cursor already invalidated")
        runtime = _runtime(fail_close=close_boom)
        await execute_ddl(runtime, "CREATE TABLE t (x INT)")
        conn = runtime.connections[0]
        assert conn.cursor_close_attempts == 1
        assert conn.commits == 1
        assert conn.rollbacks == 0
        assert conn.closed is True


class _RecordingCursor:
    """Minimal cursor stand-in: counts close() calls, optionally raises."""

    def __init__(self, fail: Exception | None = None) -> None:
        self.fail = fail
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        if self.fail is not None:
            raise self.fail


class TestCloseCursorQuietly:
    """The shared helper closes once, swallows a failing close, and logs it."""

    def test_normal_close_is_called_once_without_warning(self, caplog):
        cursor = _RecordingCursor()
        with caplog.at_level(logging.WARNING, logger="cdk.sql._adbc_utils"):
            _close_cursor_quietly(cursor)
        assert cursor.close_calls == 1
        assert _close_warnings(caplog) == 0

    def test_failing_close_is_swallowed_and_logged(self, caplog):
        cursor = _RecordingCursor(fail=RuntimeError("cursor already invalidated"))
        with caplog.at_level(logging.WARNING, logger="cdk.sql._adbc_utils"):
            _close_cursor_quietly(cursor)  # must not raise
        assert cursor.close_calls == 1
        assert _close_warnings(caplog) == 1
