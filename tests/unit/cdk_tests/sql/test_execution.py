"""Transport-agnostic execution: qmark<->named rewrite, ADBC + SQLAlchemy paths.

The ADBC path is exercised with the in-memory fake DBAPI; the SQLAlchemy path
with a real ``aiosqlite`` engine (skipped if the driver is absent) so the
``?`` -> named-bind rewrite is proven against an actual driver, not a mock.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cdk.sql.exceptions import DiscoveryError
from cdk.sql.execution import (
    _qmark_to_named,
    execute_ddl,
    fetch_rows,
)

from .conftest import FakeAdbcRuntime


def _runtime(*, responder=None) -> FakeAdbcRuntime:
    return FakeAdbcRuntime("postgresql", mapper=None, responder=responder)


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
        out = await fetch_rows(runtime, "SELECT schema_name FROM x WHERE y = ?", ["arg"])
        assert out == rows
        # One connection opened, the statement ran with the params, conn closed.
        assert len(runtime.connections) == 1
        conn = runtime.connections[0]
        assert conn.executed == [
            ("SELECT schema_name FROM x WHERE y = ?", ["arg"])
        ]
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


class TestSqlAlchemyPath:
    """Real aiosqlite engine — proves the named-bind rewrite + acquire path."""

    @pytest.mark.asyncio
    async def test_fetch_and_ddl_round_trip(self):
        aiosqlite = pytest.importorskip("aiosqlite")  # noqa: F841
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        runtime = SimpleNamespace(is_adbc=False, engine=engine)
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
