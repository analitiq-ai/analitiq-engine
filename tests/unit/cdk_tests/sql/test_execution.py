"""Transport-agnostic execution: qmark<->named rewrite, ADBC + SQLAlchemy paths.

The ADBC path is exercised with the in-memory fake DBAPI. The SQLAlchemy path is
covered two ways: a fake async engine that always runs in CI (proving the
named-bind rewrite reaches the driver and the dict-row shaping), and a real
``aiosqlite`` engine when the driver is present (skipped otherwise). The live
postgres validation of the SA path is documented in the PR.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cdk.sql.exceptions import CreateTableError, DiscoveryError
from cdk.sql.execution import (
    _qmark_to_named,
    execute_ddl,
    fetch_rows,
)

from .conftest import FakeAdbcRuntime, FakeSaRuntime


def _runtime(*, responder=None, fail_execute=None) -> FakeAdbcRuntime:
    return FakeAdbcRuntime(
        "postgresql", mapper=None, responder=responder, fail_execute=fail_execute
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
