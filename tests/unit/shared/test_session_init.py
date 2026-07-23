"""Per-connection session-init statements (``SqlDialect.session_init_sql``).

The dialect declares plain SQL statements; the CDK runs them on every new
database session either SQL transport opens — each new pooled DBAPI
connection of a SQLAlchemy engine (async and sync, build-time probe
included, after ``verify_tls_state``) and each connection an ADBC
transport opens (probe included). Statements run through one cursor and
are committed so session state survives the pool's reset-on-return
rollback on systems where it is transactional (Postgres ``SET``). The
base declares no statements, so existing connectors are unaffected
(issue #385; motivating consumer: MySQL ``SET time_zone = '+00:00'``).
"""

from __future__ import annotations

import logging
import sys
import threading
import time
import types

import pytest
from sqlalchemy import text as _sa_text

from cdk.sql.dialects import SqlDialect
from cdk.transport_factory import (
    _run_closing_on_failure,
    _session_init_statements,
    build_adbc_from_spec,
    build_sqlalchemy_from_spec,
)


def _assert_no_lingering_aiosqlite_threads() -> None:
    """Fail if an aiosqlite connection worker thread outlives its test.

    A DBAPI error escaping a pool ``connect`` listener leaves the raw
    driver connection open unless the listener closes it — for aiosqlite
    that leaks the connection's worker thread and hangs interpreter
    shutdown. The worker exits shortly after close (its queue poll times
    out), so poll briefly before declaring a leak.
    """

    def _workers() -> list[threading.Thread]:
        return [
            t for t in threading.enumerate() if "_connection_worker_thread" in t.name
        ]

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if not _workers():
            return
        time.sleep(0.05)
    raise AssertionError(f"leaked aiosqlite worker threads: {_workers()}")


class _PinningDialect(SqlDialect):
    """Dialect pinning observable per-connection state (sqlite TEMP table).

    TEMP objects live for the DBAPI connection's lifetime only, so their
    presence on a checked-out connection proves the statements ran on
    that physical connection before it was used.
    """

    name = "pinning"

    def session_init_sql(self) -> list[str]:
        return [
            "CREATE TEMP TABLE _session_pin (v INTEGER)",
            "INSERT INTO _session_pin VALUES (42)",
        ]


def _sqlite_spec(driver: str, dsn: str, mode: str | None = None) -> dict:
    return {
        "transport_type": "sqlalchemy",
        "driver": driver,
        "dsn": dsn,
        "tls": {"mode": mode, "ca_pem": None} if mode is not None else None,
        "engine_kwargs": {},
    }


class TestHookContract:
    def test_base_declares_no_statements(self):
        assert SqlDialect().session_init_sql() == []

    def test_no_dialect_means_no_statements(self):
        assert _session_init_statements(None) == []

    @pytest.mark.parametrize(
        "bad",
        ["SET x = 1", ("SET x = 1",), ["SET x = 1", ""], [b"SET x = 1"], [None]],
        ids=["str", "tuple", "empty-item", "bytes-item", "none-item"],
    )
    def test_malformed_return_fails_loud(self, bad):
        class _BadDialect(SqlDialect):
            name = "bad"

            def session_init_sql(self):
                return bad

        with pytest.raises(TypeError, match="session_init_sql"):
            _session_init_statements(_BadDialect())

    def test_declared_statements_are_logged(self, caplog):
        with caplog.at_level(logging.INFO, logger="cdk.transport_factory"):
            _session_init_statements(_PinningDialect())
        assert any(
            "pinning" in record.getMessage() and "_session_pin" in record.getMessage()
            for record in caplog.records
        )


class TestSqlAlchemySessionInit:
    """Real engines, no mocks — sqlite for the sync path, aiosqlite async."""

    @pytest.mark.asyncio
    async def test_sync_pooled_connection_carries_pinned_state(self):
        transport = await build_sqlalchemy_from_spec(
            _sqlite_spec("sqlite+pysqlite", "sqlite://"),
            sql_dialect=_PinningDialect(),
        )
        engine = transport.engine
        try:
            with engine.connect() as conn:
                value = conn.execute(
                    _sa_text("SELECT v FROM _session_pin")
                ).scalar_one()
            assert value == 42
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_every_new_pool_connection_is_initialized(self):
        transport = await build_sqlalchemy_from_spec(
            _sqlite_spec("sqlite+pysqlite", "sqlite://"),
            sql_dialect=_PinningDialect(),
        )
        engine = transport.engine
        try:
            # Drop the probe connection's pool; the next checkout opens a
            # fresh DBAPI connection, which must be initialized the same way.
            engine.dispose()
            with engine.connect() as conn:
                value = conn.execute(
                    _sa_text("SELECT v FROM _session_pin")
                ).scalar_one()
            assert value == 42
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_async_pooled_connection_carries_pinned_state(self):
        pytest.importorskip("aiosqlite")
        transport = await build_sqlalchemy_from_spec(
            _sqlite_spec("sqlite+aiosqlite", "sqlite+aiosqlite://"),
            sql_dialect=_PinningDialect(),
        )
        engine = transport.engine
        try:
            async with engine.connect() as conn:
                result = await conn.execute(_sa_text("SELECT v FROM _session_pin"))
                assert result.scalar_one() == 42
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_sync_failing_statement_fails_the_build(self):
        class _BrokenDialect(SqlDialect):
            name = "broken"

            def session_init_sql(self):
                return ["INSERT INTO no_such_table VALUES (1)"]

        with pytest.raises(Exception, match="no_such_table"):
            await build_sqlalchemy_from_spec(
                _sqlite_spec("sqlite+pysqlite", "sqlite://"),
                sql_dialect=_BrokenDialect(),
            )

    @pytest.mark.asyncio
    async def test_async_failing_statement_fails_the_build_without_leaking(self):
        pytest.importorskip("aiosqlite")

        class _BrokenDialect(SqlDialect):
            name = "broken"

            def session_init_sql(self):
                return ["INSERT INTO no_such_table VALUES (1)"]

        with pytest.raises(Exception, match="no_such_table"):
            await build_sqlalchemy_from_spec(
                _sqlite_spec("sqlite+aiosqlite", "sqlite+aiosqlite://"),
                sql_dialect=_BrokenDialect(),
            )
        _assert_no_lingering_aiosqlite_threads()

    @pytest.mark.asyncio
    async def test_base_dialect_builds_without_statements(self):
        transport = await build_sqlalchemy_from_spec(
            _sqlite_spec("sqlite+pysqlite", "sqlite://"),
            sql_dialect=SqlDialect(),
        )
        transport.engine.dispose()

    @pytest.mark.asyncio
    async def test_statements_run_after_tls_verification(self):
        # ``PRAGMA user_version`` defaults to 0 per (in-memory) database;
        # session init sets it to 7. The TLS hook fires on the same connect
        # event and must observe the pre-init value: TLS verification runs
        # against the raw session, before any dialect statements.
        class _OrderProbeDialect(SqlDialect):
            name = "order-probe"

            def __init__(self) -> None:
                self.user_version_during_tls: list[int] = []

            def build_tls_connect_arg(self, mode, ca_pem):
                return None

            def verify_tls_state(self, dbapi_connection, mode):
                cursor = dbapi_connection.cursor()
                try:
                    cursor.execute("PRAGMA user_version")
                    row = cursor.fetchone()
                finally:
                    cursor.close()
                self.user_version_during_tls.append(row[0])

            def session_init_sql(self):
                return ["PRAGMA user_version = 7"]

        dialect = _OrderProbeDialect()
        transport = await build_sqlalchemy_from_spec(
            _sqlite_spec("sqlite+pysqlite", "sqlite://", mode="require"),
            sql_dialect=dialect,
        )
        engine = transport.engine
        try:
            assert dialect.user_version_during_tls == [0]
            with engine.connect() as conn:
                assert conn.execute(_sa_text("PRAGMA user_version")).scalar_one() == 7
        finally:
            engine.dispose()


class _FakeAdbcCursor:
    def __init__(self, conn: _FakeAdbcConnection) -> None:
        self._conn = conn

    def execute(self, sql: str) -> None:
        if "boom" in sql:
            raise RuntimeError(f"statement failed: {sql}")
        self._conn.executed.append(sql)

    def fetchone(self):
        return (1,)

    def close(self) -> None:
        return None


class _FakeAdbcConnection:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.commits = 0
        self.closed = False

    def cursor(self) -> _FakeAdbcCursor:
        return _FakeAdbcCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_adbc_driver(monkeypatch):
    """Install a fake ``adbc_driver_fakedb.dbapi`` module recording connects."""
    connections: list[_FakeAdbcConnection] = []

    dbapi = types.ModuleType("adbc_driver_fakedb.dbapi")

    def connect(*args, **kwargs):
        conn = _FakeAdbcConnection()
        connections.append(conn)
        return conn

    dbapi.connect = connect
    package = types.ModuleType("adbc_driver_fakedb")
    package.dbapi = dbapi
    monkeypatch.setitem(sys.modules, "adbc_driver_fakedb", package)
    monkeypatch.setitem(sys.modules, "adbc_driver_fakedb.dbapi", dbapi)
    return connections


def _adbc_spec() -> dict:
    return {
        "transport_type": "adbc",
        "driver": "fakedb",
        "uri": None,
        "db_kwargs": {"a": "b"},
    }


class TestAdbcSessionInit:
    @pytest.mark.asyncio
    async def test_probe_and_every_opened_connection_are_initialized(
        self, fake_adbc_driver
    ):
        transport = await build_adbc_from_spec(
            _adbc_spec(), sql_dialect=_PinningDialect()
        )
        probe = fake_adbc_driver[0]
        # Statements ran (and were committed) before the SELECT 1 probe.
        assert probe.executed == [
            "CREATE TEMP TABLE _session_pin (v INTEGER)",
            "INSERT INTO _session_pin VALUES (42)",
            "SELECT 1",
        ]
        assert probe.commits == 1
        assert probe.closed is True

        conn = transport.connect()
        assert fake_adbc_driver[1] is conn
        assert conn.executed == [
            "CREATE TEMP TABLE _session_pin (v INTEGER)",
            "INSERT INTO _session_pin VALUES (42)",
        ]
        assert conn.commits == 1

    @pytest.mark.asyncio
    async def test_failing_statement_closes_connection_and_propagates(
        self, fake_adbc_driver
    ):
        class _BrokenDialect(SqlDialect):
            name = "broken"

            def session_init_sql(self):
                return ["SET boom = 1"]

        with pytest.raises(RuntimeError, match="statement failed"):
            await build_adbc_from_spec(_adbc_spec(), sql_dialect=_BrokenDialect())
        assert fake_adbc_driver[0].closed is True

    @pytest.mark.asyncio
    async def test_no_dialect_leaves_connections_untouched(self, fake_adbc_driver):
        transport = await build_adbc_from_spec(_adbc_spec())
        assert fake_adbc_driver[0].executed == ["SELECT 1"]
        assert fake_adbc_driver[0].commits == 0
        conn = transport.connect()
        assert conn.executed == []


class TestRunClosingOnFailure:
    """The connection-setup failure guard shared by the TLS probe, the
    session-init statements (pool connect event), and the ADBC open path:
    a fresh connection whose setup fails is closed before the error
    propagates — nobody else closes it (SQLAlchemy discards the record of
    a connect listener that raised a DBAPI error without closing the raw
    connection; an ADBC connection has no pool at all)."""

    def test_failure_closes_connection_and_propagates(self):
        conn = _FakeAdbcConnection()

        def _boom() -> None:
            raise RuntimeError("setup failed")

        with pytest.raises(RuntimeError, match="setup failed"):
            _run_closing_on_failure(conn, _boom)
        assert conn.closed is True

    def test_close_failure_is_suppressed_original_error_stays_primary(self):
        class _UnclosableConnection(_FakeAdbcConnection):
            def close(self):
                raise OSError("close failed too")

        def _boom() -> None:
            raise RuntimeError("setup failed")

        with pytest.raises(RuntimeError, match="setup failed"):
            _run_closing_on_failure(_UnclosableConnection(), _boom)

    def test_success_leaves_connection_open(self):
        conn = _FakeAdbcConnection()
        _run_closing_on_failure(conn, lambda: None)
        assert conn.closed is False

    @pytest.mark.asyncio
    async def test_dbapi_error_in_tls_probe_does_not_leak_connection(self):
        # The pre-existing instance of the same defect class (#376): a TLS
        # probe whose own query fails with the driver's error — not a
        # TlsVerificationError — must close the raw connection too.
        pytest.importorskip("aiosqlite")

        class _DbapiFailingProbeDialect(SqlDialect):
            name = "probe-fails"

            def build_tls_connect_arg(self, mode, ca_pem):
                return None

            def verify_tls_state(self, dbapi_connection, mode):
                cursor = dbapi_connection.cursor()
                try:
                    cursor.execute("SELECT x FROM no_such_table")
                finally:
                    cursor.close()

        with pytest.raises(Exception, match="no_such_table"):
            await build_sqlalchemy_from_spec(
                _sqlite_spec("sqlite+aiosqlite", "sqlite+aiosqlite://", "require"),
                sql_dialect=_DbapiFailingProbeDialect(),
            )
        _assert_no_lingering_aiosqlite_threads()
