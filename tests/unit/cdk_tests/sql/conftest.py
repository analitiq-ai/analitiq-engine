"""Fixtures + fakes for the CDK SQL control-plane tests.

The discovery / create_table helpers run SQL over a ``ConnectionRuntime``. These
tests drive them through a fake ADBC runtime (a programmable in-memory DBAPI
that records every executed statement and returns canned Arrow results) and,
for the SQLAlchemy execution path, a real in-process ``aiosqlite`` engine. The
type mapping uses the *real* postgres / snowflake connector type-maps so the
canonical<->native round-trip is exercised end to end, not against a stub.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from cdk.type_map.loader import load_type_map

_REPO_ROOT = Path(__file__).resolve().parents[4]
_CONNECTORS_DIR = _REPO_ROOT / "connectors"

# A responder maps an executed (sql, params) to the rows the cursor returns.
Responder = Callable[[str, Sequence[Any]], list[dict[str, Any]]]


@pytest.fixture
def pg_mapper():
    """Real postgres TypeMapper (read + write rules)."""
    return load_type_map(_CONNECTORS_DIR, "postgres")


@pytest.fixture
def sf_mapper():
    """Real snowflake TypeMapper (read + write rules)."""
    return load_type_map(_CONNECTORS_DIR, "snowflake")


@pytest.fixture
def bq_mapper():
    """Real bigquery TypeMapper (read rules only — no write-type-map shipped)."""
    return load_type_map(_CONNECTORS_DIR, "bigquery")


class FakeArrowCursor:
    """DBAPI cursor whose ``fetch_arrow_table`` returns canned rows."""

    def __init__(self, conn: FakeAdbcConnection) -> None:
        self._conn = conn
        self._rows: list[dict[str, Any]] = []

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self._conn.executed.append((sql, list(params)))
        if self._conn.fail_execute is not None:
            raise self._conn.fail_execute
        self._rows = self._conn.responder(sql, params) if self._conn.responder else []

    def fetch_arrow_table(self) -> pa.Table:
        return pa.Table.from_pylist(self._rows)

    def close(self) -> None:
        self._conn.cursor_close_attempts += 1
        if self._conn.fail_close is not None:
            raise self._conn.fail_close


class FakeAdbcConnection:
    """In-memory DBAPI connection recording statements, commits, rollbacks."""

    def __init__(
        self,
        responder: Responder | None,
        fail_execute: Exception | None,
        fail_close: Exception | None = None,
    ) -> None:
        self.responder = responder
        self.fail_execute = fail_execute
        self.fail_close = fail_close
        self.executed: list[tuple[str, list[Any]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.cursor_close_attempts = 0

    def cursor(self) -> FakeArrowCursor:
        return FakeArrowCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class FakeAdbcRuntime:
    """Minimal ConnectionRuntime stand-in for the ADBC path.

    Each ``open_adbc_connection()`` hands back a fresh connection (ADBC drivers
    do not pool), all sharing the same responder + a recorded-connections list
    so a test can assert what ran and that the connection was closed. Pass
    ``fail_execute`` to make every ``cursor.execute`` raise (driver-error path);
    pass ``fail_close`` to make every ``cursor.close`` raise (the close must not
    mask the body's exception).
    """

    def __init__(
        self,
        driver: str,
        *,
        mapper: Any = None,
        responder: Responder | None = None,
        fail_execute: Exception | None = None,
        fail_close: Exception | None = None,
    ) -> None:
        self.driver = driver
        self.is_adbc = True
        self.is_sync_sqlalchemy = False
        self._mapper = mapper
        self._responder = responder
        self._fail_execute = fail_execute
        self._fail_close = fail_close
        self.connections: list[FakeAdbcConnection] = []

    @property
    def connector_type_mapper(self) -> Any:
        return self._mapper

    def open_adbc_connection(self) -> FakeAdbcConnection:
        conn = FakeAdbcConnection(
            self._responder, self._fail_execute, self._fail_close
        )
        self.connections.append(conn)
        return conn


# --- Fake async SQLAlchemy transport ----------------------------------------
# Models just enough of the async Engine/Connection/Result surface the SA
# execution path touches (``engine.connect()`` / ``engine.begin()`` async
# context managers, ``conn.execute(text, binds)`` -> ``result.mappings().all()``
# dict rows, ``conn.exec_driver_sql``), so the SQLAlchemy branch runs in CI
# without an async DB driver. ``.all()`` (no ``.mappings()``) returns tuples, so
# a regression that drops the ``.mappings()`` call would be caught.


class _FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeMappings:
        return _FakeMappings(self._rows)

    def all(self):
        # Non-mapping access yields positional tuples (as SQLAlchemy does).
        return [tuple(r.values()) for r in self._rows]


class FakeAsyncConnection:
    def __init__(self, engine: FakeAsyncEngine) -> None:
        self._engine = engine

    async def __aenter__(self) -> FakeAsyncConnection:
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False

    async def execute(self, clause: Any, binds: dict[str, Any] | None = None):
        self._engine.executed.append((str(clause), binds))
        if self._engine.fail is not None:
            raise self._engine.fail
        return FakeResult(self._engine.rows)

    async def exec_driver_sql(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> None:
        self._engine.executed.append((sql, params))
        if self._engine.fail is not None:
            raise self._engine.fail


class FakeAsyncEngine:
    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        fail: Exception | None = None,
    ) -> None:
        self.rows = rows or []
        self.fail = fail
        self.executed: list[tuple[str, Any]] = []

    def connect(self) -> FakeAsyncConnection:  # used by acquire_connection
        return FakeAsyncConnection(self)

    def begin(self) -> FakeAsyncConnection:  # used by the execute_ddl SA path
        return FakeAsyncConnection(self)


class FakeSaRuntime:
    """ConnectionRuntime stand-in for the SQLAlchemy path."""

    def __init__(
        self,
        driver: str = "postgresql",
        *,
        mapper: Any = None,
        rows: list[dict[str, Any]] | None = None,
        fail: Exception | None = None,
    ) -> None:
        self.driver = driver
        self.is_adbc = False
        self.is_sync_sqlalchemy = False
        self._mapper = mapper
        self.engine = FakeAsyncEngine(rows=rows, fail=fail)

    @property
    def connector_type_mapper(self) -> Any:
        return self._mapper
