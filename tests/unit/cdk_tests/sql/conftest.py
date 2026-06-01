"""Fixtures + fakes for the CDK SQL control-plane tests.

The discovery / create_table helpers run SQL over a ``ConnectionRuntime``. These
tests drive them through a fake ADBC runtime (a programmable in-memory DBAPI
that records every executed statement and returns canned Arrow results) and,
for the SQLAlchemy execution path, a real in-process ``aiosqlite`` engine. The
type mapping uses the *real* postgres / snowflake connector type-maps so the
canonical<->native round-trip is exercised end to end, not against a stub.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import pyarrow as pa
import pytest

from cdk.type_map.loader import load_type_map

_REPO_ROOT = Path(__file__).resolve().parents[4]
_CONNECTORS_DIR = _REPO_ROOT / "connectors"

# A responder maps an executed (sql, params) to the rows the cursor returns.
Responder = Callable[[str, Sequence[Any]], List[Dict[str, Any]]]


@pytest.fixture
def pg_mapper():
    """Real postgres TypeMapper (read + write rules)."""
    return load_type_map(_CONNECTORS_DIR, "postgres")


@pytest.fixture
def sf_mapper():
    """Real snowflake TypeMapper (read + write rules)."""
    return load_type_map(_CONNECTORS_DIR, "snowflake")


class FakeArrowCursor:
    """DBAPI cursor whose ``fetch_arrow_table`` returns canned rows."""

    def __init__(self, conn: "FakeAdbcConnection") -> None:
        self._conn = conn
        self._rows: List[Dict[str, Any]] = []

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self._conn.executed.append((sql, list(params)))
        self._rows = self._conn.responder(sql, params) if self._conn.responder else []

    def fetch_arrow_table(self) -> pa.Table:
        return pa.Table.from_pylist(self._rows)

    def close(self) -> None:
        pass


class FakeAdbcConnection:
    """In-memory DBAPI connection recording statements + commits."""

    def __init__(self, responder: Optional[Responder]) -> None:
        self.responder = responder
        self.executed: List[tuple[str, List[Any]]] = []
        self.commits = 0
        self.closed = False

    def cursor(self) -> FakeArrowCursor:
        return FakeArrowCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


class FakeAdbcRuntime:
    """Minimal ConnectionRuntime stand-in for the ADBC path.

    Each ``open_adbc_connection()`` hands back a fresh connection (ADBC drivers
    do not pool), all sharing the same responder + a recorded-connections list
    so a test can assert what ran and that the connection was closed.
    """

    def __init__(
        self,
        driver: str,
        *,
        mapper: Any = None,
        responder: Optional[Responder] = None,
    ) -> None:
        self.driver = driver
        self.is_adbc = True
        self._mapper = mapper
        self._responder = responder
        self.connections: List[FakeAdbcConnection] = []

    @property
    def connector_type_mapper(self) -> Any:
        return self._mapper

    def open_adbc_connection(self) -> FakeAdbcConnection:
        conn = FakeAdbcConnection(self._responder)
        self.connections.append(conn)
        return conn
