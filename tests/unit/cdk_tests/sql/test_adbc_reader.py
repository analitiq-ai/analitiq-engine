"""CDK ADBC-only reader: pure execution.

The reader takes a compiled ``(sql, params)`` pair (produced by
``QueryBuilder`` in qmark mode) and returns Arrow batches. These tests
freeze its execution contract: binds params, materializes Arrow, always
closes the per-page cursor, and surfaces a use-after-close as a distinct
error. Connector-side wiring (filters + cursor composition) lives with
the engine source connector tests.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

import pyarrow as pa
import pytest

from cdk.sql import AdbcReader, AdbcReaderClosedError
from cdk.sql._adbc_utils import _adbc_execute


class _FakeCursor:
    def __init__(self, table: pa.Table) -> None:
        self._table = table
        self.executed: List[Tuple[str, Optional[list]]] = []
        self.closed = False

    def execute(self, sql: str, params: Optional[list] = None) -> None:
        self.executed.append((sql, params))

    def fetch_arrow_table(self) -> pa.Table:
        return self._table

    def close(self) -> None:
        self.closed = True


class _FakeConn:
    def __init__(self, table: pa.Table) -> None:
        self._cursor = _FakeCursor(table)
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


class TestAdbcReaderExecution:
    @pytest.mark.asyncio
    async def test_fetch_page_binds_params_and_returns_batches(self):
        table = pa.table({"id": [1, 2], "status": ["a", "b"]})
        conn = _FakeConn(table)
        reader = AdbcReader("postgresql", conn)

        batches = await reader.fetch_page('SELECT "id" FROM "t" WHERE "id" >= ?', [1])

        assert sum(b.num_rows for b in batches) == 2
        # The compiled SQL and positional params are forwarded verbatim.
        assert conn._cursor.executed == [
            ('SELECT "id" FROM "t" WHERE "id" >= ?', [1])
        ]
        # The per-page cursor is always closed, even on the happy path.
        assert conn._cursor.closed is True

    @pytest.mark.asyncio
    async def test_fetch_page_no_params_uses_single_arg_execute(self):
        conn = _FakeConn(pa.table({"id": [1]}))
        reader = AdbcReader("postgresql", conn)

        await reader.fetch_page('SELECT "id" FROM "t"')

        # No params -> execute(sql) with no second arg, so a driver that
        # treats an empty bind list as "expects 0 params" is never tripped.
        assert conn._cursor.executed == [('SELECT "id" FROM "t"', None)]

    @pytest.mark.asyncio
    async def test_fetch_page_empty_result_returns_empty_list(self):
        conn = _FakeConn(pa.table({"id": pa.array([], type=pa.int64())}))
        reader = AdbcReader("postgresql", conn)

        assert await reader.fetch_page("SELECT 1") == []

    @pytest.mark.asyncio
    async def test_fetch_after_close_raises_closed_error(self):
        conn = _FakeConn(pa.table({"id": [1]}))
        reader = AdbcReader("postgresql", conn)

        await reader.close()

        with pytest.raises(AdbcReaderClosedError):
            await reader.fetch_page("SELECT 1")
        assert conn.closed is True


class _RecordingCursor:
    """Minimal ADBC cursor that records how execute() was called."""

    def __init__(self) -> None:
        self.calls: List[Tuple[str, Optional[list]]] = []

    def execute(self, sql: str, params: Optional[list] = None) -> None:
        self.calls.append((sql, params))


class TestAdbcExecuteHelper:
    def test_binds_params_when_non_empty(self):
        cursor = _RecordingCursor()
        _adbc_execute(cursor, "SELECT ?", [42])
        assert cursor.calls == [("SELECT ?", [42])]

    def test_skips_second_arg_when_params_empty(self):
        cursor = _RecordingCursor()
        _adbc_execute(cursor, "SELECT 1", [])
        # execute() is called with a single argument so drivers that reject
        # an empty bind list (treating it as "expects 0 params") are not tripped.
        assert cursor.calls == [("SELECT 1", None)]

    def test_converts_sequence_to_list(self):
        cursor = _RecordingCursor()
        _adbc_execute(cursor, "SELECT ?", (99,))
        assert cursor.calls == [("SELECT ?", [99])]
