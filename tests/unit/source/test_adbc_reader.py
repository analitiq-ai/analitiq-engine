"""ADBC-only source reader and connector glue.

The ADBC reader is now pure execution: it takes a compiled
``(sql, params)`` pair (produced by ``QueryBuilder`` in qmark mode) and
returns Arrow batches. The hand-rolled ``_build_select_sql`` is gone --
SQL rendering, including stream filters and the incremental cursor, is
shared with the SQLAlchemy transport via ``QueryBuilder``.

These tests freeze:

* :class:`AdbcReader` execution (binds params, materializes Arrow,
  closes the cursor, surfaces a closed-reader bug as a distinct error).
* the connector's ADBC branch wiring -- filters + cursor compose into a
  single quoted, qmark, inline-paged SELECT; the non-empty column guard;
  and the first-column ORDER BY fallback when no cursor is set.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

import pyarrow as pa
import pytest

from src.source.connectors.base import ReadError
from src.source.connectors.database import DatabaseConnector
from src.source.drivers.adbc_reader import AdbcReader, AdbcReaderClosedError


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


class _RecordingReader:
    """Fake AdbcReader: records SQL/params, returns one page then drains."""

    def __init__(self, pages: List[List[pa.RecordBatch]]) -> None:
        self._pages = pages
        self.calls: List[Tuple[str, Any]] = []

    async def fetch_page(self, sql: str, params: Any = ()) -> List[pa.RecordBatch]:
        self.calls.append((sql, list(params)))
        return self._pages.pop(0) if self._pages else []


def _adbc_connector(pages: List[List[pa.RecordBatch]]) -> Tuple[DatabaseConnector, _RecordingReader]:
    connector = DatabaseConnector("t")
    connector._adbc_only = True
    connector._initialized = True
    connector._driver = "postgresql"
    connector._runtime = object()  # truthy; never dereferenced (reader is patched)
    reader = _RecordingReader(pages)

    class _CM:
        async def __aenter__(self):
            return reader

        async def __aexit__(self, *exc):
            return False

    patcher = patch(
        "src.source.connectors.database.open_adbc_reader",
        return_value=_CM(),
    )
    patcher.start()
    connector._stop_patch = patcher.stop  # type: ignore[attr-defined]
    return connector, reader


def _endpoint_config(filters=None, replication=None, columns=("id", "updated_at")):
    return {
        "endpoint_document": {
            "database_object": {"name": "orders", "schema": "public"},
            "columns": [{"name": c} for c in columns],
        },
        "stream_source": {
            "filters": filters or [],
            "replication": replication or {},
        },
    }


async def _drain(connector, config, state_manager, batch_size=2):
    out = []
    async for batch in connector.read_batches(
        config,
        state_manager=state_manager,
        stream_name="s",
        batch_size=batch_size,
    ):
        out.append(batch)
    return out


@pytest.fixture
def state_manager():
    sm = AsyncMock()
    sm.get_cursor = AsyncMock(return_value=None)
    sm.save_cursor = AsyncMock()
    return sm


class TestAdbcConnectorBranch:
    @pytest.mark.asyncio
    async def test_filters_and_cursor_compose_into_one_select(self, state_manager):
        state_manager.get_cursor = AsyncMock(return_value={"cursor": "2024-01-02"})
        page = [pa.RecordBatch.from_pydict({"id": [3], "updated_at": ["2024-01-03"]})]
        connector, reader = _adbc_connector([page, []])
        # Schema casting is exercised elsewhere; here pass the batch through.
        with patch(
            "src.source.connectors.database.SchemaContract"
        ) as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            try:
                config = _endpoint_config(
                    filters=[{"field": "status", "operator": "eq", "value": "active"}],
                    replication={"method": "incremental", "cursor_field": ["updated_at"]},
                )
                await _drain(connector, config, state_manager)
            finally:
                connector._stop_patch()

        sql, params = reader.calls[0]
        # qmark placeholders, every identifier quoted, inline paging.
        assert "?" in sql
        assert '"status" = ?' in sql
        assert '"updated_at" >= ?' in sql
        assert 'ORDER BY "updated_at"' in sql
        assert "LIMIT 2 OFFSET 0" in sql
        # Filter value first, cursor value second (WHERE build order).
        assert params == ["active", "2024-01-02"]

    @pytest.mark.asyncio
    async def test_full_refresh_with_filters_only(self, state_manager):
        page = [pa.RecordBatch.from_pydict({"id": [1], "updated_at": ["2024-01-01"]})]
        connector, reader = _adbc_connector([page, []])
        with patch("src.source.connectors.database.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            try:
                config = _endpoint_config(
                    filters=[{"field": "status", "operator": "eq", "value": "x"}],
                )
                await _drain(connector, config, state_manager)
            finally:
                connector._stop_patch()

        sql, params = reader.calls[0]
        assert '"status" = ?' in sql
        # No cursor -> no cursor predicate, ORDER BY falls back to first col.
        assert '">=' not in sql
        assert 'ORDER BY "id"' in sql
        assert params == ["x"]

    @pytest.mark.asyncio
    async def test_empty_columns_rejected(self, state_manager):
        connector, _ = _adbc_connector([])
        with patch("src.source.connectors.database.SchemaContract"):
            try:
                config = _endpoint_config(columns=())
                # endpoint columns empty AND no selected_columns -> guard fires.
                config["endpoint_document"]["columns"] = []
                with pytest.raises(ReadError, match="non-empty column projection"):
                    await _drain(connector, config, state_manager)
            finally:
                connector._stop_patch()

    @pytest.mark.asyncio
    async def test_order_by_fallback_warns_once(self, state_manager, caplog):
        from src.source.connectors import database as db_mod

        db_mod._order_by_fallback_logged.clear()
        page = [pa.RecordBatch.from_pydict({"id": [1], "updated_at": ["2024-01-01"]})]
        connector, reader = _adbc_connector([page, []])
        with patch("src.source.connectors.database.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            try:
                with caplog.at_level("WARNING"):
                    await _drain(connector, _endpoint_config(), state_manager)
            finally:
                connector._stop_patch()

        assert any("defaulting ORDER BY" in r.message for r in caplog.records)
