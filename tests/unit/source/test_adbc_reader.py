"""ADBC-only source connector branch wiring.

The ADBC reader itself (pure ``(sql, params)`` execution) is tested in
``tests/unit/cdk_tests/sql/test_adbc_reader.py`` now that it lives in the
CDK. These tests freeze the connector's ADBC branch: filters + cursor
compose into a single quoted, qmark, inline-paged SELECT; the non-empty
column guard; and the first-column ORDER BY fallback when no cursor is
set. SQL rendering is shared with the SQLAlchemy transport via
``QueryBuilder``.
"""

from __future__ import annotations

from typing import Any, List, Tuple
from unittest.mock import AsyncMock, patch

import pyarrow as pa
import pytest

from src.source.connectors.base import ReadError
from src.source.connectors.database import DatabaseConnector


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

    @pytest.mark.asyncio
    async def test_offset_advances_across_pages_with_fixed_where(self, state_manager):
        # Two full pages then drain. The cursor is pinned at the read's
        # initial value; only OFFSET moves, so the WHERE params must be
        # byte-for-byte identical across pages.
        state_manager.get_cursor = AsyncMock(return_value={"cursor": "2024-01-01"})
        page1 = [pa.RecordBatch.from_pydict(
            {"id": [1, 2], "updated_at": ["2024-01-02", "2024-01-03"]}
        )]
        page2 = [pa.RecordBatch.from_pydict(
            {"id": [3, 4], "updated_at": ["2024-01-04", "2024-01-05"]}
        )]
        connector, reader = _adbc_connector([page1, page2, []])
        with patch("src.source.connectors.database.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            try:
                config = _endpoint_config(
                    filters=[{"field": "status", "operator": "eq", "value": "active"}],
                    replication={"method": "incremental", "cursor_field": ["updated_at"]},
                )
                await _drain(connector, config, state_manager, batch_size=2)
            finally:
                connector._stop_patch()

        assert len(reader.calls) == 3
        sql0, params0 = reader.calls[0]
        sql1, params1 = reader.calls[1]
        assert "OFFSET 0" in sql0
        assert "OFFSET 2" in sql1
        # Same WHERE predicate and identical params on every page.
        assert params0 == params1 == ["active", "2024-01-01"]

    @pytest.mark.asyncio
    async def test_saves_last_cursor_value_from_batch(self, state_manager):
        state_manager.get_cursor = AsyncMock(return_value=None)
        page = [pa.RecordBatch.from_pydict(
            {"id": [1, 2], "updated_at": ["2024-01-02", "2024-01-09"]}
        )]
        connector, _ = _adbc_connector([page, []])
        with patch("src.source.connectors.database.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            try:
                config = _endpoint_config(
                    replication={"method": "incremental", "cursor_field": ["updated_at"]},
                )
                await _drain(connector, config, state_manager, batch_size=2)
            finally:
                connector._stop_patch()

        # Checkpoint advances to the last row's cursor-column value.
        saved = [c.args[2]["cursor"] for c in state_manager.save_cursor.call_args_list]
        assert saved[-1] == "2024-01-09"

    @pytest.mark.asyncio
    async def test_order_by_fallback_dedupes_across_drains(self, state_manager, caplog):
        from src.source.connectors import database as db_mod

        db_mod._order_by_fallback_logged.clear()
        with patch("src.source.connectors.database.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            with caplog.at_level("WARNING"):
                for _ in range(2):
                    page = [pa.RecordBatch.from_pydict(
                        {"id": [1], "updated_at": ["2024-01-01"]}
                    )]
                    connector, _ = _adbc_connector([page, []])
                    try:
                        await _drain(connector, _endpoint_config(), state_manager)
                    finally:
                        connector._stop_patch()

        # Same (table, column) -> warned only on the first drain.
        warnings = [r for r in caplog.records if "defaulting ORDER BY" in r.message]
        assert len(warnings) == 1

    @pytest.mark.asyncio
    async def test_named_params_rejected_on_adbc_path(self, state_manager):
        # If the dialect ignores the forced qmark paramstyle and yields a
        # dict, the ADBC execute path would bind parameter names instead of
        # values. The connector must fail loudly before that happens.
        connector, _ = _adbc_connector([])
        with patch("src.source.connectors.database.SchemaContract") as sc, patch(
            "src.source.connectors.database.QueryBuilder"
        ) as qb:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            qb.return_value.build_select_query.return_value = (
                "SELECT 1",
                {"status_1": "active"},
            )
            try:
                with pytest.raises(ReadError, match="positional qmark parameters"):
                    await _drain(connector, _endpoint_config(), state_manager)
            finally:
                connector._stop_patch()
