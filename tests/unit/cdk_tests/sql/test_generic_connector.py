"""GenericSQLConnector: the Readable + Discoverable + TableCreator surface.

The Writable (destination) half is covered by the relocated destination
suite. These tests pin the read path promoted onto the unified class and
the thin control-plane delegators:

* ``read_batches`` materializes the runtime it is handed (no prior
  ``connect()``), pages via ``QueryBuilder``, advances the checkpoint,
  and releases the runtime on exit.
* the ADBC branch composes filters + cursor into one quoted, qmark,
  inline-paged SELECT and guards an empty projection.
* ``list_schemas`` / ``list_tables`` / ``list_columns`` / ``create_table``
  delegate to the standalone ``cdk.sql`` helpers.
"""

from __future__ import annotations

from typing import Any, List, Tuple
from unittest.mock import AsyncMock, patch

import pyarrow as pa
import pytest

from cdk.sql.generic import GenericSQLConnector
from cdk.sql.exceptions import ReadError


class _FakeRuntime:
    """Minimal materialized runtime for the read path."""

    def __init__(self, *, is_adbc: bool, driver: str = "postgresql", engine: Any = None):
        self.is_adbc = is_adbc
        self.driver = driver
        self.engine = engine
        self.close = AsyncMock()


class _RecordingReader:
    """Fake AdbcReader: records SQL/params, returns one page then drains."""

    def __init__(self, pages: List[List[pa.RecordBatch]]) -> None:
        self._pages = pages
        self.calls: List[Tuple[str, Any]] = []

    async def fetch_page(self, sql: str, params: Any = ()) -> List[pa.RecordBatch]:
        self.calls.append((sql, list(params)))
        return self._pages.pop(0) if self._pages else []


def _checkpoint(cursor: dict | None = None) -> AsyncMock:
    cp = AsyncMock()
    cp.get_cursor = AsyncMock(return_value=cursor)
    cp.save_cursor = AsyncMock()
    return cp


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


async def _drain(connector, runtime, config, checkpoint, batch_size=2):
    out = []
    async for batch in connector.read_batches(
        runtime,
        config,
        checkpoint=checkpoint,
        stream_name="s",
        batch_size=batch_size,
    ):
        out.append(batch)
    return out


class TestReadGuards:
    @pytest.mark.asyncio
    async def test_missing_endpoint_document_raises(self):
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=False)
        with pytest.raises(ReadError, match="missing 'endpoint_document'"):
            await _drain(connector, runtime, {}, _checkpoint())

    @pytest.mark.asyncio
    async def test_missing_table_name_raises(self):
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=False)
        config = {"endpoint_document": {"database_object": {}, "columns": []}}
        with pytest.raises(ReadError, match="database_object.name"):
            await _drain(connector, runtime, config, _checkpoint())


class TestReadAdbcBranch:
    @pytest.mark.asyncio
    async def test_filters_and_cursor_compose_into_one_select(self):
        runtime = _FakeRuntime(is_adbc=True)
        checkpoint = _checkpoint(cursor={"cursor": "2024-01-02"})
        page = [pa.RecordBatch.from_pydict({"id": [3], "updated_at": ["2024-01-03"]})]
        reader = _RecordingReader([page, []])

        class _CM:
            async def __aenter__(self):
                return reader

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_CM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                filters=[{"field": "status", "operator": "eq", "value": "active"}],
                replication={"method": "incremental", "cursor_field": ["updated_at"]},
            )
            await _drain(connector, runtime, config, checkpoint)

        sql, params = reader.calls[0]
        assert "?" in sql
        assert '"status" = ?' in sql
        assert '"updated_at" >= ?' in sql
        assert 'ORDER BY "updated_at"' in sql
        assert "LIMIT 2 OFFSET 0" in sql
        # Filter value first, cursor value second (WHERE build order).
        assert params == ["active", "2024-01-02"]
        # Runtime released after the read.
        runtime.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_empty_columns_rejected(self):
        runtime = _FakeRuntime(is_adbc=True)
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.SchemaContract"
        ):
            config = _endpoint_config(columns=())
            config["endpoint_document"]["columns"] = []
            with pytest.raises(ReadError, match="non-empty column projection"):
                await _drain(connector, runtime, config, _checkpoint())
        # Even on the read error, the runtime is released.
        runtime.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_saves_last_cursor_value_from_batch(self):
        runtime = _FakeRuntime(is_adbc=True)
        checkpoint = _checkpoint(cursor=None)
        page = [pa.RecordBatch.from_pydict(
            {"id": [1, 2], "updated_at": ["2024-01-02", "2024-01-09"]}
        )]
        reader = _RecordingReader([page, []])

        class _CM:
            async def __aenter__(self):
                return reader

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_CM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                replication={"method": "incremental", "cursor_field": ["updated_at"]},
            )
            await _drain(connector, runtime, config, checkpoint)

        saved = [c.args[2]["cursor"] for c in checkpoint.save_cursor.call_args_list]
        assert saved[-1] == "2024-01-09"


class TestReadSqlAlchemyBranch:
    @pytest.mark.asyncio
    async def test_full_refresh_pages_via_acquire_connection(self):
        runtime = _FakeRuntime(is_adbc=False, engine=object())
        checkpoint = _checkpoint(cursor=None)

        class _Row:
            def __init__(self, mapping):
                self._mapping = mapping

        class _FakeConn:
            def __init__(self):
                self.calls = 0

            async def exec_driver_sql(self, sql, params=None):
                self.calls += 1
                if self.calls == 1:
                    return [_Row({"id": 1, "updated_at": "2024-01-01"})]
                return []

        conn = _FakeConn()

        class _AcquireCM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.acquire_connection", return_value=_AcquireCM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.from_pylist.side_effect = lambda rows: rows
            out = await _drain(
                connector, runtime, _endpoint_config(), checkpoint, batch_size=2
            )

        # One page of one row -> one yielded batch; short page ends the loop.
        assert out == [[{"id": 1, "updated_at": "2024-01-01"}]]
        runtime.close.assert_awaited()


class TestControlPlaneDelegators:
    @pytest.mark.asyncio
    async def test_discovery_and_create_table_delegate(self):
        connector = GenericSQLConnector()
        runtime = object()
        with patch(
            "cdk.sql.generic._sql_list_schemas", new=AsyncMock(return_value=["s"])
        ) as ls, patch(
            "cdk.sql.generic._sql_list_tables", new=AsyncMock(return_value=["t"])
        ) as lt, patch(
            "cdk.sql.generic._sql_list_columns", new=AsyncMock(return_value=([], []))
        ) as lc, patch(
            "cdk.sql.generic._sql_create_table", new=AsyncMock()
        ) as ct:
            assert await connector.list_schemas(runtime) == ["s"]
            assert await connector.list_tables(runtime, "public") == ["t"]
            assert await connector.list_columns(runtime, "public", "orders") == ([], [])
            await connector.create_table(runtime, "public", "orders", [], [])

        ls.assert_awaited_once_with(runtime)
        lt.assert_awaited_once_with(runtime, "public")
        lc.assert_awaited_once_with(runtime, "public", "orders")
        ct.assert_awaited_once_with(runtime, "public", "orders", [], [])
