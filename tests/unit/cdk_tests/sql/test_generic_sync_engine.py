"""GenericSQLConnector over a sync SQLAlchemy engine (issue #224).

A sync-only driver (the production case is Redshift's vendor-supported
``redshift_connector``) materializes a plain sync ``Engine``; the handler
runs the same sync-``Connection`` transaction bodies the async path uses,
dispatched via ``asyncio.to_thread``. SQLite's stdlib driver is the
in-process stand-in for a sync-only dialect, so these tests exercise the
real write/read/DDL machinery end to end — actual SQL against an actual
database, not mocks of the engine surface.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.pool import StaticPool

from cdk.sql.generic import GenericSQLConnector, _StreamState
from cdk.types import AckStatus, Cursor


def _sqlite_sync_engine():
    """One shared in-memory database visible across threads.

    The handler dispatches sync-engine work via ``asyncio.to_thread``, so
    the default per-thread SQLite memory connection would see an empty
    database; StaticPool + check_same_thread=False shares one connection.
    """
    return create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


TARGET_DDL = "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)"
COMMITS_DDL = (
    "CREATE TABLE _batch_commits ("
    " run_id TEXT NOT NULL, stream_id TEXT NOT NULL,"
    " batch_seq INTEGER NOT NULL, committed_cursor BLOB,"
    " records_written INTEGER, committed_at TIMESTAMP,"
    " PRIMARY KEY (run_id, stream_id, batch_seq))"
)


def _connected_handler(engine, write_mode: str = "insert") -> GenericSQLConnector:
    """Handler wired to *engine* with reflected tables for stream ``s1``."""
    with engine.begin() as conn:
        conn.exec_driver_sql(TARGET_DDL)
        conn.exec_driver_sql(COMMITS_DDL)
    meta = MetaData()
    table = Table("events", meta, autoload_with=engine)
    commits = Table("_batch_commits", meta, autoload_with=engine)

    contract = MagicMock()
    contract.to_db_records.side_effect = lambda rb: rb.to_pylist()

    handler = GenericSQLConnector()
    handler._connected = True
    handler._sync_engine = engine
    handler._streams["s1"] = _StreamState(
        schema_name="",
        table_name="events",
        table=table,
        batch_commits_table=commits,
        write_mode=write_mode,
        primary_keys=["id"],
        conflict_keys=[],
        schema_contract=contract,
    )
    return handler


def _batch(rows) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows)


async def _write(handler, *, seq: int = 1, rows=None, token: bytes = b"tok"):
    return await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=seq,
        record_batch=_batch(
            rows if rows is not None else [{"id": 1, "name": "a"}]
        ),
        record_ids=[],
        cursor=Cursor(token=token),
    )


def _count(engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.exec_driver_sql(f"SELECT count(*) FROM {table}").scalar_one()


class TestSyncEngineWritePath:
    @pytest.mark.asyncio
    async def test_insert_writes_rows_and_commit_record(self):
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            result = await _write(
                handler, rows=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
            )
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert result.records_written == 2
            assert _count(engine, "events") == 2
            assert _count(engine, "_batch_commits") == 1
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_replay_returns_already_committed(self):
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            first = await _write(handler, token=b"cur-1")
            assert first.status == AckStatus.ACK_STATUS_SUCCESS
            replay = await _write(handler, token=b"cur-1")
            assert replay.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED
            assert replay.records_written == 1
            assert replay.committed_cursor.token == b"cur-1"
            # The replay did not double-insert.
            assert _count(engine, "events") == 1
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_truncate_insert_replaces_rows(self):
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine, write_mode="truncate_insert")
            await _write(handler, seq=1, rows=[{"id": 1, "name": "old"}])
            result = await _write(
                handler, seq=2, rows=[{"id": 7, "name": "new"}]
            )
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            with engine.connect() as conn:
                rows = conn.exec_driver_sql("SELECT id, name FROM events").all()
            assert rows == [(7, "new")]
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_empty_batch_still_records_commit(self):
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            result = await _write(handler, rows=[])
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert result.records_written == 0
            assert _count(engine, "_batch_commits") == 1
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_failed_write_rolls_back_atomically(self):
        # A duplicate PK makes the DML raise; the commit record shares the
        # transaction, so neither the rows nor the record may survive.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            await _write(handler, seq=1, rows=[{"id": 1, "name": "a"}])
            result = await _write(
                handler, seq=2, rows=[{"id": 1, "name": "dupe"}]
            )
            assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
            assert _count(engine, "events") == 1
            assert _count(engine, "_batch_commits") == 1
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_health_check_runs_on_sync_engine(self):
        engine = _sqlite_sync_engine()
        try:
            handler = GenericSQLConnector()
            handler._connected = True
            handler._sync_engine = engine
            assert await handler.health_check() is True
        finally:
            engine.dispose()


class TestSyncEngineDdl:
    @pytest.mark.asyncio
    async def test_ddl_and_reflect_returns_bound_tables(self):
        engine = _sqlite_sync_engine()
        try:
            handler = GenericSQLConnector()
            handler._sync_engine = engine
            state = _StreamState(schema_name="", table_name="events")
            import asyncio

            table, commits = await asyncio.to_thread(
                handler._ddl_and_reflect_on_sync_engine,
                state, TARGET_DDL, COMMITS_DDL,
            )
            assert {c.name for c in table.columns} == {"id", "name"}
            assert "batch_seq" in {c.name for c in commits.columns}
        finally:
            engine.dispose()


class _SyncReadRuntime:
    """Materialized-runtime stand-in carrying a real sync engine."""

    def __init__(self, engine: Any, driver: str = "sqlite"):
        self.is_adbc = False
        self.is_sync_sqlalchemy = True
        self.sync_engine = engine
        self.driver = driver
        self.close = AsyncMock()


class TestSyncEngineReadPath:
    @pytest.mark.asyncio
    async def test_read_batches_pages_over_sync_engine(self):
        engine = _sqlite_sync_engine()
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(TARGET_DDL)
                for i in range(3):
                    conn.exec_driver_sql(
                        f"INSERT INTO events (id, name) VALUES ({i}, 'n{i}')"
                    )
            runtime = _SyncReadRuntime(engine)
            checkpoint = AsyncMock()
            checkpoint.get_cursor = AsyncMock(return_value=None)
            checkpoint.save_cursor = AsyncMock()
            config = {
                "endpoint_document": {
                    "database_object": {"name": "events"},
                    "columns": [{"name": "id"}, {"name": "name"}],
                },
                "stream_source": {},
            }
            connector = GenericSQLConnector()
            out = []
            with patch(
                "cdk.sql.generic.materialize_runtime", new=AsyncMock()
            ), patch("cdk.sql.generic.SchemaContract") as sc:
                sc.return_value.from_pylist.side_effect = lambda rows: rows
                async for batch in connector.read_batches(
                    runtime,
                    config,
                    checkpoint=checkpoint,
                    stream_name="s",
                    batch_size=2,
                ):
                    out.append(batch)
            # 3 rows at batch_size=2 -> two pages.
            assert [len(page) for page in out] == [2, 1]
            assert out[0][0] == {"id": 0, "name": "n0"}
            runtime.close.assert_awaited()
        finally:
            engine.dispose()
