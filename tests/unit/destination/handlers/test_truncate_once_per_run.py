"""truncate_insert truncates once per (run, stream), not per batch (issue #307).

Truncating per batch kept only the final batch of a multi-batch full
refresh. These tests drive the real SQLAlchemy write path against an
in-memory SQLite engine (multi-batch accumulation, per-run re-truncate,
failed-first-batch re-truncate) and the ADBC dispatch with mocked sync
writers, plus the engine-side full-refresh checkpoint view that keeps a
truncate_insert stream from resuming mid-cursor.
"""

from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.pool import StaticPool

from cdk.sql.generic import GenericSQLConnector
from cdk.sql.generic import _StreamState as SqlStreamState
from src.engine.engine import _FullRefreshCheckpoint


def _batch(rows: list[dict]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows)


def _sqlite_handler() -> tuple[GenericSQLConnector, Table]:
    """Handler wired to a real in-memory SQLite sync engine.

    ``StaticPool`` + ``check_same_thread=False`` keep the single in-memory
    database visible across threads — the sync write path runs via
    ``asyncio.to_thread``.
    """
    engine = create_engine(
        "sqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    metadata = MetaData()
    table = Table(
        "t",
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    metadata.create_all(engine)

    handler = GenericSQLConnector()
    handler._connected = True
    handler._sync_engine = engine
    handler._streams["s1"] = SqlStreamState(
        table_name="t",
        table=table,
        write_mode="truncate_insert",
    )
    # The schema-contract realignment is exercised elsewhere; here the
    # batch is already destination-shaped.
    handler._prepare_for_sqlalchemy = (  # type: ignore[method-assign]
        lambda state, record_batch: record_batch.to_pylist()
    )
    return handler, table


def _rows(handler: GenericSQLConnector, table: Table) -> list[tuple]:
    with handler._require_sync_engine().connect() as conn:
        return sorted(conn.execute(select(table.c.id, table.c.name)).fetchall())


async def _write(handler: GenericSQLConnector, run_id: str, seq: int, rows: list[dict]):
    return await handler.write_batch(
        run_id=run_id,
        stream_id="s1",
        batch_seq=seq,
        record_batch=_batch(rows),
        record_ids=[f"r{r['id']}" for r in rows],
        cursor=MagicMock(),
    )


@pytest.mark.unit
class TestTruncateOncePerRunSqlAlchemy:
    @pytest.mark.asyncio
    async def test_multi_batch_refresh_keeps_every_batch(self):
        """The defect in issue #307: batch 2 must not wipe batch 1."""
        handler, table = _sqlite_handler()

        r1 = await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        r2 = await _write(handler, "run-1", 2, [{"id": 2, "name": "b"}])

        assert r1.success and r2.success
        assert _rows(handler, table) == [(1, "a"), (2, "b")]

    @pytest.mark.asyncio
    async def test_new_run_truncates_the_previous_refresh(self):
        """A fresh run is a fresh refresh: its first write empties the
        table, so the target equals the new run's source snapshot."""
        handler, table = _sqlite_handler()

        await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        await _write(handler, "run-2", 1, [{"id": 9, "name": "z"}])

        assert _rows(handler, table) == [(9, "z")]

    @pytest.mark.asyncio
    async def test_failed_first_batch_retruncates_on_retry(self):
        """The gate is marked only after the write commits: a failed
        truncating batch must truncate again on its retry, not append
        into a table that was never emptied."""
        handler, table = _sqlite_handler()
        # Rows left behind by a previous run.
        await _write(handler, "run-0", 1, [{"id": 7, "name": "old"}])

        original = handler._insert_records
        calls = {"n": 0}

        def _flaky_insert(conn, state, records):
            calls["n"] += 1
            if calls["n"] == 1:  # run-1's first attempt fails mid-transaction
                raise RuntimeError("insert blew up")
            original(conn, state, records)

        handler._insert_records = _flaky_insert  # type: ignore[method-assign]

        failed = await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        assert failed.success is False
        # The rollback restored the previous rows and the gate stayed unset.
        assert ("run-1", "s1") not in handler._refresh_truncated
        assert _rows(handler, table) == [(7, "old")]

        retried = await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        assert retried.success
        assert _rows(handler, table) == [(1, "a")]

    @pytest.mark.asyncio
    async def test_gate_is_per_stream(self):
        """Two truncate_insert streams in one run each truncate their own
        table exactly once."""
        handler, table = _sqlite_handler()
        metadata = MetaData()
        table2 = Table(
            "t2",
            metadata,
            Column("id", Integer),
            Column("name", String),
        )
        metadata.create_all(handler._require_sync_engine())
        handler._streams["s2"] = SqlStreamState(
            table_name="t2",
            table=table2,
            write_mode="truncate_insert",
        )

        await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        r = await handler.write_batch(
            run_id="run-1",
            stream_id="s2",
            batch_seq=1,
            record_batch=_batch([{"id": 5, "name": "e"}]),
            record_ids=["r5"],
            cursor=MagicMock(),
        )
        assert r.success
        assert handler._refresh_truncated == {("run-1", "s1"), ("run-1", "s2")}
        assert _rows(handler, table) == [(1, "a")]


@pytest.mark.unit
class TestTruncateOncePerRunAdbc:
    def _adbc_handler(self) -> GenericSQLConnector:
        handler = GenericSQLConnector()
        handler._connected = True
        handler._adbc_only = True
        contract = MagicMock()
        contract.cast_arrow_batch = lambda b: b
        handler._streams["s1"] = SqlStreamState(
            table_name="t",
            write_mode="truncate_insert",
            schema_contract=contract,
        )
        handler._truncate_then_ingest_sync = MagicMock()  # type: ignore[method-assign]
        handler._adbc_only_ingest_sync = MagicMock()  # type: ignore[method-assign]
        return handler

    @pytest.mark.asyncio
    async def test_only_the_runs_first_batch_truncates(self):
        handler = self._adbc_handler()

        await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        await _write(handler, "run-1", 2, [{"id": 2, "name": "b"}])

        assert handler._truncate_then_ingest_sync.call_count == 1
        assert handler._adbc_only_ingest_sync.call_count == 1

    @pytest.mark.asyncio
    async def test_a_new_run_truncates_again(self):
        handler = self._adbc_handler()

        await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        await _write(handler, "run-2", 1, [{"id": 1, "name": "a"}])

        assert handler._truncate_then_ingest_sync.call_count == 2
        assert handler._adbc_only_ingest_sync.call_count == 0

    @pytest.mark.asyncio
    async def test_failed_truncating_batch_keeps_the_gate_unset(self):
        handler = self._adbc_handler()
        handler._truncate_then_ingest_sync.side_effect = RuntimeError("down")

        result = await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        assert result.success is False
        assert ("run-1", "s1") not in handler._refresh_truncated

        handler._truncate_then_ingest_sync.side_effect = None
        retried = await _write(handler, "run-1", 1, [{"id": 1, "name": "a"}])
        assert retried.success
        assert handler._truncate_then_ingest_sync.call_count == 2


@pytest.mark.unit
class TestFullRefreshCheckpoint:
    @pytest.mark.asyncio
    async def test_get_cursor_never_resumes(self):
        """Even with a persisted checkpoint, a truncate_insert stream must
        re-read the source from scratch (issue #307): a resumed slice would
        be the only data left after the run's truncate."""
        inner = MagicMock()
        inner.get_cursor = AsyncMock(return_value={"cursor": "2024-01-01"})
        view = _FullRefreshCheckpoint(inner)

        assert await view.get_cursor("s1", {}) is None
        inner.get_cursor.assert_not_called()

    @pytest.mark.asyncio
    async def test_save_cursor_delegates(self):
        """Watermark tracking and state emission stay unchanged."""
        inner = MagicMock()
        inner.save_cursor = AsyncMock()
        view = _FullRefreshCheckpoint(inner)

        await view.save_cursor("s1", {}, {"cursor": "x"})
        inner.save_cursor.assert_awaited_once_with("s1", {}, {"cursor": "x"})
