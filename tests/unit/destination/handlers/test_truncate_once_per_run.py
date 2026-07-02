"""truncate_insert truncates on the read's first batch only (issue #307).

Truncating per batch kept only the final batch of a multi-batch full
refresh. The truncate now keys on ``batch_seq == 1`` — the engine's own
statement of a fresh read — so the decision survives the engine and the
destination restarting independently: a fresh worker joining mid-refresh
appends (committed batches survive), and a surviving worker re-truncates
when a restarted engine re-reads from scratch.

These tests drive the real SQLAlchemy write path against an in-memory
SQLite engine, the ADBC dispatch with mocked sync writers, and the
engine-side full-refresh checkpoint view that keeps a truncate_insert
stream from resuming mid-cursor.
"""

from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.pool import StaticPool

from cdk.sql.generic import GenericSQLConnector
from cdk.sql.generic import _StreamState as SqlStreamState
from src.engine.engine import StreamingEngine, _FullRefreshCheckpoint


def _batch(rows: list[dict]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows)


def _sqlite_engine():
    """One in-memory SQLite database visible across threads.

    ``StaticPool`` + ``check_same_thread=False`` keep the single database
    shared — the sync write path runs via ``asyncio.to_thread``.
    """
    return create_engine(
        "sqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )


def _table(engine, name: str = "t") -> Table:
    metadata = MetaData()
    table = Table(
        name,
        metadata,
        Column("id", Integer),
        Column("name", String),
    )
    metadata.create_all(engine)
    return table


def _sqlite_handler(engine, table: Table) -> GenericSQLConnector:
    """Handler wired to the given SQLite sync engine and target table."""
    handler = GenericSQLConnector()
    handler._connected = True
    handler._sync_engine = engine
    handler._streams["s1"] = SqlStreamState(
        table_name=table.name,
        table=table,
        write_mode="truncate_insert",
    )
    # The schema-contract realignment is exercised elsewhere; here the
    # batch is already destination-shaped.
    handler._prepare_for_sqlalchemy = (  # type: ignore[method-assign]
        lambda state, record_batch: record_batch.to_pylist()
    )
    return handler


def _rows(engine, table: Table) -> list[tuple]:
    with engine.connect() as conn:
        return sorted(conn.execute(select(table.c.id, table.c.name)).fetchall())


async def _write(
    handler: GenericSQLConnector,
    seq: int,
    rows: list[dict],
    *,
    run_id: str = "run-1",
    stream_id: str = "s1",
):
    return await handler.write_batch(
        run_id=run_id,
        stream_id=stream_id,
        batch_seq=seq,
        record_batch=_batch(rows),
        record_ids=[f"r{r['id']}" for r in rows],
        cursor=MagicMock(),
    )


@pytest.mark.unit
class TestTruncateOnFirstBatchSqlAlchemy:
    @pytest.mark.asyncio
    async def test_multi_batch_refresh_keeps_every_batch(self):
        """The defect in issue #307: batch 2 must not wipe batch 1."""
        engine = _sqlite_engine()
        table = _table(engine)
        handler = _sqlite_handler(engine, table)

        r1 = await _write(handler, 1, [{"id": 1, "name": "a"}])
        r2 = await _write(handler, 2, [{"id": 2, "name": "b"}])

        assert r1.success and r2.success
        assert _rows(engine, table) == [(1, "a"), (2, "b")]

    @pytest.mark.asyncio
    async def test_new_read_replaces_the_previous_refresh(self):
        """A restarted read sends batch_seq 1 again: its first write
        empties the table, so the target equals the new read's snapshot."""
        engine = _sqlite_engine()
        table = _table(engine)
        handler = _sqlite_handler(engine, table)

        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 1, [{"id": 9, "name": "z"}], run_id="run-2")

        assert _rows(engine, table) == [(9, "z")]

    @pytest.mark.asyncio
    async def test_destination_restart_mid_refresh_appends(self):
        """A destination worker that restarts mid-refresh receives a
        batch_seq > 1 first — it must append, never truncate away the
        batches the previous worker process committed."""
        engine = _sqlite_engine()
        table = _table(engine)
        first_worker = _sqlite_handler(engine, table)
        await _write(first_worker, 1, [{"id": 1, "name": "a"}])
        await _write(first_worker, 2, [{"id": 2, "name": "b"}])

        restarted_worker = _sqlite_handler(engine, table)
        r = await _write(restarted_worker, 3, [{"id": 3, "name": "c"}])

        assert r.success
        assert _rows(engine, table) == [(1, "a"), (2, "b"), (3, "c")]

    @pytest.mark.asyncio
    async def test_engine_restart_retruncates_on_a_surviving_worker(self):
        """An engine that restarts re-reads from scratch and sends
        batch_seq 1 to the still-running worker — the refresh must
        replace the dead attempt's rows, not append onto them."""
        engine = _sqlite_engine()
        table = _table(engine)
        handler = _sqlite_handler(engine, table)
        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 2, [{"id": 2, "name": "b"}])

        # Same worker instance, same run_id: the engine restarted its read.
        await _write(handler, 1, [{"id": 1, "name": "a"}])
        r = await _write(handler, 2, [{"id": 2, "name": "b"}])

        assert r.success
        assert _rows(engine, table) == [(1, "a"), (2, "b")]

    @pytest.mark.asyncio
    async def test_replayed_first_batch_lands_exactly_once(self):
        """An ack lost after the first batch committed re-sends batch_seq 1;
        the re-truncate makes the replay land exactly once."""
        engine = _sqlite_engine()
        table = _table(engine)
        handler = _sqlite_handler(engine, table)

        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 1, [{"id": 1, "name": "a"}])

        assert _rows(engine, table) == [(1, "a")]

    @pytest.mark.asyncio
    async def test_failed_first_batch_rolls_back_and_retruncates_on_retry(self):
        """The delete shares the batch transaction: a failed first batch
        restores the previous rows, and its retry truncates again."""
        engine = _sqlite_engine()
        table = _table(engine)
        handler = _sqlite_handler(engine, table)
        await _write(handler, 1, [{"id": 7, "name": "old"}], run_id="run-0")

        original = handler._insert_records
        calls = {"n": 0}

        def _flaky_insert(conn, state, records):
            calls["n"] += 1
            if calls["n"] == 1:  # run-1's first attempt fails mid-transaction
                raise RuntimeError("insert blew up")
            original(conn, state, records)

        handler._insert_records = _flaky_insert  # type: ignore[method-assign]

        failed = await _write(handler, 1, [{"id": 1, "name": "a"}])
        assert failed.success is False
        assert _rows(engine, table) == [(7, "old")]

        retried = await _write(handler, 1, [{"id": 1, "name": "a"}])
        assert retried.success
        assert _rows(engine, table) == [(1, "a")]

    @pytest.mark.asyncio
    async def test_empty_first_batch_still_truncates(self):
        """A refresh whose first batch is DELIVERED with zero rows must
        still empty the table. (A source that yields no batches at all
        never reaches write_batch — that gap is issue #312.)"""
        engine = _sqlite_engine()
        table = _table(engine)
        handler = _sqlite_handler(engine, table)
        await _write(handler, 1, [{"id": 7, "name": "old"}], run_id="run-0")

        r = await _write(handler, 1, [])
        assert r.success
        assert _rows(engine, table) == []

        r2 = await _write(handler, 2, [{"id": 1, "name": "a"}])
        assert r2.success
        assert _rows(engine, table) == [(1, "a")]

    @pytest.mark.asyncio
    async def test_streams_truncate_independently(self):
        """Two truncate_insert streams sharing the handler each truncate
        their own table on their own first batch."""
        engine = _sqlite_engine()
        table = _table(engine)
        table2 = _table(engine, "t2")
        handler = _sqlite_handler(engine, table)
        handler._streams["s2"] = SqlStreamState(
            table_name="t2",
            table=table2,
            write_mode="truncate_insert",
        )

        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 1, [{"id": 5, "name": "e"}], stream_id="s2")
        await _write(handler, 2, [{"id": 2, "name": "b"}])

        assert _rows(engine, table) == [(1, "a"), (2, "b")]
        assert _rows(engine, table2) == [(5, "e")]


@pytest.mark.unit
class TestTruncateOnFirstBatchAdbc:
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
        handler._adbc_truncate_sync = MagicMock()  # type: ignore[method-assign]
        return handler

    @pytest.mark.asyncio
    async def test_only_the_first_batch_truncates(self):
        handler = self._adbc_handler()

        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 2, [{"id": 2, "name": "b"}])

        assert handler._truncate_then_ingest_sync.call_count == 1
        assert handler._adbc_only_ingest_sync.call_count == 1

    @pytest.mark.asyncio
    async def test_a_new_read_truncates_again(self):
        handler = self._adbc_handler()

        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 1, [{"id": 1, "name": "a"}], run_id="run-2")

        assert handler._truncate_then_ingest_sync.call_count == 2
        assert handler._adbc_only_ingest_sync.call_count == 0

    @pytest.mark.asyncio
    async def test_mid_refresh_batch_never_truncates_on_a_fresh_worker(self):
        """A restarted worker has no memory — and needs none: batch_seq > 1
        routes to the plain append regardless of process history."""
        handler = self._adbc_handler()

        await _write(handler, 3, [{"id": 3, "name": "c"}])

        assert handler._truncate_then_ingest_sync.call_count == 0
        assert handler._adbc_only_ingest_sync.call_count == 1

    @pytest.mark.asyncio
    async def test_failed_first_batch_truncates_again_on_retry(self):
        handler = self._adbc_handler()
        handler._truncate_then_ingest_sync.side_effect = RuntimeError("down")

        result = await _write(handler, 1, [{"id": 1, "name": "a"}])
        assert result.success is False

        handler._truncate_then_ingest_sync.side_effect = None
        retried = await _write(handler, 1, [{"id": 1, "name": "a"}])
        assert retried.success
        assert handler._truncate_then_ingest_sync.call_count == 2

    @pytest.mark.asyncio
    async def test_empty_first_batch_truncates_without_ingest(self):
        handler = self._adbc_handler()

        r = await _write(handler, 1, [])
        assert r.success
        assert handler._adbc_truncate_sync.call_count == 1
        assert handler._truncate_then_ingest_sync.call_count == 0
        assert handler._adbc_only_ingest_sync.call_count == 0


@pytest.mark.unit
class TestFirstBatchDropGuard:
    """A dropped FIRST batch must fail a truncate_insert stream whatever
    the error strategy: the destination truncates on batch_seq 1, so
    skipping/DLQ-ing it and continuing would let batch 2 append onto the
    previous refresh's rows — stale data mixed into a partial snapshot."""

    def _engine(self):
        engine = StreamingEngine.__new__(StreamingEngine)
        engine.max_retries = 0
        engine.retry_delay = 0
        engine.error_strategy = "dlq"
        engine.metrics = MagicMock()
        engine.pipeline_id = "p1"
        return engine

    def _config(self, write_mode: str) -> dict:
        resolved_source = MagicMock()
        resolved_source.replication = None
        resolved_source.primary_keys = []
        return {
            "stream_name": "s",
            "stream_id": "s1",
            "stream_version": 1,
            "source": {"_resolved_source": resolved_source},
            "destination": {"write_mode": write_mode},
        }

    async def _run_load_stage(self, engine, config):
        import asyncio

        from src.grpc.generated.analitiq.v1 import AckStatus

        input_queue: asyncio.Queue = asyncio.Queue()
        await input_queue.put(_batch([{"id": 1, "name": "a"}]))
        await input_queue.put(None)
        output_queue: asyncio.Queue = asyncio.Queue()

        grpc_client = MagicMock()
        result = MagicMock()
        result.status = AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        result.failure_summary = "db down"
        grpc_client.send_batch = AsyncMock(return_value=result)

        stream_dlq = MagicMock()
        stream_dlq.send_batch = AsyncMock()

        await engine._load_stage(
            input_queue,
            output_queue,
            grpc_client,
            config,
            stream_dlq,
            "run-1",
            {"records_processed": 0, "records_failed": 0, "batches_failed": 0},
        )
        return stream_dlq

    @pytest.mark.asyncio
    async def test_dropped_first_batch_fails_a_truncate_insert_stream(self):
        from src.engine.exceptions import StreamProcessingError

        engine = self._engine()
        with pytest.raises(StreamProcessingError, match="truncate_insert"):
            await self._run_load_stage(engine, self._config("truncate_insert"))

    @pytest.mark.asyncio
    async def test_other_write_modes_keep_the_configured_strategy(self):
        engine = self._engine()
        stream_dlq = await self._run_load_stage(engine, self._config("upsert"))
        stream_dlq.send_batch.assert_awaited_once()


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
