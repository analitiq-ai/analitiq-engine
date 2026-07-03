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


@pytest.mark.unit
class TestZeroBatchTruncate:
    """A truncate_insert source that yields no batches must still truncate.

    Without the fix (issue #312), write_batch never fires and the
    previous run's rows survive untouched. The fix has two parts:

    1. _load_stage sets stream_metrics["_zero_batch_truncate_needed"] when
       it exits cleanly with zero batches on a truncate_insert stream.

    2. _process_stream sends the synthetic batch AFTER asyncio.gather
       returns without exception — so an upstream (extract/transform)
       failure never triggers a spurious truncate.
    """

    # ------------------------------------------------------------------ #
    # Part 1: _load_stage flag-setting behaviour                          #
    # ------------------------------------------------------------------ #

    def _engine(self, max_retries=0):
        engine = StreamingEngine.__new__(StreamingEngine)
        engine.max_retries = max_retries
        engine.retry_delay = 0
        engine.error_strategy = "fail"
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

    async def _run_zero_batch_load_stage(self, engine, config):
        import asyncio

        input_queue: asyncio.Queue = asyncio.Queue()
        await input_queue.put(None)  # end-of-stream immediately — zero batches
        output_queue: asyncio.Queue = asyncio.Queue()

        grpc_client = MagicMock()
        grpc_client.send_batch = AsyncMock()

        stream_dlq = MagicMock()
        stream_dlq.send_batch = AsyncMock()

        stream_metrics = {
            "records_processed": 0,
            "records_failed": 0,
            "records_skipped": 0,
            "batches_processed": 0,
            "batches_failed": 0,
        }
        await engine._load_stage(
            input_queue,
            output_queue,
            grpc_client,
            config,
            stream_dlq,
            "run-1",
            stream_metrics,
        )
        return grpc_client, stream_metrics

    @pytest.mark.asyncio
    async def test_load_stage_sets_flag_for_zero_batch_truncate_insert(self):
        """_load_stage sets the deferred-truncate flag when it exits cleanly
        with zero batches on a truncate_insert stream — the outer scope
        (_process_stream) then sends the synthetic batch after gather."""
        engine = self._engine()
        grpc_client, sm = await self._run_zero_batch_load_stage(
            engine, self._config("truncate_insert")
        )

        assert sm.get("_zero_batch_truncate_needed") is True
        grpc_client.send_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_load_stage_sets_no_flag_for_zero_batch_insert(self):
        """Non-truncate_insert streams do not set the flag."""
        engine = self._engine()
        _, sm = await self._run_zero_batch_load_stage(engine, self._config("insert"))
        assert "_zero_batch_truncate_needed" not in sm

    @pytest.mark.asyncio
    async def test_load_stage_sets_no_flag_for_zero_batch_upsert(self):
        """Upsert streams do not set the flag either."""
        engine = self._engine()
        _, sm = await self._run_zero_batch_load_stage(engine, self._config("upsert"))
        assert "_zero_batch_truncate_needed" not in sm

    # ------------------------------------------------------------------ #
    # Part 2: outer-scope (_process_stream) retry behaviour               #
    # ------------------------------------------------------------------ #

    def _process_stream_engine(self, max_retries=0):
        """Minimal StreamingEngine for _process_stream outer-scope tests."""
        from unittest.mock import MagicMock

        engine = StreamingEngine.__new__(StreamingEngine)
        engine.max_retries = max_retries
        engine.retry_delay = 0
        engine.error_strategy = "fail"
        engine.metrics = MagicMock()
        engine.pipeline_id = "p1"
        engine.buffer_size = 1
        engine.state_manager = MagicMock()
        engine.state_manager.current_run_id = "run-1"
        engine.dlq = MagicMock()
        engine.dlq.dlq_path = "/tmp/test-dlq"
        return engine

    async def _invoke_process_stream(
        self, engine, ack_statuses, write_mode="truncate_insert"
    ):
        """Drive the zero-batch truncate section of _process_stream.

        Replaces all pipeline stages with a no-op task that sets the
        deferred-truncate flag and returns immediately, so gather
        completes synchronously and the post-gather code runs.
        """
        from unittest.mock import patch

        grpc_client = MagicMock()
        grpc_client.connect = AsyncMock(return_value=True)
        grpc_client.start_stream = AsyncMock(return_value=True)
        grpc_client.stream_retry_semantics = None
        grpc_client.disconnect = AsyncMock()

        status_iter = iter(ack_statuses)

        def _make_result(status):
            r = MagicMock()
            r.status = status
            r.failure_summary = f"ack={status}"
            r.committed_cursor = None
            r.records_written = 0
            return r

        async def _send_batch_side_effect(**_kw):
            return _make_result(next(status_iter))

        grpc_client.send_batch = AsyncMock(side_effect=_send_batch_side_effect)

        def _fake_stages(**kwargs):
            sm = kwargs["stream_metrics"]
            if write_mode == "truncate_insert":
                sm["_zero_batch_truncate_needed"] = True
            return []  # empty list → asyncio.gather(*[]) returns immediately

        stream_config = {
            "name": "s",
            "stream_id": "s1",
            "source": {},
            "destination": {"write_mode": write_mode},
            "mapping": {},
        }
        pipeline_config = {"pipeline_id": "p1"}

        with (
            patch.object(engine, "_create_source_connector", return_value=MagicMock()),
            patch.object(engine, "_create_grpc_client", return_value=grpc_client),
            patch.object(engine, "_create_pipeline_stages", side_effect=_fake_stages),
            patch("src.engine.engine.DeadLetterQueue"),
            patch("src.engine.engine.create_metrics_record", return_value=MagicMock()),
        ):
            await engine._process_stream("s1", stream_config, pipeline_config)

        return grpc_client

    @pytest.mark.asyncio
    async def test_outer_scope_success(self):
        """A SUCCESS ack for the synthetic batch completes without raising."""
        from src.grpc.generated.analitiq.v1 import AckStatus

        engine = self._process_stream_engine()
        grpc_client = await self._invoke_process_stream(
            engine, [AckStatus.ACK_STATUS_SUCCESS]
        )
        grpc_client.send_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_outer_scope_already_committed(self):
        """ALREADY_COMMITTED is accepted without raising (idempotent replay
        of a zero-batch run that was retried after a crash)."""
        from src.grpc.generated.analitiq.v1 import AckStatus

        engine = self._process_stream_engine()
        grpc_client = await self._invoke_process_stream(
            engine, [AckStatus.ACK_STATUS_ALREADY_COMMITTED]
        )
        grpc_client.send_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_outer_scope_retryable_retries_then_succeeds(self):
        """RETRYABLE_FAILURE respects max_retries: the synthetic batch is
        retried up to max_retries times before giving up."""
        from src.grpc.generated.analitiq.v1 import AckStatus

        engine = self._process_stream_engine(max_retries=2)
        # Two transient failures then a success — should complete cleanly.
        ack_seq = [
            AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            AckStatus.ACK_STATUS_SUCCESS,
        ]
        grpc_client = await self._invoke_process_stream(engine, ack_seq)
        assert grpc_client.send_batch.await_count == 3

    @pytest.mark.asyncio
    async def test_outer_scope_retryable_exhausted_raises(self):
        """When RETRYABLE_FAILURE persists beyond max_retries the stream
        fails loud — stale data must not survive silently."""
        from src.engine.exceptions import StreamProcessingError
        from src.grpc.generated.analitiq.v1 import AckStatus

        engine = self._process_stream_engine(max_retries=0)
        with pytest.raises(StreamProcessingError, match="zero-batch truncate"):
            await self._invoke_process_stream(
                engine, [AckStatus.ACK_STATUS_RETRYABLE_FAILURE]
            )

    @pytest.mark.asyncio
    async def test_outer_scope_fatal_failure_raises(self):
        """FATAL_FAILURE on the synthetic batch raises StreamProcessingError."""
        from src.engine.exceptions import StreamProcessingError
        from src.grpc.generated.analitiq.v1 import AckStatus

        engine = self._process_stream_engine()
        with pytest.raises(StreamProcessingError, match="zero-batch truncate"):
            await self._invoke_process_stream(
                engine, [AckStatus.ACK_STATUS_FATAL_FAILURE]
            )

    @pytest.mark.asyncio
    async def test_outer_scope_no_send_for_non_truncate_insert(self):
        """When write_mode is not truncate_insert, no synthetic batch is
        ever sent regardless of how many batches the source produced."""
        engine = self._process_stream_engine()
        grpc_client = await self._invoke_process_stream(engine, [], write_mode="insert")
        grpc_client.send_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_extract_failure_does_not_truncate(self):
        """An upstream (extract/transform) failure must never trigger a
        spurious truncate. The safety guarantee: gather re-raises before
        the post-gather synthetic-batch block runs, so send_batch is
        never called even if _load_stage had set the flag."""
        import asyncio
        from unittest.mock import patch

        engine = self._process_stream_engine()

        grpc_client = MagicMock()
        grpc_client.connect = AsyncMock(return_value=True)
        grpc_client.start_stream = AsyncMock(return_value=True)
        grpc_client.stream_retry_semantics = None
        grpc_client.send_batch = AsyncMock()
        grpc_client.disconnect = AsyncMock()

        def _failing_stages(**kwargs):
            sm = kwargs["stream_metrics"]
            # Simulate _load_stage having set the flag before extract
            # raised — the worst-case race scenario.
            sm["_zero_batch_truncate_needed"] = True

            async def _raise():
                raise RuntimeError("source connection refused")

            return [asyncio.create_task(_raise())]

        stream_config = {
            "name": "s",
            "stream_id": "s1",
            "source": {},
            "destination": {"write_mode": "truncate_insert"},
            "mapping": {},
        }
        pipeline_config = {"pipeline_id": "p1"}

        with (
            patch.object(engine, "_create_source_connector", return_value=MagicMock()),
            patch.object(engine, "_create_grpc_client", return_value=grpc_client),
            patch.object(
                engine, "_create_pipeline_stages", side_effect=_failing_stages
            ),
            patch("src.engine.engine.DeadLetterQueue"),
            patch("src.engine.engine.create_metrics_record", return_value=MagicMock()),
        ):
            with pytest.raises(RuntimeError, match="source connection refused"):
                await engine._process_stream("s1", stream_config, pipeline_config)

        # The whole point of the post-gather placement: gather raised, so
        # the synthetic truncate block was never reached.
        grpc_client.send_batch.assert_not_awaited()

    # ------------------------------------------------------------------ #
    # Part 3: end-to-end via SQL handler write_batch                      #
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_zero_batch_truncate_insert_clears_table_sqlalchemy(self):
        """End-to-end: an empty source with truncate_insert leaves the
        destination table empty (SQLAlchemy path), not stale from the
        previous run."""
        engine = _sqlite_engine()
        table = _table(engine)
        handler = _sqlite_handler(engine, table)

        # Previous run populated the table
        await _write(handler, 1, [{"id": 1, "name": "prev"}], run_id="run-0")
        assert _rows(engine, table) == [(1, "prev")]

        # Synthetic empty batch sent by engine when source yields nothing
        r = await _write(handler, 1, [], run_id="run-1")
        assert r.success
        assert _rows(engine, table) == []

    @pytest.mark.asyncio
    async def test_zero_batch_truncate_insert_clears_table_adbc(self):
        """End-to-end: an empty source with truncate_insert triggers the
        ADBC truncate path when the engine sends the synthetic batch."""
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
        handler._adbc_truncate_sync = MagicMock()
        handler._truncate_then_ingest_sync = MagicMock()
        handler._adbc_only_ingest_sync = MagicMock()

        r = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.record_batch([], schema=pa.schema([])),
            record_ids=[],
            cursor=MagicMock(),
        )
        assert r.success
        handler._adbc_truncate_sync.assert_called_once()
        handler._truncate_then_ingest_sync.assert_not_called()
        handler._adbc_only_ingest_sync.assert_not_called()
