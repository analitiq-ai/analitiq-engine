"""truncate_insert truncates on the read's first batch only (issue #307).

Truncating per batch kept only the final batch of a multi-batch full
refresh. The truncate now keys on ``batch_seq == 1`` — the engine's own
statement of a fresh read — so the decision survives the engine and the
destination restarting independently: a fresh worker joining mid-refresh
appends (committed batches survive), and a surviving worker re-truncates
when a restarted engine re-reads from scratch.

These tests drive the real SQLAlchemy write path against an in-memory
SQLite engine, the ADBC dispatch with a mocked transport backend
(asserting the plans the facade builds), and the
engine-side full-refresh checkpoint view that keeps a truncate_insert
stream from resuming mid-cursor.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.pool import StaticPool

from cdk.sql.backend import SqlAlchemyBackend
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import GenericSQLConnector
from cdk.sql.generic import _StreamState as SqlStreamState
from src.engine.stream_processor import StreamProcessor, _FullRefreshCheckpoint

# A fixed, timezone-aware emit instant for write_batch/send_batch calls; the
# engine stamps this per batch (issue #353). Value is arbitrary for sinks
# that ignore it.
_EMITTED_AT = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)


def _make_processor(
    config: dict,
    *,
    error_strategy: str = "fail",
    max_retries: int = 0,
    state_manager: MagicMock | None = None,
) -> StreamProcessor:
    """Build a StreamProcessor wired to mocks for load-stage tests."""
    processor = StreamProcessor(
        stream_id="s1",
        stream_config=config,
        pipeline_config={"pipeline_id": "p1"},
        pipeline_id="p1",
        state_manager=state_manager if state_manager is not None else MagicMock(),
        pipeline_metrics=MagicMock(),
        worker_readable=MagicMock(),
        dlq_root="./deadletter",
        batch_size=10,
        buffer_size=10,
        max_retries=max_retries,
        retry_delay=0,
        error_strategy=error_strategy,
    )
    processor.run_id = "run-1"
    return processor


def _resolved_source() -> MagicMock:
    """Resolved-source double whose runtime declares no error taxonomy (#401)."""
    resolved = MagicMock()
    resolved.runtime.connector_id = "demo"
    resolved.runtime.declared_error_map = None
    return resolved


def _batch(rows: list[dict]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows)


def _sqlite_engine():
    """One in-memory SQLite database visible across threads.

    ``StaticPool`` + ``check_same_thread=False`` keep the single database
    shared — the sync write path runs via ``asyncio.to_thread``. The
    isolation-level/BEGIN recipe makes pysqlite genuinely transactional
    for DDL too (by default it silently commits before DDL), so the
    declared ``transactional_ddl: true`` shape behaves here exactly as it
    does on Postgres: a rolled-back batch takes its stage table with it.
    """
    from sqlalchemy import event

    engine = create_engine(
        "sqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False, "isolation_level": None},
    )

    @event.listens_for(engine, "begin")
    def _do_begin(conn):
        conn.exec_driver_sql("BEGIN")

    return engine


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


class _SqliteStageDialect(SqlDialect):
    """Stage DDL for the SQLite stand-in (truncate_insert needs no merge)."""

    name = "sqlite"

    def stage_table_sql(self, stage, target, *, temp):
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return (
            f"{keyword} {self.quote_table(stage)} AS "
            f"SELECT * FROM {self.quote_table(target)} WHERE 0"
        )


_SQLITE_CAPS = SqlCapabilities.from_declaration(
    {
        "catalog": "none",
        "session_targeting": "per_statement",
        "merge_form": "none",
        "bulk_load": "none",
        "stage": {"scope": "temp", "schema": "target", "transactional_ddl": True},
    },
    source="<test>",
)


def _identity_contract() -> MagicMock:
    # The schema-contract realignment is exercised elsewhere; here the
    # batch is already destination-shaped.
    contract = MagicMock()
    contract.cast_arrow_batch.side_effect = lambda rb: rb
    return contract


def _sqlite_handler(engine, table: Table) -> GenericSQLConnector:
    """Handler wired to the given SQLite sync engine and target table."""
    handler = GenericSQLConnector()
    handler._connected = True
    handler._sync_engine = engine
    handler.dialect = _SqliteStageDialect()
    handler.dialect.capabilities = _SQLITE_CAPS
    handler._capabilities = _SQLITE_CAPS
    backend = SqlAlchemyBackend(handler.dialect)
    backend._sync_engine = engine
    backend._targets[TableAddress(table=table.name)] = table
    handler._backend = backend
    handler._streams["s1"] = SqlStreamState(
        address=TableAddress(table=table.name),
        write_mode="truncate_insert",
        schema_contract=_identity_contract(),
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
        emitted_at=_EMITTED_AT,
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

        # Break the first attempt INSIDE the stage cycle — after the
        # emptying DELETE ran — by corrupting the plan's mode statement;
        # the shared transaction must roll the DELETE back with it.
        import dataclasses
        from unittest.mock import patch

        from cdk.sql import generic as generic_module

        original_build = generic_module.build_stage_write_plan
        calls = {"n": 0}

        def _flaky_build(*args, **kwargs):
            plan = original_build(*args, **kwargs)
            calls["n"] += 1
            if calls["n"] == 1:  # run-1's first attempt fails mid-transaction
                return dataclasses.replace(
                    plan, mode_sql="INSERT INTO no_such_table_xyz SELECT 1"
                )
            return plan

        with patch.object(generic_module, "build_stage_write_plan", _flaky_build):
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
        handler._backend._targets[TableAddress(table="t2")] = table2
        handler._streams["s2"] = SqlStreamState(
            address=TableAddress(table="t2"),
            write_mode="truncate_insert",
            schema_contract=_identity_contract(),
        )

        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 1, [{"id": 5, "name": "e"}], stream_id="s2")
        await _write(handler, 2, [{"id": 2, "name": "b"}])

        assert _rows(engine, table) == [(1, "a"), (2, "b")]
        assert _rows(engine, table2) == [(5, "e")]


@pytest.mark.unit
class TestTruncateOnFirstBatchAdbc:
    """Truncate-once gating is decided by the facade and carried on the
    plan (``truncate_sql`` set on the read's first batch only, issue
    #307) — identical for the ADBC transport, whose backend just
    executes what the plan says."""

    def _adbc_handler(self) -> GenericSQLConnector:
        handler = GenericSQLConnector()
        handler._connected = True
        handler._adbc_only = True
        handler.dialect = _SqliteStageDialect()
        handler.dialect.capabilities = _SQLITE_CAPS
        handler._capabilities = _SQLITE_CAPS
        contract = MagicMock()
        contract.cast_arrow_batch = lambda b: b
        handler._streams["s1"] = SqlStreamState(
            address=TableAddress(table="t"),
            write_mode="truncate_insert",
            schema_contract=contract,
        )
        backend = MagicMock()
        backend.execute_write = AsyncMock()
        backend.run_ddl = AsyncMock()
        handler._backend = backend
        return handler

    @staticmethod
    def _plans(handler) -> list:
        return [c.args[0] for c in handler._backend.execute_write.call_args_list]

    @pytest.mark.asyncio
    async def test_only_the_first_batch_truncates(self):
        handler = self._adbc_handler()

        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 2, [{"id": 2, "name": "b"}])

        first, second = self._plans(handler)
        assert first.truncate_sql is not None
        assert second.truncate_sql is None

    @pytest.mark.asyncio
    async def test_a_new_read_truncates_again(self):
        handler = self._adbc_handler()

        await _write(handler, 1, [{"id": 1, "name": "a"}])
        await _write(handler, 1, [{"id": 1, "name": "a"}], run_id="run-2")

        assert all(plan.truncate_sql is not None for plan in self._plans(handler))

    @pytest.mark.asyncio
    async def test_mid_refresh_batch_never_truncates_on_a_fresh_worker(self):
        """A restarted worker has no memory — and needs none: batch_seq > 1
        plans a plain append regardless of process history."""
        handler = self._adbc_handler()

        await _write(handler, 3, [{"id": 3, "name": "c"}])

        (plan,) = self._plans(handler)
        assert plan.truncate_sql is None

    @pytest.mark.asyncio
    async def test_failed_first_batch_truncates_again_on_retry(self):
        handler = self._adbc_handler()
        handler._backend.execute_write.side_effect = RuntimeError("down")

        result = await _write(handler, 1, [{"id": 1, "name": "a"}])
        assert result.success is False

        handler._backend.execute_write.side_effect = None
        retried = await _write(handler, 1, [{"id": 1, "name": "a"}])
        assert retried.success
        assert all(plan.truncate_sql is not None for plan in self._plans(handler))

    @pytest.mark.asyncio
    async def test_empty_first_batch_truncates_without_ingest(self):
        handler = self._adbc_handler()

        r = await _write(handler, 1, [])
        assert r.success
        # No stage cycle for an empty batch: the emptying statement runs
        # on its own through run_ddl, and nothing is landed.
        handler._backend.run_ddl.assert_awaited_once_with(['DELETE FROM "t"'])
        handler._backend.execute_write.assert_not_called()


@pytest.mark.unit
class TestFirstBatchDropGuard:
    """A dropped FIRST batch must fail a truncate_insert stream whatever
    the error strategy: the destination truncates on batch_seq 1, so
    skipping/DLQ-ing it and continuing would let batch 2 append onto the
    previous refresh's rows — stale data mixed into a partial snapshot."""

    def _config(self, write_mode: str) -> dict:
        resolved_source = _resolved_source()
        resolved_source.replication = None
        resolved_source.primary_keys = []
        return {
            "name": "s",
            "stream_version": 1,
            "source": {"_resolved_source": resolved_source},
            "destination": {"write_mode": write_mode},
        }

    async def _run_load_stage(self, config):
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

        processor = _make_processor(config, error_strategy="dlq")
        processor.grpc_client = grpc_client
        processor.stream_dlq = stream_dlq

        await processor._load_stage(input_queue, output_queue)
        return stream_dlq

    @pytest.mark.asyncio
    async def test_dropped_first_batch_fails_a_truncate_insert_stream(self):
        from src.engine.exceptions import StreamProcessingError

        with pytest.raises(StreamProcessingError, match="truncate_insert"):
            await self._run_load_stage(self._config("truncate_insert"))

    @pytest.mark.asyncio
    async def test_other_write_modes_keep_the_configured_strategy(self):
        stream_dlq = await self._run_load_stage(self._config("upsert"))
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

    1. _load_stage sets the processor's zero_batch_truncate_needed flag
       when it exits cleanly with zero batches on a truncate_insert stream.

    2. run() sends the synthetic batch AFTER asyncio.gather returns
       without exception — so an upstream (extract/transform) failure
       never triggers a spurious truncate.
    """

    # ------------------------------------------------------------------ #
    # Part 1: _load_stage flag-setting behaviour                          #
    # ------------------------------------------------------------------ #

    def _config(self, write_mode: str) -> dict:
        resolved_source = _resolved_source()
        resolved_source.replication = None
        resolved_source.primary_keys = []
        return {
            "name": "s",
            "stream_version": 1,
            "source": {"_resolved_source": resolved_source},
            "destination": {"write_mode": write_mode},
        }

    async def _run_zero_batch_load_stage(self, config):
        import asyncio

        input_queue: asyncio.Queue = asyncio.Queue()
        await input_queue.put(None)  # end-of-stream immediately — zero batches
        output_queue: asyncio.Queue = asyncio.Queue()

        grpc_client = MagicMock()
        grpc_client.send_batch = AsyncMock()

        processor = _make_processor(config)
        processor.grpc_client = grpc_client
        processor.stream_dlq = MagicMock()

        await processor._load_stage(input_queue, output_queue)
        return grpc_client, processor

    @pytest.mark.asyncio
    async def test_load_stage_sets_flag_for_zero_batch_truncate_insert(self):
        """_load_stage sets the deferred-truncate flag when it exits cleanly
        with zero batches on a truncate_insert stream — run() then sends
        the synthetic batch after gather."""
        grpc_client, processor = await self._run_zero_batch_load_stage(
            self._config("truncate_insert")
        )

        assert processor.zero_batch_truncate_needed is True
        grpc_client.send_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_load_stage_sets_no_flag_for_zero_batch_insert(self):
        """Non-truncate_insert streams do not set the flag."""
        _, processor = await self._run_zero_batch_load_stage(self._config("insert"))
        assert processor.zero_batch_truncate_needed is False

    @pytest.mark.asyncio
    async def test_load_stage_sets_no_flag_for_zero_batch_upsert(self):
        """Upsert streams do not set the flag either."""
        _, processor = await self._run_zero_batch_load_stage(self._config("upsert"))
        assert processor.zero_batch_truncate_needed is False

    # ------------------------------------------------------------------ #
    # Part 2: run() post-gather retry behaviour                           #
    # ------------------------------------------------------------------ #

    def _run_processor(self, write_mode="truncate_insert", max_retries=0):
        """Minimal StreamProcessor for run() outer-scope tests."""
        stream_config = {
            "name": "s",
            "source": {"_resolved_source": _resolved_source()},
            "destination": {"write_mode": write_mode},
            "mapping": {},
        }
        state_manager = MagicMock()
        state_manager.current_run_id = "run-1"
        return _make_processor(
            stream_config, max_retries=max_retries, state_manager=state_manager
        )

    async def _invoke_run(self, processor, ack_statuses, failure_category=None):
        """Drive the zero-batch truncate section of run().

        Replaces the pipeline stages with a no-op that sets the
        deferred-truncate flag (for truncate_insert) and returns no tasks,
        so gather completes immediately and the post-gather code runs.
        """
        from unittest.mock import patch

        grpc_client = MagicMock()
        grpc_client.connect = AsyncMock(return_value=True)
        grpc_client.start_stream = AsyncMock(return_value=True)
        grpc_client.stream_retry_semantics = None
        grpc_client.disconnect = AsyncMock()

        status_iter = iter(ack_statuses)

        def _make_result(status):
            from cdk.types import FailureCategory

            r = MagicMock()
            r.status = status
            r.failure_summary = f"ack={status}"
            r.committed_cursor = None
            r.records_written = 0
            r.failure_category = (
                failure_category
                if failure_category is not None
                else FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
            )
            return r

        async def _send_batch_side_effect(**_kw):
            return _make_result(next(status_iter))

        grpc_client.send_batch = AsyncMock(side_effect=_send_batch_side_effect)

        def _fake_stages(source_readable):
            processor.zero_batch_truncate_needed = processor._is_truncate_insert()
            return []  # no tasks → asyncio.gather(*[]) returns immediately

        with (
            patch.object(processor, "_create_grpc_client", return_value=grpc_client),
            patch.object(processor, "_create_stage_tasks", side_effect=_fake_stages),
            patch("src.engine.stream_processor.DeadLetterQueue"),
            patch(
                "src.engine.stream_processor.create_metrics_record",
                return_value=MagicMock(),
            ),
        ):
            await processor.run()

        return grpc_client

    @pytest.mark.asyncio
    async def test_outer_scope_success(self):
        """A SUCCESS ack for the synthetic batch completes without raising."""
        from src.grpc.generated.analitiq.v1 import AckStatus

        processor = self._run_processor()
        grpc_client = await self._invoke_run(processor, [AckStatus.ACK_STATUS_SUCCESS])
        grpc_client.send_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_outer_scope_already_committed(self):
        """ALREADY_COMMITTED is accepted without raising (idempotent replay
        of a zero-batch run that was retried after a crash)."""
        from src.grpc.generated.analitiq.v1 import AckStatus

        processor = self._run_processor()
        grpc_client = await self._invoke_run(
            processor, [AckStatus.ACK_STATUS_ALREADY_COMMITTED]
        )
        grpc_client.send_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_outer_scope_retryable_retries_then_succeeds(self):
        """RETRYABLE_FAILURE respects max_retries: the synthetic batch is
        retried up to max_retries times before giving up."""
        from src.grpc.generated.analitiq.v1 import AckStatus

        processor = self._run_processor(max_retries=2)
        # Two transient failures then a success — should complete cleanly.
        ack_seq = [
            AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            AckStatus.ACK_STATUS_SUCCESS,
        ]
        grpc_client = await self._invoke_run(processor, ack_seq)
        assert grpc_client.send_batch.await_count == 3

    @pytest.mark.asyncio
    async def test_outer_scope_retryable_exhausted_raises(self):
        """When RETRYABLE_FAILURE persists beyond max_retries the stream
        fails loud — stale data must not survive silently."""
        from src.engine.exceptions import StreamProcessingError
        from src.grpc.generated.analitiq.v1 import AckStatus

        processor = self._run_processor(max_retries=0)
        with pytest.raises(StreamProcessingError, match="zero-batch truncate"):
            await self._invoke_run(processor, [AckStatus.ACK_STATUS_RETRYABLE_FAILURE])

    @pytest.mark.asyncio
    async def test_outer_scope_fatal_failure_raises(self):
        """FATAL_FAILURE on the synthetic batch raises StreamProcessingError."""
        from src.engine.exceptions import StreamProcessingError
        from src.grpc.generated.analitiq.v1 import AckStatus

        processor = self._run_processor()
        with pytest.raises(StreamProcessingError, match="zero-batch truncate"):
            await self._invoke_run(processor, [AckStatus.ACK_STATUS_FATAL_FAILURE])

    @pytest.mark.asyncio
    async def test_outer_scope_failure_classifies_from_declared_category(self):
        """The synthetic-truncate raise site classifies from the ack's
        declared failure category (issue #351), not a hardcoded
        DESTINATION_WRITE_FAILED: a NOT_READY rejection (the destination
        attempted nothing) must tag INTERNAL."""
        from cdk.types import FailureCategory
        from src.engine.exceptions import StreamProcessingError
        from src.grpc.generated.analitiq.v1 import AckStatus
        from src.state.error_classification import ErrorCode, read_failure_tag

        processor = self._run_processor()
        with pytest.raises(StreamProcessingError) as exc_info:
            await self._invoke_run(
                processor,
                [AckStatus.ACK_STATUS_FATAL_FAILURE],
                failure_category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
            )
        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.INTERNAL

    @pytest.mark.asyncio
    async def test_outer_scope_unknown_status_ignores_declared_category(self):
        """An ack whose status the engine cannot interpret must not have its
        advisory category trusted -- the same rule as the regular batch
        loop's unknown-status branch (issue #351). The declared NOT_READY
        must be ignored and the failure take the load-stage default."""
        from cdk.types import FailureCategory
        from src.engine.exceptions import StreamProcessingError
        from src.state.error_classification import ErrorCode, read_failure_tag

        processor = self._run_processor()
        with pytest.raises(StreamProcessingError) as exc_info:
            await self._invoke_run(
                processor,
                [99],  # not a status this engine build knows
                failure_category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
            )
        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.DESTINATION_WRITE_FAILED

    @pytest.mark.asyncio
    async def test_outer_scope_no_send_for_non_truncate_insert(self):
        """When write_mode is not truncate_insert, no synthetic batch is
        ever sent regardless of how many batches the source produced."""
        processor = self._run_processor(write_mode="insert")
        grpc_client = await self._invoke_run(processor, [])
        grpc_client.send_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_extract_failure_does_not_truncate(self):
        """An upstream (extract/transform) failure must never trigger a
        spurious truncate. The safety guarantee: gather re-raises before
        the post-gather synthetic-batch block runs, so send_batch is
        never called even if _load_stage had set the flag."""
        import asyncio
        from unittest.mock import patch

        processor = self._run_processor()

        grpc_client = MagicMock()
        grpc_client.connect = AsyncMock(return_value=True)
        grpc_client.start_stream = AsyncMock(return_value=True)
        grpc_client.stream_retry_semantics = None
        grpc_client.send_batch = AsyncMock()
        grpc_client.disconnect = AsyncMock()

        def _failing_stages(source_readable):
            # Simulate _load_stage having set the flag before extract
            # raised — the worst-case race scenario.
            processor.zero_batch_truncate_needed = True

            async def _raise():
                raise RuntimeError("source connection refused")

            return [asyncio.create_task(_raise())]

        with (
            patch.object(processor, "_create_grpc_client", return_value=grpc_client),
            patch.object(processor, "_create_stage_tasks", side_effect=_failing_stages),
            patch("src.engine.stream_processor.DeadLetterQueue"),
            patch(
                "src.engine.stream_processor.create_metrics_record",
                return_value=MagicMock(),
            ),
        ):
            with pytest.raises(RuntimeError, match="source connection refused"):
                await processor.run()

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
        """End-to-end: an empty source with truncate_insert runs the
        emptying statement through the transport backend when the engine
        sends the synthetic batch — no stage cycle, nothing landed."""
        handler = GenericSQLConnector()
        handler._connected = True
        handler._adbc_only = True
        contract = MagicMock()
        contract.cast_arrow_batch = lambda b: b
        handler._streams["s1"] = SqlStreamState(
            address=TableAddress(table="t"),
            write_mode="truncate_insert",
            schema_contract=contract,
        )
        backend = MagicMock()
        backend.run_ddl = AsyncMock()
        backend.execute_write = AsyncMock()
        handler._backend = backend

        r = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.record_batch([], schema=pa.schema([])),
            record_ids=[],
            cursor=MagicMock(),
            emitted_at=_EMITTED_AT,
        )
        assert r.success
        # The stream's own address reaches the emptying statement — the
        # dialect's empty_table_sql render, never TRUNCATE.
        backend.run_ddl.assert_awaited_once_with(['DELETE FROM "t"'])
        backend.execute_write.assert_not_called()
