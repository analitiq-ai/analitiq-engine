"""Integration tests for engine failure handling.

Tests focus on ensuring that fatal failures from destinations
properly propagate to mark streams and pipelines as failed.
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from cdk.types import FailureCategory
from src.engine.batch_policy import ErrorStrategy
from src.engine.engine import StreamingEngine
from src.engine.exceptions import StreamProcessingError
from src.engine.stream_processor import StreamProcessor
from src.grpc.generated.analitiq.v1 import AckStatus
from src.models.metrics import PipelineMetrics
from src.models.resolved import (
    BatchingConfig,
    ReplicationConfig,
    ResolvedSource,
    RuntimeConfig,
)
from src.models.stream import EndpointRef
from src.runner import PipelineRunner
from src.state.error_classification import (
    ErrorCode,
    FailureStage,
    classify_for_metrics,
    read_failure_tag,
    tag_failure,
)


@dataclass
class MockBatchResult:
    """Mock BatchResult for testing."""

    success: bool
    status: AckStatus
    records_written: int
    committed_cursor: MagicMock | None
    failed_record_ids: list[str]
    failure_summary: str
    failure_category: FailureCategory = FailureCategory.FAILURE_CATEGORY_UNSPECIFIED


def _mock_runtime() -> MagicMock:
    """Runtime double declaring no #401 blocks (StreamProcessor parses them)."""
    runtime = MagicMock()
    runtime.connector_id = "demo"
    runtime.declared_error_map = None
    return runtime


def _make_processor(
    stream_config: dict[str, Any],
    grpc_client: AsyncMock,
    stream_dlq: Any,
    *,
    error_strategy: str = "fail",
    max_retries: int = 3,
    retry_delay: float = 0.01,
    pipeline_metrics: PipelineMetrics | None = None,
    state_manager: Any | None = None,
) -> StreamProcessor:
    """Build a StreamProcessor wired to mocks, as run() would have wired it."""
    processor = StreamProcessor(
        stream_id="test-stream-001",
        stream_config=stream_config,
        pipeline_config={"pipeline_id": "test-pipeline", "name": "Test Pipeline"},
        pipeline_id="test-pipeline",
        state_manager=state_manager if state_manager is not None else MagicMock(),
        pipeline_metrics=pipeline_metrics or PipelineMetrics(),
        worker_readable=MagicMock(),
        dlq_root="./deadletter",
        batch_size=10,
        buffer_size=100,
        max_retries=max_retries,
        retry_delay=retry_delay,
        error_strategy=ErrorStrategy(error_strategy),
    )
    processor.grpc_client = grpc_client
    processor.stream_dlq = stream_dlq
    processor.run_id = "test-run-001"
    return processor


async def _skip_one_batch(
    mock_grpc_client: AsyncMock,
    stream_config: dict[str, Any],
    dlq_path: str,
    pipeline_metrics: PipelineMetrics,
) -> tuple[StreamProcessor, Any]:
    """Drive one single-record batch to retry exhaustion under 'skip'.

    Returns the processor and its dead letter queue (which must stay empty --
    a skipped batch is dropped, not dead-lettered).
    """
    from src.state.dead_letter_queue import DeadLetterQueue

    input_queue: asyncio.Queue = asyncio.Queue()
    output_queue: asyncio.Queue = asyncio.Queue()
    await input_queue.put(pa.RecordBatch.from_pylist([{"id": 1}]))
    await input_queue.put(None)

    mock_grpc_client.send_batch = AsyncMock(
        return_value=MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="Connection timeout",
        )
    )
    stream_dlq = DeadLetterQueue(dlq_path)
    processor = _make_processor(
        dict(stream_config),
        mock_grpc_client,
        stream_dlq,
        error_strategy="skip",
        retry_delay=0.01,
        pipeline_metrics=pipeline_metrics,
    )

    # Must NOT raise: the strategy drops the batch and continues.
    await processor._load_stage(input_queue, output_queue)
    return processor, stream_dlq


def _emitted_metrics_payloads(caplog) -> dict[str, dict[str, Any]]:
    """Parse captured ``ANALITIQ_METRICS::{json}`` lines, keyed by record type."""
    payloads = {}
    for record in caplog.records:
        marker, sep, payload = record.getMessage().partition("::")
        if sep and marker == "ANALITIQ_METRICS":
            parsed = json.loads(payload)
            payloads[parsed["type"]] = parsed
    return payloads


@pytest.fixture
def engine(temp_dir, tmp_project_root):
    """Create a StreamingEngine instance for testing."""
    return StreamingEngine(
        pipeline_id="test-pipeline",
        runtime=RuntimeConfig(
            batching=BatchingConfig(batch_size=10),
            buffer_size=100,
        ),
        dlq_path=temp_dir,
    )


@pytest.fixture
def mock_grpc_client():
    """Create a mock gRPC client."""
    client = AsyncMock()
    client.connect = AsyncMock(return_value=True)
    client.disconnect = AsyncMock()
    client.start_stream = AsyncMock(return_value=True)
    client.end_stream = AsyncMock()
    return client


@pytest.fixture
def sample_stream_config():
    """Sample stream config as the runner assembles it for one stream."""
    return {
        "name": "test-stream",
        "source": {
            "connector_type": "api",
            "host": "https://api.example.com",
            # The runner always attaches the typed resolved source; the load
            # stage reads replication/primary-keys off it.
            "_resolved_source": ResolvedSource(
                endpoint_ref=EndpointRef(
                    scope="connector", connection_id="c", endpoint_id="e"
                ),
                connection_ref="conn",
                runtime=_mock_runtime(),
                endpoint_document={},
                stream_source={},
                replication=ReplicationConfig(
                    method="incremental", cursor_field="updated_at"
                ),
                primary_keys=["id"],
            ),
        },
        "destination": {
            "connector_type": "api",
            "host": "https://dest.example.com",
        },
    }


@pytest.mark.integration
class TestEngineFatalFailureHandling:
    """Test suite for engine handling of fatal failures from destinations."""

    @pytest.mark.asyncio
    async def test_load_stage_raises_exception_on_fatal_failure(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """
        Test that _load_stage raises StreamProcessingError when destination
        returns ACK_STATUS_FATAL_FAILURE.

        This is the bug that was found: previously the engine just logged
        the error and continued, reporting the stream as successful.
        """
        from src.state.dead_letter_queue import DeadLetterQueue

        # Setup input/output queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        # Put a batch in the input queue
        test_batch = pa.RecordBatch.from_pylist(
            [
                {"id": 1, "name": "Record 1"},
                {"id": 2, "name": "Record 2"},
            ]
        )
        await input_queue.put(test_batch)
        await input_queue.put(None)  # Signal end of stream

        fatal_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="All 2 records failed to write to API",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=fatal_result)

        processor = _make_processor(
            sample_stream_config,
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
        )

        with pytest.raises(StreamProcessingError) as exc_info:
            await processor._load_stage(input_queue, output_queue)

        # Assert: exception contains failure info
        assert "fatal failure" in str(exc_info.value).lower()
        assert "Batch 1" in str(exc_info.value)

        # The load stage tags its failure destination-side, so classification is
        # deterministic and a driver/HTTP code in the cause can never be misread
        # as source auth (issue #264).
        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.DESTINATION_WRITE_FAILED
        assert tag.stage is FailureStage.DESTINATION_LOAD

    @pytest.mark.asyncio
    async def test_load_stage_does_not_clobber_a_deeper_tag(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """The load-stage tag is no-overwrite: a precise inner tag carried by the
        raised cause survives instead of being relabeled DESTINATION_WRITE_FAILED.
        This is the deeper-tag gate that keeps a worker's CONFIG_INVALID signal
        from being clobbered by the coarse outer stage default (issue #264)."""
        from src.state.dead_letter_queue import DeadLetterQueue

        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        await input_queue.put(pa.RecordBatch.from_pylist([{"id": 1}]))
        await input_queue.put(None)

        inner = tag_failure(
            RuntimeError("deterministic config error surfaced mid-load"),
            code=ErrorCode.CONFIG_INVALID,
            stage=FailureStage.CONFIG,
        )
        mock_grpc_client.send_batch = AsyncMock(side_effect=inner)

        processor = _make_processor(
            sample_stream_config,
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
        )

        with pytest.raises(RuntimeError) as exc_info:
            await processor._load_stage(input_queue, output_queue)

        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.CONFIG_INVALID
        assert tag.stage is FailureStage.CONFIG

    @pytest.mark.asyncio
    async def test_load_stage_config_cause_fatal_ack_tags_config_not_write(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """A fatal destination ACK that declares FAILURE_CATEGORY_CONFIG_DEFECT
        (the deterministic write-config excepts in cdk/sql/generic.py) must tag
        CONFIG_INVALID, not the generic DESTINATION_WRITE_FAILED -- so a
        user-fixable destination config error is reported as such (issues
        #264, #351). The summary prose plays no part in the classification."""
        from src.state.dead_letter_queue import DeadLetterQueue

        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        await input_queue.put(pa.RecordBatch.from_pylist([{"id": 1}]))
        await input_queue.put(None)

        fatal_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="type-map: no reverse rule for 'geography'",
            failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=fatal_result)

        processor = _make_processor(
            sample_stream_config,
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
        )

        with pytest.raises(StreamProcessingError) as exc_info:
            await processor._load_stage(input_queue, output_queue)

        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.CONFIG_INVALID
        assert tag.stage is FailureStage.DESTINATION_LOAD

    def test_missing_resolved_source_raises_config_tagged_error(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
    ):
        """The processor's source guard is the real guard for a missing
        _resolved_source (it runs before any stage starts), so it must raise
        a CONFIG_INVALID-tagged error rather than an untagged ValueError that
        falls back to INTERNAL (issue #264)."""
        processor = _make_processor(sample_stream_config, mock_grpc_client, MagicMock())
        with pytest.raises(ValueError) as exc_info:
            processor._resolve_source_readable({})  # no _resolved_source
        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.CONFIG_INVALID
        assert tag.stage is FailureStage.CONFIG

    @pytest.mark.asyncio
    async def test_load_stage_success_does_not_raise(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """Test that _load_stage completes normally when all batches succeed."""
        from src.state.dead_letter_queue import DeadLetterQueue

        # Setup queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        # Put batches in input queue
        test_batch = pa.RecordBatch.from_pylist([{"id": 1}, {"id": 2}])
        await input_queue.put(test_batch)
        await input_queue.put(None)

        # Mock gRPC client to return SUCCESS
        # Use None for cursor since the engine tries to decode it
        success_result = MockBatchResult(
            success=True,
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=2,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=success_result)

        processor = _make_processor(
            sample_stream_config,
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
        )

        # Execute - should NOT raise
        with patch.dict("os.environ", {"METRICS_ENABLED": "false"}):
            await processor._load_stage(input_queue, output_queue)

        # Assert: batch was forwarded to output queue
        output_batch = await output_queue.get()
        assert output_batch == test_batch

        # End marker
        end_marker = await output_queue.get()
        assert end_marker is None

    @pytest.mark.asyncio
    async def test_load_stage_full_refresh_skips_cursor(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """A full-refresh stream (replication=None) completes without computing
        a cursor; send_batch receives cursor=None (the typed else-None branch)."""
        from src.state.dead_letter_queue import DeadLetterQueue

        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        await input_queue.put(pa.RecordBatch.from_pylist([{"id": 1}]))
        await input_queue.put(None)

        success_result = MockBatchResult(
            success=True,
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=1,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=success_result)

        # Full-refresh: typed resolved source with no replication policy. Copy
        # the source sub-dict so the shared fixture object is not mutated.
        config = dict(sample_stream_config)
        config["source"] = dict(sample_stream_config["source"])
        config["source"]["_resolved_source"] = ResolvedSource(
            endpoint_ref=EndpointRef(
                scope="connector", connection_id="c", endpoint_id="e"
            ),
            connection_ref="conn",
            runtime=_mock_runtime(),
            endpoint_document={},
            stream_source={},
            replication=None,
            primary_keys=["id"],
        )

        processor = _make_processor(
            config,
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
        )

        with patch.dict("os.environ", {"METRICS_ENABLED": "false"}):
            await processor._load_stage(input_queue, output_queue)

        # No cursor computed for a full-refresh stream.
        assert mock_grpc_client.send_batch.await_args.kwargs["cursor"] is None
        # Batch still forwarded downstream, followed by the end marker.
        assert await output_queue.get() is not None
        assert await output_queue.get() is None

    @pytest.mark.asyncio
    async def test_load_stage_checkpoint_threads_stream_version(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """A committed batch threads the stream's version into the emitted
        checkpoint. Guards the processor -> save_stream_checkpoint call,
        which only runs when the ACK carries a committed_cursor."""
        from src.grpc.cursor import encode_cursor
        from src.state.dead_letter_queue import DeadLetterQueue

        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        await input_queue.put(pa.RecordBatch.from_pylist([{"id": 1}]))
        await input_queue.put(None)

        # A real committed cursor so cursor_to_state_dict decodes a hwm and the
        # checkpoint call site (gated on committed_cursor) actually runs.
        success_result = MockBatchResult(
            success=True,
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=1,
            committed_cursor=encode_cursor("updated_at", "2025-08-18T12:00:00Z"),
            failed_record_ids=[],
            failure_summary="",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=success_result)

        state_manager = MagicMock()
        processor = _make_processor(
            dict(sample_stream_config, stream_version=7),
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
            state_manager=state_manager,
        )

        with patch.dict("os.environ", {"METRICS_ENABLED": "false"}):
            await processor._load_stage(input_queue, output_queue)

        state_manager.save_stream_checkpoint.assert_called_once()
        assert (
            state_manager.save_stream_checkpoint.call_args.kwargs["stream_version"] == 7
        )

    @pytest.mark.asyncio
    async def test_load_stage_already_committed_checkpoints_nothing(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """An ALREADY_COMMITTED ack (idempotent same-run replay) forwards the
        batch downstream but advances neither the checkpoint nor the record
        counters: the checkpoint is an artifact of a commit THIS run
        confirmed, and this run confirmed none (issue #428, decision 1.2).
        Any cursor riding such an ack is ignored."""
        from src.grpc.cursor import encode_cursor
        from src.state.dead_letter_queue import DeadLetterQueue

        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        test_batch = pa.RecordBatch.from_pylist([{"id": 1}])
        await input_queue.put(test_batch)
        await input_queue.put(None)

        replay_result = MockBatchResult(
            success=True,
            status=AckStatus.ACK_STATUS_ALREADY_COMMITTED,
            records_written=0,
            committed_cursor=encode_cursor("updated_at", "2025-08-18T12:00:00Z"),
            failed_record_ids=[],
            failure_summary="",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=replay_result)

        state_manager = MagicMock()
        pipeline_metrics = PipelineMetrics()
        processor = _make_processor(
            sample_stream_config,
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
            state_manager=state_manager,
            pipeline_metrics=pipeline_metrics,
        )

        await processor._load_stage(input_queue, output_queue)

        # No checkpoint: only a SUCCESS ack advances the watermark.
        state_manager.save_stream_checkpoint.assert_not_called()
        # Batch forwarded downstream, then the end marker.
        assert await output_queue.get() is test_batch
        assert await output_queue.get() is None
        # An idempotent replay is not new progress.
        assert pipeline_metrics.records_processed == 0
        assert processor.metrics.records_processed == 0

    @pytest.mark.asyncio
    async def test_load_stage_retryable_failure_retries_then_dlq(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """Test that retryable failures are retried before going to DLQ."""
        from src.state.dead_letter_queue import DeadLetterQueue

        # Setup queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        test_batch = pa.RecordBatch.from_pylist([{"id": 1}])
        await input_queue.put(test_batch)
        await input_queue.put(None)

        # Mock: always return RETRYABLE_FAILURE
        retryable_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="Connection timeout",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=retryable_result)

        processor = _make_processor(
            sample_stream_config,
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
            error_strategy="dlq",
            retry_delay=0.01,  # keep exponential backoff fast in the test
        )

        # Execute - should NOT raise (goes to DLQ after retries)
        await processor._load_stage(input_queue, output_queue)

        # Assert: send_batch was called multiple times (initial + retries)
        # Initial call + 3 retries (default max_retries=3) = 4 calls
        assert mock_grpc_client.send_batch.call_count == 4

        # #353 core invariant: emitted_at is stamped ONCE per batch (before the
        # retry loop) and reused unchanged on every retry, so all four calls
        # carry the identical instant. Re-stamping per attempt would drift a
        # replayed batch across an hour/day partition boundary and reintroduce
        # the duplicate-file bug this fix removes.
        emitted = [
            call.kwargs["emitted_at"]
            for call in mock_grpc_client.send_batch.call_args_list
        ]
        assert len(emitted) == 4
        assert all(e.tzinfo is not None for e in emitted)
        assert len({e.timestamp() for e in emitted}) == 1

    @pytest.mark.asyncio
    async def test_load_stage_retryable_exhaustion_raises_with_fail_strategy(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """With error_strategy='fail' (the default), exhausting retries must
        raise StreamProcessingError instead of silently dropping the batch
        and continuing."""
        from src.state.dead_letter_queue import DeadLetterQueue

        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        test_batch = pa.RecordBatch.from_pylist([{"id": 1}])
        await input_queue.put(test_batch)
        await input_queue.put(None)

        retryable_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="Connection timeout",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=retryable_result)

        stream_dlq = DeadLetterQueue(f"{temp_dir}/dlq")
        processor = _make_processor(
            dict(sample_stream_config),
            mock_grpc_client,
            stream_dlq,
            error_strategy="fail",
            retry_delay=0.01,
        )

        with pytest.raises(StreamProcessingError, match="failed after"):
            await processor._load_stage(input_queue, output_queue)

        # No record was sent to the DLQ under 'fail'
        assert await stream_dlq.get_failed_records() == []

    @pytest.mark.asyncio
    async def test_not_ready_exhaustion_tags_internal_not_write_failed(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """A destination readiness guard (reject_batch) declares NOT_READY:
        nothing was ever attempted, so exhausting retries must classify
        INTERNAL -- not DESTINATION_WRITE_FAILED, which claims the destination
        rejected data it never saw (issue #351). Before the typed category,
        this case was indistinguishable from a constraint violation."""
        from src.state.dead_letter_queue import DeadLetterQueue

        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()
        await input_queue.put(pa.RecordBatch.from_pylist([{"id": 1}]))
        await input_queue.put(None)

        not_ready_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="Handler not connected",
            failure_category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=not_ready_result)

        processor = _make_processor(
            dict(sample_stream_config),
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
            error_strategy="fail",
            retry_delay=0.01,
        )

        with pytest.raises(StreamProcessingError) as exc_info:
            await processor._load_stage(input_queue, output_queue)

        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.INTERNAL
        assert tag.stage is FailureStage.DESTINATION_LOAD

    @pytest.mark.asyncio
    async def test_dlq_exhaustion_records_batch_codes_for_partial_run(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """The dlq strategy breaks without raising, so every exhausted batch
        must be classified where it broke -- exactly as the fail strategy's
        raise path would classify it (declared category first, text fallback
        for an undeclared ack) -- and stashed for the partial-run
        classification. Otherwise the reported code depends on the error
        strategy, and a declared batch would mask an undeclared one (#351)."""
        from src.state.dead_letter_queue import DeadLetterQueue

        stream_dlq = DeadLetterQueue(f"{temp_dir}/dlq")

        async def _exhaust(result):
            input_queue = asyncio.Queue()
            output_queue = asyncio.Queue()
            await input_queue.put(pa.RecordBatch.from_pylist([{"id": 1}]))
            await input_queue.put(None)
            mock_grpc_client.send_batch = AsyncMock(return_value=result)
            processor = _make_processor(
                dict(sample_stream_config),
                mock_grpc_client,
                stream_dlq,
                error_strategy="dlq",
                retry_delay=0.01,
            )
            await processor._load_stage(input_queue, output_queue)
            return processor

        def _retryable(summary, category=None):
            return MockBatchResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                committed_cursor=None,
                failed_record_ids=[],
                failure_summary=summary,
                failure_category=(
                    category
                    if category is not None
                    else FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
                ),
            )

        # Declared NOT_READY -> INTERNAL, whatever the summary wording.
        processor = await _exhaust(
            _retryable(
                "Handler not connected",
                FailureCategory.FAILURE_CATEGORY_NOT_READY,
            )
        )
        assert processor.exhausted_failure_codes == [ErrorCode.INTERNAL]

        # Undeclared acks take the same text fallback the raise path gets: a
        # config-class summary keeps CONFIG_INVALID, opaque driver text takes
        # the load-stage default.
        processor = await _exhaust(
            _retryable("SchemaConfigurationError: unsupported write mode")
        )
        assert processor.exhausted_failure_codes == [ErrorCode.CONFIG_INVALID]

        processor = await _exhaust(_retryable("connection reset by peer"))
        assert processor.exhausted_failure_codes == [ErrorCode.DESTINATION_WRITE_FAILED]

    def test_get_partial_error_code_reports_dominant_partial_cause(
        self, engine: StreamingEngine
    ):
        """The runner's no-exception partial classification reads this
        accessor; it must resolve several partial streams by the same
        dominance rule the exception path uses, and answer None when no
        stream completed partial so the runner keeps its default (#351)."""
        assert engine.get_partial_error_code() is None
        engine._partial_error_codes.append(ErrorCode.INTERNAL)
        assert engine.get_partial_error_code() is ErrorCode.INTERNAL
        engine._partial_error_codes.append(ErrorCode.DESTINATION_WRITE_FAILED)
        assert engine.get_partial_error_code() is ErrorCode.DESTINATION_WRITE_FAILED

    @pytest.mark.asyncio
    async def test_load_stage_retryable_exhaustion_skips_with_skip_strategy(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """With error_strategy='skip', exhausting retries drops the batch and
        continues (no raise, no DLQ), but logs and counts it as failed."""
        pipeline_metrics = PipelineMetrics()
        processor, stream_dlq = await _skip_one_batch(
            mock_grpc_client,
            sample_stream_config,
            f"{temp_dir}/dlq",
            pipeline_metrics,
        )

        # Skipped: not dead-lettered, but counted as failed AND tracked as
        # skipped (distinct from DLQ'd) at both stream and pipeline level so
        # partial-run reporting stays honest.
        assert await stream_dlq.get_failed_records() == []
        assert processor.metrics.batches_failed == 1
        assert processor.metrics.records_skipped == 1
        assert processor.metrics.records_failed == 1
        assert pipeline_metrics.records_skipped == 1

    @pytest.mark.asyncio
    async def test_skipped_count_reaches_emitted_stream_metrics_record(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
        caplog,
        monkeypatch,
    ):
        """The dropped-record count must reach the emitted stream metrics
        record, not only the log prose (issue #423). The pipeline-level half of
        the same wiring is covered by TestRunnerPartialRunReporting."""
        monkeypatch.setenv("METRICS_ENABLED", "true")
        pipeline_metrics = PipelineMetrics()

        with caplog.at_level(logging.WARNING, logger="src.engine.stream_processor"):
            processor, _ = await _skip_one_batch(
                mock_grpc_client,
                sample_stream_config,
                f"{temp_dir}/dlq",
                pipeline_metrics,
            )

        dropped_lines = [
            record.getMessage()
            for record in caplog.records
            if "records dropped" in record.getMessage()
        ]
        assert len(dropped_lines) == 1
        assert "1 records dropped" in dropped_lines[0]

        start_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_time = start_time + timedelta(seconds=5)

        caplog.clear()
        with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
            processor._emit_stream_metrics(
                status="partial",
                error_code=ErrorCode.DESTINATION_WRITE_FAILED,
                error_message="destination write failed",
                error_detail="DESTINATION_WRITE_FAILED",
                start_time=start_time,
                end_time=end_time,
            )

        emitted = _emitted_metrics_payloads(caplog)
        # The count in the record is the one the log line reported.
        assert emitted["stream"]["records_skipped"] == 1

    @pytest.mark.asyncio
    async def test_unhandled_strategy_is_refused_at_construction(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """A strategy outside {fail, dlq, skip} must fail loud, never silently
        complete a failed batch. The typed enum moves that from a defensive
        branch at the bottom of the reaction ladder to the boundary: the
        processor cannot be built with one (issue #428)."""
        from src.state.dead_letter_queue import DeadLetterQueue

        with pytest.raises(ValueError, match="bogus"):
            _make_processor(
                dict(sample_stream_config),
                mock_grpc_client,
                DeadLetterQueue(f"{temp_dir}/dlq"),
                error_strategy="bogus",
            )

    @pytest.mark.asyncio
    async def test_metrics_updated_on_fatal_failure(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """Test that pipeline-level metrics are updated on a fatal failure."""
        from src.state.dead_letter_queue import DeadLetterQueue

        # Setup queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        test_batch = pa.RecordBatch.from_pylist([{"id": 1}, {"id": 2}, {"id": 3}])
        await input_queue.put(test_batch)
        await input_queue.put(None)

        # Mock: return FATAL_FAILURE
        fatal_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="API rejected all records",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=fatal_result)

        pipeline_metrics = PipelineMetrics()
        processor = _make_processor(
            sample_stream_config,
            mock_grpc_client,
            DeadLetterQueue(f"{temp_dir}/dlq"),
            pipeline_metrics=pipeline_metrics,
        )

        # Execute
        with pytest.raises(StreamProcessingError):
            await processor._load_stage(input_queue, output_queue)

        # Assert: metrics were updated at both stream and pipeline level
        assert pipeline_metrics.records_failed == 3
        assert pipeline_metrics.batches_failed == 1
        assert processor.metrics.records_failed == 3
        assert processor.metrics.batches_failed == 1


@pytest.mark.integration
class TestEngineStreamFailurePropagation:
    """Test that stream failures propagate correctly to pipeline level."""

    @pytest.mark.asyncio
    async def test_stream_exception_collected_in_pipeline(
        self,
        engine: StreamingEngine,
        temp_dir: str,
    ):
        """
        Test that when a stream fails, the exception is collected
        and the stream is marked as failed in metrics.
        """
        # Create a pipeline config with one stream
        pipeline_config = {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "version": "1.0",
            "source": {"connector_type": "api"},
            "destination": {"connector_type": "api"},
            "runtime": {
                "buffer_size": 100,
                "batching": {"batch_size": 10},
                "logging": {"log_level": "DEBUG", "metrics_enabled": False},
                "error_handling": {
                    "strategy": "dlq",
                    "max_retries": 3,
                    "retry_delay": 1,
                },
            },
            "streams": {
                "stream-001": {
                    "name": "failing-stream",
                    "source": {"endpoint_id": "src-endpoint"},
                    "destination": {"endpoint_id": "dst-endpoint"},
                }
            },
        }

        # Mock _process_stream to raise an exception
        async def mock_process_stream(*args, **kwargs):
            raise StreamProcessingError(
                "Simulated stream failure", stream_id="stream-001"
            )

        engine._process_stream = mock_process_stream

        # Mock state manager
        engine.state_manager.start_run = MagicMock(return_value="test-run-001")

        # Reset metrics
        engine.metrics.streams_failed = 0
        engine.metrics.streams_processed = 0

        # Execute - should raise ExceptionGroup since all streams failed
        with pytest.raises(ExceptionGroup):
            await engine.stream_data(pipeline_config)

        # Assert: stream was marked as failed
        assert engine.metrics.streams_failed == 1
        assert engine.metrics.streams_processed == 0

    @pytest.mark.asyncio
    async def test_partial_stream_failure_logged_correctly(
        self,
        engine: StreamingEngine,
        temp_dir: str,
    ):
        """
        Test that when some streams fail and others succeed,
        metrics reflect this accurately.
        """
        # Create a pipeline config with two streams
        pipeline_config = {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "version": "1.0",
            "source": {"connector_type": "api"},
            "destination": {"connector_type": "api"},
            "runtime": {
                "buffer_size": 100,
                "batching": {"batch_size": 10},
                "logging": {"log_level": "DEBUG", "metrics_enabled": False},
                "error_handling": {
                    "strategy": "dlq",
                    "max_retries": 3,
                    "retry_delay": 1,
                },
            },
            "streams": {
                "stream-001": {
                    "name": "successful-stream",
                    "source": {"endpoint_id": "src-1"},
                    "destination": {"endpoint_id": "dst-1"},
                },
                "stream-002": {
                    "name": "failing-stream",
                    "source": {"endpoint_id": "src-2"},
                    "destination": {"endpoint_id": "dst-2"},
                },
            },
        }

        # Track which stream is being processed
        call_count = 0

        async def mock_process_stream(
            stream_id, stream_config, pipeline_config, pacing_gate=None
        ):
            nonlocal call_count
            call_count += 1
            if stream_config.get("name") == "failing-stream":
                raise StreamProcessingError(
                    "password authentication failed", stream_id=stream_id
                )
            # Success for other streams
            return None

        engine._process_stream = mock_process_stream
        engine.state_manager.start_run = MagicMock(return_value="test-run-001")

        # Reset metrics
        engine.metrics.streams_failed = 0
        engine.metrics.streams_processed = 0

        # Execute - should complete but log warning about partial failure
        await engine.stream_data(pipeline_config)

        # Assert: one succeeded, one failed
        assert engine.metrics.streams_failed == 1
        assert engine.metrics.streams_processed == 1

        # The failed stream's exception is surfaced so the runner can classify
        # the partial run instead of reporting success (issue #258).
        dominant = engine.get_dominant_stream_error()
        assert dominant is not None
        code, _, _ = classify_for_metrics(dominant)
        assert code is ErrorCode.SOURCE_AUTH_FAILED

    @pytest.mark.asyncio
    async def test_partial_stream_surfaces_classified_code_through_engine(
        self,
        engine: StreamingEngine,
        sample_stream_config: dict[str, Any],
    ):
        """A dlq stream that exhausts retries completes 'partial' without
        raising; the batch's classified cause must travel the whole
        processor -> engine chain so the runner's no-exception partial
        classification reports it instead of its load-stage default (#351).

        Drives the real engine._process_stream wiring (processor
        construction, run(), partial-code collection) with a real
        StreamProcessor: only the source readable and the gRPC client are
        faked. Asserting INTERNAL (from the declared NOT_READY) rather than
        the default DESTINATION_WRITE_FAILED proves the classified code
        actually traveled the chain."""
        engine.error_strategy = ErrorStrategy.DLQ
        engine.max_retries = 0
        engine.retry_delay = 0

        class OneBatchReadable:
            """Yields one batch, then ends the read."""

            async def read_batches(self, *args, **kwargs):
                yield pa.RecordBatch.from_pylist(
                    [{"id": 1, "name": "a", "updated_at": "2025-01-01T00:00:00Z"}]
                )

        engine._worker_readable = OneBatchReadable()

        not_ready_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="Handler not connected",
            failure_category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
        )
        grpc_client = AsyncMock()
        grpc_client.connect = AsyncMock(return_value=True)
        grpc_client.start_stream = AsyncMock(return_value=True)
        grpc_client.stream_retry_semantics = None
        grpc_client.send_batch = AsyncMock(return_value=not_ready_result)
        grpc_client.disconnect = AsyncMock()

        with patch.object(
            StreamProcessor, "_create_grpc_client", return_value=grpc_client
        ):
            # Must complete without raising: dlq drops the exhausted batch.
            await engine._process_stream(
                "partial-stream",
                dict(sample_stream_config),
                {"pipeline_id": "test-pipeline", "name": "Test Pipeline"},
            )

        assert engine.get_partial_error_code() is ErrorCode.INTERNAL


@pytest.mark.integration
class TestEngineDLQOnFailure:
    """Test that failed batches are sent to DLQ."""

    @pytest.mark.asyncio
    async def test_fatal_failure_sends_batch_to_dlq(
        self,
        mock_grpc_client: AsyncMock,
        sample_stream_config: dict[str, Any],
        temp_dir: str,
    ):
        """Test that batches with fatal failures are sent to DLQ before raising."""
        import os

        from src.state.dead_letter_queue import DeadLetterQueue

        # Setup queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        test_batch = pa.RecordBatch.from_pylist([{"id": 1, "data": "test"}])
        await input_queue.put(test_batch)
        await input_queue.put(None)

        # Mock: return FATAL_FAILURE
        fatal_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="API error 404: Not found",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=fatal_result)

        dlq_path = f"{temp_dir}/dlq"
        processor = _make_processor(
            sample_stream_config,
            mock_grpc_client,
            DeadLetterQueue(dlq_path),
            error_strategy="dlq",  # fatal failures DLQ before raising
        )

        # Execute
        with pytest.raises(StreamProcessingError):
            await processor._load_stage(input_queue, output_queue)

        # Assert: DLQ file was created
        dlq_files = list(os.listdir(dlq_path)) if os.path.exists(dlq_path) else []
        assert len(dlq_files) > 0, "DLQ should contain failed batch"


@pytest.mark.integration
class TestRunnerPartialRunReporting:
    """The runner is the sole builder of the pipeline-level metrics record."""

    @pytest.mark.asyncio
    async def test_pipeline_metrics_carry_the_skipped_count(
        self,
        caplog,
        monkeypatch,
    ):
        """A run whose engine reports dropped records emits a pipeline metrics
        record carrying that count, and warns with the same number (issue
        #423). Only the engine and config load are stubbed, so what the
        assertions exercise is the runner's own wiring: read the count off
        PipelineMetrics, warn with it, hand it to save_pipeline_metrics."""
        monkeypatch.setenv("PIPELINE_ID", "test-pipeline-423")
        engine = MagicMock()
        engine.stream_data = AsyncMock()
        # Distinct counts: a run whose streams split across the 'dlq' and
        # 'skip' strategies dead-letters 3 records and drops 2. Reporting
        # records_failed where the drop count belongs must fail this test.
        engine.get_metrics.return_value = PipelineMetrics(
            records_processed=7,
            records_failed=5,
            records_skipped=2,
            batches_processed=3,
        )
        engine.get_dominant_stream_error.return_value = None
        engine.get_partial_error_code.return_value = None
        config_prep = MagicMock()
        config_prep.create_config.return_value = (
            SimpleNamespace(
                pipeline_id="test-pipeline-423",
                name="Test Pipeline",
                runtime=RuntimeConfig(),
            ),
            [],
            {},
            {},
            {},
        )

        with caplog.at_level(logging.INFO), patch(
            "src.runner.PipelineConfigPrep", return_value=config_prep
        ), patch("src.runner._build_config_dict", return_value={}), patch(
            "src.runner.StreamingEngine", return_value=engine
        ):
            runner = PipelineRunner()
            assert await runner.run() is True

        assert runner.status == "partial"
        warnings = [
            record.getMessage()
            for record in caplog.records
            if record.levelno == logging.WARNING
        ]
        assert "Skipped 2 records (dropped, not dead-lettered)" in warnings

        emitted = _emitted_metrics_payloads(caplog)
        # The emitted record reports the drops, not the failed-record count.
        assert emitted["pipeline"]["records_skipped"] == 2
        assert emitted["pipeline"]["records_failed"] == 5

    @pytest.mark.asyncio
    async def test_failed_run_still_reports_what_it_processed(
        self,
        caplog,
        monkeypatch,
    ):
        """stream_data() raises only when every stream failed, but batches can
        already have been processed, dead-lettered or dropped before that. The
        emitted record carries those counters rather than the initialised zeros
        (issue #423, raised on PR #438)."""
        monkeypatch.setenv("PIPELINE_ID", "test-pipeline-423")
        engine = MagicMock()
        engine.stream_data = AsyncMock(side_effect=RuntimeError("every stream failed"))
        # The run dropped 2 records under the 'skip' strategy and dead-lettered
        # 4 before the last stream took it down. All four counters are distinct
        # and non-zero, so reporting any one of them as 0 fails this test.
        engine.get_metrics.return_value = PipelineMetrics(
            records_processed=9,
            records_failed=4,
            records_skipped=2,
            batches_processed=6,
        )
        config_prep = MagicMock()
        config_prep.create_config.return_value = (
            SimpleNamespace(
                pipeline_id="test-pipeline-423",
                name="Test Pipeline",
                runtime=RuntimeConfig(),
            ),
            [],
            {},
            {},
            {},
        )

        with caplog.at_level(logging.INFO), patch(
            "src.runner.PipelineConfigPrep", return_value=config_prep
        ), patch("src.runner._build_config_dict", return_value={}), patch(
            "src.runner.StreamingEngine", return_value=engine
        ):
            runner = PipelineRunner()
            assert await runner.run() is False

        assert runner.status == "failed"
        emitted = _emitted_metrics_payloads(caplog)
        assert emitted["pipeline"]["records_skipped"] == 2
        assert emitted["pipeline"]["records_failed"] == 4
        assert emitted["pipeline"]["records_processed"] == 9
        assert emitted["pipeline"]["batches_processed"] == 6

    @pytest.mark.asyncio
    async def test_unreadable_counters_do_not_mask_the_failure(
        self,
        caplog,
        monkeypatch,
    ):
        """A counter read that itself fails is logged and the zeros stand: the
        run still reports the exception that terminated it, not the one raised
        while reading its counters."""
        monkeypatch.setenv("PIPELINE_ID", "test-pipeline-423")
        engine = MagicMock()
        engine.stream_data = AsyncMock(side_effect=RuntimeError("every stream failed"))
        engine.get_metrics.side_effect = RuntimeError("counters unavailable")
        config_prep = MagicMock()
        config_prep.create_config.return_value = (
            SimpleNamespace(
                pipeline_id="test-pipeline-423",
                name="Test Pipeline",
                runtime=RuntimeConfig(),
            ),
            [],
            {},
            {},
            {},
        )

        with caplog.at_level(logging.INFO), patch(
            "src.runner.PipelineConfigPrep", return_value=config_prep
        ), patch("src.runner._build_config_dict", return_value={}), patch(
            "src.runner.StreamingEngine", return_value=engine
        ):
            runner = PipelineRunner()
            assert await runner.run() is False

        assert runner.status == "failed"
        emitted = _emitted_metrics_payloads(caplog)
        assert emitted["pipeline"]["records_skipped"] == 0
        warnings = [
            record.getMessage()
            for record in caplog.records
            if record.levelno == logging.WARNING
        ]
        assert any("Could not read engine counters" in w for w in warnings)
