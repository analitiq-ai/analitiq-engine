"""Integration tests for engine failure handling.

Tests focus on ensuring that fatal failures from destinations
properly propagate to mark streams and pipelines as failed.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

from src.engine.engine import StreamingEngine
from src.engine.exceptions import StreamProcessingError
from src.grpc.generated.analitiq.v1 import AckStatus


@dataclass
class MockBatchResult:
    """Mock BatchResult for testing."""
    success: bool
    status: AckStatus
    records_written: int
    committed_cursor: Optional[MagicMock]
    failed_record_ids: List[str]
    failure_summary: str


@pytest.fixture
def engine(temp_dir):
    """Create a StreamingEngine instance for testing."""
    return StreamingEngine(
        pipeline_id="test-pipeline",
        batch_size=10,
        max_concurrent_batches=2,
        buffer_size=100,
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
def mock_source_connector():
    """Create a mock source connector."""
    connector = AsyncMock()
    connector.connect = AsyncMock()
    connector.disconnect = AsyncMock()
    connector.health_check = AsyncMock(return_value=True)
    return connector


@pytest.fixture
def sample_stream_config():
    """Sample stream processing config."""
    return {
        "pipeline_id": "test-pipeline",
        "name": "Test Pipeline",
        "version": "1.0",
        "stream_id": "test-stream-001",
        "stream_name": "test-stream",
        "source": {
            "connector_type": "api",
            "host": "https://api.example.com",
        },
        "destination": {
            "connector_type": "api",
            "host": "https://dest.example.com",
        },
        "cursor_field": "updated_at",
    }


@pytest.mark.integration
class TestEngineFatalFailureHandling:
    """Test suite for engine handling of fatal failures from destinations."""

    @pytest.mark.asyncio
    async def test_load_stage_raises_exception_on_fatal_failure(
        self,
        engine: StreamingEngine,
        mock_grpc_client: AsyncMock,
        sample_stream_config: Dict[str, Any],
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
        test_batch = [
            {"id": 1, "name": "Record 1"},
            {"id": 2, "name": "Record 2"},
        ]
        await input_queue.put(test_batch)
        await input_queue.put(None)  # Signal end of stream

        # Mock gRPC client to return FATAL_FAILURE
        mock_cursor = MagicMock()
        mock_cursor.primary.field = "id"
        mock_cursor.primary.value = "2"

        fatal_result = MockBatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="All 2 records failed to write to API",
        )
        mock_grpc_client.send_batch = AsyncMock(return_value=fatal_result)

        # Create DLQ
        stream_dlq = DeadLetterQueue(f"{temp_dir}/dlq")

        # Execute and expect exception
        with pytest.raises(StreamProcessingError) as exc_info:
            await engine._load_stage(
                input_queue=input_queue,
                output_queue=output_queue,
                grpc_client=mock_grpc_client,
                config=sample_stream_config,
                stream_dlq=stream_dlq,
                run_id="test-run-001",
            )

        # Assert: exception contains failure info
        assert "fatal failure" in str(exc_info.value).lower()
        assert "Batch 1" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_load_stage_success_does_not_raise(
        self,
        engine: StreamingEngine,
        mock_grpc_client: AsyncMock,
        sample_stream_config: Dict[str, Any],
        temp_dir: str,
    ):
        """Test that _load_stage completes normally when all batches succeed."""
        from src.state.dead_letter_queue import DeadLetterQueue

        # Setup queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        # Put batches in input queue
        test_batch = [{"id": 1}, {"id": 2}]
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

        stream_dlq = DeadLetterQueue(f"{temp_dir}/dlq")

        # Execute - should NOT raise
        await engine._load_stage(
            input_queue=input_queue,
            output_queue=output_queue,
            grpc_client=mock_grpc_client,
            config=sample_stream_config,
            stream_dlq=stream_dlq,
            run_id="test-run-001",
        )

        # Assert: batch was forwarded to output queue
        output_batch = await output_queue.get()
        assert output_batch == test_batch

        # End marker
        end_marker = await output_queue.get()
        assert end_marker is None

    @pytest.mark.asyncio
    async def test_load_stage_retryable_failure_retries_then_dlq(
        self,
        engine: StreamingEngine,
        mock_grpc_client: AsyncMock,
        sample_stream_config: Dict[str, Any],
        temp_dir: str,
    ):
        """Test that retryable failures are retried before going to DLQ."""
        from src.state.dead_letter_queue import DeadLetterQueue

        # Setup queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        test_batch = [{"id": 1}]
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

        stream_dlq = DeadLetterQueue(f"{temp_dir}/dlq")

        # Patch env vars for faster retries
        with patch.dict("os.environ", {"MAX_RETRIES": "2", "RETRY_BASE_DELAY_MS": "10"}):
            # Execute - should NOT raise (goes to DLQ after retries)
            await engine._load_stage(
                input_queue=input_queue,
                output_queue=output_queue,
                grpc_client=mock_grpc_client,
                config=sample_stream_config,
                stream_dlq=stream_dlq,
                run_id="test-run-001",
            )

        # Assert: send_batch was called multiple times (initial + retries)
        # Initial call + 2 retries = 3 calls
        assert mock_grpc_client.send_batch.call_count == 3

    @pytest.mark.asyncio
    async def test_metrics_updated_on_fatal_failure(
        self,
        engine: StreamingEngine,
        mock_grpc_client: AsyncMock,
        sample_stream_config: Dict[str, Any],
        temp_dir: str,
    ):
        """Test that metrics are updated when fatal failure occurs."""
        from src.state.dead_letter_queue import DeadLetterQueue

        # Reset metrics
        engine.metrics.records_failed = 0
        engine.metrics.batches_failed = 0

        # Setup queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        test_batch = [{"id": 1}, {"id": 2}, {"id": 3}]
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

        stream_dlq = DeadLetterQueue(f"{temp_dir}/dlq")

        # Execute
        with pytest.raises(StreamProcessingError):
            await engine._load_stage(
                input_queue=input_queue,
                output_queue=output_queue,
                grpc_client=mock_grpc_client,
                config=sample_stream_config,
                stream_dlq=stream_dlq,
                run_id="test-run-001",
            )

        # Assert: metrics were updated
        assert engine.metrics.records_failed == 3
        assert engine.metrics.batches_failed == 1


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
            "engine_config": {"batch_size": 10},
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
            raise StreamProcessingError("Simulated stream failure", stream_id="stream-001")

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
            "engine_config": {"batch_size": 10},
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

        async def mock_process_stream(stream_id, stream_config, pipeline_config):
            nonlocal call_count
            call_count += 1
            if stream_config.get("name") == "failing-stream":
                raise StreamProcessingError("Stream failed", stream_id=stream_id)
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


@pytest.mark.integration
class TestEngineDLQOnFailure:
    """Test that failed batches are sent to DLQ."""

    @pytest.mark.asyncio
    async def test_fatal_failure_sends_batch_to_dlq(
        self,
        engine: StreamingEngine,
        mock_grpc_client: AsyncMock,
        sample_stream_config: Dict[str, Any],
        temp_dir: str,
    ):
        """Test that batches with fatal failures are sent to DLQ before raising."""
        from src.state.dead_letter_queue import DeadLetterQueue
        import os

        # Setup queues
        input_queue = asyncio.Queue()
        output_queue = asyncio.Queue()

        test_batch = [{"id": 1, "data": "test"}]
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
        stream_dlq = DeadLetterQueue(dlq_path)

        # Execute
        with pytest.raises(StreamProcessingError):
            await engine._load_stage(
                input_queue=input_queue,
                output_queue=output_queue,
                grpc_client=mock_grpc_client,
                config=sample_stream_config,
                stream_dlq=stream_dlq,
                run_id="test-run-001",
            )

        # Assert: DLQ file was created
        dlq_files = list(os.listdir(dlq_path)) if os.path.exists(dlq_path) else []
        assert len(dlq_files) > 0, "DLQ should contain failed batch"