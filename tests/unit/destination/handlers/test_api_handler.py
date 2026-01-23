"""Unit tests for ApiDestinationHandler.

Tests focus on failure handling scenarios to ensure:
1. When all records fail, handler returns FATAL_FAILURE
2. When some records fail (partial), handler returns FATAL_FAILURE
3. When all records succeed, handler returns SUCCESS
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.destination.connectors.api import ApiDestinationHandler
from src.destination.base_handler import BatchWriteResult
from src.grpc.generated.analitiq.v1 import AckStatus


@pytest.fixture
def api_handler():
    """Create an ApiDestinationHandler instance."""
    handler = ApiDestinationHandler()
    return handler


@pytest.fixture
def mock_cursor():
    """Create a mock cursor object."""
    cursor = MagicMock()
    cursor.primary.field = "id"
    cursor.primary.value = "123"
    return cursor


@pytest.fixture
def sample_records():
    """Sample records for testing."""
    return [
        {"id": 1, "name": "Record 1", "amount": 100.0},
        {"id": 2, "name": "Record 2", "amount": 200.0},
        {"id": 3, "name": "Record 3", "amount": 300.0},
    ]


@pytest.fixture
def sample_record_ids():
    """Sample record IDs for testing."""
    return ["rec-001", "rec-002", "rec-003"]


@pytest.mark.unit
class TestApiHandlerWriteBatchFailures:
    """Test suite for ApiDestinationHandler.write_batch() failure scenarios."""

    @pytest.mark.asyncio
    async def test_write_batch_all_records_fail_returns_fatal_failure(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: List[Dict[str, Any]],
        sample_record_ids: List[str],
    ):
        """
        Test that when ALL records fail to write, handler returns FATAL_FAILURE.

        This is the bug that was found: previously returned SUCCESS even when
        written=0, causing pipelines to report success incorrectly.
        """
        # Setup: mock the handler as connected
        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._batch_mode = ApiDestinationHandler.BATCH_MODE_SINGLE

        # Mock _write_single_mode to return 0 (all records failed)
        api_handler._write_single_mode = AsyncMock(return_value=0)

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            records=sample_records,
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        # Assert: should return FATAL_FAILURE, not SUCCESS
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 0
        assert "All 3 records failed" in result.failure_summary

    @pytest.mark.asyncio
    async def test_write_batch_partial_failure_returns_fatal_failure(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: List[Dict[str, Any]],
        sample_record_ids: List[str],
    ):
        """
        Test that when SOME records fail, handler returns FATAL_FAILURE.

        Partial failure should not be SUCCESS because:
        1. Some data was lost
        2. Retrying would duplicate successful records
        """
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._batch_mode = ApiDestinationHandler.BATCH_MODE_SINGLE

        # Mock: 1 out of 3 records succeeded
        api_handler._write_single_mode = AsyncMock(return_value=1)

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            records=sample_records,
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        # Assert: partial failure should be FATAL_FAILURE
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 1
        assert "2/3 records failed" in result.failure_summary

    @pytest.mark.asyncio
    async def test_write_batch_all_records_succeed_returns_success(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: List[Dict[str, Any]],
        sample_record_ids: List[str],
    ):
        """Test that when ALL records succeed, handler returns SUCCESS."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._batch_mode = ApiDestinationHandler.BATCH_MODE_SINGLE

        # Mock: all 3 records succeeded
        api_handler._write_single_mode = AsyncMock(return_value=3)

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            records=sample_records,
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        # Assert: should return SUCCESS
        assert result.success is True
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 3
        assert result.committed_cursor == mock_cursor

    @pytest.mark.asyncio
    async def test_write_batch_empty_records_returns_success(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """Test that empty batch returns SUCCESS (nothing to fail)."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()

        # Execute with empty records
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            records=[],
            record_ids=[],
            cursor=mock_cursor,
        )

        # Assert: empty batch is success
        assert result.success is True
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 0

    @pytest.mark.asyncio
    async def test_write_batch_not_connected_returns_retryable_failure(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: List[Dict[str, Any]],
        sample_record_ids: List[str],
    ):
        """Test that disconnected handler returns RETRYABLE_FAILURE."""
        # Setup: handler not connected
        api_handler._connected = False
        api_handler._session = None

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            records=sample_records,
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        # Assert: connection issue is retryable
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert "not connected" in result.failure_summary.lower()

    @pytest.mark.asyncio
    async def test_write_batch_exception_returns_retryable_failure(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: List[Dict[str, Any]],
        sample_record_ids: List[str],
    ):
        """Test that unexpected exceptions return RETRYABLE_FAILURE."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._batch_mode = ApiDestinationHandler.BATCH_MODE_SINGLE

        # Mock: raise exception during write
        api_handler._write_single_mode = AsyncMock(
            side_effect=Exception("Network error")
        )

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            records=sample_records,
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        # Assert: exception should be retryable
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert "Network error" in result.failure_summary


@pytest.mark.unit
class TestApiHandlerWriteSingleMode:
    """Test suite for ApiDestinationHandler._write_single_mode()."""

    @pytest.mark.asyncio
    async def test_write_single_mode_counts_failures(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """Test that _write_single_mode correctly counts successful writes."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]
        record_ids = ["rec-1", "rec-2", "rec-3"]

        # Mock _send_request: first and third succeed, second fails
        call_count = 0
        async def mock_send_request(data):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("API error 404: Not found")
            return {"status": "ok"}

        api_handler._send_request = mock_send_request

        # Execute
        written = await api_handler._write_single_mode(records, record_ids)

        # Assert: 2 out of 3 succeeded
        assert written == 2

    @pytest.mark.asyncio
    async def test_write_single_mode_all_fail(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """Test that _write_single_mode returns 0 when all fail."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [{"id": 1}, {"id": 2}]
        record_ids = ["rec-1", "rec-2"]

        # Mock: all requests fail
        api_handler._send_request = AsyncMock(
            side_effect=Exception("API error 500")
        )

        # Execute
        written = await api_handler._write_single_mode(records, record_ids)

        # Assert: 0 succeeded
        assert written == 0

    @pytest.mark.asyncio
    async def test_write_single_mode_all_succeed(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """Test that _write_single_mode returns count when all succeed."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        record_ids = ["rec-1", "rec-2", "rec-3"]

        # Mock: all requests succeed
        api_handler._send_request = AsyncMock(return_value={"status": "ok"})

        # Execute
        written = await api_handler._write_single_mode(records, record_ids)

        # Assert: all 3 succeeded
        assert written == 3


@pytest.mark.unit
class TestApiHandlerBatchModes:
    """Test different batch modes handle failures correctly."""

    @pytest.mark.asyncio
    async def test_bulk_mode_failure_affects_all_records(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """Test that bulk mode failure is handled as complete failure."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._batch_mode = ApiDestinationHandler.BATCH_MODE_BULK

        records = [{"id": i} for i in range(5)]
        record_ids = [f"rec-{i}" for i in range(5)]

        # Mock: bulk request fails
        api_handler._write_bulk_mode = AsyncMock(
            side_effect=Exception("Bulk insert failed")
        )

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            records=records,
            record_ids=record_ids,
            cursor=mock_cursor,
        )

        # Assert: exception leads to retryable failure
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE

    @pytest.mark.asyncio
    async def test_batch_mode_partial_chunk_failure(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """Test batch mode with chunked requests where some chunks fail."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._batch_mode = ApiDestinationHandler.BATCH_MODE_BATCH
        api_handler._batch_size = 2

        records = [{"id": i} for i in range(5)]
        record_ids = [f"rec-{i}" for i in range(5)]

        # Mock: 3 out of 5 records written (first chunk + part of second)
        api_handler._write_batch_mode = AsyncMock(return_value=3)

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            records=records,
            record_ids=record_ids,
            cursor=mock_cursor,
        )

        # Assert: partial success is still a failure
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 3