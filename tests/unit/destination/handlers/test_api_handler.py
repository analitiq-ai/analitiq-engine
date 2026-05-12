"""Unit tests for ApiDestinationHandler.

Covers write_batch failure handling (fatal vs retryable classification)
and the contract-driven dispatch in configure_schema (the API endpoint
document's ``operations.write.<mode>`` block selects the path / method
/ batching for the stream's write_mode).
"""

import pytest
import pyarrow as pa
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.destination.connectors.api import (
    ApiDestinationHandler,
    _API_WRITE_MODE_KEYS,
    _StreamState,
)
from src.destination.base_handler import BatchWriteResult
from src.grpc.generated.analitiq.v1 import AckStatus, SchemaMessage, WriteMode


def _to_record_batch(records: List[Dict[str, Any]]) -> pa.RecordBatch:
    """Helper: build an Arrow batch from sample dicts for the new write_batch contract."""
    return pa.RecordBatch.from_pylist(records)


@pytest.fixture
def api_handler():
    """ApiDestinationHandler primed with a default per-stream state so
    write_batch reaches the mocked write paths (instead of returning
    "Schema not configured")."""
    handler = ApiDestinationHandler()
    handler._streams["test-stream"] = _StreamState(
        endpoint="/v1/records",
        method="POST",
        batch_mode=ApiDestinationHandler.BATCH_MODE_SINGLE,
        batch_size=100,
    )
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

        # Mock _write_single_mode to return 0 (all records failed)
        api_handler._write_single_mode = AsyncMock(return_value=0)

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(sample_records),
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

        # Mock: 1 out of 3 records succeeded
        api_handler._write_single_mode = AsyncMock(return_value=1)

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(sample_records),
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

        # Mock: all 3 records succeeded
        api_handler._write_single_mode = AsyncMock(return_value=3)

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(sample_records),
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
            record_batch=pa.RecordBatch.from_pylist([]),
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
            record_batch=_to_record_batch(sample_records),
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

        # Mock: raise exception during write
        api_handler._write_single_mode = AsyncMock(
            side_effect=Exception("Network error")
        )

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(sample_records),
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
        async def mock_send_request(state, data):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Exception("API error 404: Not found")
            return {"status": "ok"}

        api_handler._send_request = mock_send_request
        state = api_handler._streams["test-stream"]

        # Execute
        written = await api_handler._write_single_mode(state, records, record_ids)

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
        state = api_handler._streams["test-stream"]

        # Execute
        written = await api_handler._write_single_mode(state, records, record_ids)

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
        state = api_handler._streams["test-stream"]

        # Execute
        written = await api_handler._write_single_mode(state, records, record_ids)

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
        api_handler._streams["test-stream"].batch_mode = ApiDestinationHandler.BATCH_MODE_BULK

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
            record_batch=_to_record_batch(records),
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
        api_handler._streams["test-stream"].batch_mode = ApiDestinationHandler.BATCH_MODE_BATCH
        api_handler._streams["test-stream"].batch_size = 2

        records = [{"id": i} for i in range(5)]
        record_ids = [f"rec-{i}" for i in range(5)]

        # Mock: 3 out of 5 records written (first chunk + part of second)
        api_handler._write_batch_mode = AsyncMock(return_value=3)

        # Execute
        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(records),
            record_ids=record_ids,
            cursor=mock_cursor,
        )

        # Assert: partial success is still a failure
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 3


@pytest.mark.unit
class TestApiHandlerConfigureSchemaModeDispatch:
    """``configure_schema`` dispatches into ``operations.write.<mode>`` of
    the preloaded API endpoint document, matching the stream's write_mode."""

    def _endpoint_doc(self, *, modes=("insert", "upsert")):
        operations: Dict[str, Any] = {"write": {}}
        for mode in modes:
            operations["write"][mode] = {
                "request": {"method": "PATCH" if mode == "upsert" else "POST", "path": f"/v1/{mode}"},
                "batching": {"mode": "batch" if mode == "upsert" else "single", "size": 50},
            }
        return {"operations": operations}

    @pytest.fixture
    def handler(self):
        """Fresh handler — no _streams entries pre-set so we exercise
        configure_schema end-to-end."""
        h = ApiDestinationHandler()
        h._connected = True
        h._session = MagicMock()
        return h

    @pytest.mark.asyncio
    async def test_dispatch_insert_mode_reads_insert_block(self, handler):
        handler.set_stream_endpoints({"s1": self._endpoint_doc()})
        msg = SchemaMessage(
            stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
        )
        ok = await handler.configure_schema(msg)
        assert ok is True
        state = handler._streams["s1"]
        assert state.endpoint == "/v1/insert"
        assert state.method == "POST"
        assert state.batch_mode == ApiDestinationHandler.BATCH_MODE_SINGLE
        assert state.batch_size == 50

    @pytest.mark.asyncio
    async def test_dispatch_upsert_mode_reads_upsert_block(self, handler):
        handler.set_stream_endpoints({"s1": self._endpoint_doc()})
        msg = SchemaMessage(
            stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_UPSERT
        )
        ok = await handler.configure_schema(msg)
        assert ok is True
        state = handler._streams["s1"]
        assert state.endpoint == "/v1/upsert"
        assert state.method == "PATCH"
        assert state.batch_mode == ApiDestinationHandler.BATCH_MODE_BATCH

    @pytest.mark.asyncio
    async def test_dispatch_unsupported_mode_returns_false(self, handler):
        """API destinations don't support truncate_insert; the handler
        must reject (not silently dispatch to upsert)."""
        handler.set_stream_endpoints({"s1": self._endpoint_doc()})
        msg = SchemaMessage(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_TRUNCATE_INSERT,
        )
        ok = await handler.configure_schema(msg)
        assert ok is False
        assert "s1" not in handler._streams

    @pytest.mark.asyncio
    async def test_missing_mode_block_returns_false(self, handler):
        """When the contract document doesn't ship the requested mode."""
        handler.set_stream_endpoints({"s1": self._endpoint_doc(modes=("insert",))})
        msg = SchemaMessage(
            stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_UPSERT
        )
        ok = await handler.configure_schema(msg)
        assert ok is False

    @pytest.mark.asyncio
    async def test_missing_path_returns_false(self, handler):
        doc = {"operations": {"write": {"insert": {"request": {"method": "POST"}}}}}
        handler.set_stream_endpoints({"s1": doc})
        msg = SchemaMessage(
            stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
        )
        ok = await handler.configure_schema(msg)
        assert ok is False

    @pytest.mark.asyncio
    async def test_unknown_stream_id_returns_false(self, handler):
        """No preloaded endpoint doc → reject."""
        handler.set_stream_endpoints({})
        msg = SchemaMessage(
            stream_id="ghost", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
        )
        ok = await handler.configure_schema(msg)
        assert ok is False

    @pytest.mark.asyncio
    async def test_per_stream_state_isolated(self, handler):
        """Two streams sharing the handler must not clobber each other's state."""
        handler.set_stream_endpoints(
            {"s1": self._endpoint_doc(), "s2": self._endpoint_doc()}
        )
        await handler.configure_schema(
            SchemaMessage(stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT)
        )
        await handler.configure_schema(
            SchemaMessage(stream_id="s2", version=1, write_mode=WriteMode.WRITE_MODE_UPSERT)
        )
        assert handler._streams["s1"].endpoint == "/v1/insert"
        assert handler._streams["s2"].endpoint == "/v1/upsert"

    def test_mode_keys_table_covers_only_supported_modes(self):
        """The dispatch table must NOT include truncate_insert (which is a
        DB-only mode); leaving it out is what makes the unsupported-mode
        branch reachable."""
        assert set(_API_WRITE_MODE_KEYS.values()) == {"insert", "upsert"}