"""Unit tests for ApiDestinationHandler.

Covers write_batch failure handling (fatal vs retryable classification)
and the contract-driven dispatch in configure_schema (the API endpoint
document's ``operations.write.<mode>`` block selects the path / method
/ batching for the stream's write_mode).
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pyarrow as pa
import pytest

from cdk.types import RetrySemantics
from src.destination.connectors.api import (
    _API_WRITE_MODE_KEYS,
    ApiDestinationHandler,
    _classify_http_error,
    _StreamState,
)
from src.grpc.generated.analitiq.v1 import AckStatus, SchemaMessage, WriteMode


def _to_record_batch(records: list[dict[str, Any]]) -> pa.RecordBatch:
    """Build an Arrow batch from sample dicts for the new write_batch contract."""
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
        sample_records: list[dict[str, Any]],
        sample_record_ids: list[str],
    ):
        """
        Test that when ALL records fail to write, handler returns FATAL_FAILURE.

        This is the bug that was found: previously returned SUCCESS even when
        written=0, causing pipelines to report success incorrectly.
        """
        # Setup: mock the handler as connected
        api_handler._connected = True
        api_handler._session = MagicMock()

        # Mock _write_single_mode to return (0, all-ids, reason) — every
        # record failed
        api_handler._write_single_mode = AsyncMock(
            return_value=(0, list(sample_record_ids), "boom")
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

        # Assert: should return FATAL_FAILURE, not SUCCESS
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 0
        assert "3/3 records failed" in result.failure_summary
        assert result.failed_record_ids == tuple(sample_record_ids)

    @pytest.mark.asyncio
    async def test_write_batch_partial_failure_returns_fatal_failure(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: list[dict[str, Any]],
        sample_record_ids: list[str],
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

        # Mock: 1 out of 3 records succeeded; the other two ids come back failed
        failed = list(sample_record_ids[1:])
        api_handler._write_single_mode = AsyncMock(return_value=(1, failed, "boom"))

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
        assert result.failed_record_ids == tuple(failed)
        # A failure result must not advance the checkpoint (#129)
        assert result.committed_cursor is None

    @pytest.mark.asyncio
    async def test_write_batch_all_records_succeed_returns_success(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: list[dict[str, Any]],
        sample_record_ids: list[str],
    ):
        """Test that when ALL records succeed, handler returns SUCCESS."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()

        # Mock: all 3 records succeeded, none failed
        api_handler._write_single_mode = AsyncMock(return_value=(3, [], ""))

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
        sample_records: list[dict[str, Any]],
        sample_record_ids: list[str],
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
    async def test_write_batch_transport_error_is_retryable(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: list[dict[str, Any]],
        sample_record_ids: list[str],
    ):
        """Transport-level errors are transient — classify as RETRYABLE."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()

        api_handler._write_single_mode = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("connection reset"),
        )

        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(sample_records),
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert "connection reset" in result.failure_summary

    @pytest.mark.asyncio
    async def test_write_batch_unexpected_exception_is_fatal(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: list[dict[str, Any]],
        sample_record_ids: list[str],
    ):
        """Non-transport exceptions (programming bugs, type errors) are
        deterministic — retrying achieves nothing. Classify as FATAL so
        the engine routes to DLQ instead of looping."""
        api_handler._connected = True
        api_handler._session = MagicMock()

        api_handler._write_single_mode = AsyncMock(
            side_effect=KeyError("missing_field"),
        )

        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(sample_records),
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "KeyError" in result.failure_summary


@pytest.mark.unit
class TestApiHandlerWriteSingleMode:
    """Test suite for ApiDestinationHandler._write_single_mode()."""

    @pytest.mark.asyncio
    async def test_write_single_mode_retryable_error_reraises(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """RETRYABLE transport errors (connection errors, timeouts) re-raise
        so write_batch can return RETRYABLE for the whole batch. The loop
        does not continue to subsequent records after a transient failure."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        record_ids = ["rec-1", "rec-2", "rec-3"]

        call_count = 0

        async def mock_send_request(state, data, extra_headers=None):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise aiohttp.ClientConnectionError("transient")
            return {"status": "ok"}

        api_handler._send_request = mock_send_request
        state = api_handler._streams["test-stream"]

        with pytest.raises(aiohttp.ClientConnectionError, match="transient"):
            await api_handler._write_single_mode(state, records, record_ids)
        # Loop aborted after the first retryable failure — record 3 never sent.
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_write_single_mode_429_reraises(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """429 is RETRYABLE — re-raises so write_batch returns RETRYABLE
        instead of permanently DLQ-ing a rate-limited record."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [{"id": 1}, {"id": 2}]
        record_ids = ["rec-1", "rec-2"]

        exc_429 = aiohttp.ClientResponseError(
            MagicMock(), (), status=429, message="Too Many Requests"
        )
        api_handler._send_request = AsyncMock(side_effect=exc_429)
        state = api_handler._streams["test-stream"]

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await api_handler._write_single_mode(state, records, record_ids)
        assert exc_info.value.status == 429

    @pytest.mark.asyncio
    async def test_write_single_mode_fatal_4xx_adds_to_failed_ids(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """A deterministic 4xx rejection (non-429) is FATAL per-record:
        the loop continues and the failed record is attributed individually."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        record_ids = ["rec-1", "rec-2", "rec-3"]

        call_count = 0

        async def mock_send_request(state, data, extra_headers=None):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise aiohttp.ClientResponseError(
                    MagicMock(), (), status=422, message="Unprocessable Entity"
                )
            return {"status": "ok"}

        api_handler._send_request = mock_send_request
        state = api_handler._streams["test-stream"]

        written, failed, first_failure = await api_handler._write_single_mode(
            state, records, record_ids
        )
        # Loop continues past the FATAL 4xx; records 1 and 3 succeed.
        assert written == 2
        assert failed == ["rec-2"]
        assert "422" in first_failure

    @pytest.mark.asyncio
    async def test_write_single_mode_all_fatal_4xx(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """All-FATAL-4xx failures: every record attributed, 0 written."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [{"id": 1}, {"id": 2}]
        record_ids = ["rec-1", "rec-2"]

        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                MagicMock(), (), status=400, message="Bad Request"
            ),
        )
        state = api_handler._streams["test-stream"]

        written, failed, _ = await api_handler._write_single_mode(
            state, records, record_ids
        )
        assert written == 0
        assert failed == ["rec-1", "rec-2"]

    @pytest.mark.asyncio
    async def test_write_batch_single_mode_429_returns_retryable(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: list[dict[str, Any]],
        sample_record_ids: list[str],
    ):
        """End-to-end: a 429 in single mode must produce RETRYABLE, not FATAL.

        Before the fix, the per-record catch swallowed 429 like any other
        transport error and _build_write_result returned FATAL, permanently
        DLQ-ing a rate-limited batch.
        """
        api_handler._connected = True
        api_handler._session = MagicMock()

        exc_429 = aiohttp.ClientResponseError(
            MagicMock(), (), status=429, message="Too Many Requests"
        )
        api_handler._send_request = AsyncMock(side_effect=exc_429)

        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(sample_records),
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.records_written == 0
        assert result.committed_cursor is None

    @pytest.mark.asyncio
    async def test_write_batch_single_mode_fatal_4xx_is_fatal(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: list[dict[str, Any]],
        sample_record_ids: list[str],
    ):
        """End-to-end: a deterministic 4xx in single mode stays FATAL with
        per-record attribution (the loop continues past each rejection)."""
        api_handler._connected = True
        api_handler._session = MagicMock()

        exc_422 = aiohttp.ClientResponseError(
            MagicMock(), (), status=422, message="Unprocessable Entity"
        )
        api_handler._send_request = AsyncMock(side_effect=exc_422)

        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(sample_records),
            record_ids=sample_record_ids,
            cursor=mock_cursor,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 0
        assert result.failed_record_ids == tuple(sample_record_ids)

    @pytest.mark.asyncio
    async def test_write_single_mode_propagates_programming_errors(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """A KeyError or similar bug must NOT be swallowed as a
        per-record failure — it propagates so write_batch can mark the
        whole batch FATAL."""
        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [{"id": 1}, {"id": 2}]
        record_ids = ["rec-1", "rec-2"]

        api_handler._send_request = AsyncMock(side_effect=KeyError("bad_field"))
        state = api_handler._streams["test-stream"]

        with pytest.raises(KeyError):
            await api_handler._write_single_mode(state, records, record_ids)

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
        written, failed, _ = await api_handler._write_single_mode(
            state, records, record_ids
        )

        # Assert: all 3 succeeded, none failed
        assert written == 3
        assert failed == []


@pytest.mark.unit
class TestApiHandlerChunkedWrites:
    """A declared ``batching`` block routes writes through the chunked
    path; its failure handling must classify correctly."""

    @pytest.mark.asyncio
    async def test_chunked_transport_failure_is_retryable(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """Chunked-mode transport-level failure → RETRYABLE."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._streams["test-stream"].max_records = 2

        records = [{"id": i} for i in range(5)]
        record_ids = [f"rec-{i}" for i in range(5)]

        api_handler._write_chunked_mode = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("Bulk insert failed"),
        )

        result = await api_handler.write_batch(
            run_id="test-run",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch(records),
            record_ids=record_ids,
            cursor=mock_cursor,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE

    @pytest.mark.asyncio
    async def test_chunked_partial_chunk_failure(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """Chunked requests where some chunks fail → FATAL with the tail ids."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._streams["test-stream"].max_records = 2

        records = [{"id": i} for i in range(5)]
        record_ids = [f"rec-{i}" for i in range(5)]

        # Mock: 3 of 5 written; the unsent tail comes back as failed ids
        api_handler._write_chunked_mode = AsyncMock(
            return_value=(3, ["rec-3", "rec-4"], "boom")
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

        # Assert: partial success is still a failure, and the unsent ids carry
        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 3
        assert result.failed_record_ids == ("rec-3", "rec-4")

    @pytest.mark.asyncio
    async def test_real_write_chunked_mode_chunk_failure_reports_unsent_ids(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """Drive the REAL _write_chunked_mode (not a mock): a mid-loop chunk
        transport failure must report the records actually sent and attribute
        every record from the failed chunk onward as failed. This is the
        load-bearing dup-on-retry fix.
        """
        import aiohttp

        state = api_handler._streams["test-stream"]
        state.max_records = 2

        calls = []

        async def _send(state_arg, data, extra_headers=None):
            calls.append(data)
            if len(calls) == 2:  # second chunk (rows 2,3) fails
                raise aiohttp.ClientConnectionError("chunk down")
            return {}

        api_handler._send_request = _send
        records = [{"id": i} for i in range(5)]
        record_ids = [f"r{i}" for i in range(5)]

        written, failed, first_failure = await api_handler._write_chunked_mode(
            state, records, record_ids
        )
        # First chunk (r0, r1) landed; everything from the failed chunk on fails.
        assert written == 2
        assert failed == ["r2", "r3", "r4"]
        assert "chunk down" in first_failure
        # The third chunk must never be attempted after a chunk fails.
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_write_batch_chunk_failure_is_fatal_no_cursor_end_to_end(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """End-to-end through write_batch: a real chunked-mode chunk failure
        must be FATAL (not RETRYABLE), carry the true written count and the
        unsent ids, and drop the cursor so the checkpoint cannot advance past
        records that were never written."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        state = api_handler._streams["test-stream"]
        state.max_records = 2

        calls = []

        async def _send(state_arg, data, extra_headers=None):
            calls.append(data)
            if len(calls) == 2:
                raise aiohttp.ClientConnectionError("chunk down")
            return {}

        api_handler._send_request = _send

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": i} for i in range(5)]),
            record_ids=[f"r{i}" for i in range(5)],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 2
        assert result.committed_cursor is None
        assert result.failed_record_ids == ("r2", "r3", "r4")

    @pytest.mark.asyncio
    async def test_chunk_body_build_failure_stops_and_attributes_tail(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """A data-dependent body-build failure mid-loop must stop the loop
        with the true written count: the batch is already doomed to FATAL,
        the engine replays the whole uncheckpointed batch, and chunked
        streams have no idempotency key — so every record sent past the
        failed chunk would land only to be duplicated by that replay."""
        state = api_handler._streams["test-stream"]
        state.max_records = 2

        real_build = api_handler._build_body

        def _build(state_arg, *, record=None, records=None):
            if records and any(r["id"] == 3 for r in records):
                raise ValueError("record 3 cannot feed the derived field")
            return real_build(state_arg, record=record, records=records)

        sent = []

        async def _send(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        api_handler._build_body = _build
        api_handler._send_request = _send
        records = [{"id": i} for i in range(6)]
        record_ids = [f"r{i}" for i in range(6)]

        written, failed, first_failure = await api_handler._write_chunked_mode(
            state, records, record_ids
        )
        # Chunk (r0,r1) landed; the build-failed chunk (r2,r3) stops the
        # loop, so (r4,r5) is never sent and the whole tail is attributed.
        assert written == 2
        assert failed == ["r2", "r3", "r4", "r5"]
        assert "ValueError" in first_failure
        assert len(sent) == 1

    @pytest.mark.asyncio
    async def test_chunk_body_build_failure_with_nothing_landed_is_not_retried(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """A build failure on the FIRST chunk must not re-raise for retry:
        it is deterministic, so the honest verdict is FATAL with every id
        attributed, even though nothing landed."""
        state = api_handler._streams["test-stream"]
        state.max_records = 2

        def _build(state_arg, *, record=None, records=None):
            raise ValueError("no record can feed the derived field")

        api_handler._build_body = _build
        api_handler._send_request = AsyncMock(return_value={})

        written, failed, first_failure = await api_handler._write_chunked_mode(
            state, [{"id": i} for i in range(4)], [f"r{i}" for i in range(4)]
        )
        assert written == 0
        assert failed == ["r0", "r1", "r2", "r3"]
        assert "ValueError" in first_failure
        api_handler._send_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_chunk_body_build_failure_is_fatal_with_detail_end_to_end(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """Through write_batch: the true written count, the failed chunk
        plus unsent tail, and the first-failure reason all reach the
        engine-facing result."""
        api_handler._connected = True
        api_handler._session = MagicMock()
        state = api_handler._streams["test-stream"]
        state.max_records = 2

        real_build = api_handler._build_body

        def _build(state_arg, *, record=None, records=None):
            if records and any(r["id"] == 3 for r in records):
                raise ValueError("record 3 cannot feed the derived field")
            return real_build(state_arg, record=record, records=records)

        api_handler._build_body = _build
        api_handler._send_request = AsyncMock(return_value={})

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": i} for i in range(6)]),
            record_ids=[f"r{i}" for i in range(6)],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 2
        assert result.failed_record_ids == ("r2", "r3", "r4", "r5")
        assert result.committed_cursor is None
        assert "first failure: ValueError" in result.failure_summary

    @pytest.mark.asyncio
    async def test_first_chunk_failure_reraises(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """A transport failure on the FIRST chunk (nothing landed yet) must
        re-raise so it surfaces as RETRYABLE — there is no landed chunk to
        duplicate, so it must not be fatally DLQ'd."""
        import aiohttp

        state = api_handler._streams["test-stream"]
        state.max_records = 2

        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("first chunk down")
        )
        records = [{"id": i} for i in range(5)]
        record_ids = [f"r{i}" for i in range(5)]

        with pytest.raises(aiohttp.ClientConnectionError):
            await api_handler._write_chunked_mode(state, records, record_ids)

    @pytest.mark.asyncio
    async def test_write_batch_first_chunk_failure_is_retryable_end_to_end(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """End-to-end: a first-chunk transport failure is RETRYABLE (no dup
        risk) instead of FATAL."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        state = api_handler._streams["test-stream"]
        state.max_records = 2

        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("first chunk down")
        )

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": i} for i in range(5)]),
            record_ids=[f"r{i}" for i in range(5)],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.records_written == 0
        assert result.committed_cursor is None


@pytest.mark.unit
class TestApiHandlerConfigureSchemaModeDispatch:
    """``configure_schema`` dispatches into ``operations.write.<mode>`` of
    the preloaded API endpoint document, matching the stream's write_mode."""

    def _endpoint_doc(self, *, modes=("insert", "upsert")):
        operations: dict[str, Any] = {"write": {}}
        for mode in modes:
            operations["write"][mode] = {
                "request": {
                    "method": "PATCH" if mode == "upsert" else "POST",
                    "path": f"/v1/{mode}",
                },
            }
            if mode == "upsert":
                operations["write"][mode]["batching"] = {"max_records": 50}
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
        assert state.max_records is None

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
        assert state.max_records == 50

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
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        await handler.configure_schema(
            SchemaMessage(
                stream_id="s2", version=1, write_mode=WriteMode.WRITE_MODE_UPSERT
            )
        )
        assert handler._streams["s1"].endpoint == "/v1/insert"
        assert handler._streams["s2"].endpoint == "/v1/upsert"

    def test_mode_keys_table_covers_only_supported_modes(self):
        """The dispatch table must NOT include truncate_insert (which is a
        DB-only mode); leaving it out is what makes the unsupported-mode
        branch reachable."""
        assert set(_API_WRITE_MODE_KEYS.values()) == {"insert", "upsert"}


@pytest.mark.unit
class TestApiHandlerContractBatching:
    """``batching`` parses the published contract shape only (issue #305):
    absent/null means one request per record; ``{"max_records": <int >= 2>}``
    means chunked requests; anything else fails the stream at configure
    time instead of silently running single-record."""

    def _doc(self, batching="absent"):
        block: dict[str, Any] = {
            "request": {"method": "POST", "path": "/v1/things"},
        }
        if batching != "absent":
            block["batching"] = batching
        return {"operations": {"write": {"insert": block}}}

    @pytest.fixture
    def handler(self):
        h = ApiDestinationHandler()
        h._connected = True
        h._session = MagicMock()
        return h

    async def _configure(self, handler, doc):
        handler.set_stream_endpoints({"s1": doc})
        return await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )

    @pytest.mark.asyncio
    async def test_absent_batching_means_single_record(self, handler):
        assert await self._configure(handler, self._doc()) is True
        assert handler._streams["s1"].max_records is None

    @pytest.mark.asyncio
    async def test_null_batching_means_single_record(self, handler):
        """The contract's nullable default: ``"batching": null``."""
        assert await self._configure(handler, self._doc(batching=None)) is True
        assert handler._streams["s1"].max_records is None

    @pytest.mark.asyncio
    async def test_contract_batching_block_enables_chunking(self, handler):
        assert (
            await self._configure(handler, self._doc(batching={"max_records": 25}))
            is True
        )
        assert handler._streams["s1"].max_records == 25

    @pytest.mark.parametrize(
        "batching",
        [
            {},  # required key missing
            {"max_records": 1},  # below the contract minimum
            {"max_records": "10"},  # wrong type
            {"max_records": True},  # bool is not an integer count
            {"max_records": None},  # null count
            {"mode": "bulk", "size": 100},  # the pre-contract shape (#305)
            {"max_records": 10, "max_record": 5},  # closed shape: typo sibling
            {"max_records": 100, "mode": "single"},  # closed shape: blended
            "bulk",  # not an object at all
        ],
    )
    @pytest.mark.asyncio
    async def test_non_contract_batching_fails_the_stream(self, handler, batching):
        """A batching block that is not ``{"max_records": <int >= 2>}`` must
        fail configure_schema loud — the silent single-record fallback is
        exactly the defect in issue #305 — and the reason must ride the
        SchemaAck channel (issue #231), not just the sidecar log."""
        assert await self._configure(handler, self._doc(batching=batching)) is False
        assert "s1" not in handler._streams
        assert "max_records" in handler.last_schema_rejection

    @pytest.mark.asyncio
    async def test_accepted_configure_clears_rejection_reason(self, handler):
        """A rejection reason from an earlier failed configure must not
        leak into a later accepted stream's ack."""
        assert await self._configure(handler, self._doc(batching={})) is False
        assert handler.last_schema_rejection is not None
        assert (
            await self._configure(handler, self._doc(batching={"max_records": 2}))
            is True
        )
        assert handler.last_schema_rejection is None

    @pytest.mark.asyncio
    async def test_chunked_writes_respect_max_records_end_to_end(self, handler):
        """5 records through a max_records=2 stream land as chunks 2/2/1."""
        assert (
            await self._configure(handler, self._doc(batching={"max_records": 2}))
            is True
        )
        sent = []

        async def _capture(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        handler._send_request = _capture
        records = [{"id": i} for i in range(5)]
        result = await handler.write_batch(
            run_id="r",
            stream_id="s1",
            batch_seq=0,
            record_batch=_to_record_batch(records),
            record_ids=[f"r{i}" for i in range(5)],
            cursor=MagicMock(),
        )
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 5
        assert [len(chunk) for chunk in sent] == [2, 2, 1]
        assert sent[0] == [{"id": 0}, {"id": 1}]


@pytest.mark.unit
class TestApiHandlerSupportsUpsert:
    """``supports_upsert`` is contract-driven: it reflects whether a
    registered endpoint declares ``operations.write.upsert``, never a
    hardcoded value. This is what ``GetCapabilities`` advertises."""

    def _doc(self, *, modes):
        write: dict[str, Any] = {}
        for mode in modes:
            write[mode] = {"request": {"method": "POST", "path": f"/v1/{mode}"}}
        return {"operations": {"write": write}}

    @pytest.fixture
    def handler(self):
        return ApiDestinationHandler()

    def test_false_when_no_endpoints_registered(self, handler):
        assert handler.supports_upsert is False

    def test_false_when_only_insert_endpoint(self, handler):
        handler.set_stream_endpoints({"s1": self._doc(modes=("insert",))})
        assert handler.supports_upsert is False

    def test_true_when_endpoint_declares_upsert(self, handler):
        handler.set_stream_endpoints({"s1": self._doc(modes=("insert", "upsert"))})
        assert handler.supports_upsert is True

    def test_true_when_any_of_several_endpoints_declares_upsert(self, handler):
        handler.set_stream_endpoints(
            {
                "s1": self._doc(modes=("insert",)),
                "s2": self._doc(modes=("insert", "upsert")),
            }
        )
        assert handler.supports_upsert is True

    def test_false_when_upsert_block_has_no_request_path(self, handler):
        """A malformed upsert block (no request.path) is not a usable
        upsert operation, so capability must not be advertised — mirroring
        configure_schema's rejection of the same block."""
        doc = {"operations": {"write": {"upsert": {"request": {"method": "POST"}}}}}
        handler.set_stream_endpoints({"s1": doc})
        assert handler.supports_upsert is False

    @pytest.mark.parametrize(
        "doc",
        [
            {"operations": "write"},  # operations not a mapping
            {"operations": {"write": "upsert"}},  # write not a mapping
            {"operations": {"write": {"upsert": "/v1/upsert"}}},  # block not a mapping
            {
                "operations": {"write": {"upsert": {"request": "/v1/upsert"}}}
            },  # request not a mapping
        ],
    )
    def test_malformed_contract_returns_false_not_raises(self, handler, doc):
        """supports_upsert runs at startup over arbitrary contract docs.
        A malformed document must yield False, never crash the
        GetCapabilities RPC with AttributeError."""
        handler.set_stream_endpoints({"s1": doc})
        assert handler.supports_upsert is False


@pytest.mark.unit
class TestApiHandlerSupportsBulkLoad:
    """``supports_bulk_load`` is contract-driven like ``supports_upsert``:
    it reflects whether any registered endpoint declares a ``batching``
    block on a usable write mode, never a hardcoded value."""

    def _doc(self, *, batching=None):
        block: dict[str, Any] = {"request": {"method": "POST", "path": "/v1/x"}}
        if batching is not None:
            block["batching"] = batching
        return {"operations": {"write": {"insert": block}}}

    @pytest.fixture
    def handler(self):
        return ApiDestinationHandler()

    def test_false_when_no_endpoints_registered(self, handler):
        assert handler.supports_bulk_load is False

    def test_false_when_no_endpoint_declares_batching(self, handler):
        handler.set_stream_endpoints({"s1": self._doc()})
        assert handler.supports_bulk_load is False

    def test_true_when_any_endpoint_declares_batching(self, handler):
        handler.set_stream_endpoints(
            {
                "s1": self._doc(),
                "s2": self._doc(batching={"max_records": 10}),
            }
        )
        assert handler.supports_bulk_load is True

    @pytest.mark.parametrize(
        "batching",
        [
            {},
            {"max_records": 1},
            {"mode": "bulk", "size": 100},
            {"max_records": 10, "max_record": 5},
            "bulk",
        ],
    )
    def test_false_for_batching_block_configure_would_reject(self, handler, batching):
        """Capability advertisement and configure_schema share one
        acceptance predicate: a block configure rejects must not be
        advertised as bulk-load capability."""
        handler.set_stream_endpoints({"s1": self._doc(batching=batching)})
        assert handler.supports_bulk_load is False

    def test_malformed_contract_returns_false_not_raises(self, handler):
        """Capability advertisement over arbitrary docs must not crash."""
        handler.set_stream_endpoints({"s1": {"operations": "write"}})
        assert handler.supports_bulk_load is False


@pytest.mark.unit
class TestApiHandlerJsonFields:
    """Json-typed body fields travel as JSON-encoded ``pa.large_string``
    over the wire; the handler must reverse the encoding so aiohttp posts
    a nested object, not a quoted string."""

    def _doc_with_json_field(self):
        """An api-endpoint document declaring one Json-typed body field."""
        return {
            "operations": {
                "write": {
                    "insert": {
                        "request": {"method": "POST", "path": "/v1/things"},
                        "input": {
                            "schema": {
                                "properties": {
                                    "id": {"arrow_type": "Utf8"},
                                    "metadata": {"arrow_type": "Json"},
                                }
                            }
                        },
                    }
                }
            }
        }

    @pytest.mark.asyncio
    async def test_configure_schema_collects_json_fields(self):
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        handler.set_stream_endpoints({"s1": self._doc_with_json_field()})
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is True
        assert handler._streams["s1"].json_fields == {"metadata"}

    @pytest.mark.asyncio
    async def test_write_batch_decodes_json_field_before_post(self):
        """The body delivered to ``_send_request`` must carry a dict for the
        Json column — not a string. Regression catcher for: 'drops the
        decode loop', 'mis-spells json_fields', 'configure_schema forgets
        to populate'."""
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        handler.set_stream_endpoints({"s1": self._doc_with_json_field()})
        await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )

        # Wire batch — metadata is the JSON-encoded string produced by the
        # source/transform stage.
        batch = pa.RecordBatch.from_pylist(
            [{"id": "r1", "metadata": '{"k": "v", "n": 42}'}]
        )

        sent_payloads = []

        async def _capture(state, data, extra_headers=None):
            sent_payloads.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        result = await handler.write_batch(
            run_id="run",
            stream_id="s1",
            batch_seq=0,
            record_batch=batch,
            record_ids=["r1"],
            cursor=MagicMock(),
        )

        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert sent_payloads == [{"id": "r1", "metadata": {"k": "v", "n": 42}}]

    @pytest.mark.asyncio
    async def test_configure_schema_collects_json_fields_columns_shape(self):
        """Some api-endpoint authors use the flat ``columns`` array under
        ``input.schema`` instead of JSON-Schema-style ``properties``. The
        collector must find Json fields in either shape, otherwise
        columns-style endpoints silently ship JSON strings to the API."""
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        doc = {
            "operations": {
                "write": {
                    "insert": {
                        "request": {"method": "POST", "path": "/v1/things"},
                        "input": {
                            "schema": {
                                "columns": [
                                    {"name": "id", "arrow_type": "Utf8"},
                                    {"name": "metadata", "arrow_type": "Json"},
                                ]
                            }
                        },
                    }
                }
            }
        }
        handler.set_stream_endpoints({"s1": doc})
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is True
        assert handler._streams["s1"].json_fields == {"metadata"}

    @pytest.mark.asyncio
    async def test_write_batch_preserves_native_types_for_orjson(self):
        """The dict delivered to ``_send_request`` carries Arrow-native
        Python types (``datetime``, ``Decimal``). orjson at the HTTP
        boundary handles datetime natively and routes Decimal through
        the default-hook — both end up as canonical JSON strings on the
        wire. Asserting on the dict shape locks in that we don't waste
        cycles pre-converting in Arrow space.
        """
        from datetime import datetime, timezone
        from decimal import Decimal

        import orjson

        from src.destination.connectors.api import _orjson_default

        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        doc = {
            "operations": {
                "write": {
                    "insert": {
                        "request": {"method": "POST", "path": "/v1/things"},
                        "input": {"schema": {"properties": {}}},
                    }
                }
            }
        }
        handler.set_stream_endpoints({"s1": doc})
        await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )

        batch_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("created", pa.timestamp("us", tz="UTC")),
                pa.field("amount", pa.decimal128(18, 4)),
            ]
        )
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1], type=pa.int64()),
                pa.array(
                    [datetime(2026, 3, 23, 10, 18, 24, tzinfo=timezone.utc)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                pa.array([Decimal("42.5000")], type=pa.decimal128(18, 4)),
            ],
            schema=batch_schema,
        )

        sent_payloads = []

        async def _capture(state, data, extra_headers=None):
            sent_payloads.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        result = await handler.write_batch(
            run_id="run",
            stream_id="s1",
            batch_seq=0,
            record_batch=batch,
            record_ids=["r1"],
            cursor=MagicMock(),
        )
        assert result.status == AckStatus.ACK_STATUS_SUCCESS

        sent = sent_payloads[0]
        # No pre-conversion: types are still Python-native here.
        assert sent["id"] == 1
        assert isinstance(sent["created"], datetime)
        assert isinstance(sent["amount"], Decimal)

        # And the orjson boundary turns them into RFC 3339 / canonical
        # forms without any Arrow-side cast pass.
        encoded = orjson.dumps(sent, default=_orjson_default).decode("utf-8")
        assert '"created":"2026-03-23T10:18:24+00:00"' in encoded
        assert '"amount":"42.5000"' in encoded

    @pytest.mark.asyncio
    async def test_write_batch_malformed_json_returns_fatal(self):
        """A non-JSON string in a Json column is a data-shape failure,
        not a transport failure — must classify FATAL and surface the
        offending column."""
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        handler.set_stream_endpoints({"s1": self._doc_with_json_field()})
        await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )

        batch = pa.RecordBatch.from_pylist(
            [{"id": "r1", "metadata": "this is not json"}]
        )
        handler._send_request = AsyncMock(return_value={})

        result = await handler.write_batch(
            run_id="run",
            stream_id="s1",
            batch_seq=0,
            record_batch=batch,
            record_ids=["r1"],
            cursor=MagicMock(),
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "metadata" in (result.failure_summary or "")


@pytest.mark.unit
class TestOrjsonDefault:
    """The default-hook is the only seam where Decimal and bytes reach
    orjson — everything else (datetime, date, time, UUID, dataclass,
    enum, numpy) orjson serialises natively."""

    def test_decimal_becomes_canonical_string(self):
        from decimal import Decimal

        from src.destination.connectors.api import _orjson_default

        assert _orjson_default(Decimal("42.5000")) == "42.5000"
        assert _orjson_default(Decimal("-0.00001")) == "-0.00001"

    def test_bytes_become_base64(self):
        import base64

        from src.destination.connectors.api import _orjson_default

        out = _orjson_default(b"hello")
        assert out == base64.b64encode(b"hello").decode("ascii")
        # bytearray and memoryview share the path.
        assert _orjson_default(bytearray(b"\xff\xfe")) == base64.b64encode(
            b"\xff\xfe"
        ).decode("ascii")

    def test_unknown_type_raises_typeerror(self):
        from src.destination.connectors.api import _orjson_default

        class Custom:
            pass

        with pytest.raises(TypeError, match="cannot serialise Custom"):
            _orjson_default(Custom())

    def test_orjson_uses_default_for_decimal_in_nested_dict(self):
        """The hook fires per-value inside nested structures — proves
        no Arrow-level recursion needed to handle nested Decimals."""
        from datetime import datetime, timezone
        from decimal import Decimal

        import orjson

        from src.destination.connectors.api import _orjson_default

        payload = {
            "id": 1,
            "created": datetime(2026, 5, 12, 10, 0, tzinfo=timezone.utc),
            "items": [
                {"sku": "A", "price": Decimal("9.99")},
                {"sku": "B", "price": Decimal("12.50")},
            ],
        }
        encoded = orjson.dumps(payload, default=_orjson_default).decode("utf-8")
        assert '"price":"9.99"' in encoded
        assert '"price":"12.50"' in encoded
        assert '"created":"2026-05-12T10:00:00+00:00"' in encoded


@pytest.mark.unit
class TestClassifyHttpError:
    """``_classify_http_error`` classifies ClientResponseError by HTTP status
    and leaves all other exception types as RETRYABLE."""

    def _response_error(self, status: int) -> "aiohttp.ClientResponseError":
        req = MagicMock()
        return aiohttp.ClientResponseError(req, (), status=status)

    def test_400_is_fatal(self):
        assert (
            _classify_http_error(self._response_error(400))
            == AckStatus.ACK_STATUS_FATAL_FAILURE
        )

    def test_408_is_retryable(self):
        assert (
            _classify_http_error(self._response_error(408))
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )

    def test_422_is_fatal(self):
        assert (
            _classify_http_error(self._response_error(422))
            == AckStatus.ACK_STATUS_FATAL_FAILURE
        )

    def test_401_is_fatal(self):
        assert (
            _classify_http_error(self._response_error(401))
            == AckStatus.ACK_STATUS_FATAL_FAILURE
        )

    def test_404_is_fatal(self):
        assert (
            _classify_http_error(self._response_error(404))
            == AckStatus.ACK_STATUS_FATAL_FAILURE
        )

    def test_429_is_retryable(self):
        assert (
            _classify_http_error(self._response_error(429))
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )

    def test_500_is_retryable(self):
        assert (
            _classify_http_error(self._response_error(500))
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )

    def test_503_is_retryable(self):
        assert (
            _classify_http_error(self._response_error(503))
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )

    def test_connection_error_is_retryable(self):
        import aiohttp

        assert (
            _classify_http_error(aiohttp.ClientConnectionError("refused"))
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )

    def test_timeout_is_retryable(self):
        import asyncio

        assert (
            _classify_http_error(asyncio.TimeoutError())
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )

    def test_payload_error_is_retryable(self):
        import aiohttp

        assert (
            _classify_http_error(aiohttp.ClientPayloadError("truncated"))
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )

    def test_399_is_retryable_boundary(self):
        assert (
            _classify_http_error(self._response_error(399))
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )

    def test_499_is_fatal_boundary(self):
        assert (
            _classify_http_error(self._response_error(499))
            == AckStatus.ACK_STATUS_FATAL_FAILURE
        )

    def test_non_response_client_error_is_retryable(self):
        import aiohttp

        assert (
            _classify_http_error(aiohttp.ClientError())
            == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        )


@pytest.mark.unit
class TestApiHandlerDeterministic4xxClassification:
    """HTTP 4xx rejections (except 429) are deterministic: the engine must
    not burn retries on them. Tests cover write_batch's outer exception
    handler and the _write_chunked_mode re-raise decision.

    Note: in single mode _write_single_mode catches ClientError per-record
    and never re-raises, so write_batch's outer catch is only exercised in
    practice by chunked mode's first-chunk re-raise. The outer-catch tests
    below mock _write_single_mode to raise explicitly, isolating the
    classification logic from the single/chunked dispatch."""

    def _response_error(self, status: int) -> "aiohttp.ClientResponseError":
        req = MagicMock()
        return aiohttp.ClientResponseError(
            req, (), status=status, message=f"HTTP {status}"
        )

    @pytest.mark.asyncio
    async def test_write_batch_outer_catch_4xx_is_fatal(
        self, api_handler: ApiDestinationHandler, mock_cursor: MagicMock
    ):
        """write_batch's outer exception handler classifies a 4xx
        ClientResponseError as FATAL, not RETRYABLE."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._write_single_mode = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                MagicMock(), (), status=400, message="Bad Request"
            )
        )

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": 1}]),
            record_ids=["r1"],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 0
        assert result.committed_cursor is None

    @pytest.mark.asyncio
    async def test_write_batch_outer_catch_429_is_retryable(
        self, api_handler: ApiDestinationHandler, mock_cursor: MagicMock
    ):
        """write_batch's outer exception handler classifies 429 as RETRYABLE."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._write_single_mode = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                MagicMock(), (), status=429, message="Too Many Requests"
            )
        )

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": 1}]),
            record_ids=["r1"],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE

    @pytest.mark.asyncio
    async def test_write_batch_outer_catch_5xx_is_retryable(
        self, api_handler: ApiDestinationHandler, mock_cursor: MagicMock
    ):
        """write_batch's outer exception handler classifies 5xx as RETRYABLE."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._write_single_mode = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                MagicMock(), (), status=503, message="Service Unavailable"
            )
        )

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": 1}]),
            record_ids=["r1"],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE

    @pytest.mark.asyncio
    async def test_single_mode_4xx_is_fatal_via_failed_records(
        self, api_handler: ApiDestinationHandler, mock_cursor: MagicMock
    ):
        """In single mode, a 4xx from _send_request is caught per-record by
        _write_single_mode (not re-raised). The record goes into failed_ids
        and write_batch returns FATAL via _build_write_result, not via the
        outer exception handler."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                MagicMock(), (), status=400, message="Bad Request"
            )
        )

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": 1}]),
            record_ids=["r1"],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 0
        assert result.committed_cursor is None
        assert result.failed_record_ids == ("r1",)

    @pytest.mark.asyncio
    async def test_first_chunk_4xx_does_not_reraise(
        self, api_handler: ApiDestinationHandler
    ):
        """A 4xx on the first chunk (nothing landed) must NOT re-raise:
        the rejection is deterministic, so re-raising as RETRYABLE would
        burn engine retries on a payload the API will always refuse."""
        import aiohttp

        state = api_handler._streams["test-stream"]
        state.max_records = 2

        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                MagicMock(), (), status=400, message="Bad Request"
            )
        )
        records = [{"id": i} for i in range(4)]
        record_ids = [f"r{i}" for i in range(4)]

        written, failed, first_failure = await api_handler._write_chunked_mode(
            state, records, record_ids
        )
        assert written == 0
        assert failed == record_ids
        assert "400" in first_failure or "Bad Request" in first_failure

    @pytest.mark.asyncio
    async def test_first_chunk_4xx_is_fatal_end_to_end(
        self, api_handler: ApiDestinationHandler, mock_cursor: MagicMock
    ):
        """End-to-end: a 4xx on the first (and only attempted) chunk must
        yield FATAL with every record id attributed and no cursor advance."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        state = api_handler._streams["test-stream"]
        state.max_records = 2

        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                MagicMock(), (), status=400, message="Bad Request"
            )
        )

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": i} for i in range(4)]),
            record_ids=[f"r{i}" for i in range(4)],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 0
        assert result.committed_cursor is None
        assert result.failed_record_ids == ("r0", "r1", "r2", "r3")

    @pytest.mark.asyncio
    async def test_first_chunk_429_still_reraises(
        self, api_handler: ApiDestinationHandler
    ):
        """429 on the first chunk is RETRYABLE and must still re-raise so
        write_batch can classify it RETRYABLE — nothing landed, safe to retry."""
        import aiohttp

        state = api_handler._streams["test-stream"]
        state.max_records = 2

        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                MagicMock(), (), status=429, message="Too Many Requests"
            )
        )

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await api_handler._write_chunked_mode(
                state, [{"id": 0}, {"id": 1}], ["r0", "r1"]
            )
        assert exc_info.value.status == 429

    @pytest.mark.asyncio
    async def test_mid_batch_4xx_attributes_tail_as_fatal(
        self, api_handler: ApiDestinationHandler, mock_cursor: MagicMock
    ):
        """A 4xx on a non-first chunk (some records already landed) must
        attribute the failed chunk plus unsent tail and return FATAL.
        The 4xx must not re-raise (nothing-landed invariant only applies at
        written == 0; mid-batch duplication risk doesn't change the rule)."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        state = api_handler._streams["test-stream"]
        state.max_records = 2

        calls = []

        async def _send(state_arg, data, extra_headers=None):
            calls.append(data)
            if len(calls) == 2:
                raise aiohttp.ClientResponseError(
                    MagicMock(), (), status=400, message="Bad Request"
                )
            return {}

        api_handler._send_request = _send

        result = await api_handler.write_batch(
            run_id="r",
            stream_id="test-stream",
            batch_seq=1,
            record_batch=_to_record_batch([{"id": i} for i in range(5)]),
            record_ids=[f"r{i}" for i in range(5)],
            cursor=mock_cursor,
        )

        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 2
        assert result.committed_cursor is None
        assert result.failed_record_ids == ("r2", "r3", "r4")
        assert len(calls) == 2


@pytest.mark.unit
class TestApiHandlerBodySpec:
    """``operations.write.<mode>.request.body`` is the optional body
    template (#166): ``from_input`` nodes bind the in-flight record(s),
    value expressions resolve against the connection scopes, and
    unresolved expressions omit their field instead of going onto the
    wire raw. Without a declared body the record(s) remain the body."""

    def _handler_with_resolver(self, parameters=None) -> ApiDestinationHandler:
        from cdk.connection_runtime import ConnectionRuntime
        from cdk.secrets import InMemorySecretsResolver

        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        runtime = ConnectionRuntime(
            raw_config={"parameters": parameters or {}},
            connection_id="conn-1",
            connector_id="testapi",
            connector_type="api",
            resolver=InMemorySecretsResolver({}),
        )
        # connect() builds this from the runtime; tests wire it directly.
        handler._request_resolver = runtime.request_resolver()
        return handler

    def _doc_with_body(self, body, *, batching=None):
        block = {
            "request": {"method": "POST", "path": "/v1/things", "body": body},
        }
        if batching:
            block["batching"] = batching
        return {"operations": {"write": {"insert": block}}}

    @pytest.mark.asyncio
    async def test_configure_schema_captures_body_spec(self):
        handler = self._handler_with_resolver()
        body = {"data": {"from_input": "record"}}
        handler.set_stream_endpoints({"s1": self._doc_with_body(body)})
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is True
        assert handler._streams["s1"].body_spec == body

    def test_no_body_spec_keeps_record_as_body(self):
        handler = self._handler_with_resolver()
        state = _StreamState(endpoint="/v1/things")
        record = {"id": 1, "name": "a"}
        assert handler._build_body(state, record=record) is record
        records = [record]
        assert handler._build_body(state, records=records) is records

    def test_body_spec_binds_single_record(self):
        handler = self._handler_with_resolver(parameters={"source_tag": "crm"})
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={
                "data": {"from_input": "record"},
                "external_id": {"from_input": "record.id"},
                "source": {"ref": "connection.parameters.source_tag"},
                "missing": {"ref": "connection.parameters.not_there"},
            },
        )
        body = handler._build_body(state, record={"id": 7, "name": "a"})
        assert body == {
            "data": {"id": 7, "name": "a"},
            "external_id": 7,
            "source": "crm",
            # "missing" omitted per the contract's drop-unresolved rule
        }

    def test_body_spec_binds_record_batch(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"items": {"from_input": "records"}, "mode": "bulk"},
        )
        records = [{"id": 1}, {"id": 2}]
        body = handler._build_body(state, records=records)
        assert body == {"items": [{"id": 1}, {"id": 2}], "mode": "bulk"}

    def test_record_data_is_never_re_resolved(self):
        # Record values are payload: a record field that *looks* like an
        # expression node must pass through verbatim.
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"data": {"from_input": "record"}},
        )
        record = {"id": 1, "payload": {"ref": "user supplied text"}}
        body = handler._build_body(state, record=record)
        assert body == {"data": record}

    def test_missing_record_field_is_dropped(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={
                "id": {"from_input": "record.id"},
                "x": {"from_input": "record.nope"},
            },
        )
        body = handler._build_body(state, record={"id": 3})
        assert body == {"id": 3}

    def test_from_param_binds_declared_write_param(self):
        # Write bodies may mix {"from_param": ...} with {"from_input": ...};
        # the param table comes from the mode block's declared params.
        handler = self._handler_with_resolver(parameters={"account": "acc-1"})
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={
                "account_id": {"from_param": "account_id"},
                "data": {"from_input": "record"},
            },
            params_spec={
                "account_id": {
                    "in": "body",
                    "type": "string",
                    "required": True,
                    "default": {"ref": "connection.parameters.account"},
                },
            },
        )
        body = handler._build_body(state, record={"id": 1})
        assert body == {"account_id": "acc-1", "data": {"id": 1}}

    def test_from_param_for_undeclared_write_param_drops_field(self):
        # Never the raw {"from_param": ...} dict on the wire: a missing
        # param binds None and the field is omitted.
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={
                "mode": {"from_param": "undeclared"},
                "data": {"from_input": "record"},
            },
        )
        body = handler._build_body(state, record={"id": 1})
        assert body == {"data": {"id": 1}}

    def test_body_spec_without_resolver_raises(self):
        # configure_schema ran but connect() never did: a body spec with no
        # request resolver must fail loud, not build a broken body.
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"data": {"from_input": "record"}},
        )
        with pytest.raises(RuntimeError, match="no request resolver"):
            handler._build_body(state, record={"id": 1})

    def test_batch_record_data_is_never_re_resolved(self):
        # The literal wrapper covers the whole batch list: a record inside
        # a bulk body that looks like an expression node stays verbatim.
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"items": {"from_input": "records"}},
        )
        records = [
            {"id": 1, "payload": {"template": "x${y}"}},
            {"id": 2, "payload": {"ref": "user text"}},
        ]
        body = handler._build_body(state, records=records)
        assert body == {"items": records}

    def test_from_input_record_on_batched_stream_raises(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"data": {"from_input": "record"}},
        )
        with pytest.raises(ValueError, match="requires a single-record stream"):
            handler._build_body(state, records=[{"id": 1}])

    def test_from_input_records_on_single_record_stream_raises(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"items": {"from_input": "records"}},
        )
        with pytest.raises(ValueError, match="requires a batching declaration"):
            handler._build_body(state, record={"id": 1})

    def test_unknown_from_input_selector_raises(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"x": {"from_input": "response.id"}},
        )
        with pytest.raises(ValueError, match="Unsupported `from_input`"):
            handler._build_body(state, record={"id": 1})

    def test_from_input_with_siblings_raises(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"x": {"from_input": "record", "extra": 1}},
        )
        with pytest.raises(ValueError, match="only key"):
            handler._build_body(state, record={"id": 1})

    def test_body_resolving_to_nothing_raises(self):
        # Posting "null" per record would silently write nothing — an
        # entire body resolving away is an authoring error, not a drop.
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"ref": "connection.parameters.not_there"},
        )
        with pytest.raises(ValueError, match="resolved to nothing"):
            handler._build_body(state, record={"id": 1})

    @pytest.mark.asyncio
    async def test_single_mode_sends_built_body_per_record(self):
        handler = self._handler_with_resolver(parameters={"source_tag": "crm"})
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={
                "data": {"from_input": "record"},
                "source": {"ref": "connection.parameters.source_tag"},
            },
        )
        handler._streams["s1"] = state

        sent = []

        async def _capture(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        written, failed, _ = await handler._write_single_mode(
            state, [{"id": 1}, {"id": 2}], ["r1", "r2"]
        )
        assert written == 2
        assert failed == []
        assert sent == [
            {"data": {"id": 1}, "source": "crm"},
            {"data": {"id": 2}, "source": "crm"},
        ]

    @pytest.mark.asyncio
    async def test_chunked_mode_sends_one_request_when_batch_fits(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            max_records=10,
            body_spec={"items": {"from_input": "records"}},
        )

        sent = []

        async def _capture(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        written, failed, _ = await handler._write_chunked_mode(
            state, [{"id": 1}, {"id": 2}], ["r1", "r2"]
        )
        assert written == 2
        assert failed == []
        assert sent == [{"items": [{"id": 1}, {"id": 2}]}]

    @pytest.mark.asyncio
    async def test_chunked_mode_sends_built_body_per_chunk(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            max_records=2,
            body_spec={"items": {"from_input": "records"}},
        )

        sent = []

        async def _capture(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        written, failed, _ = await handler._write_chunked_mode(
            state, [{"id": 1}, {"id": 2}, {"id": 3}], ["r1", "r2", "r3"]
        )
        assert written == 3
        assert failed == []
        assert sent == [
            {"items": [{"id": 1}, {"id": 2}]},
            {"items": [{"id": 3}]},
        ]

    @pytest.mark.asyncio
    async def test_configure_schema_rejects_records_selector_without_batching(self):
        # The mismatch is statically knowable: reject the stream up front
        # instead of failing every record at write time.
        handler = self._handler_with_resolver()
        handler.set_stream_endpoints(
            {"s1": self._doc_with_body({"items": {"from_input": "records"}})}
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False
        assert "s1" not in handler._streams

    @pytest.mark.asyncio
    async def test_configure_schema_rejects_record_selector_on_batched_stream(self):
        handler = self._handler_with_resolver()
        handler.set_stream_endpoints(
            {
                "s1": self._doc_with_body(
                    {"data": {"from_input": "record"}},
                    batching={"max_records": 10},
                )
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False

    @pytest.mark.asyncio
    async def test_body_spec_mode_mismatch_backstop_is_fatal(self):
        # Runtime backstop for a state that bypassed configure_schema:
        # every record fails body construction (with a per-record warning
        # carrying the cause) and the batch classifies FATAL.
        handler = self._handler_with_resolver()
        handler._streams["s1"] = _StreamState(
            endpoint="/v1/things",
            body_spec={
                "items": {"from_input": "records"}
            },  # single mode sends one record
        )
        result = await handler.write_batch(
            run_id="run",
            stream_id="s1",
            batch_seq=0,
            record_batch=_to_record_batch([{"id": 1}]),
            record_ids=["r1"],
            cursor=MagicMock(),
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "1/1 records failed" in result.failure_summary

    @pytest.mark.asyncio
    async def test_bad_record_body_build_keeps_partial_write_counts(self):
        # A record whose data breaks body construction (non-string into
        # base64_encode) must not discard the counts of records already
        # sent: partial success, accurate records_written.
        handler = self._handler_with_resolver()
        handler._streams["s1"] = _StreamState(
            endpoint="/v1/things",
            body_spec={
                "token": {
                    "function": "base64_encode",
                    "input": {"from_input": "record.id"},
                },
            },
        )

        sent = []

        async def _capture(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        result = await handler.write_batch(
            run_id="run",
            stream_id="s1",
            batch_seq=0,
            record_batch=_to_record_batch(
                [{"id": "ok-1"}, {"id": None}, {"id": "ok-2"}]  # null breaks base64
            ),
            record_ids=["r1", "r2", "r3"],
            cursor=MagicMock(),
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.records_written == 2
        assert "1/3 records failed" in result.failure_summary
        import base64 as b64

        assert sent == [
            {"token": b64.b64encode(b"ok-1").decode("ascii")},
            {"token": b64.b64encode(b"ok-2").decode("ascii")},
        ]


@pytest.mark.unit
class TestApiHandlerIdempotencyConfig:
    """``configure_schema`` validation of the endpoint's
    ``operations.write.<mode>.idempotency`` block (infra#890 / issue #286).

    The block declares where the engine-owned per-record key lands; a
    block that cannot work (non-single batching, non-object body for a
    body target, a reserved-field collision) must reject the stream at
    configure time, never at write time.
    """

    def _doc(
        self,
        *,
        mode="insert",
        idempotency=None,
        batching="absent",
        body=None,
    ):
        block: dict[str, Any] = {
            "request": {"method": "POST", "path": f"/v1/{mode}"},
        }
        if batching != "absent":
            block["batching"] = batching
        if body is not None:
            block["request"]["body"] = body
        if idempotency is not None:
            block["idempotency"] = idempotency
        return {"operations": {"write": {mode: block}}}

    @pytest.fixture
    def handler(self):
        h = ApiDestinationHandler()
        h._connected = True
        h._session = MagicMock()
        return h

    @pytest.mark.asyncio
    async def test_header_block_accepted_and_recorded(self, handler):
        handler.set_stream_endpoints(
            {"s1": self._doc(idempotency={"in": "header", "name": "Idempotency-Key"})}
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is True
        state = handler._streams["s1"]
        assert state.idempotency_in == "header"
        assert state.idempotency_name == "Idempotency-Key"

    @pytest.mark.asyncio
    async def test_body_block_accepted_on_upsert(self, handler):
        """The block is allowed on both write modes (a Square-style upsert
        requires a body idempotency key)."""
        handler.set_stream_endpoints(
            {
                "s1": self._doc(
                    mode="upsert",
                    idempotency={"in": "body", "name": "idempotency_key"},
                )
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_UPSERT
            )
        )
        assert ok is True
        assert handler._streams["s1"].idempotency_in == "body"

    @pytest.mark.asyncio
    async def test_rejects_when_batching_block_present(self, handler):
        """The contract has no batching mode — a present block IS the
        multi-record case. A restart re-batches records, so a per-request
        key over several records cannot dedup (issue #286 / infra#890)."""
        handler.set_stream_endpoints(
            {
                "s1": self._doc(
                    idempotency={"in": "header", "name": "Idempotency-Key"},
                    batching={"max_records": 5},
                )
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False
        assert "s1" not in handler._streams

    @pytest.mark.asyncio
    async def test_explicit_null_batching_is_accepted(self, handler):
        """The contract's nullable default: "batching": null means
        single-record, so the exclusion must not fire on it."""
        handler.set_stream_endpoints(
            {
                "s1": self._doc(
                    idempotency={"in": "header", "name": "Idempotency-Key"},
                    batching=None,
                )
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "idempotency",
        [
            "Idempotency-Key",  # not an object
            {"in": "query", "name": "k"},  # unknown target
            {"name": "k"},  # missing in
            {"in": "header"},  # missing name
            {"in": "header", "name": ""},  # empty name
            {"in": "header", "name": 7},  # non-string name
        ],
    )
    async def test_rejects_malformed_block(self, handler, idempotency):
        handler.set_stream_endpoints({"s1": self._doc(idempotency=idempotency)})
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False

    @pytest.mark.asyncio
    async def test_rejects_body_target_with_non_object_body_spec(self, handler):
        """A body key needs a JSON object to land in; a declared array
        body can never carry it."""
        handler.set_stream_endpoints(
            {
                "s1": self._doc(
                    idempotency={"in": "body", "name": "idempotency_key"},
                    body=[{"from_input": "record"}],
                )
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False

    @pytest.mark.asyncio
    async def test_rejects_body_target_when_spec_declares_key_field(self, handler):
        handler.set_stream_endpoints(
            {
                "s1": self._doc(
                    idempotency={"in": "body", "name": "idempotency_key"},
                    body={"idempotency_key": {"from_input": "record.id"}},
                )
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False

    @pytest.mark.asyncio
    async def test_body_target_ok_without_body_spec(self, handler):
        """No declared body: the record dict is the body, which is an
        object — the key can land."""
        handler.set_stream_endpoints(
            {"s1": self._doc(idempotency={"in": "body", "name": "idempotency_key"})}
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is True


@pytest.mark.unit
class TestApiHandlerIdempotencyInjection:
    """Single-mode write path injects the engine-owned key per record."""

    def _handler_with_state(self, **state_kwargs):
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        state = _StreamState(
            endpoint="/v1/records",
            **state_kwargs,
        )
        handler._streams["s1"] = state
        return handler, state

    @pytest.mark.asyncio
    async def test_header_target_sends_record_id_per_request(self):
        handler, state = self._handler_with_state(
            idempotency_in="header", idempotency_name="Idempotency-Key"
        )
        captured = []

        async def _capture(state_arg, data, extra_headers=None):
            captured.append((data, extra_headers))
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        written, failed, _ = await handler._write_single_mode(
            state, [{"id": 1}, {"id": 2}], ["r1", "r2"]
        )
        assert (written, failed) == (2, [])
        assert captured == [
            ({"id": 1}, {"Idempotency-Key": "r1"}),
            ({"id": 2}, {"Idempotency-Key": "r2"}),
        ]

    @pytest.mark.asyncio
    async def test_body_target_adds_key_field_per_request(self):
        handler, state = self._handler_with_state(
            idempotency_in="body", idempotency_name="idempotency_key"
        )
        captured = []

        async def _capture(state_arg, data, extra_headers=None):
            captured.append((data, extra_headers))
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        written, failed, _ = await handler._write_single_mode(
            state, [{"id": 1}], ["r1"]
        )
        assert (written, failed) == (1, [])
        assert captured == [({"id": 1, "idempotency_key": "r1"}, None)]

    @pytest.mark.asyncio
    async def test_body_target_does_not_mutate_source_record(self):
        handler, state = self._handler_with_state(
            idempotency_in="body", idempotency_name="idempotency_key"
        )
        handler._send_request = AsyncMock(return_value={})
        record = {"id": 1}
        await handler._write_single_mode(state, [record], ["r1"])
        assert record == {"id": 1}

    @pytest.mark.asyncio
    async def test_body_target_collision_fails_only_that_record(self):
        """A record that already carries the reserved field must not be
        silently overwritten; it fails alone and the rest still write."""
        handler, state = self._handler_with_state(
            idempotency_in="body", idempotency_name="idempotency_key"
        )
        sent = []

        async def _capture(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        written, failed, _ = await handler._write_single_mode(
            state,
            [{"id": 1, "idempotency_key": "mine"}, {"id": 2}],
            ["r1", "r2"],
        )
        assert (written, failed) == (1, ["r1"])
        assert sent == [{"id": 2, "idempotency_key": "r2"}]

    @pytest.mark.asyncio
    async def test_no_idempotency_sends_no_extra_headers(self):
        handler, state = self._handler_with_state()
        captured = []

        async def _capture(state_arg, data, extra_headers=None):
            captured.append(extra_headers)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        await handler._write_single_mode(state, [{"id": 1}], ["r1"])
        assert captured == [None]

    @pytest.mark.asyncio
    async def test_first_failure_reason_reaches_the_failure_summary(self):
        """A reserved-field collision on every record must surface its
        actionable reason in the engine-facing summary, not just this
        container's per-record warnings."""
        handler, _state = self._handler_with_state(
            idempotency_in="body", idempotency_name="idempotency_key"
        )
        handler._connected = True
        result = await handler.write_batch(
            run_id="run",
            stream_id="s1",
            batch_seq=0,
            record_batch=_to_record_batch([{"id": 1, "idempotency_key": "mine"}]),
            record_ids=["r1"],
            cursor=MagicMock(),
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "1/1 records failed" in result.failure_summary
        assert "reserves" in result.failure_summary


@pytest.mark.unit
class TestApiHandlerRetrySemantics:
    """Per-stream retry-safety verdict (issue #286)."""

    def _doc(self, *, mode="insert", idempotency=None):
        block: dict[str, Any] = {
            "request": {"method": "POST", "path": f"/v1/{mode}"},
        }
        if idempotency is not None:
            block["idempotency"] = idempotency
        return {"operations": {"write": {mode: block}}}

    async def _configured(self, doc, write_mode):
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        handler.set_stream_endpoints({"s1": doc})
        ok = await handler.configure_schema(
            SchemaMessage(stream_id="s1", version=1, write_mode=write_mode)
        )
        assert ok is True
        return handler

    @pytest.mark.asyncio
    async def test_upsert_reports_exactly_once(self):
        handler = await self._configured(
            self._doc(mode="upsert"), WriteMode.WRITE_MODE_UPSERT
        )
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert "conflict keys" in verdict.reason

    @pytest.mark.asyncio
    async def test_insert_with_idempotency_reports_exactly_once(self):
        handler = await self._configured(
            self._doc(idempotency={"in": "header", "name": "Idempotency-Key"}),
            WriteMode.WRITE_MODE_INSERT,
        )
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert "Idempotency-Key" in verdict.reason

    @pytest.mark.asyncio
    async def test_insert_without_idempotency_reports_at_least_once(self):
        handler = await self._configured(self._doc(), WriteMode.WRITE_MODE_INSERT)
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE

    def test_unconfigured_stream_falls_back_to_base_default(self):
        handler = ApiDestinationHandler()
        verdict = handler.retry_semantics("ghost")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "declares no retry-safety" in verdict.reason


@pytest.mark.unit
class TestApiHandlerIdempotencyHeaderCollision:
    """The engine owns Content-Type on every request; an idempotency key
    named after it would silently override the header instead of failing."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", ["Content-Type", "content-type"])
    async def test_rejects_content_type_as_key_name(self, name):
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        handler.set_stream_endpoints(
            {
                "s1": {
                    "operations": {
                        "write": {
                            "insert": {
                                "request": {"method": "POST", "path": "/v1/insert"},
                                "idempotency": {"in": "header", "name": name},
                            }
                        }
                    }
                }
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False


@pytest.mark.unit
class TestApiHandlerIdempotencyKeyPerMode:
    """The key value follows the write mode's identity semantics: insert
    sends the identity-derived record_id (SQL-insert parity); upsert
    sends a full-content hash so a changed row gets a new key and the
    provider applies the update instead of replaying its cached
    response."""

    def _handler(self, write_mode_key):
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        state = _StreamState(
            endpoint="/v1/records",
            idempotency_in="header",
            idempotency_name="Idempotency-Key",
            write_mode_key=write_mode_key,
        )
        handler._streams["s1"] = state
        return handler, state

    @pytest.mark.asyncio
    async def test_insert_sends_record_id(self):
        handler, state = self._handler("insert")
        captured = []

        async def _capture(state_arg, data, extra_headers=None):
            captured.append(extra_headers)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        await handler._write_single_mode(state, [{"id": 1}], ["rid-1"])
        assert captured == [{"Idempotency-Key": "rid-1"}]

    @pytest.mark.asyncio
    async def test_upsert_sends_content_hash_not_record_id(self):
        from src.destination.connectors.api import _content_idempotency_key

        handler, state = self._handler("upsert")
        captured = []

        async def _capture(state_arg, data, extra_headers=None):
            captured.append(extra_headers)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        record_v1 = {"id": 1, "name": "a"}
        record_v2 = {"id": 1, "name": "b"}
        await handler._write_single_mode(
            state, [record_v1, record_v2], ["rid-1", "rid-1"]
        )
        keys = [h["Idempotency-Key"] for h in captured]
        assert keys[0] == _content_idempotency_key(record_v1)
        assert keys[1] == _content_idempotency_key(record_v2)
        # Same entity (same record_id), changed content: the keys differ,
        # so the provider applies the update rather than deduping it.
        assert keys[0] != keys[1]
        assert "rid-1" not in keys

    @pytest.mark.asyncio
    async def test_upsert_replay_of_identical_record_reuses_key(self):
        handler, state = self._handler("upsert")
        captured = []

        async def _capture(state_arg, data, extra_headers=None):
            captured.append(extra_headers)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        record = {"id": 1, "name": "a"}
        await handler._write_single_mode(
            state, [record, dict(record)], ["rid-1", "rid-1"]
        )
        keys = [h["Idempotency-Key"] for h in captured]
        assert keys[0] == keys[1]


@pytest.mark.unit
class TestApiHandlerIdempotencyReservedCollisions:
    """Configure-time rejections for keys that would collide with
    connection-owned headers or pass-through body fields."""

    def _doc(self, *, idempotency, input_schema=None):
        block: dict[str, Any] = {
            "request": {"method": "POST", "path": "/v1/insert"},
            "idempotency": idempotency,
        }
        if input_schema is not None:
            block["input"] = {"schema": input_schema}
        return {"operations": {"write": {"insert": block}}}

    def _handler(self, *, session_headers=frozenset()):
        handler = ApiDestinationHandler()
        handler._connected = True
        handler._session = MagicMock()
        handler._session_header_names = set(session_headers)
        return handler

    @pytest.mark.asyncio
    async def test_rejects_key_shadowing_connection_header(self):
        """An idempotency header named after a connection default header
        (e.g. Authorization) would send the record id as the credential."""
        handler = self._handler(session_headers={"authorization"})
        handler.set_stream_endpoints(
            {"s1": self._doc(idempotency={"in": "header", "name": "Authorization"})}
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False

    @pytest.mark.asyncio
    async def test_accepts_header_not_sent_by_connection(self):
        handler = self._handler(session_headers={"authorization"})
        handler.set_stream_endpoints(
            {"s1": self._doc(idempotency={"in": "header", "name": "Idempotency-Key"})}
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is True

    @pytest.mark.asyncio
    async def test_rejects_body_key_declared_in_input_schema(self):
        """No body template: the record is the body, shaped by the write
        input schema — a declared field with the reserved name would
        collide on every record after the ack promised exactly-once."""
        handler = self._handler()
        handler.set_stream_endpoints(
            {
                "s1": self._doc(
                    idempotency={"in": "body", "name": "idempotency_key"},
                    input_schema={
                        "properties": {
                            "id": {"arrow_type": "Int64"},
                            "idempotency_key": {"arrow_type": "Utf8"},
                        }
                    },
                )
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is False

    @pytest.mark.asyncio
    async def test_accepts_body_key_absent_from_input_schema(self):
        handler = self._handler()
        handler.set_stream_endpoints(
            {
                "s1": self._doc(
                    idempotency={"in": "body", "name": "idempotency_key"},
                    input_schema={"properties": {"id": {"arrow_type": "Int64"}}},
                )
            }
        )
        ok = await handler.configure_schema(
            SchemaMessage(
                stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT
            )
        )
        assert ok is True
