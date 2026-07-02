"""Unit tests for ApiDestinationHandler.

Covers write_batch failure handling (fatal vs retryable classification)
and the contract-driven dispatch in configure_schema (the API endpoint
document's ``operations.write.<mode>`` block selects the path / method
/ batching for the stream's write_mode).
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from cdk.types import RetrySemantics
from src.destination.connectors.api import (
    _API_WRITE_MODE_KEYS,
    ApiDestinationHandler,
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

        # Mock _write_single_mode to return (0, all-ids) — every record failed
        api_handler._write_single_mode = AsyncMock(
            return_value=(0, list(sample_record_ids))
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
        api_handler._write_single_mode = AsyncMock(return_value=(1, failed))

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
        api_handler._write_single_mode = AsyncMock(return_value=(3, []))

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
    async def test_write_single_mode_counts_transport_failures(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """Transport failures are per-record data issues; loop continues
        and the batch reports partial success."""
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

        written, failed = await api_handler._write_single_mode(
            state, records, record_ids
        )
        assert written == 2
        assert failed == ["rec-2"]

    @pytest.mark.asyncio
    async def test_write_single_mode_all_transport_failures(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """All-transport-failures yields 0 successes; deterministic
        exceptions are NOT caught here — they propagate to write_batch."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()

        records = [{"id": 1}, {"id": 2}]
        record_ids = ["rec-1", "rec-2"]

        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("Connection refused"),
        )
        state = api_handler._streams["test-stream"]

        written, failed = await api_handler._write_single_mode(
            state, records, record_ids
        )
        assert written == 0
        assert failed == ["rec-1", "rec-2"]

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
        written, failed = await api_handler._write_single_mode(
            state, records, record_ids
        )

        # Assert: all 3 succeeded, none failed
        assert written == 3
        assert failed == []


@pytest.mark.unit
class TestApiHandlerBatchModes:
    """Test different batch modes handle failures correctly."""

    @pytest.mark.asyncio
    async def test_bulk_mode_transport_failure_is_retryable(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """Bulk mode transport-level failure → RETRYABLE."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._streams[
            "test-stream"
        ].batch_mode = ApiDestinationHandler.BATCH_MODE_BULK

        records = [{"id": i} for i in range(5)]
        record_ids = [f"rec-{i}" for i in range(5)]

        api_handler._write_bulk_mode = AsyncMock(
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
    async def test_batch_mode_partial_chunk_failure(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """Test batch mode with chunked requests where some chunks fail."""
        # Setup
        api_handler._connected = True
        api_handler._session = MagicMock()
        api_handler._streams[
            "test-stream"
        ].batch_mode = ApiDestinationHandler.BATCH_MODE_BATCH
        api_handler._streams["test-stream"].batch_size = 2

        records = [{"id": i} for i in range(5)]
        record_ids = [f"rec-{i}" for i in range(5)]

        # Mock: 3 of 5 written; the unsent tail comes back as failed ids
        api_handler._write_batch_mode = AsyncMock(return_value=(3, ["rec-3", "rec-4"]))

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
    async def test_real_write_batch_mode_chunk_failure_reports_unsent_ids(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """Drive the REAL _write_batch_mode (not a mock): a mid-loop chunk
        transport failure must report the records actually sent and attribute
        every record from the failed chunk onward as failed. This is the
        load-bearing dup-on-retry fix.
        """
        import aiohttp

        state = api_handler._streams["test-stream"]
        state.batch_mode = ApiDestinationHandler.BATCH_MODE_BATCH
        state.batch_size = 2

        calls = []

        async def _send(state_arg, data, extra_headers=None):
            calls.append(data)
            if len(calls) == 2:  # second chunk (rows 2,3) fails
                raise aiohttp.ClientConnectionError("chunk down")
            return {}

        api_handler._send_request = _send
        records = [{"id": i} for i in range(5)]
        record_ids = [f"r{i}" for i in range(5)]

        written, failed = await api_handler._write_batch_mode(
            state, records, record_ids
        )
        # First chunk (r0, r1) landed; everything from the failed chunk on fails.
        assert written == 2
        assert failed == ["r2", "r3", "r4"]
        # The third chunk must never be attempted after a chunk fails.
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_write_batch_chunk_failure_is_fatal_no_cursor_end_to_end(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """End-to-end through write_batch: a real batch-mode chunk failure must
        be FATAL (not RETRYABLE), carry the true written count and the unsent
        ids, and drop the cursor so the checkpoint cannot advance past records
        that were never written."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        state = api_handler._streams["test-stream"]
        state.batch_mode = ApiDestinationHandler.BATCH_MODE_BATCH
        state.batch_size = 2

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
    async def test_batch_mode_first_chunk_failure_reraises(
        self,
        api_handler: ApiDestinationHandler,
    ):
        """A transport failure on the FIRST chunk (nothing landed yet) must
        re-raise so it surfaces as RETRYABLE — there is no landed chunk to
        duplicate, so it must not be fatally DLQ'd."""
        import aiohttp

        state = api_handler._streams["test-stream"]
        state.batch_mode = ApiDestinationHandler.BATCH_MODE_BATCH
        state.batch_size = 2

        api_handler._send_request = AsyncMock(
            side_effect=aiohttp.ClientConnectionError("first chunk down")
        )
        records = [{"id": i} for i in range(5)]
        record_ids = [f"r{i}" for i in range(5)]

        with pytest.raises(aiohttp.ClientConnectionError):
            await api_handler._write_batch_mode(state, records, record_ids)

    @pytest.mark.asyncio
    async def test_write_batch_first_chunk_failure_is_retryable_end_to_end(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
    ):
        """End-to-end: a first-chunk transport failure is RETRYABLE (no dup
        risk), matching bulk mode, instead of FATAL."""
        import aiohttp

        api_handler._connected = True
        api_handler._session = MagicMock()
        state = api_handler._streams["test-stream"]
        state.batch_mode = ApiDestinationHandler.BATCH_MODE_BATCH
        state.batch_size = 2

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
                "batching": {
                    "mode": "batch" if mode == "upsert" else "single",
                    "size": 50,
                },
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

    def test_from_input_record_in_bulk_mode_raises(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"data": {"from_input": "record"}},
        )
        with pytest.raises(ValueError, match="requires batching mode single"):
            handler._build_body(state, records=[{"id": 1}])

    def test_from_input_records_in_single_mode_raises(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            body_spec={"items": {"from_input": "records"}},
        )
        with pytest.raises(ValueError, match="bulk or batch"):
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
        written, failed = await handler._write_single_mode(
            state, [{"id": 1}, {"id": 2}], ["r1", "r2"]
        )
        assert written == 2
        assert failed == []
        assert sent == [
            {"data": {"id": 1}, "source": "crm"},
            {"data": {"id": 2}, "source": "crm"},
        ]

    @pytest.mark.asyncio
    async def test_bulk_mode_sends_built_body_once(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            batch_mode=ApiDestinationHandler.BATCH_MODE_BULK,
            body_spec={"items": {"from_input": "records"}},
        )

        sent = []

        async def _capture(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        written, failed = await handler._write_bulk_mode(state, [{"id": 1}, {"id": 2}])
        assert written == 2
        assert failed == []
        assert sent == [{"items": [{"id": 1}, {"id": 2}]}]

    @pytest.mark.asyncio
    async def test_batch_mode_sends_built_body_per_chunk(self):
        handler = self._handler_with_resolver()
        state = _StreamState(
            endpoint="/v1/things",
            batch_mode=ApiDestinationHandler.BATCH_MODE_BATCH,
            batch_size=2,
            body_spec={"items": {"from_input": "records"}},
        )

        sent = []

        async def _capture(state_arg, data, extra_headers=None):
            sent.append(data)
            return {}

        handler._send_request = _capture  # type: ignore[assignment]
        written, failed = await handler._write_batch_mode(
            state, [{"id": 1}, {"id": 2}, {"id": 3}], ["r1", "r2", "r3"]
        )
        assert written == 3
        assert failed == []
        assert sent == [
            {"items": [{"id": 1}, {"id": 2}]},
            {"items": [{"id": 3}]},
        ]

    @pytest.mark.asyncio
    async def test_configure_schema_rejects_records_selector_in_single_mode(self):
        # The mismatch is statically knowable: reject the stream up front
        # instead of failing every record at write time.
        handler = self._handler_with_resolver()
        handler.set_stream_endpoints(
            {
                "s1": self._doc_with_body(
                    {"items": {"from_input": "records"}},
                    batching={"mode": "single"},
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
    async def test_configure_schema_rejects_record_selector_in_bulk_mode(self):
        handler = self._handler_with_resolver()
        handler.set_stream_endpoints(
            {
                "s1": self._doc_with_body(
                    {"data": {"from_input": "record"}},
                    batching={"mode": "bulk"},
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
        batching_mode="single",
        body=None,
    ):
        block: dict[str, Any] = {
            "request": {"method": "POST", "path": f"/v1/{mode}"},
            "batching": {"mode": batching_mode, "size": 10},
        }
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
    @pytest.mark.parametrize("batching_mode", ["bulk", "batch"])
    async def test_rejects_non_single_batching(self, handler, batching_mode):
        """A restart re-batches records, so a per-request key over several
        records cannot dedup — reject at configure time (issue #286)."""
        handler.set_stream_endpoints(
            {
                "s1": self._doc(
                    idempotency={"in": "header", "name": "Idempotency-Key"},
                    batching_mode=batching_mode,
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
            batch_mode=ApiDestinationHandler.BATCH_MODE_SINGLE,
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
        written, failed = await handler._write_single_mode(
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
        written, failed = await handler._write_single_mode(state, [{"id": 1}], ["r1"])
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
        written, failed = await handler._write_single_mode(
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


@pytest.mark.unit
class TestApiHandlerRetrySemantics:
    """Per-stream retry-safety verdict (issue #286)."""

    def _doc(self, *, mode="insert", idempotency=None):
        block: dict[str, Any] = {
            "request": {"method": "POST", "path": f"/v1/{mode}"},
            "batching": {"mode": "single"},
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
