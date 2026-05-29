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
    async def test_write_batch_transport_error_is_retryable(
        self,
        api_handler: ApiDestinationHandler,
        mock_cursor: MagicMock,
        sample_records: List[Dict[str, Any]],
        sample_record_ids: List[str],
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
        sample_records: List[Dict[str, Any]],
        sample_record_ids: List[str],
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
        async def mock_send_request(state, data):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise aiohttp.ClientConnectionError("transient")
            return {"status": "ok"}

        api_handler._send_request = mock_send_request
        state = api_handler._streams["test-stream"]

        written = await api_handler._write_single_mode(state, records, record_ids)
        assert written == 2

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

        written = await api_handler._write_single_mode(state, records, record_ids)
        assert written == 0

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
        written = await api_handler._write_single_mode(state, records, record_ids)

        # Assert: all 3 succeeded
        assert written == 3


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
        api_handler._streams["test-stream"].batch_mode = ApiDestinationHandler.BATCH_MODE_BULK

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


@pytest.mark.unit
class TestApiHandlerSupportsUpsert:
    """``supports_upsert`` is contract-driven: it reflects whether a
    registered endpoint declares ``operations.write.upsert``, never a
    hardcoded value. This is what ``GetCapabilities`` advertises."""

    def _doc(self, *, modes):
        write: Dict[str, Any] = {}
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
            {"operations": {"write": {"upsert": {"request": "/v1/upsert"}}}},  # request not a mapping
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
            SchemaMessage(stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT)
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
            SchemaMessage(stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT)
        )

        # Wire batch — metadata is the JSON-encoded string produced by the
        # source/transform stage.
        batch = pa.RecordBatch.from_pylist(
            [{"id": "r1", "metadata": '{"k": "v", "n": 42}'}]
        )

        sent_payloads = []

        async def _capture(state, data):
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
            SchemaMessage(stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT)
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
            SchemaMessage(stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT)
        )

        batch_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("created", pa.timestamp("us", tz="UTC")),
            pa.field("amount", pa.decimal128(18, 4)),
        ])
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

        async def _capture(state, data):
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
            SchemaMessage(stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_INSERT)
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
