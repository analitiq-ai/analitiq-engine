"""Unit tests for gRPC client."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import grpc
from src.grpc.client import BatchResult, DestinationGRPCClient, generate_record_id
from src.grpc.generated.analitiq.v1 import AckStatus

# A fixed, timezone-aware emit instant for write_batch/send_batch calls; the
# engine stamps this per batch (issue #353). Value is arbitrary for sinks
# that ignore it.
_EMITTED_AT = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)


class TestGenerateRecordId:
    """Record identity is content-derived and position-independent (#282).

    The id is a function of the record content alone -- never the run, the
    batch sequence, or the row's offset -- so the same logical row hashes to
    the same id across attempts and across an inclusive cursor re-read, which
    is what lets the destination enforce idempotency on row identity instead
    of a positional ledger.
    """

    def test_same_record_same_id_regardless_of_position(self):
        """The same record content always yields the same id; no run/batch/
        index dimension can move it (positional independence is the point)."""
        record = {"id": 1, "name": "test"}
        assert generate_record_id(record) == generate_record_id(dict(record))

    def test_different_content_different_id(self):
        """Distinct record content hashes to distinct ids."""
        assert generate_record_id({"id": 1, "name": "a"}) != generate_record_id(
            {"id": 2, "name": "b"}
        )

    def test_primary_key_fields_derive_id_from_keys_only(self):
        """With primary_key_fields the id derives only from those fields, so two
        rows sharing the key values but differing elsewhere hash equal."""
        a = {"id": 7, "name": "first", "value": 1}
        b = {"id": 7, "name": "second", "value": 99}
        assert generate_record_id(a, primary_key_fields=["id"]) == generate_record_id(
            b, primary_key_fields=["id"]
        )
        # Different key values still diverge.
        c = {"id": 8, "name": "first", "value": 1}
        assert generate_record_id(a, primary_key_fields=["id"]) != generate_record_id(
            c, primary_key_fields=["id"]
        )

    def test_missing_primary_key_fields_fall_back_to_full_record(self):
        """When a mapping renames/drops the configured key it is absent from the
        (transformed) record; the id falls back to the whole record so distinct
        rows keep distinct ids instead of all hashing the same missing-key value
        (issue #282 -- otherwise the keyless ``_record_hash`` dedup would
        silently drop every row but the first)."""
        # Configured PK "id" is absent (a mapping renamed it to "user_id").
        a = {"user_id": 7, "name": "first"}
        b = {"user_id": 8, "name": "second"}
        assert generate_record_id(a, primary_key_fields=["id"]) != generate_record_id(
            b, primary_key_fields=["id"]
        )
        # The fallback is exactly the keyless (no-key) hash of the same record.
        assert generate_record_id(a, primary_key_fields=["id"]) == generate_record_id(a)

    def test_two_identical_keyless_records_share_id(self):
        """Byte-identical keyless records produce the same id -- the synthetic
        ``_record_hash`` dedup key for a keyless insert stream."""
        assert generate_record_id({"id": 1, "name": "test"}) == generate_record_id(
            {"id": 1, "name": "test"}
        )

    def test_returned_id_is_full_sha256_hex(self):
        """The id is the full (untruncated) 64-char SHA-256 hex digest."""
        rid = generate_record_id({"id": 1, "name": "test"})
        assert len(rid) == 64
        assert all(c in "0123456789abcdef" for c in rid)


class TestBatchResult:
    """Tests for BatchResult dataclass."""

    def test_batch_result_success(self):
        """Test creating a successful batch result."""
        result = BatchResult(
            success=True,
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=100,
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="",
        )

        assert result.success is True
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 100

    def test_batch_result_failure(self):
        """Test creating a failed batch result."""
        result = BatchResult(
            success=False,
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            committed_cursor=None,
            failed_record_ids=["rid1", "rid2"],
            failure_summary="Constraint violation",
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert len(result.failed_record_ids) == 2
        assert "Constraint" in result.failure_summary


class TestProcessAckFailureCategory:
    """_process_ack is where the failure category comes off the wire (#351).

    On the worker-proxy hop the sender is the untrusted connector process, so
    the value must be bounds-checked: an unrecognised integer degrades to
    UNSPECIFIED (text-matching fallback) instead of aborting the stream, and a
    category paired with a success status is zeroed so the field is only ever
    meaningful on a failure result.
    """

    def test_declared_category_passes_through(self):
        from cdk.types import FailureCategory
        from src.grpc.generated.analitiq.v1 import BatchAck
        from src.grpc.generated.analitiq.v1 import (
            FailureCategory as WireFailureCategory,
        )

        ack = BatchAck(
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            failure_summary="type-map: no rule",
            failure_category=WireFailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
        )
        result = DestinationGRPCClient()._process_ack(ack)
        assert result.failure_category is FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT

    def test_unknown_category_degrades_to_unspecified(self):
        from cdk.types import FailureCategory
        from src.grpc.generated.analitiq.v1 import BatchAck

        # A worker sending an integer this engine build does not know:
        # proto3 enums are open, so 99 survives assignment and the wire.
        ack = BatchAck(status=AckStatus.ACK_STATUS_FATAL_FAILURE, failure_category=99)
        assert ack.failure_category == 99
        result = DestinationGRPCClient()._process_ack(ack)
        assert result.failure_category is FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

    def test_category_on_success_ack_is_zeroed(self):
        from cdk.types import FailureCategory
        from src.grpc.generated.analitiq.v1 import BatchAck
        from src.grpc.generated.analitiq.v1 import (
            FailureCategory as WireFailureCategory,
        )

        ack = BatchAck(
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=1,
            failure_category=WireFailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
        )
        result = DestinationGRPCClient()._process_ack(ack)
        assert result.success is True
        assert result.failure_category is FailureCategory.FAILURE_CATEGORY_UNSPECIFIED


class TestDestinationGRPCClient:
    """Tests for DestinationGRPCClient."""

    def test_client_initialization(self):
        """Test client initialization with default values."""
        client = DestinationGRPCClient()

        assert client.address == "localhost:50051"
        assert client.timeout == 30
        assert client.max_retries == 3

    def test_client_initialization_custom(self):
        """Test client initialization with custom values."""
        client = DestinationGRPCClient(
            host="destination",
            port=9999,
            timeout_seconds=60,
            max_retries=5,
        )

        assert client.address == "destination:9999"
        assert client.timeout == 60
        assert client.max_retries == 5

    def test_blank_host_falls_back_to_localhost(self):
        """A blank host (env baked as ``DESTINATION_GRPC_HOST=""``) must
        coalesce to localhost, never a hostless ``:50051`` address."""
        client = DestinationGRPCClient(host="", port=50051)
        assert client.address == "localhost:50051"

    def test_client_not_connected_initially(self):
        """Test that client is not connected initially."""
        client = DestinationGRPCClient()
        assert client._connected is False
        assert client._stream_active is False

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(self):
        """Test disconnect when not connected is safe."""
        client = DestinationGRPCClient()
        # Should not raise
        await client.disconnect()
        assert client._connected is False


class TestGetCapabilities:
    """get_capabilities raises on failure rather than collapsing to None, so a
    transport failure is never mistaken for a populated empty response."""

    @staticmethod
    def _rpc_error() -> grpc.aio.AioRpcError:
        return grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="destination unreachable",
        )

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(self):
        client = DestinationGRPCClient()  # _stub is None
        with pytest.raises(ConnectionError, match="not connected"):
            await client.get_capabilities()

    @pytest.mark.asyncio
    async def test_raises_on_rpc_error(self):
        client = DestinationGRPCClient()
        client._stub = MagicMock()
        client._stub.GetCapabilities = AsyncMock(side_effect=self._rpc_error())
        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_capabilities()

    @pytest.mark.asyncio
    async def test_returns_response_on_success(self):
        from src.grpc.generated.analitiq.v1 import GetCapabilitiesResponse

        client = DestinationGRPCClient()
        client._stub = MagicMock()
        resp = GetCapabilitiesResponse(connector_type="database")
        client._stub.GetCapabilities = AsyncMock(return_value=resp)
        assert await client.get_capabilities() is resp


class TestConnectRetryLogLevels:
    """Attempt 1 logs at DEBUG (expected during concurrent startup);
    attempts 2+ log at WARNING.
    """

    @staticmethod
    def _unavailable_error() -> grpc.aio.AioRpcError:
        return grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="destination not ready",
        )

    @pytest.mark.asyncio
    async def test_first_attempt_failure_logs_at_debug(self):
        """First attempt failure logs at DEBUG, never WARNING."""
        client = DestinationGRPCClient()
        mock_stub = MagicMock()
        mock_stub.HealthCheck = AsyncMock(side_effect=self._unavailable_error())

        with patch("src.grpc.client.grpc_aio.insecure_channel"), patch(
            "src.grpc.client.DestinationServiceStub", return_value=mock_stub
        ), patch("src.grpc.client.logger") as mock_logger:
            result = await client.connect(
                max_connect_retries=1,
                retry_delay_seconds=0.0,
            )

        assert result is False
        debug_msgs = [c.args[0] for c in mock_logger.debug.call_args_list]
        warning_msgs = [c.args[0] for c in mock_logger.warning.call_args_list]
        assert any("Connection attempt 1/1 failed" in m for m in debug_msgs)
        assert not any("Connection attempt" in m for m in warning_msgs)

    @pytest.mark.asyncio
    async def test_subsequent_attempt_failures_log_at_warning(self):
        """Failures on attempts 2+ escalate to WARNING; the terminal ERROR
        carries the gRPC status code; Retrying tracks the failure level.
        """
        client = DestinationGRPCClient()
        mock_stub = MagicMock()
        mock_stub.HealthCheck = AsyncMock(side_effect=self._unavailable_error())

        with patch("src.grpc.client.grpc_aio.insecure_channel"), patch(
            "src.grpc.client.DestinationServiceStub", return_value=mock_stub
        ), patch("src.grpc.client.logger") as mock_logger:
            result = await client.connect(
                max_connect_retries=3,
                retry_delay_seconds=0.0,
            )

        assert result is False
        debug_msgs = [c.args[0] for c in mock_logger.debug.call_args_list]
        info_msgs = [c.args[0] for c in mock_logger.info.call_args_list]
        warning_msgs = [c.args[0] for c in mock_logger.warning.call_args_list]
        error_msgs = [c.args[0] for c in mock_logger.error.call_args_list]

        assert any("Connection attempt 1/3 failed" in m for m in debug_msgs)
        assert any("Connection attempt 2/3 failed" in m for m in warning_msgs)
        assert any("Connection attempt 3/3 failed" in m for m in warning_msgs)
        # Retrying after a DEBUG failure stays at DEBUG; after a WARNING
        # failure it promotes to INFO so backoff cadence remains visible.
        assert sum("Retrying" in m for m in debug_msgs) == 1
        assert sum("Retrying" in m for m in info_msgs) == 1
        # Final ERROR surfaces the failure mode for single-attempt callers
        # (send_shutdown / main.py) where attempt 1 was DEBUG.
        assert any("Failed to connect" in m and "UNAVAILABLE" in m for m in error_msgs)

    @pytest.mark.asyncio
    async def test_first_attempt_fails_then_succeeds(self):
        """Production scenario: attempt 1 fails (DEBUG), attempt 2 connects
        cleanly with INFO and no WARNING."""
        from src.grpc.generated.analitiq.v1 import HealthCheckResponse

        client = DestinationGRPCClient()
        serving = HealthCheckResponse(
            status=HealthCheckResponse.ServingStatus.SERVING,
            message="ok",
        )
        mock_stub = MagicMock()
        mock_stub.HealthCheck = AsyncMock(
            side_effect=[self._unavailable_error(), serving]
        )

        with patch("src.grpc.client.grpc_aio.insecure_channel"), patch(
            "src.grpc.client.DestinationServiceStub", return_value=mock_stub
        ), patch("src.grpc.client.logger") as mock_logger:
            result = await client.connect(
                max_connect_retries=3,
                retry_delay_seconds=0.0,
            )

        assert result is True
        assert client._connected is True
        debug_msgs = [c.args[0] for c in mock_logger.debug.call_args_list]
        info_msgs = [c.args[0] for c in mock_logger.info.call_args_list]
        warning_msgs = [c.args[0] for c in mock_logger.warning.call_args_list]
        assert any("Connection attempt 1/3 failed" in m for m in debug_msgs)
        assert any("Connected to destination" in m for m in info_msgs)
        assert not any("Connection attempt" in m for m in warning_msgs)

    @pytest.mark.asyncio
    async def test_first_attempt_not_serving_logs_at_debug(self):
        """A 'not serving' health response on attempt 1 also logs at DEBUG."""
        from src.grpc.generated.analitiq.v1 import HealthCheckResponse

        client = DestinationGRPCClient()
        not_serving = HealthCheckResponse(
            status=HealthCheckResponse.ServingStatus.NOT_SERVING,
            message="warming up",
        )
        mock_stub = MagicMock()
        mock_stub.HealthCheck = AsyncMock(return_value=not_serving)

        with patch("src.grpc.client.grpc_aio.insecure_channel"), patch(
            "src.grpc.client.DestinationServiceStub", return_value=mock_stub
        ), patch("src.grpc.client.logger") as mock_logger:
            result = await client.connect(
                max_connect_retries=1,
                retry_delay_seconds=0.0,
            )

        assert result is False
        debug_msgs = [c.args[0] for c in mock_logger.debug.call_args_list]
        warning_msgs = [c.args[0] for c in mock_logger.warning.call_args_list]
        assert any("Destination not serving (attempt 1/1)" in m for m in debug_msgs)
        assert not any("Destination not serving" in m for m in warning_msgs)


class TestClientPayloadEncoding:
    """Tests for client payload encoding."""

    def test_encode_arrow_ipc_roundtrip(self):
        """Arrow IPC encode → decode preserves typed columnar data.

        Arrow IPC is the only supported wire format; the encoded bytes
        carry both the schema and the record batch so the destination
        decodes them together.
        """
        import io

        import pyarrow as pa

        batch = pa.RecordBatch.from_pylist(
            [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]
        )
        payload = DestinationGRPCClient._encode_arrow_ipc(batch)

        with pa.ipc.open_stream(io.BytesIO(payload)) as reader:
            decoded = reader.read_all()

        assert decoded.num_rows == 2
        assert decoded.to_pylist() == [
            {"id": 1, "name": "first"},
            {"id": 2, "name": "second"},
        ]
        assert decoded.schema == batch.schema


class TestClientSchemaBuilder:
    """Tests for schema message building."""

    def test_build_schema_message_carries_identification_and_mode(self):
        """The slim SchemaMessage carries stream_id, version, write_mode, and
        the ack budget — every other field comes from the preloaded contract
        endpoint document on the destination side."""
        from src.grpc.generated.analitiq.v1 import WriteMode

        client = DestinationGRPCClient()
        schema_msg = client._build_schema_message(
            "stream-1", {"write_mode": "upsert", "schema_version": 7}
        )
        assert schema_msg.stream_id == "stream-1"
        assert schema_msg.version == 7
        assert schema_msg.write_mode == WriteMode.WRITE_MODE_UPSERT

    def test_build_schema_message_rejects_unknown_mode(self):
        """Unknown write_mode strings must surface instead of silently
        defaulting to UPSERT (which would mask config typos)."""
        client = DestinationGRPCClient()
        with pytest.raises(ValueError, match="Unknown write_mode"):
            client._build_schema_message("s", {"write_mode": "upsert_typo"})

    def test_schema_message_stamps_clients_own_ack_budget(self):
        """The stamped ack budget is the wait this client actually applies
        (self.timeout), so the destination's statement timeout can never
        drift from it (issue #234)."""
        client = DestinationGRPCClient(timeout_seconds=42)
        schema_msg = client._build_schema_message(
            "s", {"write_mode": "upsert", "schema_version": 1}
        )
        assert schema_msg.ack_timeout_seconds == 42

    def test_schema_message_forwards_smaller_upstream_budget(self):
        """A forwarding hop (the worker proxy) passes the engine's budget in
        schema_config; when it is tighter than this client's own wait, the
        engine's value wins — the worker must stay below every waiter on
        the path."""
        client = DestinationGRPCClient(timeout_seconds=300)
        schema_msg = client._build_schema_message(
            "s",
            {"write_mode": "upsert", "schema_version": 1, "ack_timeout_seconds": 30},
        )
        assert schema_msg.ack_timeout_seconds == 30

    def test_schema_message_caps_upstream_budget_at_own_wait(self):
        """An upstream budget larger than this client's own wait must not
        widen the stamp: this hop gives up sooner, so the statement bound
        derives from the tighter wait."""
        client = DestinationGRPCClient(timeout_seconds=30)
        schema_msg = client._build_schema_message(
            "s",
            {"write_mode": "upsert", "schema_version": 1, "ack_timeout_seconds": 300},
        )
        assert schema_msg.ack_timeout_seconds == 30


class TestStreamTaskFailurePropagation:
    """Cover the _STREAM_TASK_FAILED sentinel paths in send_batch.

    The reader/writer asyncio tasks push the sentinel onto the response
    queue when they exit (exception or clean EOF). Without this signal,
    send_batch would block on response_queue.get until self.timeout —
    300s by default. These tests pin the fast-fail behavior and the
    distinct diagnostics for "task failed" vs "peer closed cleanly".
    """

    @pytest.mark.asyncio
    async def test_writer_exception_surfaces_as_fatal(self):
        import asyncio

        import pyarrow as pa

        from src.grpc.client import _STREAM_TASK_FAILED
        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client._stream_active = True
        client._request_queue = asyncio.Queue()
        client._response_queue = asyncio.Queue()
        client._task_failure = RuntimeError("writer blew up")
        client._response_queue.put_nowait(_STREAM_TASK_FAILED)

        result = await client.send_batch(
            run_id="r",
            stream_id="s",
            batch_seq=7,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "RuntimeError" in result.failure_summary
        assert "writer blew up" in result.failure_summary
        # The sentinel means the tasks are gone; the stream must be marked
        # inactive so the worker-proxy retry self-heals instead of writing
        # into a dead stream.
        assert client._stream_active is False

    @pytest.mark.asyncio
    async def test_clean_peer_close_surfaces_distinct_diagnostic(self):
        """Reader hitting clean EOF (no exception) must produce an
        actionable message — not 'NoneType: None' — so operators can
        distinguish premature peer close from in-task errors."""
        import asyncio

        import pyarrow as pa

        from src.grpc.client import _STREAM_TASK_FAILED
        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client._stream_active = True
        client._request_queue = asyncio.Queue()
        client._response_queue = asyncio.Queue()
        client._task_failure = None
        client._peer_closed_stream = True
        client._response_queue.put_nowait(_STREAM_TASK_FAILED)

        result = await client.send_batch(
            run_id="r",
            stream_id="s",
            batch_seq=42,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "Destination closed stream" in result.failure_summary
        assert "42" in result.failure_summary
        assert "NoneType" not in result.failure_summary
        assert client._stream_active is False

    @pytest.mark.asyncio
    async def test_read_responses_signals_sentinel_on_exception(self):
        """The reader task must push _STREAM_TASK_FAILED before exiting on
        exception so consumers don't block until the gRPC timeout."""
        import asyncio

        from src.grpc.client import _STREAM_TASK_FAILED

        client = DestinationGRPCClient()
        client._response_queue = asyncio.Queue()

        async def _exploding_stream():
            raise ConnectionError("peer rst")
            yield  # pragma: no cover — make this an async generator

        client._stream = _exploding_stream()

        with pytest.raises(ConnectionError):
            await client._read_responses()

        # Sentinel must be queued and _task_failure must carry the cause.
        assert client._response_queue.qsize() == 1
        assert client._response_queue.get_nowait() is _STREAM_TASK_FAILED
        assert isinstance(client._task_failure, ConnectionError)
        assert client._peer_closed_stream is False

    @pytest.mark.asyncio
    async def test_read_responses_signals_sentinel_on_clean_eof(self):
        """When the server closes the stream gracefully, the reader still
        pushes the sentinel — but with _peer_closed_stream=True and
        _task_failure=None, so send_batch produces a distinct diagnostic."""
        import asyncio

        from src.grpc.client import _STREAM_TASK_FAILED

        client = DestinationGRPCClient()
        client._response_queue = asyncio.Queue()

        async def _empty_stream():
            return
            yield  # pragma: no cover

        client._stream = _empty_stream()

        await client._read_responses()

        assert client._response_queue.qsize() == 1
        assert client._response_queue.get_nowait() is _STREAM_TASK_FAILED
        assert client._task_failure is None
        assert client._peer_closed_stream is True


class TestAckTimeoutAndTeardown:
    """Cover the ACK-timeout path: stream teardown, real-cause surfacing,
    heartbeat logging, and the no-keepalive channel-options invariant.

    Acceptance: a destination that goes silent after accepting the schema
    causes send_batch to fail loudly within self.timeout seconds, tear down
    the stream, and surface a real RPC error when the reader task set one.
    """

    @pytest.mark.asyncio
    async def test_ack_timeout_tears_down_stream(self):
        """A silent destination triggers timeout and leaves the stream inactive.

        After the call returns, _stream_active must be False and the reader/writer
        tasks must be cancelled — so a retry gets a clean channel rather than
        pushing onto a zombie stream.
        """
        import asyncio

        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client.timeout = 0.05  # fast for test
        client._stream_active = True
        client._request_queue = asyncio.Queue()
        client._response_queue = asyncio.Queue()

        async def _noop():
            await asyncio.sleep(60)

        reader = asyncio.create_task(_noop())
        writer = asyncio.create_task(_noop())
        client._reader_task = reader
        client._writer_task = writer

        result = await client.send_batch(
            run_id="r",
            stream_id="s",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["1"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        # An ACK timeout rendered no verdict — retryable, not fatal. The
        # teardown still fires; the next send_batch self-heals via cached params.
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.transport_failure is True
        assert "Timeout" in result.failure_summary
        assert client._stream_active is False
        assert client._reader_task is None
        assert client._writer_task is None
        assert reader.cancelled()

    @pytest.mark.asyncio
    async def test_ack_timeout_surfaces_task_failure_when_set(self):
        """When _task_failure is populated at timeout-handling time the real
        error (not the generic 'Timeout waiting for ACK') must appear in the
        failure_summary — this is how the 'Too many pings' RPC error surfaces.
        """
        import asyncio

        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client.timeout = 0.05
        client._stream_active = True
        client._request_queue = asyncio.Queue()
        client._response_queue = asyncio.Queue()
        client._task_failure = grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="Too many pings",
        )

        async def _already_done():
            pass

        reader = asyncio.create_task(_already_done())
        writer = asyncio.create_task(_already_done())
        await asyncio.sleep(0)  # let tasks finish
        client._reader_task = reader
        client._writer_task = writer

        result = await client.send_batch(
            run_id="r",
            stream_id="s",
            batch_seq=2,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["2"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.transport_failure is True
        assert "Too many pings" in result.failure_summary
        assert client._stream_active is False

    @pytest.mark.asyncio
    async def test_wait_with_heartbeat_returns_queued_item(self):
        """_wait_with_heartbeat returns the item immediately when the queue is ready."""
        import asyncio

        client = DestinationGRPCClient()
        client.timeout = 5.0
        client._reader_task = None
        client._response_queue = asyncio.Queue()

        sentinel = object()
        client._response_queue.put_nowait(sentinel)

        result = await client._wait_with_heartbeat(batch_seq=1)
        assert result is sentinel

    @pytest.mark.asyncio
    async def test_wait_with_heartbeat_raises_timeout_on_empty_queue(self):
        """_wait_with_heartbeat raises asyncio.TimeoutError when timeout expires."""
        import asyncio

        client = DestinationGRPCClient()
        client.timeout = 0.05
        client._reader_task = None
        client._response_queue = asyncio.Queue()

        with pytest.raises(asyncio.TimeoutError):
            await client._wait_with_heartbeat(batch_seq=3)

    @pytest.mark.asyncio
    async def test_wait_with_heartbeat_logs_info_while_waiting(self):
        """_wait_with_heartbeat logs an INFO line each time the heartbeat interval
        expires without a response — batch_seq must be present in the log args."""
        import asyncio

        client = DestinationGRPCClient()
        # timeout must exceed one heartbeat interval (10s) so the log fires
        # before the overall deadline.
        client.timeout = 25.0
        client._reader_task = None
        client._response_queue = asyncio.Queue()

        sentinel = object()
        call_count = 0
        original_wait = asyncio.wait

        async def patched_wait(fs, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call: simulate interval expiry with no response
                return set(), set(fs)
            # Second call: deliver the item and let the real wait complete
            client._response_queue.put_nowait(sentinel)
            return await original_wait(fs, timeout=1.0)

        with patch("src.grpc.client.asyncio.wait", patched_wait), patch(
            "src.grpc.client.logger"
        ) as mock_logger:
            result = await client._wait_with_heartbeat(batch_seq=7)

        assert result is sentinel
        # logger.info("Still waiting for ACK batch=%d ...", batch_seq, ...)
        # The format string and numeric args are separate positional args.
        heartbeat_calls = [
            c
            for c in mock_logger.info.call_args_list
            if c.args and "Still waiting" in c.args[0]
        ]
        assert heartbeat_calls, "Expected at least one heartbeat INFO log"
        assert any(c.args[1] == 7 for c in heartbeat_calls)

    @pytest.mark.asyncio
    async def test_connect_channel_has_no_keepalive_options(self):
        """The gRPC channel must not include any keepalive options.

        PR #85 removed client-side keepalives that tripped the destination's
        HTTP/2 ping-flood policy. This test pins the absence so they cannot be
        quietly re-added.
        """
        from src.grpc.generated.analitiq.v1 import HealthCheckResponse

        client = DestinationGRPCClient()
        captured_options = []

        def fake_channel(address, options=None):
            captured_options.extend(options or [])
            return MagicMock()

        serving = HealthCheckResponse(
            status=HealthCheckResponse.ServingStatus.SERVING, message="ok"
        )
        mock_stub = MagicMock()
        mock_stub.HealthCheck = AsyncMock(return_value=serving)

        with patch(
            "src.grpc.client.grpc_aio.insecure_channel", side_effect=fake_channel
        ), patch("src.grpc.client.DestinationServiceStub", return_value=mock_stub):
            await client.connect(max_connect_retries=1)

        keepalive_keys = {k for k, _ in captured_options if "keepalive" in k.lower()}
        assert (
            not keepalive_keys
        ), f"Channel must not use keepalive options; found: {keepalive_keys}"

    @pytest.mark.asyncio
    async def test_ack_timeout_surfaces_task_failure_set_during_grace(self):
        """When the reader task is still live at timeout time and sets
        _task_failure during the 2-second grace window, that real error must
        appear in failure_summary — not the generic 'Timeout waiting for ACK'.

        This is the core race the grace window exists to handle: the real RPC
        cause (e.g. 'Too many pings') finishes just after the ACK timeout fires.
        """
        import asyncio

        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client.timeout = 0.05
        client._stream_active = True
        client._request_queue = asyncio.Queue()
        client._response_queue = asyncio.Queue()

        rpc_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="Too many pings — live race",
        )

        async def _reader_sets_failure():
            # Sleeps slightly longer than the ACK timeout so _reader_task is
            # still alive when the TimeoutError handler checks .done(), then
            # completes within the 2-second grace window.
            await asyncio.sleep(0.08)
            client._task_failure = rpc_error

        reader = asyncio.create_task(_reader_sets_failure())
        client._reader_task = reader
        client._writer_task = asyncio.create_task(asyncio.sleep(60))

        result = await client.send_batch(
            run_id="r",
            stream_id="s",
            batch_seq=9,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["9"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.transport_failure is True
        assert "Too many pings — live race" in result.failure_summary
        assert client._stream_active is False

    @pytest.mark.asyncio
    async def test_ack_timeout_grace_window_swallows_reader_exception(self):
        """A reader task that *raises* during the grace window must not
        propagate its exception out of send_batch.

        The grace block awaits asyncio.shield(reader); a raising reader
        re-raises through the shield. send_batch must catch it so the
        recorded _task_failure path runs and teardown still fires — never
        leak the raw error past the BatchResult contract.
        """
        import asyncio

        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client.timeout = 0.05
        client._stream_active = True
        client._request_queue = asyncio.Queue()
        client._response_queue = asyncio.Queue()

        rpc_error = grpc.aio.AioRpcError(
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details="reader raised in grace window",
        )

        async def _reader_raises():
            # Still alive when the timeout handler checks .done(); records its
            # failure like _read_responses would, then raises within the grace
            # window so shield(reader) re-raises into send_batch.
            await asyncio.sleep(0.08)
            client._task_failure = rpc_error
            raise rpc_error

        reader = asyncio.create_task(_reader_raises())
        client._reader_task = reader
        client._writer_task = asyncio.create_task(asyncio.sleep(60))

        # Must return a BatchResult, not raise rpc_error.
        result = await client.send_batch(
            run_id="r",
            stream_id="s",
            batch_seq=11,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["11"],
            cursor=Cursor(token=b""),
            emitted_at=_EMITTED_AT,
        )

        assert result.success is False
        assert result.transport_failure is True
        assert "reader raised in grace window" in result.failure_summary
        assert client._stream_active is False
        # Grace-window reader exception is consumed; no task left dangling.
        assert client._reader_task is None

    @pytest.mark.asyncio
    async def test_teardown_stream_resets_all_state(self):
        """_teardown_stream cancels live tasks and clears all stream-lifetime
        state, including _task_failure and _peer_closed_stream.

        Calling it with None tasks (no stream was started) must also be safe.
        """
        import asyncio

        client = DestinationGRPCClient()
        client._stream_active = True
        client._request_queue = asyncio.Queue()
        client._response_queue = asyncio.Queue()
        client._task_failure = RuntimeError("prior error")
        client._peer_closed_stream = True

        async def _noop():
            await asyncio.sleep(60)

        reader = asyncio.create_task(_noop())
        writer = asyncio.create_task(_noop())
        client._reader_task = reader
        client._writer_task = writer

        await client._teardown_stream()

        assert client._stream_active is False
        assert client._stream is None
        assert client._request_queue is None
        assert client._response_queue is None
        assert client._reader_task is None
        assert client._writer_task is None
        assert client._task_failure is None
        assert client._peer_closed_stream is False
        assert reader.cancelled()
        assert writer.cancelled()

    @pytest.mark.asyncio
    async def test_teardown_stream_safe_with_none_tasks(self):
        """_teardown_stream must not raise when called before any stream is started."""
        client = DestinationGRPCClient()
        # All task/queue references are None by default
        await client._teardown_stream()  # must not raise
        assert client._stream_active is False


class TestSendBatchSelfHeal:
    """send_batch rebuilds a torn-down stream from the params cached at
    start_stream, so the engine's retry of the same batch lands on a live
    stream — the direct engine->destination path has no proxy to rebuild it.
    """

    @pytest.mark.asyncio
    async def test_send_batch_without_start_stream_raises(self):
        """send_batch before any start_stream is a programming error: no cached
        params means there is nothing to rebuild, so it must raise."""
        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        # _stream_active is False and _stream_params is None by default.
        with pytest.raises(RuntimeError, match="Stream not active"):
            await client.send_batch(
                run_id="r",
                stream_id="s",
                batch_seq=0,
                record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
                record_ids=["1"],
                cursor=Cursor(token=b""),
                emitted_at=_EMITTED_AT,
            )

    @pytest.mark.asyncio
    async def test_send_batch_rebuilds_after_teardown_and_succeeds(self):
        """After a teardown left the stream inactive, the next send_batch
        reconnects and re-runs start_stream with the cached params, then
        sends successfully."""
        import asyncio

        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        # Simulate a stream that was started then torn down by a prior timeout:
        # params cached, but stream inactive and channel dropped.
        client._stream_params = {
            "run_id": "run-1",
            "stream_id": "s1",
            "schema_config": {"write_mode": "upsert", "schema_version": 1},
        }
        client._stream_active = False
        client._connected = False

        connect_mock = AsyncMock(return_value=True)
        start_calls = []

        async def fake_start_stream(run_id, stream_id, schema_config):
            start_calls.append((run_id, stream_id, schema_config))
            client._stream_active = True
            client._connected = True
            return True

        with patch.object(client, "connect", connect_mock), patch.object(
            client, "start_stream", side_effect=fake_start_stream
        ), patch.object(client, "_request_queue", new=asyncio.Queue()), patch.object(
            client,
            "_wait_with_heartbeat",
            AsyncMock(return_value=_batch_ack(AckStatus.ACK_STATUS_SUCCESS)),
        ):
            result = await client.send_batch(
                run_id="run-1",
                stream_id="s1",
                batch_seq=0,
                record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
                record_ids=["1"],
                cursor=Cursor(token=b""),
                emitted_at=_EMITTED_AT,
            )

        connect_mock.assert_awaited_once()
        assert start_calls == [
            ("run-1", "s1", {"write_mode": "upsert", "schema_version": 1})
        ]
        assert result.success is True
        assert result.status == AckStatus.ACK_STATUS_SUCCESS

    @pytest.mark.asyncio
    async def test_send_batch_rebuild_failure_returns_retryable_not_exception(self):
        """If the rebuild fails (reconnect or restart fails), send_batch returns
        a RETRYABLE transport_failure result — never raises — so the engine's
        bounded retry loop handles backoff and DLQ."""
        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client._stream_params = {
            "run_id": "run-1",
            "stream_id": "s1",
            "schema_config": {"write_mode": "upsert", "schema_version": 1},
        }
        client._stream_active = False
        client._connected = False

        with patch.object(client, "connect", AsyncMock(return_value=False)):
            result = await client.send_batch(
                run_id="run-1",
                stream_id="s1",
                batch_seq=0,
                record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
                record_ids=["1"],
                cursor=Cursor(token=b""),
                emitted_at=_EMITTED_AT,
            )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.transport_failure is True
        assert "rebuild" in result.failure_summary.lower()

    @pytest.mark.asyncio
    async def test_end_stream_clears_cached_params(self):
        """A deliberate end_stream drops cached params so a later send_batch
        raises rather than resurrecting an ended stream."""
        import asyncio

        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client._stream_params = {
            "run_id": "r",
            "stream_id": "s",
            "schema_config": {"write_mode": "upsert", "schema_version": 1},
        }
        client._stream_active = True
        client._request_queue = asyncio.Queue()

        async def _noop():
            await asyncio.sleep(0)

        client._writer_task = asyncio.create_task(_noop())
        client._reader_task = asyncio.create_task(_noop())

        await client.end_stream()
        assert client._stream_params is None

        with pytest.raises(RuntimeError, match="Stream not active"):
            await client.send_batch(
                run_id="r",
                stream_id="s",
                batch_seq=0,
                record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
                record_ids=["1"],
                cursor=Cursor(token=b""),
                emitted_at=_EMITTED_AT,
            )

    @pytest.mark.asyncio
    async def test_end_stream_after_teardown_still_clears_cached_params(self):
        """end_stream called after a timeout teardown (stream already inactive)
        must still drop the cached params — the caller deliberately ended the
        stream, so a later send_batch raises instead of self-healing."""
        import pyarrow as pa

        from src.grpc.generated.analitiq.v1 import Cursor

        client = DestinationGRPCClient()
        client._stream_params = {
            "run_id": "r",
            "stream_id": "s",
            "schema_config": {"write_mode": "upsert", "schema_version": 1},
        }
        client._stream_active = False  # prior teardown already deactivated

        await client.end_stream()
        assert client._stream_params is None

        with pytest.raises(RuntimeError, match="Stream not active"):
            await client.send_batch(
                run_id="r",
                stream_id="s",
                batch_seq=0,
                record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
                record_ids=["1"],
                cursor=Cursor(token=b""),
                emitted_at=_EMITTED_AT,
            )

    @pytest.mark.asyncio
    async def test_start_stream_failure_tears_down_half_built_stream(self):
        """A failed schema handshake must not leave _stream_active True with
        live queues/tasks: send_batch gates the self-heal rebuild on the flag,
        so stale truth would send every retry into a dead stream."""
        client = DestinationGRPCClient()
        client._connected = True
        client.timeout = 0.05  # force the schema-ACK wait to time out
        client._stub = MagicMock()
        client._stub.StreamRecords = MagicMock(return_value=MagicMock())

        with patch.object(client, "_read_responses", new=AsyncMock()), patch.object(
            client, "_write_requests", new=AsyncMock()
        ):
            accepted = await client.start_stream(
                run_id="r",
                stream_id="s",
                schema_config={"write_mode": "upsert", "schema_version": 1},
            )

        assert accepted is False
        assert client._stream_active is False
        assert client._reader_task is None
        assert client._writer_task is None
        # Params survive the failed start so a retry can still self-heal.
        assert client._stream_params is not None


def _batch_ack(status):
    """Build a BatchAck stand-in for _wait_with_heartbeat return values."""
    from src.grpc.generated.analitiq.v1 import BatchAck

    return BatchAck(status=status, records_written=1)


@pytest.mark.asyncio
async def test_send_batch_encodes_emitted_at_as_unix_ms():
    """The client encodes emitted_at as UTC epoch milliseconds on the wire
    RecordBatch (issue #353) -- the exact value the destination decodes back
    to derive a replay-stable partition path. Dropping the ``* 1000`` or the
    field would silently drift the destination to 1970 or a wrong hour."""
    import asyncio

    import pyarrow as pa

    from src.grpc.generated.analitiq.v1 import Cursor

    client = DestinationGRPCClient()
    client._stream_active = True
    client._request_queue = asyncio.Queue()
    client._response_queue = asyncio.Queue()
    # Pre-seed the ACK so send_batch returns as soon as it enqueues the batch.
    client._response_queue.put_nowait(_batch_ack(AckStatus.ACK_STATUS_SUCCESS))

    await client.send_batch(
        run_id="r",
        stream_id="s",
        batch_seq=1,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["1"],
        cursor=Cursor(token=b""),
        emitted_at=_EMITTED_AT,
    )

    enqueued = client._request_queue.get_nowait()
    assert enqueued.batch.emitted_at_unix_ms == int(_EMITTED_AT.timestamp() * 1000)
