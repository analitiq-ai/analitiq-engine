"""Unit tests for gRPC client."""

import grpc
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.grpc.client import (
    DestinationGRPCClient,
    BatchResult,
    generate_record_id,
)
from src.grpc.generated.analitiq.v1 import AckStatus


class TestGenerateRecordId:
    """Tests for record ID generation."""

    def test_generate_record_id_with_position(self):
        """Test generating record ID using batch position."""
        record = {"id": 1, "name": "test"}
        rid = generate_record_id(
            record=record,
            run_id="run-123",
            batch_seq=1,
            index=0,
        )

        assert rid is not None
        assert len(rid) == 16  # sha256[:16]

    def test_generate_record_id_with_primary_key(self):
        """Test generating record ID using primary key fields."""
        record = {"id": 123, "name": "test", "value": 42}
        rid = generate_record_id(
            record=record,
            run_id="run-456",
            batch_seq=2,
            index=5,
            primary_key_fields=["id"],
        )

        assert rid is not None
        assert len(rid) == 16

    def test_record_id_deterministic(self):
        """Test that record ID is deterministic for same inputs."""
        record = {"id": 1, "name": "test"}
        rid1 = generate_record_id(record, "run-123", 1, 0)
        rid2 = generate_record_id(record, "run-123", 1, 0)

        assert rid1 == rid2

    def test_record_id_differs_by_batch(self):
        """Test that record ID differs for different batches."""
        record = {"id": 1, "name": "test"}
        rid1 = generate_record_id(record, "run-123", 1, 0)
        rid2 = generate_record_id(record, "run-123", 2, 0)

        assert rid1 != rid2

    def test_record_id_differs_by_run(self):
        """Test that record ID differs for different runs."""
        record = {"id": 1, "name": "test"}
        rid1 = generate_record_id(record, "run-123", 1, 0)
        rid2 = generate_record_id(record, "run-456", 1, 0)

        assert rid1 != rid2


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


class TestDestinationGRPCClient:
    """Tests for DestinationGRPCClient."""

    def test_client_initialization(self):
        """Test client initialization with default values."""
        client = DestinationGRPCClient()

        assert client.address == "localhost:50051"
        assert client.timeout == 300
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

        with patch("src.grpc.client.grpc_aio.insecure_channel"), \
             patch("src.grpc.client.DestinationServiceStub", return_value=mock_stub), \
             patch("src.grpc.client.logger") as mock_logger:
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

        with patch("src.grpc.client.grpc_aio.insecure_channel"), \
             patch("src.grpc.client.DestinationServiceStub", return_value=mock_stub), \
             patch("src.grpc.client.logger") as mock_logger:
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

        with patch("src.grpc.client.grpc_aio.insecure_channel"), \
             patch("src.grpc.client.DestinationServiceStub", return_value=mock_stub), \
             patch("src.grpc.client.logger") as mock_logger:
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

        with patch("src.grpc.client.grpc_aio.insecure_channel"), \
             patch("src.grpc.client.DestinationServiceStub", return_value=mock_stub), \
             patch("src.grpc.client.logger") as mock_logger:
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
        """The slim SchemaMessage carries only stream_id, version, and
        write_mode — every other field comes from the preloaded contract
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
