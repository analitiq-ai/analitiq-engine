"""Unit tests for gRPC client."""

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


class TestClientPayloadEncoding:
    """Tests for client payload encoding."""

    def test_encode_jsonl_payload(self):
        """Test JSONL payload encoding."""
        from src.grpc.generated.analitiq.v1 import PayloadFormat

        client = DestinationGRPCClient()
        records = [
            {"id": 1, "name": "first"},
            {"id": 2, "name": "second"},
        ]

        payload = client._encode_payload(records, PayloadFormat.PAYLOAD_FORMAT_JSONL)

        # Decode and verify
        lines = payload.decode("utf-8").split("\n")
        assert len(lines) == 2

        import json
        assert json.loads(lines[0]) == {"id": 1, "name": "first"}
        assert json.loads(lines[1]) == {"id": 2, "name": "second"}

    def test_encode_msgpack_payload(self):
        """Test MessagePack payload encoding."""
        pytest.importorskip("msgpack")

        from src.grpc.generated.analitiq.v1 import PayloadFormat
        import msgpack

        client = DestinationGRPCClient()
        records = [
            {"id": 1, "name": "first"},
            {"id": 2, "name": "second"},
        ]

        payload = client._encode_payload(records, PayloadFormat.PAYLOAD_FORMAT_MSGPACK)

        # Decode and verify
        decoded = msgpack.unpackb(payload, raw=False)
        assert len(decoded) == 2
        assert decoded[0] == {"id": 1, "name": "first"}


class TestClientSchemaBuilder:
    """Tests for schema message building."""

    def test_build_schema_message(self):
        """Test building a schema message from config."""
        client = DestinationGRPCClient()

        config = {
            "type": "database",
            "driver": "postgresql",
            "schema": "public",
            "table": "users",
            "primary_key": ["id"],
            "write_mode": "upsert",
            "endpoint_schema": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                },
            },
            "conflict_resolution": {
                "on_conflict": "id",
                "action": "update",
                "update_columns": ["name"],
            },
        }

        schema_msg = client._build_schema_message("stream-1", config)

        assert schema_msg.stream_id == "stream-1"
        assert len(schema_msg.primary_key) == 1
        assert schema_msg.primary_key[0] == "id"
        assert schema_msg.destination_config.connector_type == "postgresql"
        assert schema_msg.destination_config.database.table_name == "users"
