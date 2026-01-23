"""gRPC client for streaming data to destination services.

This client handles bidirectional streaming to destination services,
including schema negotiation, batch sending, and ACK handling.
"""

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional

import grpc
from grpc import aio as grpc_aio

from .cursor import Cursor, encode_cursor
from .generated.analitiq.v1 import (
    AckStatus,
    ApiConfig,
    AutoConfigureOptions,
    BatchAck,
    ConflictResolution,
    DatabaseConfig,
    DestinationConfig,
    GetCapabilitiesRequest,
    GetCapabilitiesResponse,
    HealthCheckRequest,
    HealthCheckResponse,
    IndexDefinition,
    PayloadFormat,
    RecordBatch,
    SchemaAck,
    SchemaMessage,
    ShutdownAck,
    ShutdownRequest,
    StreamRequest,
    StreamResponse,
    WriteMode,
    DestinationServiceStub,
)

logger = logging.getLogger(__name__)


# Default configuration from environment
DEFAULT_GRPC_HOST = os.getenv("DESTINATION_GRPC_HOST", "localhost")
DEFAULT_GRPC_PORT = int(os.getenv("DESTINATION_GRPC_PORT", "50051"))
DEFAULT_GRPC_TIMEOUT = int(os.getenv("GRPC_TIMEOUT_SECONDS", "300"))
DEFAULT_MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024  # 16MB


@dataclass
class BatchResult:
    """Result of sending a batch to the destination."""

    success: bool
    status: AckStatus
    records_written: int
    committed_cursor: Optional[Cursor]
    failed_record_ids: List[str]
    failure_summary: str


class DestinationGRPCClient:
    """
    gRPC client for bidirectional streaming to destination services.

    Features:
    - Sends SCHEMA once at stream start
    - Sends RECORD batches with idempotency keys
    - Receives ACKs with committed cursor for checkpointing
    - Handles retry for RETRYABLE_FAILURE
    - Routes FATAL_FAILURE batches to DLQ
    """

    def __init__(
        self,
        host: str = DEFAULT_GRPC_HOST,
        port: int = DEFAULT_GRPC_PORT,
        timeout_seconds: int = DEFAULT_GRPC_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE,
    ):
        self.address = f"{host}:{port}"
        self.timeout = timeout_seconds
        self.max_retries = max_retries
        self.max_message_size = max_message_size

        self._channel: Optional[grpc_aio.Channel] = None
        self._stub: Optional[DestinationServiceStub] = None

        # Stream state
        self._stream: Optional[grpc_aio.StreamStreamCall] = None
        self._request_queue: Optional[asyncio.Queue] = None
        self._response_queue: Optional[asyncio.Queue] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._writer_task: Optional[asyncio.Task] = None

        # Connection state
        self._connected = False
        self._stream_active = False

    async def connect(
        self,
        max_connect_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay_seconds: float = 2.0,
    ) -> bool:
        """
        Establish gRPC channel and verify destination readiness.

        Retries connection with backoff to wait for destination container
        to be ready (useful in sidecar deployments where destination starts
        slightly after engine).

        Args:
            max_connect_retries: Maximum connection attempts (default: MAX_RETRIES env var or 3)
            retry_delay_seconds: Delay between retries (default: 2s)

        Returns:
            True if connection successful and destination is healthy
        """
        options = [
            ("grpc.max_send_message_length", self.max_message_size),
            ("grpc.max_receive_message_length", self.max_message_size),
            ("grpc.keepalive_time_ms", 10000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.keepalive_permit_without_calls", True),
        ]

        self._channel = grpc_aio.insecure_channel(self.address, options=options)
        self._stub = DestinationServiceStub(self._channel)

        # Retry loop to wait for destination to be ready
        for attempt in range(1, max_connect_retries + 1):
            try:
                response: HealthCheckResponse = await self._stub.HealthCheck(
                    HealthCheckRequest(),
                    timeout=10.0,
                )
                if response.status == HealthCheckResponse.ServingStatus.SERVING:
                    logger.info(f"Connected to destination at {self.address}")
                    self._connected = True
                    return True
                else:
                    logger.warning(
                        f"Destination not serving (attempt {attempt}/{max_connect_retries}): "
                        f"{response.message}"
                    )
            except grpc.aio.AioRpcError as e:
                logger.warning(
                    f"Connection attempt {attempt}/{max_connect_retries} failed: {e.code()}"
                )

            if attempt < max_connect_retries:
                logger.info(f"Retrying in {retry_delay_seconds}s...")
                await asyncio.sleep(retry_delay_seconds)

        logger.error(
            f"Failed to connect to destination at {self.address} "
            f"after {max_connect_retries} attempts"
        )
        return False

    async def disconnect(self) -> None:
        """Close gRPC channel and clean up resources."""
        if self._stream_active:
            await self.end_stream()

        if self._channel:
            await self._channel.close()
            self._channel = None
            self._stub = None

        self._connected = False
        logger.info("Disconnected from destination")

    async def send_shutdown(self, reason: str = "pipeline_completed") -> bool:
        """
        Send shutdown signal to destination server.

        This signals the destination server to shut down gracefully after
        all streams have completed. The server will acknowledge and then
        terminate.

        Args:
            reason: Reason for shutdown (e.g., "pipeline_completed", "error")

        Returns:
            True if shutdown was acknowledged, False otherwise
        """
        if not self._stub:
            # Try to reconnect if needed
            if not await self.connect(max_connect_retries=1, retry_delay_seconds=1.0):
                logger.warning("Cannot send shutdown: not connected to destination")
                return False

        try:
            response: ShutdownAck = await self._stub.Shutdown(
                ShutdownRequest(reason=reason),
                timeout=10.0,
            )
            logger.info(f"Shutdown acknowledged: {response.message}")
            return response.acknowledged
        except grpc.aio.AioRpcError as e:
            logger.warning(f"Shutdown request failed: {e.code()}")
            return False

    async def get_capabilities(self) -> Optional[GetCapabilitiesResponse]:
        """
        Query destination capabilities.

        Returns:
            GetCapabilitiesResponse or None if failed
        """
        if not self._stub:
            return None

        try:
            return await self._stub.GetCapabilities(
                GetCapabilitiesRequest(),
                timeout=10.0,
            )
        except grpc.aio.AioRpcError as e:
            logger.error(f"Failed to get capabilities: {e}")
            return None

    async def start_stream(
        self,
        run_id: str,
        stream_id: str,
        schema_config: Dict[str, Any],
    ) -> bool:
        """
        Start bidirectional stream and send initial SCHEMA message.

        Args:
            run_id: Unique run identifier
            stream_id: Stream identifier
            schema_config: Schema configuration dict

        Returns:
            True if schema accepted by destination
        """
        if not self._connected:
            raise RuntimeError("Not connected to destination")

        # Create queues for bidirectional communication
        self._request_queue = asyncio.Queue()
        self._response_queue = asyncio.Queue()

        # Start the bidirectional stream
        self._stream = self._stub.StreamRecords()
        self._stream_active = True

        # Start reader and writer tasks
        self._reader_task = asyncio.create_task(
            self._read_responses(),
            name=f"grpc-reader-{stream_id}",
        )
        self._writer_task = asyncio.create_task(
            self._write_requests(),
            name=f"grpc-writer-{stream_id}",
        )

        # Build and send schema message
        schema_msg = self._build_schema_message(stream_id, schema_config)
        await self._request_queue.put(StreamRequest(schema=schema_msg))

        # Wait for schema ACK
        try:
            response = await asyncio.wait_for(
                self._response_queue.get(),
                timeout=self.timeout,
            )

            if isinstance(response, SchemaAck):
                if response.accepted:
                    logger.info(f"Schema accepted for stream {stream_id}")
                    return True
                else:
                    logger.error(f"Schema rejected: {response.message}")
                    return False
            else:
                logger.error(f"Unexpected response type: {type(response)}")
                return False

        except asyncio.TimeoutError:
            logger.error("Timeout waiting for schema ACK")
            return False

    async def send_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        records: List[Dict[str, Any]],
        record_ids: List[str],
        cursor: Cursor,
        payload_format: PayloadFormat = PayloadFormat.PAYLOAD_FORMAT_JSONL,
    ) -> BatchResult:
        """
        Send a batch of records and wait for ACK.

        This implements strict in-order: send batch -> await ACK -> return.

        Args:
            run_id: Unique run identifier
            stream_id: Stream identifier
            batch_seq: Monotonically increasing batch sequence number
            records: List of records to send
            record_ids: List of record identifiers for DLQ correlation
            cursor: Opaque cursor representing max watermark in batch
            payload_format: Encoding format for payload

        Returns:
            BatchResult with status and committed cursor
        """
        if not self._stream_active:
            raise RuntimeError("Stream not active")

        # Encode payload
        payload = self._encode_payload(records, payload_format)

        # Build record batch message
        batch_msg = RecordBatch(
            run_id=run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
            format=payload_format,
            payload=payload,
            record_count=len(records),
            record_ids=record_ids,
            cursor=cursor,
        )

        # Send batch
        await self._request_queue.put(StreamRequest(batch=batch_msg))

        # Wait for ACK (strict in-order)
        try:
            response = await asyncio.wait_for(
                self._response_queue.get(),
                timeout=self.timeout,
            )

            if isinstance(response, BatchAck):
                return self._process_ack(response)
            else:
                logger.error(f"Unexpected response type: {type(response)}")
                return BatchResult(
                    success=False,
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=0,
                    committed_cursor=None,
                    failed_record_ids=[],
                    failure_summary="Unexpected response type",
                )

        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for ACK on batch {batch_seq}")
            return BatchResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                committed_cursor=None,
                failed_record_ids=[],
                failure_summary="Timeout waiting for ACK",
            )

    async def end_stream(self) -> None:
        """Signal end of stream and clean up."""
        if not self._stream_active:
            return

        # Signal writer to stop
        if self._request_queue:
            await self._request_queue.put(None)

        # Wait for writer task
        if self._writer_task:
            try:
                await asyncio.wait_for(self._writer_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._writer_task.cancel()

        # Cancel reader task
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

        self._stream_active = False
        logger.info("Stream ended")

    async def _write_requests(self) -> None:
        """Write requests from queue to gRPC stream."""
        try:
            while True:
                request = await self._request_queue.get()
                if request is None:
                    # End of stream signal
                    await self._stream.done_writing()
                    break
                await self._stream.write(request)
        except Exception as e:
            logger.error(f"Writer task error: {e}")
            raise

    async def _read_responses(self) -> None:
        """Read responses from gRPC stream and put in queue."""
        try:
            async for response in self._stream:
                msg_type = response.WhichOneof("message")
                if msg_type == "ack":
                    await self._response_queue.put(response.ack)
                elif msg_type == "schema_ack":
                    await self._response_queue.put(response.schema_ack)
                else:
                    logger.warning(f"Unknown response type: {msg_type}")
        except grpc.aio.AioRpcError as e:
            logger.error(f"Reader task error: {e}")
        except asyncio.CancelledError:
            pass

    def _build_schema_message(
        self,
        stream_id: str,
        config: Dict[str, Any],
    ) -> SchemaMessage:
        """Build SchemaMessage from configuration dict."""
        # Determine connector type
        connector_type = config.get("driver") or config.get("type") or "database"

        # Extract database config if present
        db_config = None
        api_config = None

        if connector_type in ("api",):
            # Build API config for API destinations
            api_config = ApiConfig(
                endpoint=config.get("endpoint", ""),
                method=config.get("method", "POST"),
                batch_support=config.get("batch_support", False),
                batch_size=config.get("batch_size", 100),
                idempotency_header=config.get("idempotency_header", ""),
            )
        elif connector_type == "database" or "endpoint" in config:
            conflict_res = config.get("conflict_resolution", {})
            auto_config = config.get("configure", {})

            indexes = []
            for idx in auto_config.get("auto_create_indexes", []):
                indexes.append(
                    IndexDefinition(
                        name=idx.get("name", ""),
                        columns=idx.get("columns", []),
                        type=idx.get("type", "btree"),
                        unique=idx.get("unique", False),
                    )
                )

            # Parse schema and table from endpoint field (format: "schema/table")
            # Falls back to explicit schema/table config if endpoint not present
            schema_name = config.get("schema", "public")
            table_name = config.get("table", "")

            endpoint = config.get("endpoint", "")
            if endpoint and "/" in endpoint:
                parts = endpoint.split("/", 1)
                schema_name = parts[0] or "public"
                table_name = parts[1]
            elif endpoint:
                # No slash - treat as table name only
                table_name = endpoint

            db_config = DatabaseConfig(
                schema_name=schema_name,
                table_name=table_name,
                unique_constraints=config.get("unique_constraints", []),
                conflict_resolution=ConflictResolution(
                    on_conflict=conflict_res.get("on_conflict", ""),
                    action=conflict_res.get("action", "update"),
                    update_columns=conflict_res.get("update_columns", []),
                ),
                auto_configure=AutoConfigureOptions(
                    auto_create_schema=auto_config.get("auto_create_schema", False),
                    auto_create_table=auto_config.get("auto_create_table", False),
                    auto_create_indexes=indexes,
                ),
            )

        # Determine write mode
        write_mode_str = config.get("write_mode", "upsert").lower()
        write_mode_map = {
            "insert": WriteMode.WRITE_MODE_INSERT,
            "upsert": WriteMode.WRITE_MODE_UPSERT,
            "truncate_insert": WriteMode.WRITE_MODE_TRUNCATE_INSERT,
        }
        write_mode = write_mode_map.get(write_mode_str, WriteMode.WRITE_MODE_UPSERT)

        # Build schema hash
        schema_json = config.get("endpoint_schema", {})
        schema_str = json.dumps(schema_json, sort_keys=True)
        schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()[:16]

        return SchemaMessage(
            stream_id=stream_id,
            version=config.get("schema_version", 1),
            json_schema=json.dumps(schema_json),
            primary_key=config.get("primary_key", []),
            write_mode=write_mode,
            destination_config=DestinationConfig(
                connector_type=connector_type,
                database=db_config,
                api=api_config,
            ),
            schema_hash=schema_hash,
        )

    def _encode_payload(
        self,
        records: List[Dict[str, Any]],
        format: PayloadFormat,
    ) -> bytes:
        """Encode records to bytes based on format."""
        if format == PayloadFormat.PAYLOAD_FORMAT_JSONL:
            lines = [json.dumps(record, default=str) for record in records]
            return "\n".join(lines).encode("utf-8")
        elif format == PayloadFormat.PAYLOAD_FORMAT_MSGPACK:
            import msgpack

            return msgpack.packb(records, default=str)
        else:
            raise ValueError(f"Unsupported payload format: {format}")

    def _process_ack(self, ack: BatchAck) -> BatchResult:
        """Process BatchAck into BatchResult."""
        success = ack.status in (
            AckStatus.ACK_STATUS_SUCCESS,
            AckStatus.ACK_STATUS_ALREADY_COMMITTED,
        )

        return BatchResult(
            success=success,
            status=ack.status,
            records_written=ack.records_written,
            committed_cursor=ack.committed_cursor if ack.committed_cursor else None,
            failed_record_ids=list(ack.failed_record_ids),
            failure_summary=ack.failure_summary,
        )


def generate_record_id(
    record: Dict[str, Any],
    run_id: str,
    batch_seq: int,
    index: int,
    primary_key_fields: Optional[List[str]] = None,
) -> str:
    """
    Generate a stable record ID for DLQ correlation.

    Args:
        record: The record dict
        run_id: Current run ID
        batch_seq: Current batch sequence
        index: Index of record within batch
        primary_key_fields: Optional primary key fields to include in hash

    Returns:
        Stable record ID string
    """
    if primary_key_fields:
        # Use primary key values for ID
        pk_values = [str(record.get(field, "")) for field in primary_key_fields]
        pk_str = "|".join(pk_values)
        hash_input = f"{run_id}:{batch_seq}:{pk_str}"
    else:
        # Use batch position as ID
        hash_input = f"{run_id}:{batch_seq}:{index}"

    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]
