"""gRPC server for destination services.

This server implements the DestinationService gRPC interface, handling:
- Bidirectional streaming of records from engine
- Schema negotiation
- Batch processing with ACK responses
- Health checks and capability discovery
"""

import asyncio
import json
import logging
import os
from typing import Any, AsyncIterator, Dict, Optional

import grpc
from grpc import aio as grpc_aio

from ..grpc.generated.analitiq.v1 import (
    AckStatus,
    BatchAck,
    ConnectionStatus,
    GetCapabilitiesRequest,
    GetCapabilitiesResponse,
    HealthCheckRequest,
    HealthCheckResponse,
    PayloadFormat,
    SchemaAck,
    ShutdownAck,
    ShutdownRequest,
    StreamRequest,
    StreamResponse,
    WriteMode,
    add_DestinationServiceServicer_to_server,
    DestinationServiceServicer,
)
from .base_handler import BaseDestinationHandler
from .connectors import get_handler
from ..engine.type_map import InvalidTypeMapError, UnmappedTypeError

logger = logging.getLogger(__name__)

# Default configuration from environment
DEFAULT_GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024  # 16MB


class DestinationGRPCServer:
    """
    gRPC server for destination services.

    This server:
    - Listens for incoming gRPC connections from engine
    - Handles bidirectional streaming via StreamRecords RPC
    - Delegates data operations to handler implementations
    - Provides health checks for container orchestration
    """

    def __init__(
        self,
        handler: BaseDestinationHandler,
        port: int = DEFAULT_GRPC_PORT,
        max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE,
    ):
        self.handler = handler
        self.port = port
        self.max_message_size = max_message_size
        self._server: Optional[grpc_aio.Server] = None
        self._servicer: Optional["DestinationServicer"] = None
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the gRPC server."""
        self._servicer = DestinationServicer(self.handler, self)

        self._server = grpc_aio.server(
            options=[
                ("grpc.max_send_message_length", self.max_message_size),
                ("grpc.max_receive_message_length", self.max_message_size),
            ]
        )

        add_DestinationServiceServicer_to_server(self._servicer, self._server)
        self._server.add_insecure_port(f"[::]:{self.port}")

        await self._server.start()
        logger.info(f"Destination gRPC server started on port {self.port}")

    async def stop(self, grace_period: float = 5.0) -> None:
        """Stop the gRPC server gracefully."""
        if self._server:
            await self._server.stop(grace_period)
            logger.info("Destination gRPC server stopped")

    async def wait_for_termination(self) -> None:
        """Wait for shutdown signal, then stop server gracefully."""
        await self._shutdown_event.wait()
        logger.info("Shutdown signal received, stopping server...")
        await self.stop()

    def signal_shutdown(self) -> None:
        """Signal the server to shut down."""
        self._shutdown_event.set()


class DestinationServicer(DestinationServiceServicer):
    """
    Implementation of DestinationService gRPC interface.

    Handles:
    - StreamRecords: Bidirectional streaming for data transfer
    - HealthCheck: Container health probes
    - GetCapabilities: Feature discovery
    - Shutdown: Graceful shutdown signal from engine
    """

    def __init__(
        self,
        handler: BaseDestinationHandler,
        server: "DestinationGRPCServer",
    ):
        self.handler = handler
        self._server = server
        self._schema_configured = False
        self._current_stream_id: Optional[str] = None

    async def StreamRecords(
        self,
        request_iterator: AsyncIterator[StreamRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[StreamResponse]:
        """
        Handle bidirectional streaming of records.

        Protocol:
        1. Engine sends SchemaMessage first
        2. Server responds with SchemaAck
        3. Engine sends RecordBatch messages
        4. Server responds with BatchAck for each batch
        """
        logger.info("StreamRecords: New stream started")
        self._schema_configured = False
        self._current_stream_id = None

        try:
            async for request in request_iterator:
                msg_type = request.WhichOneof("message")

                if msg_type == "schema":
                    # Handle schema message
                    schema_msg = request.schema
                    self._current_stream_id = schema_msg.stream_id

                    logger.info(
                        f"Received schema for stream {schema_msg.stream_id}, "
                        f"version {schema_msg.version}"
                    )

                    # Configure handler with schema. Deterministic type-map
                    # errors bubble out of the handler and we relay them in
                    # the SchemaAck so the engine sees the exact unmapped
                    # native type instead of an opaque stream abort.
                    try:
                        accepted = await self.handler.configure_schema(schema_msg)
                        ack_message = "" if accepted else "Schema configuration failed"
                    except (UnmappedTypeError, InvalidTypeMapError) as e:
                        logger.error(
                            "type-map error configuring stream %s: %s",
                            schema_msg.stream_id,
                            e,
                        )
                        accepted = False
                        ack_message = f"type-map: {e}"
                    self._schema_configured = accepted

                    yield StreamResponse(
                        schema_ack=SchemaAck(
                            stream_id=schema_msg.stream_id,
                            accepted=accepted,
                            message=ack_message,
                        )
                    )

                elif msg_type == "batch":
                    # Handle record batch
                    batch_msg = request.batch

                    if not self._schema_configured:
                        logger.error("Received batch before schema was configured")
                        yield StreamResponse(
                            ack=BatchAck(
                                run_id=batch_msg.run_id,
                                stream_id=batch_msg.stream_id,
                                batch_seq=batch_msg.batch_seq,
                                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                                records_written=0,
                                failure_summary="Schema not configured",
                            )
                        )
                        continue

                    logger.debug(
                        f"Received batch {batch_msg.batch_seq} for stream "
                        f"{batch_msg.stream_id} with {batch_msg.record_count} records"
                    )

                    # Decode payload
                    records = self._decode_payload(
                        batch_msg.payload, batch_msg.format
                    )

                    # Write batch via handler
                    result = await self.handler.write_batch(
                        run_id=batch_msg.run_id,
                        stream_id=batch_msg.stream_id,
                        batch_seq=batch_msg.batch_seq,
                        records=records,
                        record_ids=list(batch_msg.record_ids),
                        cursor=batch_msg.cursor,
                    )

                    # Build ACK response
                    yield StreamResponse(
                        ack=BatchAck(
                            run_id=batch_msg.run_id,
                            stream_id=batch_msg.stream_id,
                            batch_seq=batch_msg.batch_seq,
                            status=result.status,
                            records_written=result.records_written,
                            committed_cursor=result.committed_cursor,
                            failed_record_ids=result.failed_record_ids,
                            failure_summary=result.failure_summary,
                        )
                    )

                else:
                    logger.warning(f"Unknown message type: {msg_type}")

        except Exception as e:
            logger.error(f"StreamRecords error: {e}")
            raise

        finally:
            logger.info("StreamRecords: Stream ended")
            self._schema_configured = False
            self._current_stream_id = None

    async def HealthCheck(
        self,
        request: HealthCheckRequest,
        context: grpc.aio.ServicerContext,
    ) -> HealthCheckResponse:
        """Handle health check requests."""
        try:
            is_healthy = await self.handler.health_check()

            if is_healthy:
                return HealthCheckResponse(
                    status=HealthCheckResponse.ServingStatus.SERVING,
                    message="Destination service healthy",
                    db_connection=ConnectionStatus.CONNECTION_STATUS_CONNECTED,
                )
            else:
                return HealthCheckResponse(
                    status=HealthCheckResponse.ServingStatus.NOT_SERVING,
                    message="Destination service unhealthy",
                    db_connection=ConnectionStatus.CONNECTION_STATUS_DISCONNECTED,
                )

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthCheckResponse(
                status=HealthCheckResponse.ServingStatus.NOT_SERVING,
                message=str(e),
                db_connection=ConnectionStatus.CONNECTION_STATUS_DISCONNECTED,
            )

    async def GetCapabilities(
        self,
        request: GetCapabilitiesRequest,
        context: grpc.aio.ServicerContext,
    ) -> GetCapabilitiesResponse:
        """Return destination capabilities."""
        supported_modes = [WriteMode.WRITE_MODE_UPSERT]
        if self.handler.supports_upsert:
            supported_modes.append(WriteMode.WRITE_MODE_INSERT)
        supported_modes.append(WriteMode.WRITE_MODE_TRUNCATE_INSERT)

        return GetCapabilitiesResponse(
            connector_type=self.handler.connector_type,
            supported_write_modes=supported_modes,
            supports_transactions=self.handler.supports_transactions,
            supports_auto_create=True,
            supports_upsert=self.handler.supports_upsert,
            supports_bulk_load=self.handler.supports_bulk_load,
            max_batch_size=self.handler.max_batch_size,
            max_batch_bytes=self.handler.max_batch_bytes,
            supported_formats=[
                PayloadFormat.PAYLOAD_FORMAT_JSONL,
                PayloadFormat.PAYLOAD_FORMAT_MSGPACK,
            ],
            protocol_version="1.0.0",
        )

    async def Shutdown(
        self,
        request: ShutdownRequest,
        context: grpc.aio.ServicerContext,
    ) -> ShutdownAck:
        """Handle shutdown request from engine."""
        logger.info(f"Received shutdown request: reason={request.reason}")
        self._server.signal_shutdown()
        return ShutdownAck(acknowledged=True, message="Shutting down")

    def _decode_payload(
        self, payload: bytes, format: PayloadFormat
    ) -> list[Dict[str, Any]]:
        """Decode payload bytes to list of records."""
        if format == PayloadFormat.PAYLOAD_FORMAT_JSONL:
            lines = payload.decode("utf-8").strip().split("\n")
            return [json.loads(line) for line in lines if line]

        elif format == PayloadFormat.PAYLOAD_FORMAT_MSGPACK:
            import msgpack
            return msgpack.unpackb(payload, raw=False)

        else:
            raise ValueError(f"Unsupported payload format: {format}")


async def run_destination_server(
    connection_config: Dict[str, Any],
    connector_type: str = "postgresql",
    port: int = DEFAULT_GRPC_PORT,
) -> None:
    """
    Run destination server with specified handler.

    Args:
        connection_config: Database/API connection configuration
        connector_type: Type of destination (postgresql, mysql, etc.)
        port: gRPC port to listen on
    """
    # Create handler
    handler = get_handler(connector_type)

    # Connect to destination
    await handler.connect(connection_config)

    # Create and start server
    server = DestinationGRPCServer(handler, port=port)

    try:
        await server.start()
        await server.wait_for_termination()
    finally:
        await handler.disconnect()
        await server.stop()
