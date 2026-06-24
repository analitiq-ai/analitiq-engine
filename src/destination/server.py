"""gRPC server for destination services.

This server implements the DestinationService gRPC interface, handling:
- Bidirectional streaming of records from engine
- Schema negotiation
- Batch processing with ACK responses
- Health checks and capability discovery
"""

import asyncio
import io
import logging
import os
from typing import Any, AsyncIterator, Dict, Optional

import grpc
import pyarrow as pa
from grpc import aio as grpc_aio

from ..grpc.generated.analitiq.v1 import (
    AckStatus,
    BatchAck,
    ConnectionStatus,
    Cursor,
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
from cdk.adbc_registry import AdbcConfigurationError
from cdk.base_handler import BaseDestinationHandler
from cdk.secrets.exceptions import PlaceholderExpansionError
from cdk.sql.exceptions import (
    SchemaConfigurationError,
    UnsupportedDialectOperationError,
)
from cdk.types import Cursor as CdkCursor, SchemaSpec, WriteMode as CdkWriteMode
from cdk.type_map import InvalidTypeMapError, UnmappedTypeError

from ..grpc import DEFAULT_MAX_MESSAGE_SIZE

logger = logging.getLogger(__name__)

# Default configuration from environment
DEFAULT_GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))

# A destination SQL statement is cancelled before the sender's gRPC ack
# timeout, so the database returns the cancelled statement instead of the
# engine abandoning the handshake with a bare "ACK timeout" (issue #231).
# The ack budget arrives stamped on the SchemaMessage by the sender's client
# (issue #234), so the statement timeout always tracks the budget the sender
# actually waits on — no shared-env assumption across processes.
_STATEMENT_TIMEOUT_ACK_MARGIN_SECONDS = 5
# For budgets too small to spare the full margin, fall back to a fraction of
# the budget. Both terms are below the budget, so the result always is too -
# leaving head-room for the cancel + rejection to reach the engine first.
_STATEMENT_TIMEOUT_BUDGET_FRACTION = 0.5


def derive_statement_timeout_seconds(ack_timeout_seconds: int) -> float:
    """Per-statement budget kept strictly below the sender's gRPC ack timeout
    so a blocked DDL/write is cancelled before the sender gives up waiting for
    the ack.

    Returns the full ack budget minus a fixed margin where the budget is large
    enough, otherwise half the budget. Both candidates are strictly below the
    ack budget for any positive budget, so the statement timeout can never
    meet or exceed it - the orphaned-statement race this guards against
    (issue #231).
    """
    return float(
        max(
            ack_timeout_seconds - _STATEMENT_TIMEOUT_ACK_MARGIN_SECONDS,
            ack_timeout_seconds * _STATEMENT_TIMEOUT_BUDGET_FRACTION,
        )
    )


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
        address: Optional[str] = None,
    ):
        self.handler = handler
        self.port = port
        # Full gRPC bind address; overrides ``port`` when set. Workers bind
        # ``unix:/path/worker.sock`` so the channel never leaves the host.
        self.address = address
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
                # Reject excessively aggressive client pings so that a client
                # which re-adds keepalive options cannot flood us (incident: PR #85).
                # min_ping_interval gates how often a data-less ping is allowed;
                # max_ping_strikes is how many violations we tolerate before
                # sending GOAWAY. max_pings_without_data is a sender-side throttle
                # and does not enforce anything on incoming pings, so it is not used.
                ("grpc.http2.min_ping_interval_without_data_ms", 300_000),
                ("grpc.http2.max_ping_strikes", 2),
            ]
        )

        add_DestinationServiceServicer_to_server(self._servicer, self._server)
        if self.address is not None:
            # Explicit address (e.g. a worker's ``unix:/path/worker.sock``)
            # wins over the TCP port. UDS keeps the worker channel local:
            # no exposed port, filesystem-permission access control.
            self._server.add_insecure_port(self.address)
            bound = self.address
        else:
            self._server.add_insecure_port(f"[::]:{self.port}")
            bound = f"[::]:{self.port}"

        await self._server.start()
        logger.info(f"Destination gRPC server started on {bound}")

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
        # Per-RPC state must be function-local: the servicer instance is
        # shared across every concurrent ``StreamRecords`` call, so storing
        # ``schema_configured`` / ``current_stream_id`` on ``self`` would
        # let one stream's bookkeeping clobber another's.
        schema_configured = False
        current_stream_id: Optional[str] = None

        try:
            async for request in request_iterator:
                msg_type = request.WhichOneof("message")

                if msg_type == "schema":
                    # Handle schema message
                    schema_msg = request.schema
                    current_stream_id = schema_msg.stream_id

                    logger.info(
                        f"Received schema for stream {schema_msg.stream_id}, "
                        f"version {schema_msg.version}"
                    )

                    # Configure handler with schema. Deterministic errors
                    # (type-map, SchemaConfigurationError, KeyError/
                    # ValueError/TypeError on a malformed endpoint document)
                    # surface in the SchemaAck with the exception type and
                    # message, so the engine logs a precise rejection
                    # reason instead of a generic "schema configuration
                    # failed". Anything else is a defect: it escapes to the
                    # stream's outer except, which logs the traceback and
                    # re-raises, failing the RPC with the real error instead
                    # of a generic schema rejection.
                    #
                    # Default the transport-vs-schema verdict to "genuine schema
                    # rejection" (issue #264): every deterministic error caught
                    # below is the destination evaluating and refusing the schema.
                    # The try path overrides it -- False when accepted, or the
                    # proxy's forwarded inner verdict when a worker hop failed.
                    schema_rejected = True
                    try:
                        if not schema_msg.ack_timeout_seconds:
                            # Every conforming sender stamps its ack budget on
                            # the handshake (issue #234); without it the
                            # destination cannot bound its statements below
                            # the sender's wait. Reject loudly instead of
                            # running statements unbounded.
                            raise ValueError(
                                "schema message carries no ack_timeout_seconds;"
                                " the destination cannot derive its statement"
                                " timeout (issue #234)"
                            )
                        # Translate the wire message to the CDK-native SchemaSpec
                        # the handler contract now takes (the CDK must not import
                        # gRPC types). Field-for-field; structurally identical.
                        schema_spec = SchemaSpec(
                            stream_id=schema_msg.stream_id,
                            version=schema_msg.version,
                            write_mode=CdkWriteMode(schema_msg.write_mode),
                            ack_timeout_seconds=schema_msg.ack_timeout_seconds,
                        )
                        # Bound statements before configure_schema runs DDL:
                        # the CREATE TABLE handshake is exactly the statement
                        # that must be cancelled ahead of the sender's ack
                        # wait (issues #230/#231). Only the async-SQLAlchemy
                        # handler path can enforce the bound; ADBC and
                        # sync-engine statements run on worker threads and
                        # rely on driver timeouts (the handler logs a warning
                        # when it accepts a budget it cannot enforce). No-op
                        # for handlers that run no SQL (API, file, stdout,
                        # the worker proxy).
                        self.handler.set_statement_timeout(
                            derive_statement_timeout_seconds(
                                schema_msg.ack_timeout_seconds
                            )
                        )
                        accepted = await self.handler.configure_schema(schema_spec)
                        # A handler that proxies to a worker (WorkerProxyHandler)
                        # records the worker's real rejection reason; surface it
                        # verbatim so the engine ack is not the generic message
                        # (issue #231). Handlers that raise instead are caught
                        # below; those that return False with no reason fall back.
                        ack_message = (
                            ""
                            if accepted
                            else (
                                getattr(self.handler, "last_schema_rejection", None)
                                or "Schema configuration failed"
                            )
                        )
                        # Distinguish a genuine schema rejection from a proxied
                        # inner transport failure (issue #264). The proxy records
                        # which it was; a direct handler has no such attribute and
                        # returns False only on a real rejection, so default True.
                        schema_rejected = (
                            False
                            if accepted
                            else bool(getattr(self.handler, "last_schema_rejected", True))
                        )
                    except (UnmappedTypeError, InvalidTypeMapError) as e:
                        logger.error(
                            "type-map error configuring stream %s: %s",
                            schema_msg.stream_id,
                            e,
                        )
                        accepted = False
                        ack_message = f"type-map: {e}"
                    except (
                        AdbcConfigurationError,
                        SchemaConfigurationError,
                        UnsupportedDialectOperationError,
                        PlaceholderExpansionError,
                    ) as e:
                        # The handler's configure_schema deliberately
                        # propagates these deterministic errors so the
                        # SchemaAck carries the precise reason; translate
                        # them here instead of crashing the stream.
                        logger.error(
                            "configuration error for stream %s: %s",
                            schema_msg.stream_id,
                            e,
                        )
                        accepted = False
                        ack_message = f"{type(e).__name__}: {e}"
                    except (KeyError, TypeError, ValueError) as e:
                        logger.exception(
                            "deterministic error configuring stream %s",
                            schema_msg.stream_id,
                        )
                        accepted = False
                        ack_message = f"{type(e).__name__}: {e}"
                    schema_configured = accepted

                    yield StreamResponse(
                        schema_ack=SchemaAck(
                            stream_id=schema_msg.stream_id,
                            accepted=accepted,
                            message=ack_message,
                            schema_rejected=schema_rejected,
                        )
                    )

                elif msg_type == "batch":
                    # Handle record batch
                    batch_msg = request.batch

                    if not schema_configured:
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

                    record_batch = self._decode_arrow_ipc(batch_msg)
                    result = await self.handler.write_batch(
                        run_id=batch_msg.run_id,
                        stream_id=batch_msg.stream_id,
                        batch_seq=batch_msg.batch_seq,
                        record_batch=record_batch,
                        record_ids=list(batch_msg.record_ids),
                        cursor=CdkCursor(token=batch_msg.cursor.token),
                    )

                    # Build ACK response
                    yield StreamResponse(
                        ack=BatchAck(
                            run_id=batch_msg.run_id,
                            stream_id=batch_msg.stream_id,
                            batch_seq=batch_msg.batch_seq,
                            status=result.status,
                            records_written=result.records_written,
                            # CDK-native Cursor -> wire Cursor (or omit when the
                            # handler advanced no cursor, e.g. a failure result).
                            committed_cursor=(
                                Cursor(token=result.committed_cursor.token)
                                if result.committed_cursor is not None
                                else None
                            ),
                            failed_record_ids=result.failed_record_ids,
                            failure_summary=result.failure_summary,
                        )
                    )

                else:
                    logger.warning(f"Unknown message type: {msg_type}")

        except Exception as e:
            logger.exception("StreamRecords error: %s", e)
            raise

        finally:
            logger.info(
                "StreamRecords: Stream ended%s",
                f" (stream_id={current_stream_id!r})" if current_stream_id else "",
            )

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
            logger.exception("Health check failed: %s", e)
            return HealthCheckResponse(
                status=HealthCheckResponse.ServingStatus.NOT_SERVING,
                message=f"{type(e).__name__}: {e}",
                db_connection=ConnectionStatus.CONNECTION_STATUS_DISCONNECTED,
            )

    async def GetCapabilities(
        self,
        request: GetCapabilitiesRequest,
        context: grpc.aio.ServicerContext,
    ) -> GetCapabilitiesResponse:
        """Return destination capabilities."""
        try:
            supported_modes = [WriteMode.WRITE_MODE_INSERT]
            if self.handler.supports_upsert:
                supported_modes.append(WriteMode.WRITE_MODE_UPSERT)
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
                supported_formats=[PayloadFormat.PAYLOAD_FORMAT_ARROW_IPC],
                protocol_version="1.0.0",
            )
        except AttributeError as e:
            # A broken capability surface (attribute missing on the
            # handler, or an AttributeError from inside a capability
            # property) would otherwise surface as a bare INTERNAL with
            # no detail. Name the handler class and carry the original
            # message — which says what attribute failed and where — so
            # the defect is actionable.
            detail = (
                f"handler {type(self.handler).__name__} failed to provide "
                f"capabilities: {e}"
            )
            logger.exception("GetCapabilities failed: %s", detail)
            await context.abort(grpc.StatusCode.INTERNAL, detail)
            raise  # abort always raises; backstop so this cannot return None

    async def Shutdown(
        self,
        request: ShutdownRequest,
        context: grpc.aio.ServicerContext,
    ) -> ShutdownAck:
        """Handle shutdown request from engine."""
        logger.info(f"Received shutdown request: reason={request.reason}")
        self._server.signal_shutdown()
        return ShutdownAck(acknowledged=True, message="Shutting down")

    @staticmethod
    def _decode_arrow_ipc(batch_msg: Any) -> pa.RecordBatch:
        """Decode an Arrow IPC payload into a single ``pa.RecordBatch``.

        ``combine_chunks`` collapses any multi-batch writers down to one
        record batch; an empty payload produces an empty batch with the
        schema preserved.
        """
        if batch_msg.format != PayloadFormat.PAYLOAD_FORMAT_ARROW_IPC:
            raise ValueError(
                f"Unsupported payload format: {batch_msg.format}; "
                f"only PAYLOAD_FORMAT_ARROW_IPC is supported"
            )
        with pa.ipc.open_stream(io.BytesIO(batch_msg.payload)) as reader:
            table = reader.read_all()
        if table.num_rows == 0:
            return pa.RecordBatch.from_pylist([], schema=table.schema)
        return table.combine_chunks().to_batches()[0]


