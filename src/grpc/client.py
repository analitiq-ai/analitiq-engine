"""gRPC client for streaming data to destination services.

This client handles bidirectional streaming to destination services,
including schema negotiation, batch sending, and ACK handling.
"""

import asyncio
import hashlib
import io
import logging
import os
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional

import grpc
import pyarrow as pa
from grpc import aio as grpc_aio

from .cursor import Cursor, encode_cursor
from . import DEFAULT_MAX_MESSAGE_SIZE
from .generated.analitiq.v1 import (
    AckStatus,
    BatchAck,
    GetCapabilitiesRequest,
    GetCapabilitiesResponse,
    HealthCheckRequest,
    HealthCheckResponse,
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


_STREAM_TASK_FAILED = object()  # Sentinel pushed onto the response queue when
                                # the reader/writer task exits abnormally so
                                # send_batch / start_stream fail fast instead of
                                # blocking on `response_queue.get` until timeout.


# Default configuration from environment
# An empty DESTINATION_GRPC_HOST (the var is set but blank — e.g. baked into the
# image to mean "gRPC mode not configured") must fall back to localhost rather
# than yield a hostless ":50051" address. ``or`` covers both unset and blank.
DEFAULT_GRPC_HOST = os.getenv("DESTINATION_GRPC_HOST") or "localhost"
DEFAULT_GRPC_PORT = int(os.getenv("DESTINATION_GRPC_PORT", "50051"))
DEFAULT_GRPC_TIMEOUT = int(os.getenv("GRPC_TIMEOUT_SECONDS", "30"))
DEFAULT_MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


@dataclass
class BatchResult:
    """Result of sending a batch to the destination."""

    success: bool
    status: AckStatus
    records_written: int
    committed_cursor: Optional[Cursor]
    failed_record_ids: List[str]
    failure_summary: str
    # True when the stream/channel died before an ACK arrived — the
    # destination never rendered a verdict on the batch. Callers that can
    # restart the peer (the worker proxy) treat this as retryable; the
    # cross-container engine path keeps its existing fatal handling.
    transport_failure: bool = False


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
        target: Optional[str] = None,
    ):
        # ``target`` is a full gRPC address and wins over host/port — the
        # destination shell uses it to reach its connector worker over a
        # Unix domain socket (``unix:/.../worker.sock``).
        # Coalesce a blank host (env baked as ``DESTINATION_GRPC_HOST=""`` or an
        # explicit ``host=""``) to localhost so the address is never hostless
        # (``:50051``). Mirrors the engine-side fallback in engine.py.
        host = host or "localhost"
        self.address = target or f"{host}:{port}"
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

        # Last exception observed by the reader or writer task. Read by
        # send_batch / start_stream after seeing the _STREAM_TASK_FAILED
        # sentinel so the failure reason in the BatchResult is the real
        # underlying error instead of "Timeout waiting for ACK".
        self._task_failure: Optional[BaseException] = None
        # True when the reader observed the server closing the stream
        # without an error. Distinguishes graceful peer-close from an
        # in-task exception so send_batch / start_stream can surface a
        # specific diagnostic ("destination closed stream") instead of
        # a generic "stream signaled failure".
        self._peer_closed_stream: bool = False

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
        ]

        self._channel = grpc_aio.insecure_channel(self.address, options=options)
        self._stub = DestinationServiceStub(self._channel)

        # Attempt 1 failures are expected during concurrent engine/destination
        # startup; attempts 2+ escalate to WARNING.
        last_failure: Optional[str] = None
        for attempt in range(1, max_connect_retries + 1):
            log_failure = logger.warning if attempt > 1 else logger.debug
            try:
                response: HealthCheckResponse = await self._stub.HealthCheck(
                    HealthCheckRequest(),
                    timeout=10.0,
                )
                if response.status == HealthCheckResponse.ServingStatus.SERVING:
                    logger.info(f"Connected to destination at {self.address}")
                    self._connected = True
                    return True
                last_failure = f"not serving: {response.message}"
                log_failure(
                    f"Destination not serving (attempt {attempt}/{max_connect_retries}): "
                    f"{response.message}"
                )
            except grpc.aio.AioRpcError as e:
                last_failure = e.code().name
                log_failure(
                    f"Connection attempt {attempt}/{max_connect_retries} failed: {e.code()}"
                )

            if attempt < max_connect_retries:
                log_retry = logger.info if attempt > 1 else logger.debug
                log_retry(f"Retrying in {retry_delay_seconds}s...")
                await asyncio.sleep(retry_delay_seconds)

        logger.error(
            f"Failed to connect to destination at {self.address} "
            f"after {max_connect_retries} attempts: {last_failure or 'unknown'}"
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
            logger.warning(
                "Shutdown request failed: code=%s details=%s",
                e.code(), e.details(), exc_info=True,
            )
            return False

    async def health_check(self) -> bool:
        """Probe the destination's HealthCheck rpc (True == SERVING)."""
        if not self._stub:
            return False
        try:
            response = await self._stub.HealthCheck(
                HealthCheckRequest(), timeout=10.0
            )
            return response.status == HealthCheckResponse.ServingStatus.SERVING
        except Exception as e:
            logger.warning("Destination health check failed: %s", e)
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
            logger.error(
                "Failed to get capabilities: code=%s details=%s",
                e.code(), e.details(), exc_info=True,
            )
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

        # Reset stream-lifetime state so a previous failed run cannot
        # poison the diagnostic surfaced by this run's send_batch.
        self._task_failure = None
        self._peer_closed_stream = False

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

            if response is _STREAM_TASK_FAILED:
                cause = self._task_failure
                if cause is not None:
                    logger.error(
                        "Stream reader/writer exited before schema ACK: %s",
                        cause,
                    )
                elif self._peer_closed_stream:
                    logger.error(
                        "Destination closed stream before sending schema ACK"
                    )
                else:
                    logger.error(
                        "Stream signaled failure before schema ACK without "
                        "a recorded cause"
                    )
                return False

            if isinstance(response, SchemaAck):
                if response.accepted:
                    logger.info(f"Schema accepted for stream {stream_id}")
                    return True
                logger.error(f"Schema rejected: {response.message}")
                return False

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
        record_batch: pa.RecordBatch,
        record_ids: List[str],
        cursor: Cursor,
    ) -> BatchResult:
        """Send a batch and wait for ACK.

        Strict in-order: send -> await ACK -> return. The wire format is
        Arrow IPC; the engine ships ``pa.RecordBatch`` straight through.
        """
        if not self._stream_active:
            raise RuntimeError("Stream not active")

        batch_msg = RecordBatch(
            run_id=run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
            format=PayloadFormat.PAYLOAD_FORMAT_ARROW_IPC,
            payload=self._encode_arrow_ipc(record_batch),
            record_count=record_batch.num_rows,
            record_ids=record_ids,
            cursor=cursor,
        )

        # Send batch
        await self._request_queue.put(StreamRequest(batch=batch_msg))

        # Wait for ACK with periodic heartbeat logging (strict in-order)
        try:
            response = await self._wait_with_heartbeat(batch_seq)

            if response is _STREAM_TASK_FAILED:
                cause = self._task_failure
                if cause is not None:
                    summary = (
                        f"Stream reader/writer task exited before ACK: "
                        f"{type(cause).__name__}: {cause}"
                    )
                elif self._peer_closed_stream:
                    summary = (
                        "Destination closed stream before sending ACK "
                        f"for batch {batch_seq}"
                    )
                else:
                    summary = (
                        f"Stream signaled failure before ACK for batch "
                        f"{batch_seq} without a recorded cause"
                    )
                logger.error("Batch %d: %s", batch_seq, summary)
                return BatchResult(
                    success=False,
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=0,
                    committed_cursor=None,
                    failed_record_ids=[],
                    failure_summary=summary,
                    transport_failure=True,
                )

            if isinstance(response, BatchAck):
                return self._process_ack(response)

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
            # Give the reader task up to 2s to record its own RPC error —
            # the real cause (e.g. "Too many pings") races with the ACK timeout.
            if self._reader_task and not self._reader_task.done():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._reader_task), timeout=2.0
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    # Grace window elapsed or the task was cancelled: the
                    # _task_failure check below picks up whatever the reader
                    # recorded, so no diagnostic is lost by swallowing here.
                    pass
                except Exception:
                    # The reader raised inside the grace window. Its exception
                    # was already stored in _task_failure by _read_responses,
                    # so let the recorded-failure path below surface it and run
                    # teardown — never propagate the raw error out of send_batch.
                    pass

            if self._task_failure is not None:
                summary = (
                    f"Stream task failed (surfaced on ACK timeout for batch "
                    f"{batch_seq}): {type(self._task_failure).__name__}: "
                    f"{self._task_failure}"
                )
            else:
                summary = f"Timeout waiting for ACK on batch {batch_seq}"

            logger.error(summary)
            await self._teardown_stream()
            return BatchResult(
                success=False,
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                committed_cursor=None,
                failed_record_ids=[],
                failure_summary=summary,
                transport_failure=True,
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
        self._stream = None
        self._request_queue = None
        self._response_queue = None
        self._reader_task = None
        self._writer_task = None
        logger.info("Stream ended")

    async def _teardown_stream(self) -> None:
        """Force-cancel stream tasks and reset state after a non-clean exit.

        Unlike end_stream(), skips the graceful writer shutdown and cancels
        immediately — appropriate after a timeout or transport error where the
        stream is already in an unknown state. Resets _task_failure and
        _peer_closed_stream so stale diagnostics don't bleed into the next run.
        """
        if self._writer_task and not self._writer_task.done():
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                # Expected: we just cancelled the task ourselves.
                pass
            except Exception as e:
                logger.warning("Writer task raised during teardown: %s", e)
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                # Expected: we just cancelled the task ourselves.
                pass
            except Exception as e:
                logger.warning("Reader task raised during teardown: %s", e)
        self._stream_active = False
        self._stream = None
        self._request_queue = None
        self._response_queue = None
        self._reader_task = None
        self._writer_task = None
        self._task_failure = None
        self._peer_closed_stream = False
        logger.warning("Stream torn down after non-clean exit; channel retained for reconnect")

    async def _wait_with_heartbeat(self, batch_seq: int) -> Any:
        """Wait for an item from the response queue, logging progress every 10s.

        Logs an INFO line after each 10-second slice that passes without a
        response. The log is suppressed on the final slice that triggers the
        timeout, so a hang lasting exactly self.timeout may produce one fewer
        log line than expected.

        Raises asyncio.TimeoutError after self.timeout seconds.
        """
        get_task = asyncio.ensure_future(self._response_queue.get())
        heartbeat_interval = 10.0
        elapsed = 0.0
        try:
            while True:
                remaining = self.timeout - elapsed
                if remaining <= 0:
                    raise asyncio.TimeoutError()
                wait_time = min(heartbeat_interval, remaining)
                done, _ = await asyncio.wait({get_task}, timeout=wait_time)
                if done:
                    return get_task.result()
                elapsed += wait_time
                if elapsed >= self.timeout:
                    raise asyncio.TimeoutError()
                reader_alive = (
                    self._reader_task is not None
                    and not self._reader_task.done()
                )
                logger.info(
                    "Still waiting for ACK batch=%d elapsed=%.0fs "
                    "reader_task_alive=%s",
                    batch_seq,
                    elapsed,
                    reader_alive,
                )
        finally:
            if not get_task.done():
                get_task.cancel()
                try:
                    await get_task
                except asyncio.CancelledError:
                    # Expected: we cancel the pending queue.get() ourselves
                    # when leaving on timeout or after a response arrived.
                    pass

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
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            logger.error("Writer task error: %s", e, exc_info=True)
            self._task_failure = e
            # Unblock any send_batch / start_stream waiting on a response.
            if self._response_queue is not None:
                self._response_queue.put_nowait(_STREAM_TASK_FAILED)
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
        except asyncio.CancelledError:
            return
        except BaseException as e:
            logger.error("Reader task error: %s", e, exc_info=True)
            self._task_failure = e
            if self._response_queue is not None:
                self._response_queue.put_nowait(_STREAM_TASK_FAILED)
            raise
        else:
            # Stream closed by the server with no further responses;
            # signal waiters so they don't block until timeout.
            self._peer_closed_stream = True
            if self._response_queue is not None:
                self._response_queue.put_nowait(_STREAM_TASK_FAILED)

    def _build_schema_message(
        self,
        stream_id: str,
        config: Dict[str, Any],
    ) -> SchemaMessage:
        """Build the slim SchemaMessage for ``stream_id``.

        The destination loads the contract endpoint document via
        ``PipelineConfigPrep`` using the same ``PIPELINE_ID`` as the
        engine, so this message only carries the identification fields
        needed to look it up and the write mode for this stream.
        """
        write_mode_str = str(config.get("write_mode", "upsert")).lower()
        write_mode_map = {
            "insert": WriteMode.WRITE_MODE_INSERT,
            "upsert": WriteMode.WRITE_MODE_UPSERT,
            "truncate_insert": WriteMode.WRITE_MODE_TRUNCATE_INSERT,
        }
        if write_mode_str not in write_mode_map:
            raise ValueError(
                f"Unknown write_mode {write_mode_str!r}; expected one of "
                f"{sorted(write_mode_map)}"
            )
        write_mode = write_mode_map[write_mode_str]

        return SchemaMessage(
            stream_id=stream_id,
            version=int(config.get("schema_version", 1)),
            write_mode=write_mode,
        )

    @staticmethod
    def _encode_arrow_ipc(record_batch: pa.RecordBatch) -> bytes:
        """Serialize a ``pa.RecordBatch`` as a single-batch Arrow IPC stream.

        The stream format carries the schema in the same buffer, so the
        destination decodes batch and schema together without out-of-band
        coordination.
        """
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, record_batch.schema) as writer:
            writer.write_batch(record_batch)
        return sink.getvalue()

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
