"""Source worker service: stream a connector's batches to the engine shell.

Implements ``SourceService.ReadStream`` over the worker's UDS. The worker
owns the ``Readable`` connector instance and its connection; the engine
shell owns the checkpoint store. Cursor saves are relayed as ordered
``cursor_save`` events in the response stream, and the initial cursor rides
the request — the worker never calls back into the engine.
"""

from __future__ import annotations

import io
import json
import logging
from typing import Any, AsyncIterator, Dict, List, Optional

import grpc
import pyarrow as pa

from cdk.connection_runtime import ConnectionRuntime
from cdk.sql.exceptions import ReadError, UnsupportedDialectOperationError
from cdk.type_map import InvalidTypeMapError, UnmappedTypeError

from src.grpc.generated.analitiq.v1.source_service_pb2 import (
    CursorSave,
    ReadBatchChunk,
    ReadComplete,
    ReadError as ReadErrorMsg,
    ReadRequest,
    ReadResponse,
)
from src.grpc.generated.analitiq.v1.source_service_pb2_grpc import (
    SourceServiceServicer,
)
from src.grpc.generated.analitiq.v1.stream_pb2 import PayloadFormat

logger = logging.getLogger(__name__)

# Errors retrying cannot heal: contract/configuration problems. The engine
# shell fails the stream fatally on these instead of retrying.
_DETERMINISTIC_READ_ERRORS = (
    ReadError,
    UnsupportedDialectOperationError,
    UnmappedTypeError,
    InvalidTypeMapError,
    KeyError,
    TypeError,
    ValueError,
)


class _RelayCheckpoint:
    """CheckpointStore facade for the worker side.

    ``get_cursor`` answers from the request's initial cursor; ``save_cursor``
    queues the state for relay to the engine, which persists it. Order is
    preserved: saves are drained into the response stream at the point the
    connector made them.
    """

    def __init__(self, initial: Optional[Dict[str, Any]]) -> None:
        self._initial = initial
        self.pending: List[Dict[str, Any]] = []

    async def get_cursor(
        self, stream_name: str, partition: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        return self._initial

    async def save_cursor(
        self,
        stream_name: str,
        partition: Optional[Dict[str, Any]],
        cursor: Dict[str, Any],
    ) -> None:
        self.pending.append(cursor)


def _encode_arrow_ipc(batch: pa.RecordBatch) -> bytes:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


class SourceWorkerServicer(SourceServiceServicer):
    """Serves one bootstrapped stream's reads from the connector instance."""

    def __init__(
        self,
        readable: Any,
        runtime: ConnectionRuntime,
        source_config: Dict[str, Any],
    ) -> None:
        self._readable = readable
        self._runtime = runtime
        self._source_config = source_config

    async def ReadStream(
        self,
        request: ReadRequest,
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[ReadResponse]:
        initial = (
            json.loads(request.initial_cursor_json)
            if request.initial_cursor_json
            else None
        )
        partition = (
            json.loads(request.partition_json) if request.partition_json else {}
        )
        relay = _RelayCheckpoint(initial)
        total_records = 0
        total_batches = 0
        try:
            async for batch in self._readable.read_batches(
                self._runtime,
                self._source_config,
                checkpoint=relay,
                stream_name=request.stream_name,
                partition=partition,
                batch_size=request.batch_size or 1000,
            ):
                yield ReadResponse(
                    batch=ReadBatchChunk(
                        format=PayloadFormat.PAYLOAD_FORMAT_ARROW_IPC,
                        payload=_encode_arrow_ipc(batch),
                        record_count=batch.num_rows,
                    )
                )
                total_records += batch.num_rows
                total_batches += 1
                for cursor in relay.pending:
                    yield ReadResponse(
                        cursor_save=CursorSave(cursor_json=json.dumps(cursor))
                    )
                relay.pending.clear()
        except Exception as exc:  # noqa: BLE001 — every failure crosses as a typed event
            deterministic = isinstance(exc, _DETERMINISTIC_READ_ERRORS)
            logger.error(
                "source worker read failed (%s, deterministic=%s): %s",
                type(exc).__name__,
                deterministic,
                exc,
                exc_info=True,
            )
            yield ReadResponse(
                error=ReadErrorMsg(
                    message=str(exc),
                    deterministic=deterministic,
                    error_type=type(exc).__name__,
                )
            )
            return
        # Trailing saves after the generator finished (e.g. final checkpoint).
        for cursor in relay.pending:
            yield ReadResponse(
                cursor_save=CursorSave(cursor_json=json.dumps(cursor))
            )
        relay.pending.clear()
        yield ReadResponse(
            complete=ReadComplete(
                total_records=total_records, total_batches=total_batches
            )
        )
