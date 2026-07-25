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
from collections.abc import AsyncIterator
from typing import Any

import pyarrow as pa

import grpc
from cdk.connection_runtime import ConnectionRuntime
from cdk.declarations import DECLARED_READ_DETERMINISTIC, ErrorMap, error_map_for
from cdk.exceptions import TransportSpecError
from cdk.sql.exceptions import (
    ReadError,
    TlsVerificationError,
    UnsupportedDialectOperationError,
)
from cdk.type_map import InvalidTypeMapError, UnmappedTypeError
from src.grpc.generated.analitiq.v1 import (
    CursorSave,
    PayloadFormat,
    ReadBatchChunk,
    ReadComplete,
)
from src.grpc.generated.analitiq.v1 import ReadError as ReadErrorMsg
from src.grpc.generated.analitiq.v1 import ReadRequest, ReadResponse
from src.grpc.generated.analitiq.v1.source_service_pb2_grpc import SourceServiceServicer
from src.source.connectors.base import ReadError as ApiReadError
from src.state.store import decode_cursor_state, encode_cursor_state

logger = logging.getLogger(__name__)

# Errors retrying cannot heal: contract/configuration problems. The engine
# shell fails the stream fatally on these instead of retrying. The two
# ReadError classes are distinct types raised for the same intent — the SQL
# connectors raise the CDK one, the API connector its base-module one — so
# both must classify identically here.
_DETERMINISTIC_READ_ERRORS = (
    ReadError,
    ApiReadError,
    UnsupportedDialectOperationError,
    UnmappedTypeError,
    InvalidTypeMapError,
    # Deterministic connector/transport-spec validation failures (its own
    # contract): an authoring defect in a value expression that escapes a
    # connector unwrapped must not classify as retryable.
    TransportSpecError,
    # A mid-run pool connection that fails the declared TLS mode's
    # post-connect check (a non-TLS or downgraded server) cannot heal by
    # retrying against the same endpoint — and under an active MITM,
    # retrying is exactly wrong.
    TlsVerificationError,
    KeyError,
    TypeError,
    ValueError,
)


def classify_read_error(
    exc: BaseException, error_map: ErrorMap | None
) -> tuple[bool, str | None]:
    """Classify a read failure: declared verdicts first, isinstance ladder after.

    Returns ``(deterministic, declared_category)`` where
    ``declared_category`` is the engine-vocabulary value the connector's
    declarations claimed for this failure — carried on the typed error by
    its birth site (a connector's HTTP status match) or matched here
    against the raw driver exception — and ``None`` when the verdict came
    from the type ladder. The category crosses the process boundary on the
    ``ReadError`` wire message so the engine reports the declared code
    instead of re-deriving from text. Resolution order per issue #401:
    declared verdicts, then the connector's sanctioned typed errors
    (``_DETERMINISTIC_READ_ERRORS`` — the hook), never text.
    """
    birth_site = getattr(exc, "declared_category", None)
    if isinstance(birth_site, str) and birth_site in DECLARED_READ_DETERMINISTIC:
        return DECLARED_READ_DETERMINISTIC[birth_site], birth_site
    match = error_map.match_exception(exc) if error_map is not None else None
    if match is not None:
        return DECLARED_READ_DETERMINISTIC[match.category], match.category
    return isinstance(exc, _DETERMINISTIC_READ_ERRORS), None


class _RelayCheckpoint:
    """CheckpointStore facade for the worker side.

    ``get_cursor`` answers from the request's initial cursor; ``save_cursor``
    queues the state for relay to the engine, which holds it in its in-run cache
    (the durable per-stream checkpoint advances only on a destination ACK, not
    from this pre-ACK source position). Order is preserved: saves are drained
    into the response stream at the point the connector made them.
    """

    def __init__(self, initial: dict[str, Any] | None) -> None:
        self._initial = initial
        self.pending: list[dict[str, Any]] = []

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        return self._initial

    async def save_cursor(
        self,
        stream_name: str,
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
    ) -> None:
        self.pending.append(cursor)


def _cursor_json(cursor: dict[str, Any]) -> str:
    """Serialize a cursor-state dict for the wire.

    Tagged encoding round-trips ``datetime``/``date`` losslessly;
    ``default=str`` is the same last-resort the on-disk store applies to
    other non-JSON types (e.g. ``Decimal``).
    """
    return json.dumps(encode_cursor_state(cursor), default=str)


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
        source_config: dict[str, Any],
    ) -> None:
        self._readable = readable
        self._runtime = runtime
        self._source_config = source_config
        # The connector's declared error taxonomy (issue #401), restored
        # from the resolved payload the bootstrap carried. A declared fact
        # classifies a driver failure with zero connector Python; None
        # keeps the typed-error ladder (additive absence).
        self._error_map = error_map_for(runtime)

    async def ReadStream(
        self,
        request: ReadRequest,
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[ReadResponse]:
        initial = (
            decode_cursor_state(json.loads(request.initial_cursor_json))
            if request.initial_cursor_json
            else None
        )
        partition = json.loads(request.partition_json) if request.partition_json else {}
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
                # Async generators are pull-based: a connector that calls
                # save_cursor AFTER its yield for batch N runs that save only
                # when batch N+1 is requested, so N's cursor_save drains here
                # one batch late. Safe under the at-least-once upsert
                # contract (a stale cursor re-reads, never loses data); the
                # trailing drain below catches the final save.
                for cursor in relay.pending:
                    yield ReadResponse(
                        cursor_save=CursorSave(cursor_json=_cursor_json(cursor))
                    )
                relay.pending.clear()
        except (
            Exception
        ) as exc:  # noqa: BLE001 — every failure crosses as a typed event
            deterministic, declared = classify_read_error(exc, self._error_map)
            logger.error(
                "source worker read failed (%s, deterministic=%s, "
                "classified by %s): %s",
                type(exc).__name__,
                deterministic,
                f"declared category {declared}" if declared else "type ladder",
                exc,
                exc_info=True,
            )
            yield ReadResponse(
                error=ReadErrorMsg(
                    message=str(exc),
                    deterministic=deterministic,
                    error_type=type(exc).__name__,
                    declared_category=declared or "",
                )
            )
            return
        # Trailing saves after the generator finished (e.g. final checkpoint).
        for cursor in relay.pending:
            yield ReadResponse(cursor_save=CursorSave(cursor_json=_cursor_json(cursor)))
        relay.pending.clear()
        yield ReadResponse(
            complete=ReadComplete(
                total_records=total_records, total_batches=total_batches
            )
        )
