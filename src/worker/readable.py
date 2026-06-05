"""Engine-side worker Readable.

``WorkerReadable`` implements the ``Readable`` contract the engine's
extract stage consumes, but runs no connector code in the engine process:
each ``read_batches`` call resolves the connection into a bootstrap,
spawns a source worker, and streams the worker's ``ReadStream`` responses
— Arrow IPC batches are yielded to the engine, ``cursor_save`` events are
persisted into the engine's checkpoint store (which never leaves the
engine), and a terminal ``error`` event re-raises with the worker's
retryability classification.
"""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import grpc
import pyarrow as pa

from cdk.connection_runtime import ConnectionRuntime
from cdk.sql.exceptions import ReadError
from cdk.types import CheckpointStore

from src.grpc import DEFAULT_MAX_MESSAGE_SIZE
from src.grpc.generated.analitiq.v1.source_service_pb2 import ReadRequest
from src.grpc.generated.analitiq.v1.source_service_pb2_grpc import SourceServiceStub
from src.state.store import decode_cursor_state, encode_cursor_state
from src.worker.shell import build_bootstrap
from src.worker.spawn import spawn_worker

logger = logging.getLogger(__name__)


def _decode_arrow_ipc(payload: bytes) -> pa.RecordBatch:
    with pa.ipc.open_stream(io.BytesIO(payload)) as reader:
        table = reader.read_all()
    if table.num_rows == 0:
        return pa.RecordBatch.from_pylist([], schema=table.schema)
    return table.combine_chunks().to_batches()[0]


class WorkerReadable:
    """Reads a stream through an isolated source worker."""

    def __init__(self, *, connectors_dir: Path, connections_dir: Path) -> None:
        self._connectors_dir = connectors_dir
        self._connections_dir = connections_dir

    async def read_batches(
        self,
        runtime: ConnectionRuntime,
        config: Dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        partition = partition or {}
        # The bootstrap must be JSON-safe; the runtime object is passed
        # separately to build_bootstrap and never embedded in source_config.
        source_config = config
        label = f"src-worker:{runtime.connector_id}:{stream_name}"

        initial_cursor = await checkpoint.get_cursor(stream_name, partition)

        bootstrap = await build_bootstrap(
            runtime,
            role="source",
            connectors_dir=self._connectors_dir,
            connections_dir=self._connections_dir,
            source_config=source_config,
        )
        handle = await spawn_worker(bootstrap, label=label)
        try:
            # Limits must match the worker server's, or batches between the
            # default 4MB and the server's ceiling die here as
            # RESOURCE_EXHAUSTED.
            channel = grpc.aio.insecure_channel(
                handle.target,
                options=[
                    ("grpc.max_send_message_length", DEFAULT_MAX_MESSAGE_SIZE),
                    ("grpc.max_receive_message_length", DEFAULT_MAX_MESSAGE_SIZE),
                ],
            )
            try:
                stub = SourceServiceStub(channel)
                request = ReadRequest(
                    stream_name=stream_name,
                    partition_json=json.dumps(partition),
                    batch_size=batch_size,
                    # Tagged encoding: a persisted timestamp cursor comes
                    # back from the store as a datetime and must survive
                    # the JSON hop as one.
                    initial_cursor_json=(
                        json.dumps(
                            encode_cursor_state(initial_cursor), default=str
                        )
                        if initial_cursor
                        else ""
                    ),
                )
                completed = False
                async for response in stub.ReadStream(request):
                    kind = response.WhichOneof("message")
                    if kind == "batch":
                        yield _decode_arrow_ipc(response.batch.payload)
                    elif kind == "cursor_save":
                        await checkpoint.save_cursor(
                            stream_name,
                            partition,
                            decode_cursor_state(
                                json.loads(response.cursor_save.cursor_json)
                            ),
                        )
                    elif kind == "error":
                        err = response.error
                        if err.deterministic:
                            raise ReadError(
                                f"{err.error_type}: {err.message} "
                                f"(worker {label}, deterministic)"
                            )
                        raise RuntimeError(
                            f"{err.error_type}: {err.message} (worker {label})"
                        )
                    elif kind == "complete":
                        logger.info(
                            "%s complete: %d records / %d batches",
                            label,
                            response.complete.total_records,
                            response.complete.total_batches,
                        )
                        completed = True
                        break
                if not completed:
                    # Stream ended without a terminal event: the worker died
                    # mid-read. Retryable — the engine's retry machinery owns it.
                    raise RuntimeError(
                        f"{label}: worker stream ended without complete/error "
                        f"(worker crash?)"
                    )
            finally:
                await channel.close()
        finally:
            await handle.close()
            # Match the in-process lifecycle: the source role owns its
            # runtime reference for the duration of the read.
            await runtime.close()
