"""Destination-side worker proxy.

``WorkerProxyHandler`` is the handler the destination shell's TCP gRPC
server wraps. It runs no connector code itself: ``connect`` resolves the
connection into a bootstrap, spawns the connector worker, and opens a
control channel to it over the worker's Unix domain socket; every
subsequent handler call is forwarded over that socket — the existing
``DestinationService`` contract, reused verbatim on the local hop.

The cross-container TCP channel stays exactly as before (no credentials);
the worker received its resolved values once, at launch, on stdin.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import pyarrow as pa

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import AckStatus, Cursor, SchemaSpec, WriteMode

from src.grpc.client import DestinationGRPCClient
from src.grpc.generated.analitiq.v1 import Cursor as ProtoCursor
from src.worker.shell import build_bootstrap
from src.worker.spawn import WorkerHandle, spawn_worker

logger = logging.getLogger(__name__)

_WRITE_MODE_NAMES = {
    WriteMode.WRITE_MODE_INSERT: "insert",
    WriteMode.WRITE_MODE_UPSERT: "upsert",
    WriteMode.WRITE_MODE_TRUNCATE_INSERT: "truncate_insert",
}


class WorkerProxyHandler(BaseDestinationHandler):
    """Forwards the destination handler contract to a connector worker."""

    def __init__(self, *, connectors_dir: Path, connections_dir: Path) -> None:
        self._connectors_dir = connectors_dir
        self._connections_dir = connections_dir
        self._endpoint_refs: Dict[str, Any] = {}
        self._stream_endpoints: Dict[str, Any] = {}
        self._handle: Optional[WorkerHandle] = None
        self._control: Optional[DestinationGRPCClient] = None
        # One forwarded StreamRecords stream per stream_id.
        self._streams: Dict[str, DestinationGRPCClient] = {}
        self._capabilities: Optional[Any] = None
        self._label = "dest-worker"

    # ------------------------------------------------------------------
    # Contract endpoints registration (kept shell-side AND forwarded via
    # the bootstrap so the worker has them before connect()).
    # ------------------------------------------------------------------

    def set_endpoint_refs(self, endpoint_refs: Mapping[str, Any]) -> None:
        self._endpoint_refs = dict(endpoint_refs)

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        self._stream_endpoints = {k: dict(v) for k, v in stream_endpoints.items()}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, runtime: ConnectionRuntime) -> None:
        self._label = f"dest-worker:{runtime.connector_id}"
        bootstrap = await build_bootstrap(
            runtime,
            role="destination",
            connectors_dir=self._connectors_dir,
            connections_dir=self._connections_dir,
            endpoint_refs=self._endpoint_refs,
            stream_endpoints=self._stream_endpoints,
        )
        self._handle = await spawn_worker(bootstrap, label=self._label)
        control = DestinationGRPCClient(target=self._handle.target)
        if not await control.connect(max_connect_retries=3):
            await self._teardown()
            raise ConnectionError(
                f"{self._label}: worker spawned but its channel did not connect"
            )
        self._control = control
        self._capabilities = await control.get_capabilities()
        if self._capabilities is None:
            await self._teardown()
            raise ConnectionError(
                f"{self._label}: worker did not answer GetCapabilities"
            )
        logger.info(
            "%s ready (connector_type=%s, upsert=%s)",
            self._label,
            self._capabilities.connector_type,
            self._capabilities.supports_upsert,
        )

    async def disconnect(self) -> None:
        await self._teardown()

    async def _teardown(self) -> None:
        for stream_id, client in list(self._streams.items()):
            try:
                await client.disconnect()
            except Exception:
                logger.warning(
                    "%s: stream client %s close failed", self._label, stream_id,
                    exc_info=True,
                )
        self._streams.clear()
        if self._control is not None:
            try:
                await self._control.send_shutdown(reason="shell_disconnect")
            except Exception:
                logger.debug("%s: worker shutdown rpc failed", self._label, exc_info=True)
            try:
                await self._control.disconnect()
            except Exception:
                logger.debug("%s: control close failed", self._label, exc_info=True)
            self._control = None
        if self._handle is not None:
            await self._handle.close()
            self._handle = None

    # ------------------------------------------------------------------
    # Forwarded contract
    # ------------------------------------------------------------------

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        if self._handle is None:
            logger.error("%s: configure_schema before connect", self._label)
            return False
        stream_id = schema_spec.stream_id
        client = DestinationGRPCClient(target=self._handle.target)
        if not await client.connect(max_connect_retries=3):
            return False
        accepted = await client.start_stream(
            run_id="",  # idempotency keys ride each forwarded batch
            stream_id=stream_id,
            schema_config={
                "write_mode": _WRITE_MODE_NAMES[schema_spec.write_mode],
                "schema_version": schema_spec.version,
            },
        )
        if not accepted:
            await client.disconnect()
            return False
        self._streams[stream_id] = client
        return True

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: List[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        client = self._streams.get(stream_id)
        if client is None:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="worker stream not configured",
            )
        # The servicer hands the handler the CDK cursor; the forwarding
        # client builds proto messages — convert at the boundary.
        proto_cursor = ProtoCursor(token=cursor.token if cursor else b"")
        result = await client.send_batch(
            run_id, stream_id, batch_seq, record_batch, record_ids, proto_cursor
        )
        committed = (
            Cursor(token=result.committed_cursor.token)
            if result.committed_cursor is not None
            else None
        )
        status = AckStatus(int(result.status))
        if result.transport_failure:
            # The worker died (or its UDS stream closed) before an ACK —
            # the connector never rendered a verdict. A worker crash is
            # retryable by design: the idempotency table resolves a
            # committed-before-crash batch on resend. Fatal is reserved
            # for verdicts the connector actually returned.
            status = AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        return BatchWriteResult(
            status=status,
            records_written=result.records_written,
            committed_cursor=committed,
            failed_record_ids=tuple(result.failed_record_ids),
            failure_summary=result.failure_summary,
        )

    async def health_check(self) -> bool:
        if self._control is None:
            return False
        return await self._control.health_check()

    # ------------------------------------------------------------------
    # Capabilities (cached from the worker at connect)
    # ------------------------------------------------------------------

    @property
    def connector_type(self) -> str:
        if self._capabilities is not None:
            return self._capabilities.connector_type
        return "unknown"

    @property
    def supports_transactions(self) -> bool:
        return bool(self._capabilities and self._capabilities.supports_transactions)

    @property
    def supports_upsert(self) -> bool:
        return bool(self._capabilities and self._capabilities.supports_upsert)

    @property
    def supports_bulk_load(self) -> bool:
        return bool(self._capabilities and self._capabilities.supports_bulk_load)

    @property
    def max_batch_size(self) -> int:
        if self._capabilities is not None and self._capabilities.max_batch_size:
            return self._capabilities.max_batch_size
        return super().max_batch_size

    @property
    def max_batch_bytes(self) -> int:
        if self._capabilities is not None and self._capabilities.max_batch_bytes:
            return self._capabilities.max_batch_bytes
        return super().max_batch_bytes
