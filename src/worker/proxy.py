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
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult, reject_batch
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import (
    SUCCESS_STATUSES,
    AckStatus,
    Cursor,
    RetrySemantics,
    RetryVerdict,
    SchemaSpec,
    WriteMode,
)
from src.destination.server import SHUTDOWN_REASON_SUCCESS
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
        """Wire the shell-side proxy with the config directories it forwards."""
        self._connectors_dir = connectors_dir
        self._connections_dir = connections_dir
        self._endpoint_refs: dict[str, Any] = {}
        self._stream_endpoints: dict[str, Any] = {}
        self._stream_conflict_keys: dict[str, list[str]] = {}
        self._handle: WorkerHandle | None = None
        self._control: DestinationGRPCClient | None = None
        # One forwarded StreamRecords stream per stream_id. The client caches
        # its own start_stream params and self-heals after a transport teardown,
        # so the proxy no longer tracks schema config for rebuilds.
        self._streams: dict[str, DestinationGRPCClient] = {}
        # Retry-safety verdict per stream, captured from the worker's
        # SchemaAck so the shell's engine-facing ack re-reports the
        # worker's real verdict (issue #286).
        self._retry_verdicts: dict[str, RetryVerdict] = {}
        self._capabilities: Any | None = None
        self._label = "dest-worker"
        # Reason the most recent configure_schema returned False, forwarded
        # from the worker's SchemaAck. The destination service reads it so the
        # engine-facing ack carries the worker's real reason (issue #231)
        # instead of a generic "Schema configuration failed".
        self.last_schema_rejection: str | None = None
        # Terminal-run outcome, set by ``finalize_run`` (called from the shell
        # server's Shutdown handler) and forwarded to the worker at teardown so
        # the worker only prunes its idempotency ledger on a successful run.
        # Defaults to False so an abnormal teardown (no Shutdown received)
        # leaves the ledger intact.
        self._run_succeeded = False

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

    def set_stream_conflict_keys(
        self, stream_conflict_keys: Mapping[str, list[str]]
    ) -> None:
        """Record stream_id -> upsert conflict keys for the worker bootstrap."""
        self._stream_conflict_keys = {
            k: list(v) for k, v in stream_conflict_keys.items()
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """Spawn the connector worker and open its control channel."""
        self._label = f"dest-worker:{runtime.connector_id}"
        bootstrap = await build_bootstrap(
            runtime,
            role="destination",
            connectors_dir=self._connectors_dir,
            connections_dir=self._connections_dir,
            endpoint_refs=self._endpoint_refs,
            stream_endpoints=self._stream_endpoints,
            stream_conflict_keys=self._stream_conflict_keys,
        )
        self._handle = await spawn_worker(bootstrap, label=self._label)
        control = DestinationGRPCClient(target=self._handle.target)
        if not await control.connect(max_connect_retries=3):
            await self._teardown()
            raise ConnectionError(
                f"{self._label}: worker spawned but its channel did not connect"
            )
        self._control = control
        try:
            self._capabilities = await control.get_capabilities()
        except Exception as e:
            # get_capabilities raises on a transport/RPC failure (it no longer
            # collapses to None). Tear the spawned worker down before surfacing
            # so a failed handshake never leaks the subprocess.
            await self._teardown()
            raise ConnectionError(f"{self._label}: GetCapabilities failed") from e
        logger.info(
            "%s ready (connector_type=%s, upsert=%s)",
            self._label,
            self._capabilities.connector_type,
            self._capabilities.supports_upsert,
        )

    async def finalize_run(self, *, succeeded: bool) -> None:
        # The shell has no ledger of its own; just record the outcome so
        # ``_teardown`` can forward it to the worker, which does the pruning.
        self._run_succeeded = succeeded

    async def disconnect(self) -> None:
        await self._teardown()

    async def _teardown(self) -> None:
        for stream_id, client in list(self._streams.items()):
            try:
                await client.disconnect()
            except Exception:
                logger.warning(
                    "%s: stream client %s close failed",
                    self._label,
                    stream_id,
                    exc_info=True,
                )
        self._streams.clear()
        self._retry_verdicts.clear()
        if self._control is not None:
            # Forward the terminal-run outcome as the worker's shutdown reason
            # so it prunes its idempotency ledger only on a successful run.
            reason = (
                SHUTDOWN_REASON_SUCCESS if self._run_succeeded else "shell_disconnect"
            )
            try:
                await self._control.send_shutdown(reason=reason)
            except Exception:
                logger.debug(
                    "%s: worker shutdown rpc failed", self._label, exc_info=True
                )
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
        self.last_schema_rejection = None
        if self._handle is None:
            logger.error("%s: configure_schema before connect", self._label)
            self.last_schema_rejection = "destination worker not started"
            return False
        stream_id = schema_spec.stream_id
        schema_config = {
            "write_mode": _WRITE_MODE_NAMES[schema_spec.write_mode],
            "schema_version": schema_spec.version,
            # Forward the engine-stamped ack budget so the worker derives its
            # statement timeout from the budget the engine actually waits on
            # (issue #234). _open_stream also adopts it as the UDS client's
            # own ack wait, so the worker hop honors the engine's budget
            # instead of this container's env default.
            "ack_timeout_seconds": schema_spec.ack_timeout_seconds,
        }
        client = await self._open_stream(stream_id, schema_config)
        if client is None:
            return False
        self._streams[stream_id] = client
        # Re-report the worker's retry-safety verdict on the shell hop
        # (issue #286). Drop any verdict from an earlier configure of this
        # stream first, so a reconfigure can never serve a stale one. Every
        # conforming worker stamps a specified verdict on an accepted ack;
        # an unspecified one is a worker defect — warn and fall through to
        # the base default rather than report it as the worker's claim.
        self._retry_verdicts.pop(stream_id, None)
        verdict = client.stream_retry_semantics
        if verdict is not None and verdict[0] != int(
            RetrySemantics.RETRY_SEMANTICS_UNSPECIFIED
        ):
            self._retry_verdicts[stream_id] = RetryVerdict(
                semantics=RetrySemantics(verdict[0]),
                reason=verdict[1],
            )
        else:
            logger.warning(
                "%s: accepted schema ack for stream %s carried no "
                "retry-safety verdict; reporting the at-least-once default",
                self._label,
                stream_id,
            )
        return True

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Return the worker's verdict captured from its SchemaAck (#286)."""
        verdict = self._retry_verdicts.get(stream_id)
        if verdict is None:
            return super().retry_semantics(stream_id)
        return verdict

    async def _open_stream(
        self, stream_id: str, schema_config: dict[str, Any]
    ) -> DestinationGRPCClient | None:
        """Open a fresh forwarded StreamRecords stream and send its schema.

        Returns the connected client, or None if the worker channel did not
        connect or rejected the schema. The caller owns caching the result.
        """
        if self._handle is None:
            logger.error("%s: open_stream before connect", self._label)
            return None
        # The forwarded hop adopts the engine-stamped ack budget as its own
        # wait: the engine is the only real waiter behind this hop, so waiting
        # any less would abandon a worker statement the engine still has
        # budget for, and any more would outwait the engine. The stamp the
        # client puts on the forwarded schema message then mins to the same
        # value, so the worker's statement bound tracks the engine budget
        # end-to-end (issue #234).
        client = DestinationGRPCClient(
            target=self._handle.target,
            timeout_seconds=schema_config["ack_timeout_seconds"],
        )
        if not await client.connect(max_connect_retries=3):
            self.last_schema_rejection = "destination worker channel did not connect"
            return None
        accepted = await client.start_stream(
            run_id="",  # idempotency keys ride each forwarded batch
            stream_id=stream_id,
            schema_config=schema_config,
        )
        if not accepted:
            # Forward the worker's real rejection reason so the engine-facing
            # ack is not the generic "Schema configuration failed" (issue #231).
            self.last_schema_rejection = client.schema_rejection_message
            await client.disconnect()
            return None
        return client

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        client = self._streams.get(stream_id)
        if client is None:
            return reject_batch(
                logger,
                "worker stream not configured",
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
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
            # for verdicts the connector actually returned. (An ACK timeout
            # already reports retryable; this also remaps the reader/writer
            # task-failure and peer-close transport failures, which still
            # carry a fatal status.)
            #
            # No rebuild here: the cached client self-heals on the engine's
            # next send_batch using the params it cached at start_stream, so
            # the retryable ACK returns immediately without blocking on a
            # rebuild inside this call.
            status = AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        if committed is not None and status not in SUCCESS_STATUSES:
            # The ack crossed an untrusted process boundary: a connector
            # that pairs a failure status with a cursor violates the
            # BatchWriteResult contract. Drop the cursor (a failed batch
            # must never advance the checkpoint) but keep the connector's
            # verdict and failure_summary — letting the constructor raise
            # here would abort the whole stream and lose both.
            logger.error(
                "%s: worker sent committed_cursor on a failure ack "
                "(stream_id=%s, batch_seq=%s, status=%r); dropping the "
                "cursor to protect the checkpoint",
                self._label,
                stream_id,
                batch_seq,
                status,
            )
            committed = None
        return BatchWriteResult(
            status=status,
            records_written=result.records_written,
            committed_cursor=committed,
            failed_record_ids=tuple(result.failed_record_ids),
            failure_summary=result.failure_summary,
            # Already bounds-checked (and zeroed on success) by the client's
            # _process_ack on this untrusted hop; forward like the summary.
            failure_category=result.failure_category,
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
            connector_type: str = self._capabilities.connector_type
            return connector_type
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
    def supports_auto_create(self) -> bool:
        return bool(self._capabilities and self._capabilities.supports_auto_create)

    @property
    def supports_truncate(self) -> bool:
        # The wire response advertises truncate through the write-mode list,
        # not a dedicated bool; mirror exactly what the worker advertised so
        # the proxy and the underlying connector cannot drift.
        if not self._capabilities:
            return False
        return (
            WriteMode.WRITE_MODE_TRUNCATE_INSERT
            in self._capabilities.supported_write_modes
        )

    @property
    def max_batch_size(self) -> int:
        if self._capabilities is not None and self._capabilities.max_batch_size:
            max_batch_size: int = self._capabilities.max_batch_size
            return max_batch_size
        return super().max_batch_size

    @property
    def max_batch_bytes(self) -> int:
        if self._capabilities is not None and self._capabilities.max_batch_bytes:
            max_batch_bytes: int = self._capabilities.max_batch_bytes
            return max_batch_bytes
        return super().max_batch_bytes
