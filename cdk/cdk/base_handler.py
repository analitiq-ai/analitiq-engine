"""Abstract base class for destination handlers.

Destination handlers implement the logic for writing data to specific
destination types (PostgreSQL, MySQL, APIs, etc.). The gRPC server
delegates all data operations to these handlers.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from .types import (
    AckStatus,
    BatchWriteResult,
    Cursor,
    FailureCategory,
    RetrySemantics,
    RetryVerdict,
    SchemaSpec,
)

if TYPE_CHECKING:
    from .connection_runtime import ConnectionRuntime

# ``BatchWriteResult``, ``Cursor`` and ``SchemaSpec`` are CDK-native value types
# (``cdk.types``), decoupled from the gRPC messages this module once imported —
# the CDK must not depend on ``src/grpc`` (ADR §4.1). The engine's gRPC server
# translates protobuf <-> these types at the wire boundary. ``BatchWriteResult``
# is re-exported here because handlers/tests import it as
# ``from cdk.base_handler import BaseDestinationHandler, BatchWriteResult``.
__all__ = ["BaseDestinationHandler", "BatchWriteResult", "reject_batch"]


def reject_batch(
    logger: logging.Logger,
    reason: str,
    *,
    run_id: str,
    stream_id: str,
    batch_seq: int,
    status: AckStatus = AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
) -> BatchWriteResult:
    """Log and build the result for a batch rejected before any write.

    A ``write_batch`` pre-flight guard rejects without raising, so the
    rejecting process -- the destination -- recorded nothing at all. The
    reason only ever surfaced one hop away, in the engine's retry warning
    (``src/engine/engine.py``), which names neither the handler nor which
    guard fired. Logging here puts the rejection in the log of the process
    that made it (issue #327).

    Routing every guard through one function keeps the log and the returned
    status in step, and means a new guard cannot be added without one. It
    also stamps the NOT_READY failure category once for every guard: a
    pre-flight rejection by definition attempted nothing, and the engine
    must be able to tell that apart from a write the destination actually
    rejected (issue #351).

    *logger* is the calling handler's module logger, so the record names the
    handler that rejected the batch.
    """
    logger.error(
        "write_batch rejected: %s (run=%s, stream=%s, seq=%s)",
        reason,
        run_id,
        stream_id,
        batch_seq,
    )
    return BatchWriteResult(
        status=status,
        records_written=0,
        failure_summary=reason,
        failure_category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
    )


class BaseDestinationHandler(ABC):
    """
    Abstract base class for all destination handlers.

    Implementations must handle:
    1. Connection management (connect/disconnect)
    2. Schema configuration (auto-create tables, indexes, etc.)
    3. Batch writing with idempotency
    4. Cursor storage for checkpointing

    Idempotency Contract:
    - A retried or replayed batch must not duplicate or drop rows; the
      mechanism is the handler's (a SQL handler dedups on the write mode's
      keys -- MERGE on conflict_keys, or the synthetic _record_hash for a
      keyless insert; a file handler content-addresses each batch file so
      a replay overwrites the same bytes)
    - A handler that detects a prior commit may return ALREADY_COMMITTED with
      the stored cursor; a sink that writes idempotently returns SUCCESS
    - All writes within a batch must be atomic (all-or-nothing)
    """

    def set_endpoint_refs(self, endpoint_refs: Mapping[str, Any]) -> None:
        """Register the ``stream_id → endpoint_ref`` index for this handler.

        ``endpoint_refs`` values are dict-shape ``EndpointRef`` payloads
        (``{"scope", "connection_id", "endpoint_id"}`` plus optional ``x-*``
        extension keys). Called once by the
        destination entrypoint before the gRPC server starts. The default
        implementation is a no-op; handlers that need per-stream endpoint
        context (e.g. picking a type-mapper by scope) override it.
        """
        _ = endpoint_refs  # no-op default

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Register the ``stream_id → contract endpoint document`` index.

        The destination loads its configuration with the same
        ``PipelineConfigPrep`` as the engine, so the contract endpoint
        document (database object, columns, primary keys, API operations,
        …) is already on disk by the time ``configure_schema`` fires for
        an incoming :class:`~cdk.types.SchemaSpec`. Handlers read from this
        map instead of unpacking the spec.

        Called once by the destination entrypoint before the gRPC server
        starts. Default is a no-op; handlers that need the document
        override it.
        """
        _ = stream_endpoints  # no-op default

    # Template method: the no-op default ignores self so handlers can
    # override with instance state (same shape as its siblings above).
    def set_stream_conflict_keys(  # skipcq: PYL-R0201
        self, stream_conflict_keys: Mapping[str, list[str]]
    ) -> None:
        """Register the ``stream_id → upsert conflict keys`` index.

        The conflict keys are the stream's validated ``write.conflict_keys``,
        copied verbatim by the destination entrypoint. They are stream
        configuration, not part of the contract endpoint document, so they
        travel on their own channel instead of being smuggled into the
        document map. An absent or empty entry means INSERT mode — an
        upsert always carries an explicit conflict target under the
        contract.

        Called once by the destination entrypoint before the gRPC server
        starts. Default is a no-op; handlers that upsert override it.
        """
        _ = stream_conflict_keys  # no-op default

    def set_statement_timeout(self, seconds: float | None) -> None:
        """Bound each destination statement to *seconds*.

        Cancelling a statement that blocks lets the engine surface the real
        reason instead of a bare gRPC ACK timeout (issue #231).
        Called by the destination servicer on every schema handshake, before
        ``configure_schema``, with a value derived from the ack budget the
        sender stamped into the schema message (issue #234) — the bound
        always stays at or below the budget of every waiter on the path.
        ``None`` means unbounded. Default is a no-op; only SQL destinations
        honor it.
        """
        _ = seconds  # no-op default

    @abstractmethod
    async def connect(self, runtime: "ConnectionRuntime") -> None:
        """
        Establish connection to the destination.

        Args:
            runtime: ConnectionRuntime that manages connection lifecycle

        Raises:
            ConnectionError: If connection cannot be established
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close connection to the destination.

        Should be idempotent - safe to call multiple times.
        """
        pass

    async def finalize_run(self, *, succeeded: bool) -> None:
        """Release run-scoped state at the end of a run.

        Invoked from the destination server's ``Shutdown`` handler while the
        handler is still connected. The worker process is torn down (SIGTERM)
        before ``disconnect`` could run connection-bound cleanup, so anything
        needing the live connection at end-of-run belongs here. Default is a
        no-op; handlers with run-scoped state to release (e.g. a connector
        flushing a per-run buffer or temporary object) override it.

        ``succeeded`` is the engine's terminal-run outcome: ``True`` only when
        the pipeline finished successfully. Cleanup that would break a resume
        of a failed run (e.g. pruning the idempotency ledger) must run only
        when ``succeeded`` is ``True``. Best-effort: the server logs and
        swallows any error so teardown never fails.
        """
        return None

    @abstractmethod
    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        """
        Configure destination schema based on the SchemaSpec.

        This may involve:
        - Creating schemas/databases
        - Creating tables
        - Creating indexes
        - Setting up constraints

        Args:
            schema_spec: Schema identification from engine (CDK-native)

        Returns:
            True if schema configuration succeeded, False otherwise
        """
        pass

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Retry-safety verdict for a stream this handler has configured.

        Called by the destination servicer after an accepted
        ``configure_schema``; the verdict rides the SchemaAck so the engine
        can log, per stream, whether a same-run restart re-sends committed
        records (issue #286). The verdict is the handler's to make — write
        mode, keys, transport, and declared idempotency all factor in — so
        handlers override this per stream. The default is the only honest
        claim for a handler that declares nothing: at-least-once.
        """
        _ = stream_id
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
            reason=(
                "handler declares no retry-safety; a same-run restart may "
                "re-send already-committed records"
            ),
        )

    @abstractmethod
    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
        cursor: Cursor,
        emitted_at: datetime,
    ) -> BatchWriteResult:
        """Write a batch of records to the destination.

        Idempotency Requirements:
        - A retried/replayed batch must not duplicate or drop rows; dedup on
          row identity (the write mode's keys, or the synthetic _record_hash
          for a keyless insert), never the batch's position
        - Write records atomically (all-or-nothing), store the cursor, and
          return SUCCESS; a handler that detects a prior commit may instead
          return ALREADY_COMMITTED with the stored cursor
        - On failure: return RETRYABLE_FAILURE or FATAL_FAILURE

        Args:
            run_id: Unique pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Monotonically increasing batch sequence number
            record_batch: Records as a ``pa.RecordBatch``. Arrow IPC is
                the only supported wire format.
            record_ids: Stable identifiers for each record (for DLQ correlation)
            cursor: Opaque cursor representing max watermark in batch
            emitted_at: Timezone-aware UTC instant the engine emitted this
                batch, stamped once and identical across every retry of the
                same batch. A destination that renders time-based output
                paths (file/S3 partitioning) MUST derive the path from this
                value rather than its own wall clock, so a replayed batch
                resolves the same path instead of drifting across an
                hour/day boundary (issue #353). Sinks without time-based
                partitioning ignore it.

        Returns:
            BatchWriteResult with status, records written, and cursor
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if destination is healthy and ready to receive data.

        Returns:
            True if healthy, False otherwise
        """
        pass

    @property
    @abstractmethod
    def connector_type(self) -> str:
        """Return the connector type identifier (e.g., 'postgresql', 'mysql')."""
        pass

    @property
    def supports_transactions(self) -> bool:
        """Whether this destination supports transactions."""
        return True

    @property
    def supports_insert(self) -> bool:
        """Whether this destination can run plain-insert writes right now.

        True for every handler that can write at all; a handler whose
        insert path has runtime preconditions (the SQL stage-then-merge
        cycle needs declared capabilities and a stage-rendering dialect)
        overrides this so GetCapabilities never advertises a mode every
        stream of which would be refused at the schema handshake.
        """
        return True

    @property
    def supports_upsert(self) -> bool:
        """Whether this destination supports upsert operations."""
        return True

    @property
    def supports_bulk_load(self) -> bool:
        """Whether this destination supports bulk loading (COPY, LOAD DATA, etc.)."""
        return False

    @property
    def supports_auto_create(self) -> bool:
        """Whether this destination can create the target relation if absent.

        Auto-create is a relational/DDL capability; the neutral base cannot
        assume it (an API, file, or stdout destination has no schema to
        create), so it defaults False. A SQL handler that issues CREATE TABLE
        overrides this to True. Advertised capability must follow what the
        handler can actually do, never a constructor literal.
        """
        return False

    @property
    def supports_truncate(self) -> bool:
        """Report whether the truncate-insert (full-refresh) mode is supported.

        Only a destination that can truncate the target before insert
        advertises ``WRITE_MODE_TRUNCATE_INSERT``; the neutral base cannot, so
        it defaults False. A SQL handler that issues TRUNCATE overrides this to
        True.
        """
        return False

    @property
    def max_batch_size(self) -> int:
        """Maximum recommended batch size for this destination."""
        return 5000

    @property
    def max_batch_bytes(self) -> int:
        """Maximum recommended batch size in bytes."""
        return 8 * 1024 * 1024  # 8MB
