"""Abstract base class for destination handlers.

Destination handlers implement the logic for writing data to specific
destination types (PostgreSQL, MySQL, APIs, etc.). The gRPC server
delegates all data operations to these handlers.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Mapping

import pyarrow as pa

from .types import BatchWriteResult, Cursor, SchemaSpec

# ``BatchWriteResult``, ``Cursor`` and ``SchemaSpec`` are CDK-native value types
# (``cdk.types``), decoupled from the gRPC messages this module once imported —
# the CDK must not depend on ``src/grpc`` (ADR §4.1). The engine's gRPC server
# translates protobuf <-> these types at the wire boundary. ``BatchWriteResult``
# is re-exported here because handlers/tests import it as
# ``from cdk.base_handler import BaseDestinationHandler, BatchWriteResult``.
__all__ = ["BaseDestinationHandler", "BatchWriteResult"]


class BaseDestinationHandler(ABC):
    """
    Abstract base class for all destination handlers.

    Implementations must handle:
    1. Connection management (connect/disconnect)
    2. Schema configuration (auto-create tables, indexes, etc.)
    3. Batch writing with idempotency
    4. Cursor storage for checkpointing

    Idempotency Contract:
    - Handlers must track (run_id, stream_id, batch_seq) in a metadata table
    - Duplicate batches should return ALREADY_COMMITTED with stored cursor
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

    @abstractmethod
    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: List[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        """Write a batch of records to the destination.

        Idempotency Requirements:
        - Must check if (run_id, stream_id, batch_seq) was already committed
        - If already committed: return ALREADY_COMMITTED with stored cursor
        - If new: write records atomically, store cursor, return SUCCESS
        - On failure: return RETRYABLE_FAILURE or FATAL_FAILURE

        Args:
            run_id: Unique pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Monotonically increasing batch sequence number
            record_batch: Records as a ``pa.RecordBatch``. Arrow IPC is
                the only supported wire format.
            record_ids: Stable identifiers for each record (for DLQ correlation)
            cursor: Opaque cursor representing max watermark in batch

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
    def supports_upsert(self) -> bool:
        """Whether this destination supports upsert operations."""
        return True

    @property
    def supports_bulk_load(self) -> bool:
        """Whether this destination supports bulk loading (COPY, LOAD DATA, etc.)."""
        return False

    @property
    def max_batch_size(self) -> int:
        """Maximum recommended batch size for this destination."""
        return 5000

    @property
    def max_batch_bytes(self) -> int:
        """Maximum recommended batch size in bytes."""
        return 8 * 1024 * 1024  # 8MB
