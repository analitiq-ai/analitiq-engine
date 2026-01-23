"""Abstract base class for destination handlers.

Destination handlers implement the logic for writing data to specific
destination types (PostgreSQL, MySQL, APIs, etc.). The gRPC server
delegates all data operations to these handlers.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..grpc.generated.analitiq.v1 import (
    AckStatus,
    Cursor,
    SchemaMessage,
)


@dataclass
class BatchWriteResult:
    """Result of writing a batch to the destination."""

    success: bool
    status: AckStatus
    records_written: int
    committed_cursor: Optional[Cursor] = None
    failed_record_ids: List[str] = field(default_factory=list)
    failure_summary: str = ""


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

    @abstractmethod
    async def connect(self, connection_config: Dict[str, Any]) -> None:
        """
        Establish connection to the destination.

        Args:
            connection_config: Connection parameters (host, port, credentials, etc.)

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
    async def configure_schema(self, schema_msg: SchemaMessage) -> bool:
        """
        Configure destination schema based on SchemaMessage.

        This may involve:
        - Creating schemas/databases
        - Creating tables
        - Creating indexes
        - Setting up constraints

        Args:
            schema_msg: Schema configuration from engine

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
        records: List[Dict[str, Any]],
        record_ids: List[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        """
        Write a batch of records to the destination.

        Idempotency Requirements:
        - Must check if (run_id, stream_id, batch_seq) was already committed
        - If already committed: return ALREADY_COMMITTED with stored cursor
        - If new: write records atomically, store cursor, return SUCCESS
        - On failure: return RETRYABLE_FAILURE or FATAL_FAILURE

        Args:
            run_id: Unique pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Monotonically increasing batch sequence number
            records: List of record dictionaries to write
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
