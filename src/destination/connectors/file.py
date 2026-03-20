"""File destination handler for writing to local filesystem or S3.

This handler writes records to files using configurable formatters and storage backends.
"""

import logging
from typing import Any, Dict, List, Optional

from ..base_handler import BaseDestinationHandler, BatchWriteResult
from ..formatters import get_formatter
from ..formatters.base import BaseFormatter
from ..storage import get_storage_backend
from ..storage.base import BaseStorageBackend
from ..idempotency.manifest import ManifestTracker
from ...grpc.generated.analitiq.v1 import (
    AckStatus,
    Cursor,
    SchemaMessage,
)
from ...shared.connection_runtime import ConnectionRuntime


logger = logging.getLogger(__name__)


class FileDestinationHandler(BaseDestinationHandler):
    """
    Destination handler that writes records to files.

    Supports:
    - Multiple storage backends (local, s3)
    - Multiple output formats (jsonl, csv, parquet)
    - Manifest-based idempotency tracking
    - Configurable file paths with partitioning

    Configuration:
    - connector_type: "file" or "s3"
    - file_format: Output format (jsonl, csv, parquet). Default: jsonl
    - path: Base path for files (required for local storage)
    - bucket: S3 bucket name (required for S3 storage)
    - prefix: S3 key prefix (optional)
    - path_template: Template for file paths with placeholders
    """

    def __init__(self) -> None:
        """Initialize the file handler."""
        self._runtime: ConnectionRuntime | None = None
        self._storage: BaseStorageBackend | None = None
        self._formatter: BaseFormatter | None = None
        self._manifest: ManifestTracker | None = None
        self._config: Dict[str, Any] = {}
        self._connector_type: str = "file"
        self._connected: bool = False
        self._path_template: str | None = None

    @property
    def connector_type(self) -> str:
        """Return the connector type identifier."""
        return self._connector_type

    @property
    def supports_transactions(self) -> bool:
        """File destinations do not support transactions."""
        return False

    @property
    def supports_upsert(self) -> bool:
        """File destinations do not support upsert."""
        return False

    @property
    def supports_bulk_load(self) -> bool:
        """File destinations support bulk writes."""
        return True

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """
        Initialize the file handler with configuration.

        Args:
            runtime: ConnectionRuntime with enriched config
        """
        runtime.acquire()
        await runtime.materialize()
        connection_config = runtime.resolved_config
        self._config = connection_config
        self._connector_type = connection_config.get("connector_type", "file")
        self._runtime = runtime

        # Determine storage backend type
        storage_type = "local" if self._connector_type == "file" else self._connector_type

        # Create storage backend
        self._storage = get_storage_backend(storage_type)
        await self._storage.connect(connection_config)

        # Create formatter
        file_format = connection_config.get("file_format", "jsonl")
        self._formatter = get_formatter(file_format)

        # Configure formatter with any format-specific options
        formatter_config = connection_config.get("formatter_config", {})
        self._formatter.configure(formatter_config)

        # Determine base path
        base_path = connection_config.get("path", "")
        if self._connector_type == "s3":
            base_path = connection_config.get("prefix", "")

        # Create manifest tracker for idempotency
        self._manifest = ManifestTracker(self._storage, base_path)
        await self._manifest.load()

        # Store path template if provided
        self._path_template = connection_config.get("path_template")

        self._connected = True
        logger.info(
            f"FileDestinationHandler connected: "
            f"storage={storage_type}, format={file_format}"
        )

    async def disconnect(self) -> None:
        """Disconnect the file handler."""
        if self._storage and self._connected:
            await self._storage.disconnect()
        if self._runtime:
            await self._runtime.close()
        self._connected = False
        logger.info("FileDestinationHandler disconnected")

    async def configure_schema(self, schema_msg: SchemaMessage) -> bool:
        """
        Configure schema for file output.

        For file destinations, schema configuration is used to:
        - Configure formatter with schema information
        - No actual schema creation needed

        Args:
            schema_msg: Schema configuration

        Returns:
            Always True for file destinations
        """
        # Store schema for formatter use (e.g., CSV headers, Parquet schema)
        # The schema is available in schema_msg.json_schema
        logger.info("FileDestinationHandler: Schema accepted")
        return True

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
        Write a batch of records to a file.

        Args:
            run_id: Pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Batch sequence number
            records: Records to write
            record_ids: Record identifiers
            cursor: Cursor to return on success

        Returns:
            BatchWriteResult with status
        """
        if not self._connected:
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        if self._storage is None or self._formatter is None or self._manifest is None:
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler components not initialized",
            )

        # Check idempotency - has this batch already been committed?
        existing_commit = await self._manifest.check_committed(run_id, stream_id, batch_seq)
        if existing_commit:
            logger.info(
                f"Batch already committed: run={run_id}, stream={stream_id}, seq={batch_seq}"
            )
            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_ALREADY_COMMITTED,
                records_written=existing_commit.records_written,
                committed_cursor=Cursor(token=existing_commit.cursor_bytes),
            )

        if not records:
            # Empty batch - still record the commit for idempotency
            await self._manifest.record_commit(
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
                records_written=0,
                cursor_bytes=cursor.token,
                file_path="",
            )
            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=0,
                committed_cursor=cursor,
            )

        try:
            # Build file path
            base_path = self._config.get("path", "") or self._config.get("prefix", "")
            file_path = self._storage.build_path(
                base_path=base_path,
                stream_id=stream_id,
                batch_seq=batch_seq,
                extension=self._formatter.file_extension,
                partition_template=self._path_template,
            )

            # Serialize records
            data = self._formatter.serialize_batch(records)

            # Write to storage
            written_path = await self._storage.write_file(
                path=file_path,
                data=data,
                content_type=self._formatter.content_type,
            )

            # Record commit in manifest
            await self._manifest.record_commit(
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
                records_written=len(records),
                cursor_bytes=cursor.token,
                file_path=written_path,
            )

            logger.info(
                f"Wrote batch {batch_seq}: {len(records)} records, "
                f"{len(data)} bytes to {written_path}"
            )

            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=len(records),
                committed_cursor=cursor,
            )

        except Exception as e:
            logger.error(f"Error writing batch: {e}")
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(e),
            )

    async def health_check(self) -> bool:
        """
        Check if file destination is healthy.

        Returns:
            True if storage is accessible
        """
        if not self._connected or self._storage is None:
            return False

        return await self._storage.health_check()
