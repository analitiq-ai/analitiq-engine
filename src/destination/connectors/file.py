"""File destination handler for writing to the local filesystem.

This handler writes records to files using configurable formatters and a
local storage backend.
"""

import errno
import hashlib
import logging
from typing import Any

import pyarrow as pa

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import AckStatus, Cursor, RetrySemantics, RetryVerdict, SchemaSpec

from ..formatters import get_formatter
from ..formatters.base import BaseFormatter
from ..idempotency.manifest import ManifestTracker
from ..storage import get_storage_backend
from ..storage.base import BaseStorageBackend

logger = logging.getLogger(__name__)


class FileDestinationHandler(BaseDestinationHandler):
    """
    Destination handler that writes records to files.

    Supports:
    - Multiple storage backends (local, s3)
    - Multiple output formats (jsonl, csv, parquet)
    - Manifest-based idempotency tracking
    - Configurable file paths with partitioning

    The storage backend follows the runtime's connector kind ("file" or
    "s3"). Configuration:
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
        self._config: dict[str, Any] = {}
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

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """File replay safety does not hold across a restart (issue #286).

        The manifest dedups by batch position (run_id, stream_id,
        batch_seq), which is sound for an in-run replay of the same batch
        but not for a same-run restart: the source resumes from the
        committed cursor while batch_seq restarts, so a committed position
        can re-arrive carrying different rows and be skipped as a replay
        (the row-drop class of issue #282). File overwrites on restart are
        prevented by the content hash in the filename (issue #319), but
        manifest dedup by batch position still drops new rows. Until the
        manifest keys on content (issue #306), the honest claim is that a
        restart is not replay-safe.
        """
        _ = stream_id
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
            reason=(
                "the manifest dedups by batch position, and a same-run "
                "restart re-numbers re-batched rows; a committed position "
                "carrying different rows would be skipped as a replay"
            ),
        )

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """
        Initialize the file handler with configuration.

        Args:
            runtime: ConnectionRuntime with enriched config
        """
        runtime.acquire()
        await runtime.materialize()
        connection_config = runtime.resolved_config
        # The kind lives on the runtime (resolved from the connector
        # definition), not in the connection config — an s3 connection's
        # JSON carries no "connector_type" key.
        self._connector_type = runtime.connector_type
        self._runtime = runtime

        try:
            # Determine storage backend type
            storage_type = (
                "local" if self._connector_type == "file" else self._connector_type
            )

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

            # Retain only the non-secret fields needed after connect().
            # write_batch() uses path or prefix (as fallback) for build_path().
            # _path_template is stored separately above.
            self._config = {
                "path": connection_config.get("path", ""),
                "prefix": connection_config.get("prefix", ""),
            }
        finally:
            runtime.scrub_resolved_config()

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

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        """Accept the schema for a stream.

        File destinations don't pre-create anything; the formatter shapes
        each batch on write. If a formatter ever needs the column list, it
        can look up the contract endpoint via ``set_stream_endpoints``.
        """
        logger.info(
            "FileDestinationHandler: schema accepted for stream %s",
            schema_spec.stream_id,
        )
        return True

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        """Write an Arrow record batch to a file.

        Formatters consume dicts, so the batch is materialized once at
        this boundary.
        """
        if not self._connected:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        if self._storage is None or self._formatter is None or self._manifest is None:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler components not initialized",
            )

        try:
            records = record_batch.to_pylist()

            existing_commit = await self._manifest.check_committed(
                run_id, stream_id, batch_seq
            )
            if existing_commit:
                logger.info(
                    f"Batch already committed: run={run_id}, stream={stream_id}, "
                    f"seq={batch_seq}"
                )
                return BatchWriteResult(
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
                    status=AckStatus.ACK_STATUS_SUCCESS,
                    records_written=0,
                    committed_cursor=cursor,
                )

            # Serialize before building path so the filename includes a content
            # hash — prevents same-batch_seq overwrites on restart (issue #319).
            data = self._formatter.serialize_batch(records)

            if not data:
                # A non-empty records list that serialized to empty bytes is a
                # formatter contract violation; writing a zero-byte file and
                # committing records_written=N would silently drop all N rows
                # (issue #322). Fail loud so the batch routes to the DLQ.
                msg = (
                    f"{type(self._formatter).__name__}.serialize_batch() "
                    f"returned empty bytes for {len(records)} records "
                    f"(run={run_id}, stream={stream_id}, seq={batch_seq})"
                )
                logger.error(msg)
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=0,
                    failure_summary=msg,
                )

            content_hash = hashlib.sha256(data).hexdigest()[:16]

            # Build file path
            base_path = self._config.get("path", "") or self._config.get("prefix", "")
            file_path = self._storage.build_path(
                base_path=base_path,
                stream_id=stream_id,
                batch_seq=batch_seq,
                content_hash=content_hash,
                extension=self._formatter.file_extension,
                partition_template=self._path_template,
            )

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
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=len(records),
                committed_cursor=cursor,
            )

        except OSError as e:
            # ENOSPC / EACCES / EROFS / EDQUOT are not transient — retrying
            # without operator intervention is hopeless. Classify as FATAL
            # so the engine routes to DLQ instead of looping.
            errno_label = (
                errno.errorcode.get(e.errno, str(e.errno))
                if e.errno is not None
                else "unknown"
            )
            fatal_errnos = {errno.ENOSPC, errno.EACCES, errno.EROFS, errno.EDQUOT}
            if e.errno in fatal_errnos:
                logger.error(
                    "Fatal filesystem error writing batch "
                    "(run=%s, stream=%s, seq=%s, errno=%s): %s",
                    run_id,
                    stream_id,
                    batch_seq,
                    errno_label,
                    e,
                    exc_info=True,
                )
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=0,
                    failure_summary=f"OSError[{errno_label}]: {e}",
                )
            logger.error(
                "Retryable I/O error writing batch (run=%s, stream=%s, seq=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=f"OSError[{errno_label}]: {e}",
            )
        except Exception as e:
            logger.error(
                "Fatal error writing batch (run=%s, stream=%s, seq=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"{type(e).__name__}: {e}",
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
