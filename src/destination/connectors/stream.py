"""Stream destination handler for stdout output.

This handler writes records to stdout, useful for testing and debugging.
It does not implement idempotency since stdout is not persistent.
"""

import errno
import logging
import sys
from typing import Any

import pyarrow as pa

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import AckStatus, Cursor, RetrySemantics, RetryVerdict, SchemaSpec

from ..formatters import get_formatter
from ..formatters.base import BaseFormatter

logger = logging.getLogger(__name__)


class StreamDestinationHandler(BaseDestinationHandler):
    """
    Destination handler that writes records to stdout.

    This is the simplest handler, useful for:
    - Testing pipeline configurations
    - Debugging data flow
    - Quick validation of source extractions

    Note: This handler does NOT implement idempotency.
    Replaying batches will result in duplicate output.
    """

    def __init__(self) -> None:
        """Initialize the stream handler."""
        self._runtime: ConnectionRuntime | None = None
        self._formatter: BaseFormatter | None = None
        self._config: dict[str, Any] = {}
        self._connected: bool = False

    @property
    def connector_type(self) -> str:
        """Return the connector type identifier."""
        return "stdout"

    @property
    def supports_transactions(self) -> bool:
        """Stdout does not support transactions."""
        return False

    @property
    def supports_upsert(self) -> bool:
        """Stdout does not support upsert."""
        return False

    @property
    def supports_bulk_load(self) -> bool:
        """Stdout does not support bulk load."""
        return False

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Stdout has no dedup by construction (issue #286)."""
        _ = stream_id
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
            reason="stdout only prints; a replayed batch prints its records again",
        )

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """
        Initialize the stream handler with configuration.

        Args:
            runtime: ConnectionRuntime with enriched config
        """
        self._runtime = runtime
        runtime.acquire()
        await runtime.materialize()
        connection_config = runtime.resolved_config

        try:
            # Get format from config, default to jsonl
            file_format = connection_config.get("file_format", "jsonl")

            # Create formatter
            self._formatter = get_formatter(file_format)

            # Configure formatter with any format-specific options
            formatter_config = connection_config.get("formatter_config", {})
            self._formatter.configure(formatter_config)

            # Do not retain resolved config — it may contain secrets and is
            # never read after connect(). Scrub is in finally to cover failures.
            self._config = {}
        finally:
            runtime.scrub_resolved_config()

        self._connected = True
        logger.info(f"StreamDestinationHandler connected with format: {file_format}")

    async def disconnect(self) -> None:
        """
        Disconnect the stream handler.

        For stdout, this just flushes any pending output.
        """
        if self._connected:
            sys.stdout.flush()
        if self._runtime:
            await self._runtime.close()
        self._connected = False
        logger.info("StreamDestinationHandler disconnected")

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        """
        Configure schema for stream output.

        For stdout, schema configuration is always successful since
        we don't need to create any structures.

        Args:
            schema_spec: Schema configuration (ignored for stdout)

        Returns:
            Always True
        """
        logger.info(
            "StreamDestinationHandler: Schema accepted (no configuration needed)"
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
        """Write an Arrow record batch to stdout.

        The stdout formatter consumes dicts, so the batch is materialized
        once at this boundary.
        """
        if not self._connected or self._formatter is None:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        try:
            records = record_batch.to_pylist()

            if not records:
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_SUCCESS,
                    records_written=0,
                    committed_cursor=cursor,
                )

            # Serialize and write to stdout
            data = self._formatter.serialize_batch(records)
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()

            logger.debug(
                f"Wrote batch {batch_seq} to stdout: "
                f"{len(records)} records, {len(data)} bytes"
            )

            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=len(records),
                committed_cursor=cursor,
            )

        except OSError as e:
            # Closed/broken stdout (EPIPE), permissions, disk-full on a
            # redirected stream — none are recoverable by retry.
            errno_code = (
                errno.errorcode.get(e.errno, str(e.errno))
                if e.errno is not None
                else "unknown"
            )
            fatal_errnos = {errno.EPIPE, errno.ENOSPC, errno.EACCES, errno.EBADF}
            status = (
                AckStatus.ACK_STATUS_FATAL_FAILURE
                if e.errno in fatal_errnos
                else AckStatus.ACK_STATUS_RETRYABLE_FAILURE
            )
            logger.error(
                "%s I/O error writing to stdout: %s",
                "Fatal"
                if status == AckStatus.ACK_STATUS_FATAL_FAILURE
                else "Retryable",
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=status,
                records_written=0,
                failure_summary=(f"OSError[{errno_code}]: {e}"),
            )
        except Exception as e:
            logger.error("Fatal error writing to stdout: %s", e, exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"{type(e).__name__}: {e}",
            )

    async def health_check(self) -> bool:
        """
        Check if stdout is available.

        Returns:
            True if connected and stdout is writable
        """
        if not self._connected:
            return False

        try:
            # Check if stdout is writable
            return sys.stdout.writable()
        except (ValueError, OSError) as e:
            logger.warning(
                "stdout health check failed: %s: %s",
                type(e).__name__,
                e,
                exc_info=True,
            )
            return False
