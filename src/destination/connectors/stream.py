"""Stream destination handler for stdout output.

This handler writes records to stdout, useful for testing and debugging.
It does not implement idempotency since stdout is not persistent.
"""

import logging
import sys
from typing import Any, Dict, List

from ..base_handler import BaseDestinationHandler, BatchWriteResult
from ..formatters import get_formatter
from ..formatters.base import BaseFormatter
from ...grpc.generated.analitiq.v1 import (
    AckStatus,
    Cursor,
    SchemaMessage,
)
from ...shared.connection_runtime import ConnectionRuntime


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
        self._config: Dict[str, Any] = {}
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

    async def configure_schema(self, schema_msg: SchemaMessage) -> bool:
        """
        Configure schema for stream output.

        For stdout, schema configuration is always successful since
        we don't need to create any structures.

        Args:
            schema_msg: Schema configuration (ignored for stdout)

        Returns:
            Always True
        """
        logger.info("StreamDestinationHandler: Schema accepted (no configuration needed)")
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
        Write a batch of records to stdout.

        Args:
            run_id: Pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Batch sequence number
            records: Records to write
            record_ids: Record identifiers (for logging)
            cursor: Cursor to return on success

        Returns:
            BatchWriteResult with SUCCESS status
        """
        if not self._connected or self._formatter is None:
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        if not records:
            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=0,
                committed_cursor=cursor,
            )

        try:
            # Serialize and write to stdout
            data = self._formatter.serialize_batch(records)
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()

            logger.debug(
                f"Wrote batch {batch_seq} to stdout: "
                f"{len(records)} records, {len(data)} bytes"
            )

            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=len(records),
                committed_cursor=cursor,
            )

        except Exception as e:
            logger.error(f"Error writing to stdout: {e}")
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(e),
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
        except Exception:
            return False
