"""Stream destination handler for stdout output.

This handler writes records to stdout, useful for testing and debugging.
It does not implement idempotency since stdout is not persistent.
"""

import logging
import sys
from typing import Any

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult, LandingBatch
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import AckStatus, RetrySemantics, RetryVerdict, SchemaSpec

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

    def not_ready_reason(self, stream_id: str) -> str | None:
        """Report what stdout is still missing: a live handler and its formatter."""
        _ = stream_id
        if not self._connected:
            return "Handler not connected"
        if self._formatter is None:
            return "Handler components not initialized: formatter"
        return None

    async def land(self, batch: LandingBatch) -> int:
        """Serialize the records and write them to stdout.

        ``emitted_at`` is part of the contract for time-partitioned sinks;
        stdout has no output path, so it goes unread here.
        """
        assert self._formatter is not None  # not_ready_reason checked it
        data = self._formatter.serialize_batch(batch.records)
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()
        logger.debug(
            "Wrote batch %s to stdout: %s records, %s bytes",
            batch.batch_seq,
            len(batch.records),
            len(data),
        )
        return len(batch.records)

    def unexpected_write_failure(
        self,
        error: Exception,
        *,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> BatchWriteResult:
        """Name the formatter: it is the pluggable part that can fail (#328)."""
        logger.error(
            "Fatal error writing to stdout "
            "(run=%s, stream=%s, seq=%s, formatter=%s): %s",
            run_id,
            stream_id,
            batch_seq,
            type(self._formatter).__name__,
            error,
            exc_info=True,
        )
        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            failure_summary=f"{type(error).__name__}: {error}",
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
