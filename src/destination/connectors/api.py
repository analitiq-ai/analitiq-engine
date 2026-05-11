"""API destination handler for writing records to REST APIs.

This handler sends records to REST API endpoints with support for
rate limiting, retries, and different batch modes.
"""

import json
import logging
from typing import Any, Dict, List, Mapping, Optional

import aiohttp
import pyarrow as pa
from aiohttp_retry import ExponentialRetry, RetryClient

from ..base_handler import BaseDestinationHandler, BatchWriteResult
from ...grpc.generated.analitiq.v1 import (
    AckStatus,
    Cursor,
    SchemaMessage,
)
from ...shared.connection_runtime import ConnectionRuntime
from ...shared.rate_limiter import RateLimiter


logger = logging.getLogger(__name__)


class ApiDestinationHandler(BaseDestinationHandler):
    """
    Destination handler that writes records to REST APIs.

    Supports:
    - Multiple batch modes: single, bulk, batch
    - Rate limiting with configurable limits
    - Retry with exponential backoff
    - Multiple authentication methods

    Configuration (connection config):
    - host: Base URL for the API
    - headers: HTTP headers (including auth headers)
    - timeout: Request timeout in seconds (default: 30)
    - rate_limit: Optional rate limiting config
      - max_requests: Max requests per time window
      - time_window: Time window in seconds

    Configuration (endpoint config via SchemaMessage):
    - endpoint: API endpoint path (e.g., /v1/records)
    - method: HTTP method (default: POST)
    - batch_mode: single, bulk, or batch (default: single)
    - batch_size: Records per batch for batch mode (default: 100)
    """

    # Batch modes
    BATCH_MODE_SINGLE = "single"  # POST each record individually
    BATCH_MODE_BULK = "bulk"  # POST all records in one request
    BATCH_MODE_BATCH = "batch"  # POST records in chunks

    def __init__(self) -> None:
        """Initialize the API handler."""
        self._runtime: ConnectionRuntime | None = None
        self._session: RetryClient | None = None
        self._config: Dict[str, Any] = {}
        self._connected: bool = False

        # Connection settings
        self._base_url: str = ""
        self._timeout: int = 30

        # Rate limiter
        self._rate_limiter: RateLimiter | None = None

        # Endpoint settings, derived from the contract endpoint document
        # at configure_schema() time.
        self._endpoint: str = ""
        self._method: str = "POST"
        self._batch_mode: str = self.BATCH_MODE_SINGLE
        self._batch_size: int = 100

        # stream_id -> contract API endpoint document. Populated by
        # set_stream_endpoints() at startup so configure_schema() can read
        # operations.write.* directly from the contract.
        self._stream_endpoints: Dict[str, Dict[str, Any]] = {}

        # Retry settings - statuses that trigger retry with exponential backoff
        # 429 (rate limit), 500, 503, 504 (server errors) get up to 3 attempts
        # 502 and other 4xx errors are not retried (single attempt)
        self._retry_statuses: set = {429, 500, 503, 504}

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Register stream_id → contract API endpoint document. The handler
        reads ``operations.write.request.{path,method}`` and batching hints
        from the document at ``configure_schema`` time.
        """
        self._stream_endpoints = {
            sid: dict(doc) for sid, doc in stream_endpoints.items()
        }

    @property
    def connector_type(self) -> str:
        """Return the connector type identifier."""
        return "api"

    @property
    def supports_transactions(self) -> bool:
        """APIs don't support transactions."""
        return False

    @property
    def supports_upsert(self) -> bool:
        """Upsert depends on API implementation."""
        return False

    @property
    def supports_bulk_load(self) -> bool:
        """APIs support bulk mode."""
        return True

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """
        Establish connection to the API using ConnectionRuntime.

        Args:
            runtime: ConnectionRuntime with enriched config
        """
        self._runtime = runtime
        runtime.acquire()
        await runtime.materialize()
        self._base_url = runtime.base_url
        self._rate_limiter = runtime.rate_limiter
        self._max_retries = runtime.raw_config.get("max_retries", 3)

        # Wrap plain session with retry (destination-specific)
        retry_options = ExponentialRetry(
            attempts=self._max_retries,
            statuses=self._retry_statuses,
        )
        self._session = RetryClient(
            client_session=runtime.session,
            retry_options=retry_options,
        )

        self._connected = True
        logger.info(f"ApiDestinationHandler connected to {self._base_url}")

    async def disconnect(self) -> None:
        """Close API connection."""
        if self._session:
            await self._session.close()
        if self._runtime:
            # runtime.close() is idempotent — safe even if RetryClient already closed the session
            await self._runtime.close()
        self._connected = False
        logger.info("ApiDestinationHandler disconnected")

    async def configure_schema(self, schema_msg: SchemaMessage) -> bool:
        """Configure the API endpoint from the preloaded contract document.

        Reads ``operations.write.request.{path,method}`` and the optional
        ``operations.write.batching`` block from the contract API endpoint
        document keyed by ``schema_msg.stream_id``.
        """
        stream_id = schema_msg.stream_id
        endpoint_doc = self._stream_endpoints.get(stream_id)
        if endpoint_doc is None:
            logger.error(
                "No preloaded endpoint document for stream_id=%r; "
                "call set_stream_endpoints() before the gRPC server starts",
                stream_id,
            )
            return False

        operations = endpoint_doc.get("operations") or {}
        write = operations.get("write") or {}
        request = write.get("request") or {}
        path = request.get("path") or ""
        if not path:
            logger.error(
                "API endpoint document for stream %r is missing "
                "operations.write.request.path",
                stream_id,
            )
            return False

        self._endpoint = path
        self._method = (request.get("method") or "POST").upper()

        batching = write.get("batching") or {}
        b_mode = (batching.get("mode") or "single").lower()
        if b_mode == "bulk":
            self._batch_mode = self.BATCH_MODE_BULK
        elif b_mode == "batch":
            self._batch_mode = self.BATCH_MODE_BATCH
        else:
            self._batch_mode = self.BATCH_MODE_SINGLE
        self._batch_size = int(batching.get("size") or 100)

        logger.info(
            "API schema configured: endpoint=%s, method=%s, batch_mode=%s",
            self._endpoint,
            self._method,
            self._batch_mode,
        )
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
        """Write an Arrow record batch to the API.

        HTTP request bodies are dict-shaped, so the batch is materialized
        once at this boundary.
        """
        if not self._session or not self._connected:
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        records = record_batch.to_pylist()

        if not records:
            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=0,
                committed_cursor=cursor,
            )

        try:
            # Write records based on batch mode
            if self._batch_mode == self.BATCH_MODE_SINGLE:
                written = await self._write_single_mode(records, record_ids)
            elif self._batch_mode == self.BATCH_MODE_BULK:
                written = await self._write_bulk_mode(records)
            else:  # batch mode
                written = await self._write_batch_mode(records)

            logger.info(f"API wrote batch {batch_seq}: {written}/{len(records)} records")

            # Determine success based on how many records were written
            if written == 0:
                # All records failed - this is a failure
                return BatchWriteResult(
                    success=False,
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=0,
                    failure_summary=f"All {len(records)} records failed to write to API",
                )
            elif written < len(records):
                # Partial success - some records failed
                # Use FATAL_FAILURE since retrying would duplicate successful records
                failed_count = len(records) - written
                return BatchWriteResult(
                    success=False,
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=written,
                    failure_summary=f"{failed_count}/{len(records)} records failed to write to API",
                    committed_cursor=cursor,
                )
            else:
                # All records succeeded
                return BatchWriteResult(
                    success=True,
                    status=AckStatus.ACK_STATUS_SUCCESS,
                    records_written=written,
                    committed_cursor=cursor,
                )

        except Exception as e:
            logger.error(f"Error writing to API: {e}")
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(e),
            )

    async def _write_single_mode(
        self,
        records: List[Dict[str, Any]],
        record_ids: List[str],
    ) -> int:
        """Write records one at a time."""
        written = 0
        failed_ids = []

        for i, record in enumerate(records):
            try:
                await self._send_request(record)
                written += 1
            except Exception as e:
                logger.warning(f"Failed to write record {record_ids[i]}: {e}")
                failed_ids.append(record_ids[i])

        if failed_ids:
            logger.warning(f"Failed to write {len(failed_ids)} records: {failed_ids[:5]}...")

        return written

    async def _write_bulk_mode(self, records: List[Dict[str, Any]]) -> int:
        """Write all records in a single request."""
        await self._send_request(records)
        return len(records)

    async def _write_batch_mode(self, records: List[Dict[str, Any]]) -> int:
        """Write records in batches."""
        written = 0

        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]
            await self._send_request(batch)
            written += len(batch)

        return written

    async def _send_request(self, data: Any) -> Dict[str, Any]:
        """
        Send HTTP request with rate limiting.

        Retry logic is handled by RetryClient (configured in connect()):
        - 429, 500, 503, 504: Retry up to max_retries with exponential backoff
        - 502 and other 4xx: Single attempt, no retry

        Args:
            data: Request body (single record or list of records)

        Returns:
            Response JSON

        Raises:
            aiohttp.ClientResponseError: On non-success status
        """
        if not self._session:
            raise RuntimeError("Session not initialized")

        # Apply rate limiting
        if self._rate_limiter:
            await self._rate_limiter.acquire()

        url = f"{self._base_url}{self._endpoint}"

        async with self._session.request(
            method=self._method,
            url=url,
            json=data,
        ) as response:
            # Check for non-success status
            if response.status >= 400:
                text = await response.text()
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=f"API error {response.status}: {text[:200]}",
                )

            # Try to parse JSON response
            try:
                return await response.json()
            except Exception:
                return {"status": "ok", "status_code": response.status}

    async def health_check(self) -> bool:
        """
        Check if API is reachable.

        Returns:
            True if API responds to a simple request
        """
        if not self._session or not self._connected:
            return False

        try:
            # Try a simple GET to the base URL
            async with self._session.get(self._base_url) as response:
                return response.status < 500
        except Exception as e:
            logger.warning(f"API health check failed: {e}")
            return False

    def _mask_sensitive_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Mask sensitive header values for logging."""
        sensitive_keys = {"authorization", "x-api-key", "api-key", "token", "bearer"}
        masked = {}
        for key, value in headers.items():
            if key.lower() in sensitive_keys:
                if len(value) > 14:
                    masked[key] = value[:14] + "***MASKED***"
                else:
                    masked[key] = "***MASKED***"
            else:
                masked[key] = value
        return masked
