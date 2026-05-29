"""API destination handler for writing records to REST APIs.

This handler sends records to REST API endpoints with support for
rate limiting, retries, and different batch modes.
"""

import asyncio
import base64
import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Set

import aiohttp
import orjson
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


# Proto WriteMode int -> contract operations.write.<mode> key. The
# api-endpoint contract's ``operations.write`` is a closed map keyed by
# mode name (v1 keys: ``insert``, ``upsert``); the destination handler
# must dispatch to the block matching the stream's write_mode.
_API_WRITE_MODE_KEYS: Dict[int, str] = {
    1: "insert",
    2: "upsert",
}


def _endpoint_write_mode_block(
    endpoint_doc: Mapping[str, Any], mode_key: str
) -> Optional[Dict[str, Any]]:
    """Return the ``operations.write.<mode_key>`` block when it is a usable
    request definition, else ``None``.

    A block is usable when it is a dict carrying a non-empty
    ``request.path``. This is the single acceptance predicate for an API
    write mode: ``supports_upsert`` (capability advertisement) and
    ``configure_schema`` (per-stream dispatch) both go through it, so
    advertised capability and runtime dispatch cannot diverge.
    """
    operations = endpoint_doc.get("operations") or {}
    write = operations.get("write") or {}
    mode_block = write.get(mode_key)
    if not isinstance(mode_block, dict):
        return None
    request = mode_block.get("request") or {}
    if not request.get("path"):
        return None
    return mode_block


def _orjson_default(obj: Any) -> Any:
    """orjson default-hook for types it does not natively serialise.

    orjson handles ``datetime`` / ``date`` / ``time`` / ``UUID`` /
    dataclasses / enums / numpy scalars directly — only ``Decimal`` and
    ``bytes`` reach this hook. ``Decimal`` is rendered as a string to
    preserve precision (most APIs accept string-or-number for numeric
    fields); ``bytes`` is base64-encoded per JSON convention.
    """
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(obj)).decode("ascii")
    raise TypeError(
        f"orjson cannot serialise {type(obj).__name__}; add a handler "
        f"if this type should appear in API destination bodies"
    )


def _decode_json_fields(
    records: List[Dict[str, Any]], json_fields: Set[str]
) -> None:
    """Reverse the source-side ``json.dumps`` on Json columns in place.

    Malformed input raises ``ValueError`` with the offending column and
    row. ``write_batch`` catches it in its broad ``except Exception``
    branch and returns ``ACK_STATUS_FATAL_FAILURE`` — bad JSON is a
    data-shape failure, not a transport one, so retrying cannot help.
    """
    if not json_fields:
        return
    for row, record in enumerate(records):
        for col in json_fields:
            value = record.get(col)
            if not isinstance(value, str):
                continue
            try:
                record[col] = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Json field {col!r} at row {row}: value is not "
                    f"valid JSON ({exc})"
                ) from exc


def _collect_json_fields(mode_block: Mapping[str, Any]) -> Set[str]:
    """Body field names declared with ``arrow_type: "Json"``.

    Handles both shapes the api-endpoint contract permits:
    JSON-Schema-style ``input.schema.properties`` (most common) and the
    flat ``input.schema.columns`` array.
    """
    schema = (mode_block.get("input") or {}).get("schema") or {}
    names: Set[str] = set()
    for name, prop in (schema.get("properties") or {}).items():
        if isinstance(prop, dict) and prop.get("arrow_type") == "Json":
            names.add(name)
    for col in schema.get("columns") or []:
        if isinstance(col, dict) and col.get("arrow_type") == "Json":
            col_name = col.get("name")
            if col_name:
                names.add(col_name)
    return names


@dataclass
class _StreamState:
    """Per-stream API destination state.

    The handler instance is shared across every stream writing to this
    destination; per-stream values (endpoint path, HTTP method, batching
    hints) must live here keyed by ``stream_id`` so concurrent
    ``configure_schema`` calls do not clobber each other.
    """

    endpoint: str = ""
    method: str = "POST"
    batch_mode: str = "single"
    batch_size: int = 100
    # Names of body fields declared with ``arrow_type: "Json"`` in the
    # endpoint's write input schema. The wire carries them as
    # JSON-encoded strings (so they fit a ``pa.large_string`` column);
    # the handler must ``json.loads`` them before aiohttp serializes the
    # body, otherwise the API receives a quoted string instead of a
    # nested object.
    json_fields: Set[str] = field(default_factory=set)


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

    Per-stream endpoint settings (path, method, batch mode, batch size)
    are read from the preloaded contract API endpoint document at
    ``configure_schema`` time, keyed by ``operations.write.<mode>``. The
    SchemaMessage off the wire only carries ``stream_id``, ``version``,
    and ``write_mode``.
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

        # Per-stream endpoint settings derived from the contract document
        # at configure_schema() time. Keyed by stream_id so concurrent
        # streams sharing this handler do not race shared state.
        self._streams: Dict[str, _StreamState] = {}

        # stream_id -> contract API endpoint document. Populated by
        # set_stream_endpoints() at startup so configure_schema() can read
        # operations.write.<mode>.* directly from the contract.
        self._stream_endpoints: Dict[str, Dict[str, Any]] = {}

        # HTTP statuses that trigger retry with exponential backoff; the
        # attempt count comes from ``runtime.raw_config["max_retries"]`` at
        # connect() time. Everything else (4xx other than 429, and 5xx
        # other than 500/503/504 such as 502) is single-attempt.
        self._retry_statuses: set = {429, 500, 503, 504}

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Register stream_id → contract API endpoint document. The handler
        reads ``operations.write.<mode>.request.{path,method}`` and
        optional ``operations.write.<mode>.batching`` from the document
        at ``configure_schema`` time, where ``<mode>`` matches the
        stream's ``write.mode``.
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
        """True when any registered endpoint declares an upsert write
        operation (``operations.write.upsert``) in its contract.

        Upsert capability is contract-driven, never hardcoded: the
        api-endpoint document owns whether an endpoint can upsert.
        ``GetCapabilities`` advertises a single connector-wide boolean,
        so the handler reports upsert support when at least one
        registered endpoint declares it; ``configure_schema`` still
        rejects an individual stream whose own endpoint lacks the
        upsert block.
        """
        return any(
            _endpoint_write_mode_block(doc, "upsert") is not None
            for doc in self._stream_endpoints.values()
        )

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

        ``operations.write`` is a closed map keyed by write mode in the
        api-endpoint contract; dispatch into the block matching the
        stream's ``write.mode`` and read its ``request.{path,method}``
        plus optional ``batching`` block.
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

        mode_key = _API_WRITE_MODE_KEYS.get(schema_msg.write_mode)
        if mode_key is None:
            logger.error(
                "API destination does not support write_mode=%s for stream %r; "
                "valid api-endpoint modes are %s",
                schema_msg.write_mode,
                stream_id,
                sorted(_API_WRITE_MODE_KEYS.values()),
            )
            return False

        mode_block = _endpoint_write_mode_block(endpoint_doc, mode_key)
        if mode_block is None:
            logger.error(
                "API endpoint document for stream %r does not define a usable "
                "operations.write.%s block (needs a dict with request.path)",
                stream_id,
                mode_key,
            )
            return False

        request = mode_block.get("request") or {}
        state = _StreamState(
            endpoint=request["path"],
            method=(request.get("method") or "POST").upper(),
            json_fields=_collect_json_fields(mode_block),
        )

        batching = mode_block.get("batching") or {}
        b_mode = (batching.get("mode") or "single").lower()
        if b_mode == "bulk":
            state.batch_mode = self.BATCH_MODE_BULK
        elif b_mode == "batch":
            state.batch_mode = self.BATCH_MODE_BATCH
        else:
            state.batch_mode = self.BATCH_MODE_SINGLE
        state.batch_size = int(batching.get("size") or 100)

        self._streams[stream_id] = state
        logger.info(
            "API schema configured for stream %r: %s %s, batch_mode=%s",
            stream_id,
            state.method,
            state.endpoint,
            state.batch_mode,
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
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        state = self._streams.get(stream_id)
        if state is None:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Schema not configured",
            )

        # Materialise once, stay row-oriented. Arrow-native Python types
        # (``datetime`` / ``Decimal`` / ``date`` / ``time``) survive into
        # the records dict — ``orjson`` handles them at the serialisation
        # boundary (natively for datetime/date/time, via the default-hook
        # for Decimal/bytes). Pre-casting in Arrow space would be a
        # second pass for no gain.
        records = record_batch.to_pylist()

        if not records:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=0,
                committed_cursor=cursor,
            )

        try:
            _decode_json_fields(records, state.json_fields)
            if state.batch_mode == self.BATCH_MODE_SINGLE:
                written = await self._write_single_mode(state, records, record_ids)
            elif state.batch_mode == self.BATCH_MODE_BULK:
                written = await self._write_bulk_mode(state, records)
            else:
                written = await self._write_batch_mode(state, records)

            logger.info(f"API wrote batch {batch_seq}: {written}/{len(records)} records")

            if written == 0:
                # All records failed - this is a failure
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=0,
                    failure_summary=f"All {len(records)} records failed to write to API",
                )
            elif written < len(records):
                # Partial success - some records failed
                # Use FATAL_FAILURE since retrying would duplicate successful records
                failed_count = len(records) - written
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=written,
                    failure_summary=f"{failed_count}/{len(records)} records failed to write to API",
                    committed_cursor=cursor,
                )
            else:
                # All records succeeded
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_SUCCESS,
                    records_written=written,
                    committed_cursor=cursor,
                )

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error("Transport error writing to API: %s", e, exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=f"{type(e).__name__}: {e}",
            )
        except Exception as e:
            logger.error("Fatal error writing to API: %s", e, exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"{type(e).__name__}: {e}",
            )

    async def _write_single_mode(
        self,
        state: _StreamState,
        records: List[Dict[str, Any]],
        record_ids: List[str],
    ) -> int:
        """Write records one at a time."""
        written = 0
        failed_ids = []

        for i, record in enumerate(records):
            try:
                await self._send_request(state, record)
                written += 1
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # Transport-level errors are per-record data issues; log
                # the cause and move on so the batch reports partial
                # success. Programming errors (KeyError, TypeError, …)
                # propagate to write_batch where they become FATAL.
                logger.warning(
                    "Failed to write record %s: %s: %s",
                    record_ids[i], type(e).__name__, e,
                )
                failed_ids.append(record_ids[i])

        if failed_ids:
            logger.warning(f"Failed to write {len(failed_ids)} records: {failed_ids[:5]}...")

        return written

    async def _write_bulk_mode(
        self, state: _StreamState, records: List[Dict[str, Any]]
    ) -> int:
        """Write all records in a single request."""
        await self._send_request(state, records)
        return len(records)

    async def _write_batch_mode(
        self, state: _StreamState, records: List[Dict[str, Any]]
    ) -> int:
        """Write records in batches."""
        written = 0

        for i in range(0, len(records), state.batch_size):
            batch = records[i : i + state.batch_size]
            await self._send_request(state, batch)
            written += len(batch)

        return written

    async def _send_request(self, state: _StreamState, data: Any) -> Dict[str, Any]:
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

        # Preserve both the base URL's path (e.g. ``/api/v1``) and the
        # endpoint's path; ``urljoin``-style behavior would drop the
        # base's path when the endpoint starts with ``/``.
        url = self._base_url.rstrip("/") + "/" + state.endpoint.lstrip("/")

        # ``aiohttp.request(json=...)`` would call stdlib ``json.dumps``
        # which doesn't understand ``datetime`` / ``Decimal``. orjson is
        # C-based, handles ``datetime`` natively, and falls through to
        # the explicit default-hook only for ``Decimal`` / ``bytes`` —
        # one C-speed pass instead of a separate Python or Arrow cast.
        body = orjson.dumps(data, default=_orjson_default)

        async with self._session.request(
            method=state.method,
            url=url,
            data=body,
            headers={"Content-Type": "application/json"},
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

            try:
                return await response.json(content_type=None)
            except (json.JSONDecodeError, aiohttp.ContentTypeError, UnicodeDecodeError) as err:
                body = (await response.text())[:500]
                raise aiohttp.ClientPayloadError(
                    f"API returned status {response.status} with non-JSON body: "
                    f"{err}; body[:500]={body!r}"
                ) from err

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
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                "API health check failed: %s: %s",
                type(e).__name__, e, exc_info=True,
            )
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
