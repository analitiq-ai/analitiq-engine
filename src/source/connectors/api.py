"""API source connector.

Drives HTTP reads from a typed :class:`ResolvedSource` (with an
:class:`ApiReadEndpoint`). Pagination and replication are taken from the
endpoint definition; runtime context (session, base URL, rate limiter)
comes from the :class:`ConnectionRuntime` passed to ``connect()``.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import urljoin, urlencode

from .base import BaseConnector, ConnectionError, ReadError
from ...engine.resolved import (
    ApiReadEndpoint,
    CursorPagination,
    KeysetPagination,
    LinkPagination,
    OffsetPagination,
    PagePagination,
    ResolvedSource,
)
from ...models.state import CursorField, StreamCursor, StreamStats
from ...shared.connection_runtime import ConnectionRuntime
from ...shared.rate_limiter import RateLimiter
from ...state.state_manager import StateManager

logger = logging.getLogger(__name__)


class APIConnector(BaseConnector):
    """HTTP API source connector."""

    def __init__(self, name: str = "APIConnector"):
        super().__init__(name)
        self._runtime: ConnectionRuntime | None = None
        self.session = None
        self.base_url: str | None = None
        self.rate_limiter: RateLimiter | None = None

    async def connect(self, runtime: ConnectionRuntime) -> None:
        try:
            self._runtime = runtime
            runtime.acquire()
            await runtime.materialize()
            self.session = runtime.session
            self.base_url = runtime.base_url
            self.rate_limiter = runtime.rate_limiter
            self.is_connected = True
            logger.debug("Connected to API: %s", self.base_url)
        except Exception as e:
            logger.error("Failed to connect to API: %s", e)
            raise ConnectionError(f"API connection failed: {e}") from e

    async def disconnect(self) -> None:
        if self._runtime:
            await self._runtime.close()
            await asyncio.sleep(0.25)
        self.session = None
        self.is_connected = False

    async def read_batches(
        self,
        source: ResolvedSource,
        *,
        state_manager: StateManager,
        stream_id: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        if partition is None:
            partition = {}

        endpoint = source.endpoint
        if not isinstance(endpoint, ApiReadEndpoint):
            raise ReadError(
                f"APIConnector requires an ApiReadEndpoint, got {type(endpoint).__name__}"
            )

        path = endpoint.request.path
        method = endpoint.request.method or "GET"
        full_url = urljoin(self.base_url or "", path)

        base_query = self._initial_query_params(source, endpoint)
        cursor_field = source.replication.cursor_field
        replication_method = source.replication.method
        safety_window = source.replication.safety_window_seconds

        # Inject incremental cursor as request param when state has one.
        cursor_state = (
            await state_manager.get_cursor(stream_id, partition)
            if replication_method == "incremental"
            else None
        )
        if cursor_state and cursor_field:
            self._apply_incremental_filter(
                endpoint=endpoint,
                query_params=base_query,
                cursor_value=cursor_state.get("cursor"),
                safety_window=safety_window,
            )

        pagination = endpoint.pagination

        batch_count = 0
        total_records = 0

        try:
            if pagination is None:
                batch = await self._read_single(full_url, method, base_query, endpoint)
                if batch:
                    yield batch
                    batch_count = 1
                    total_records = len(batch)
                    if cursor_field:
                        await self._save_cursor(
                            state_manager,
                            stream_id,
                            partition,
                            batch,
                            cursor_field,
                            source,
                            total_records,
                            batch_count,
                        )
            elif isinstance(pagination, OffsetPagination):
                async for batch in self._read_offset(
                    full_url, method, base_query, endpoint, pagination, batch_size
                ):
                    yield batch
                    batch_count += 1
                    total_records += len(batch)
                    if cursor_field:
                        await self._save_cursor(
                            state_manager, stream_id, partition, batch, cursor_field,
                            source, total_records, batch_count,
                        )
            elif isinstance(pagination, CursorPagination):
                async for batch in self._read_cursor(
                    full_url, method, base_query, endpoint, pagination, batch_size
                ):
                    yield batch
                    batch_count += 1
                    total_records += len(batch)
                    if cursor_field:
                        await self._save_cursor(
                            state_manager, stream_id, partition, batch, cursor_field,
                            source, total_records, batch_count,
                        )
            elif isinstance(pagination, PagePagination):
                async for batch in self._read_page(
                    full_url, method, base_query, endpoint, pagination, batch_size
                ):
                    yield batch
                    batch_count += 1
                    total_records += len(batch)
                    if cursor_field:
                        await self._save_cursor(
                            state_manager, stream_id, partition, batch, cursor_field,
                            source, total_records, batch_count,
                        )
            elif isinstance(pagination, (KeysetPagination, LinkPagination)):
                raise ReadError(
                    f"Pagination type {pagination.type!r} is not yet implemented in APIConnector"
                )
            else:
                raise ReadError(f"Unknown pagination type: {type(pagination).__name__}")
        except Exception as e:
            self.metrics["errors"] += 1
            raise ReadError(f"API {method} to {full_url} failed: {e}") from e

    # ------------------------------------------------------------------
    # Query parameter assembly
    # ------------------------------------------------------------------

    @staticmethod
    def _initial_query_params(
        source: ResolvedSource, endpoint: ApiReadEndpoint
    ) -> Dict[str, Any]:
        """Build the static query params (everything except pagination + cursor).

        Resolves declared `params` defaults that aren't `controlled_by` the
        engine, then layers `source.filters` on top.
        """
        params: Dict[str, Any] = {}

        for name, spec in endpoint.params.items():
            if spec.location != "query":
                continue
            if spec.controlled_by:
                # `pagination` / `replication`-controlled params are populated
                # by the pagination loop / incremental injector.
                continue
            value = _resolve_default(spec.default, source)
            if value is not None:
                params[name] = value

        # Layer ResolvedSource filters (already typed with field/op/value).
        for f in source.filters:
            if f.value is None:
                continue
            value = f.value
            if isinstance(value, list):
                params[f.field] = ",".join(str(v) for v in value)
            else:
                params[f.field] = value

        return params

    def _apply_incremental_filter(
        self,
        *,
        endpoint: ApiReadEndpoint,
        query_params: Dict[str, Any],
        cursor_value: Any,
        safety_window: Optional[int],
    ) -> None:
        """Inject the cursor value into the right query param using endpoint
        cursor_mappings."""
        if cursor_value is None:
            return
        cursor_field = endpoint.replication.cursor_mappings
        for mapping in cursor_field:
            param_name = mapping.param
            window = safety_window or 0
            query_params[param_name] = _shift_cursor_value(cursor_value, window)
            logger.debug(
                "Injected incremental cursor: %s = %s", param_name, query_params[param_name]
            )
            break

    # ------------------------------------------------------------------
    # Pagination implementations
    # ------------------------------------------------------------------

    async def _read_offset(
        self,
        url: str,
        method: str,
        base_query: Dict[str, Any],
        endpoint: ApiReadEndpoint,
        pagination: OffsetPagination,
        batch_size: int,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        offset = pagination.offset_initial
        page_size = pagination.limit_max or batch_size
        while True:
            params = {**base_query, pagination.offset_param: offset, pagination.limit_param: page_size}
            records = await self._fetch(url, method, params, endpoint)
            if not records:
                break
            yield records
            if len(records) < page_size:
                break
            offset += page_size

    async def _read_cursor(
        self,
        url: str,
        method: str,
        base_query: Dict[str, Any],
        endpoint: ApiReadEndpoint,
        pagination: CursorPagination,
        batch_size: int,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        cursor: Any = pagination.initial_cursor
        while True:
            params = {**base_query}
            if cursor is not None:
                params[pagination.cursor_param] = cursor
            data, records = await self._fetch_with_envelope(url, method, params, endpoint)
            if not records:
                break
            yield records
            cursor = _extract_path(data, pagination.cursor_response_ref)
            if not cursor:
                break

    async def _read_page(
        self,
        url: str,
        method: str,
        base_query: Dict[str, Any],
        endpoint: ApiReadEndpoint,
        pagination: PagePagination,
        batch_size: int,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        page = pagination.page_initial
        page_size = batch_size
        while True:
            params = {**base_query, pagination.page_param: page}
            if pagination.size_param:
                params[pagination.size_param] = page_size
            records = await self._fetch(url, method, params, endpoint)
            if not records:
                break
            yield records
            if len(records) < page_size:
                break
            page += 1

    async def _read_single(
        self,
        url: str,
        method: str,
        base_query: Dict[str, Any],
        endpoint: ApiReadEndpoint,
    ) -> Optional[List[Dict[str, Any]]]:
        return await self._fetch(url, method, base_query, endpoint)

    # ------------------------------------------------------------------
    # HTTP & extraction
    # ------------------------------------------------------------------

    async def _fetch(
        self,
        url: str,
        method: str,
        params: Dict[str, Any],
        endpoint: ApiReadEndpoint,
    ) -> List[Dict[str, Any]]:
        _, records = await self._fetch_with_envelope(url, method, params, endpoint)
        return records

    async def _fetch_with_envelope(
        self,
        url: str,
        method: str,
        params: Dict[str, Any],
        endpoint: ApiReadEndpoint,
    ) -> tuple[Any, List[Dict[str, Any]]]:
        if self.rate_limiter:
            await self.rate_limiter.acquire()
        full = f"{url}?{urlencode(params)}" if params else url
        logger.debug("Making API request: %s %s", method, full)

        async with self.session.request(method, url, params=params) as response:
            if response.status != 200:
                body = await response.text()
                logger.error("API %s response body: %s", response.status, body[:500])
                raise ReadError(f"API request failed with status {response.status}")
            data = await response.json()

        records = _extract_records(data, endpoint.response.records_ref)
        if records:
            self.metrics["records_read"] += len(records)
            self.metrics["batches_read"] += 1
        return data, records

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    async def _save_cursor(
        self,
        state_manager: StateManager,
        stream_id: str,
        partition: Dict[str, Any],
        batch: List[Dict[str, Any]],
        cursor_field: str,
        source: ResolvedSource,
        total_records: int,
        batch_count: int,
    ) -> None:
        last_record = batch[-1]
        cursor_value = last_record.get(cursor_field)
        if cursor_value is None:
            return

        primary = CursorField(field=cursor_field, value=cursor_value, inclusive=True)
        cursor = StreamCursor(primary=primary)

        tie_breakers = source.replication.tie_breaker_fields or ()
        if tie_breakers:
            tbs = []
            for fields in tie_breakers:
                # tie_breaker_fields is a tuple of tuples; flatten dotted path lookups
                path = fields if isinstance(fields, tuple) else (fields,)
                value = _nested_value(last_record, ".".join(path))
                if value is not None:
                    tbs.append(CursorField(field=".".join(path), value=value, inclusive=True))
            if tbs:
                cursor.tiebreakers = tbs

        stats = StreamStats(
            records_synced=total_records,
            batches_written=batch_count,
            last_checkpoint_at=datetime.now(timezone.utc),
            errors_since_checkpoint=0,
        )
        state_manager.save_stream_checkpoint(
            stream_name=stream_id,
            partition=partition,
            cursor=asdict(cursor),
            hwm=cursor_value,
            stats=asdict(stats),
        )

    def supports_incremental_read(self) -> bool:
        return True


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------


def _resolve_default(default: Any, source: ResolvedSource) -> Any:
    """Resolve a ParamSpec.default value.

    The schema lets defaults be either literal values or
    ``{"ref": "connection.selections.X"}`` style references. We support
    selections/parameters lookups against the ``ResolvedSource.connection``.
    """
    if default is None:
        return None
    if isinstance(default, dict) and "ref" in default:
        ref = default["ref"]
        # connection.selections.foo / connection.parameters.foo
        parts = ref.split(".")
        if len(parts) >= 3 and parts[0] == "connection":
            scope, key = parts[1], parts[2]
            scopes = {
                "selections": source.connection.selections,
                "parameters": source.connection.parameters,
            }
            return scopes.get(scope, {}).get(key)
        # runtime.batch_size etc. — caller assigns via pagination loop, leave None
        return None
    return default


def _extract_records(data: Any, ref: str) -> List[Dict[str, Any]]:
    """Pull the record array out of an envelope using a dotted ``records_ref``.

    `response.body` (the default) means "the body is the records array".
    """
    if not ref or ref in ("response.body", "$"):
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []

    # Strip the leading "response." marker, navigate the body.
    path = ref.split(".")
    if path and path[0] == "response":
        path = path[1:]
    if path and path[0] == "body":
        path = path[1:]

    value: Any = data
    for key in path:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    return []


def _extract_path(data: Any, ref: Optional[str]) -> Any:
    """Walk dotted ``$.foo.bar`` style ref to extract a single value."""
    if not ref:
        return None
    path = ref.split(".")
    if path and path[0] in ("response", "$"):
        path = path[1:]
    if path and path[0] == "body":
        path = path[1:]
    value: Any = data
    for key in path:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value


def _shift_cursor_value(cursor: Any, safety_window_seconds: int) -> str:
    """Subtract safety window from cursor (best-effort: datetime or numeric ID).

    Uses stdlib :func:`datetime.fromisoformat` (Python 3.11+ handles the
    trailing ``Z`` suffix natively) so the engine doesn't need a
    third-party date parser at runtime.
    """
    if safety_window_seconds <= 0:
        return str(cursor)
    raw = str(cursor)
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        shifted = dt - timedelta(seconds=safety_window_seconds)
        return shifted.isoformat().replace("+00:00", "Z")
    except (ValueError, TypeError):
        try:
            return str(max(0, int(cursor) - safety_window_seconds))
        except (ValueError, TypeError):
            return raw


def _nested_value(record: Dict[str, Any], path: str) -> Any:
    current: Any = record
    for key in path.split("."):
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current
