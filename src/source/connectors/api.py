"""Contract-native REST API source connector.

This connector consumes the published API-endpoint contract directly:

* ``operations.read.request.{method, path, query}`` — URL + HTTP verb.
* ``operations.read.params.<name>`` — declared params with optional
  ``default`` value expressions resolved against the connection scopes
  (``connection.parameters``/``selections``/``discovered``) and runtime
  scopes (``runtime.batch_size``).
* ``operations.read.pagination`` — offset / page / cursor / keyset
  loops driven by their respective ``.param`` + ``.initial`` / ``.max``
  declarations.
* ``operations.read.response.records`` — record extraction path
  (e.g. ``response.body`` or ``response.body.<field>``).
* ``operations.read.replication.cursor_mappings`` — cursor-field ↔
  query-param map used to build incremental ``WHERE`` filters.

The source-config dict carries the resolved contract documents and
per-stream overrides; nothing else.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import pyarrow as pa

from .base import BaseConnector, ConnectionError, ReadError
from ...destination.schema_contract import SchemaContract
from ...models.state import CursorField, StreamCursor, StreamStats
from ...shared.connection_runtime import ConnectionRuntime
from ...shared.expressions import resolve_value_expression
from ...shared.http_utils import join_url
from ...state.state_manager import StateManager

logger = logging.getLogger(__name__)


class APIConnector(BaseConnector):
    """Modern API connector consuming the contract endpoint document."""

    def __init__(self, name: str = "APIConnector"):
        super().__init__(name)
        self._runtime: ConnectionRuntime | None = None
        self.session = None
        self.base_url = None
        self.rate_limiter = None

    async def connect(self, runtime: ConnectionRuntime):
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

    async def disconnect(self):
        if self._runtime:
            await self._runtime.close()
            await asyncio.sleep(0.25)
        self.session = None
        self.is_connected = False

    # ------------------------------------------------------------------
    # Read loop
    # ------------------------------------------------------------------

    async def read_batches(
        self,
        config: Dict[str, Any],
        *,
        state_manager: StateManager,
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Read upstream records as Arrow batches.

        Each yielded batch corresponds to one upstream page; the
        destination realigns to its declared schema via
        :meth:`SchemaContract.cast_arrow_batch`.
        """
        if partition is None:
            partition = {}
        if self._runtime is None or self.session is None:
            raise ReadError("APIConnector.read_batches() called before connect()")

        endpoint_doc = config.get("endpoint_document")
        if not endpoint_doc:
            raise ReadError(
                "APIConnector: source config missing 'endpoint_document'"
            )
        stream_source = config.get("stream_source") or {}
        endpoint_ref = stream_source.get("endpoint_ref")
        if not endpoint_ref:
            raise ReadError(
                "APIConnector: stream_source missing 'endpoint_ref'; "
                "the source contract requires it to declare per-field types"
            )
        read_spec = ((endpoint_doc.get("operations") or {}).get("read") or {})
        records_items_schema = self._resolve_records_items_schema(
            endpoint_doc, read_spec,
        )
        schema_contract = SchemaContract(records_items_schema)
        request = read_spec.get("request") or {}
        path = request.get("path")
        method = (request.get("method") or "GET").upper()
        if not isinstance(path, str) or not path:
            raise ReadError(
                f"endpoint {endpoint_doc.get('endpoint_id')!r}: operations.read.request.path is required"
            )
        full_url = join_url(self.base_url, path)

        replication_block = stream_source.get("replication") or {}
        replication_method = replication_block.get("method", "full_refresh")
        cursor_field = replication_block.get("cursor_field")
        if isinstance(cursor_field, list):
            cursor_field = cursor_field[0] if cursor_field else None
        safety_window = replication_block.get("safety_window_seconds")
        tie_breaker_fields = list(replication_block.get("tie_breaker_fields") or [])

        # Build query params from the declared ``params`` block: defaults
        # via value-expression resolution, then stream-level filter
        # overrides. ``controlled_by: pagination|replication`` params are
        # filled by their respective loops.
        base_params, controlled_params = self._build_base_params(
            read_spec, config, stream_source.get("filters") or []
        )

        # Set up incremental replication: subtract safety window from the
        # stored cursor, then write the value into the cursor's mapped
        # param.
        if replication_method == "incremental":
            await self._apply_incremental_replication(
                base_params,
                read_spec,
                state_manager,
                stream_name,
                partition,
                cursor_field,
                safety_window,
            )

        records_ref = ((read_spec.get("response") or {}).get("records") or {}).get(
            "ref", "response.body"
        )

        state = {
            "bookmarks": [],
            "cursor_field": cursor_field,
            "replication_method": replication_method,
        }

        batch_count = 0
        total_records = 0
        async for batch in self._iterate_pages(
            full_url=full_url,
            method=method,
            base_params=base_params,
            controlled_params=controlled_params,
            pagination=read_spec.get("pagination") or {},
            batch_size=batch_size,
            records_ref=records_ref,
        ):
            if not batch:
                continue
            deduped = self._deduplicate_records(batch, state, cursor_field, tie_breaker_fields)
            if not deduped:
                continue
            yield schema_contract.from_pylist(deduped)

            batch_count += 1
            total_records += len(deduped)
            if cursor_field:
                self._save_checkpoint(
                    state_manager=state_manager,
                    stream_name=stream_name,
                    partition=partition,
                    last_record=deduped[-1],
                    cursor_field=cursor_field,
                    tie_breaker_fields=tie_breaker_fields,
                    total_records=total_records,
                    batch_count=batch_count,
                )

    @staticmethod
    def _resolve_records_items_schema(
        endpoint_doc: Dict[str, Any], read_spec: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Walk operations.read.response.schema via records.ref to the per-record items block.

        Accepted records.ref forms: ``response.body`` and
        ``response.body.<field>[.<field>...]``.
        """
        endpoint_id = endpoint_doc.get("endpoint_id")
        response_block = read_spec.get("response") or {}
        response_schema = response_block.get("schema") or {}
        records_ref = (response_block.get("records") or {}).get(
            "ref", "response.body"
        )

        if records_ref == "response.body":
            node = response_schema
        elif records_ref.startswith("response.body."):
            node = response_schema
            for field in records_ref[len("response.body."):].split("."):
                properties = node.get("properties") if isinstance(node, dict) else None
                if not isinstance(properties, dict) or field not in properties:
                    available = sorted(properties.keys()) if isinstance(properties, dict) else []
                    raise ReadError(
                        f"endpoint {endpoint_id!r}: records.ref "
                        f"{records_ref!r} references field {field!r} that is "
                        f"not declared under properties; available: {available}"
                    )
                node = properties[field]
        else:
            raise ReadError(
                f"endpoint {endpoint_id!r}: unsupported "
                f"records.ref {records_ref!r}; expected 'response.body' "
                f"or 'response.body.<field>[.<field>...]'"
            )

        items = node.get("items") if node.get("type") == "array" else node
        if not isinstance(items, dict) or not items.get("properties"):
            raise ReadError(
                f"endpoint {endpoint_id!r}: cannot resolve "
                f"record schema at {records_ref!r} (no 'properties' under "
                f"the addressed items)"
            )
        return items

    def _build_base_params(
        self,
        read_spec: Dict[str, Any],
        config: Dict[str, Any],
        stream_filters: List[Dict[str, Any]],
    ) -> tuple[Dict[str, Any], Dict[str, str]]:
        """Resolve declared param defaults and apply stream filter overrides.

        Returns ``(base_params, controlled_params)`` where
        ``controlled_params`` maps controller name (``pagination`` or
        ``replication``) to the controlled-by group recorded for the
        param. Callers use it to keep pagination/replication loops from
        clobbering each other's values.
        """
        base_params: Dict[str, Any] = {}
        controlled: Dict[str, str] = {}
        declared = read_spec.get("params") or {}
        for name, decl in declared.items():
            if not isinstance(decl, dict):
                continue
            controlled_by = decl.get("controlled_by")
            if controlled_by:
                controlled[name] = controlled_by
                continue
            if "default" in decl:
                value = resolve_value_expression(
                    decl["default"],
                    self._runtime,
                    extra_scopes={"runtime": {"batch_size": config.get("batch_size")}},
                )
                if value is not None:
                    base_params[name] = value

        for f in stream_filters:
            target = f.get("field")
            if not target:
                continue
            value = f.get("value")
            if value is not None:
                base_params[target] = value
        return base_params, controlled

    # ------------------------------------------------------------------
    # Incremental replication
    # ------------------------------------------------------------------

    async def _apply_incremental_replication(
        self,
        params: Dict[str, Any],
        read_spec: Dict[str, Any],
        state_manager: StateManager,
        stream_name: str,
        partition: Dict[str, Any],
        cursor_field: Optional[str],
        safety_window_seconds: Optional[int],
    ) -> None:
        if not cursor_field:
            return
        mappings = (read_spec.get("replication") or {}).get("cursor_mappings") or []
        param_name = next(
            (m.get("param") for m in mappings if m.get("cursor_field") == cursor_field),
            None,
        )
        if not param_name:
            logger.warning(
                "No replication.cursor_mappings entry for cursor field %r; "
                "running full replication",
                cursor_field,
            )
            return
        cursor_state = await state_manager.get_cursor(stream_name, partition)
        cursor_value = (cursor_state or {}).get("primary", {}).get("value")
        if not cursor_value:
            logger.info(
                "No prior cursor for stream %r; first run performs full replication",
                stream_name,
            )
            return
        if safety_window_seconds is None:
            raise ReadError(
                f"stream {stream_name!r}: incremental replication requires "
                f"safety_window_seconds in the replication block"
            )
        effective_start = self._compute_effective_start(
            cursor_value, safety_window_seconds
        )
        params[param_name] = effective_start
        logger.info(
            "Incremental replication: %s -> %s = %s",
            cursor_field,
            param_name,
            effective_start,
        )

    @staticmethod
    def _compute_effective_start(cursor: Any, safety_window_seconds: int) -> str:
        from datetime import timedelta

        from dateutil.parser import isoparse

        cursor_str = str(cursor)
        try:
            cursor_dt = isoparse(cursor_str)
        except (ValueError, TypeError):
            try:
                cursor_id = int(cursor_str)
            except ValueError as err:
                raise ReadError(
                    f"cursor value {cursor!r} is neither an ISO timestamp nor "
                    f"an integer; cannot apply safety window"
                ) from err
            return str(max(0, cursor_id - safety_window_seconds))
        if cursor_dt.tzinfo is None:
            cursor_dt = cursor_dt.replace(tzinfo=timezone.utc)
        effective_dt = cursor_dt - timedelta(seconds=safety_window_seconds)
        return effective_dt.isoformat().replace("+00:00", "Z")

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    async def _iterate_pages(
        self,
        *,
        full_url: str,
        method: str,
        base_params: Dict[str, Any],
        controlled_params: Dict[str, str],
        pagination: Dict[str, Any],
        batch_size: int,
        records_ref: str,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        p_type = pagination.get("type")
        if not p_type:
            records = await self._request_records(
                full_url, method, base_params, records_ref
            )
            if records:
                yield records
            return

        limit_block = pagination.get("limit") or {}
        limit_param = limit_block.get("param")
        if limit_param:
            base_params[limit_param] = batch_size

        if p_type == "offset":
            offset_block = pagination.get("offset") or {}
            offset_param = offset_block.get("param")
            if not offset_param:
                raise ReadError("offset pagination requires offset.param")
            offset = int(offset_block.get("initial", 0))
            while True:
                params = dict(base_params)
                params[offset_param] = offset
                records = await self._request_records(
                    full_url, method, params, records_ref
                )
                if not records:
                    return
                yield records
                if len(records) < batch_size:
                    return
                offset += batch_size

        elif p_type == "page":
            page_block = pagination.get("page") or {}
            page_param = page_block.get("param")
            if not page_param:
                raise ReadError("page pagination requires page.param")
            page = int(page_block.get("initial", 1))
            while True:
                params = dict(base_params)
                params[page_param] = page
                records = await self._request_records(
                    full_url, method, params, records_ref
                )
                if not records:
                    return
                yield records
                if len(records) < batch_size:
                    return
                page += 1

        elif p_type == "cursor":
            cursor_block = pagination.get("cursor") or {}
            cursor_param = cursor_block.get("param")
            if not cursor_param:
                raise ReadError("cursor pagination requires cursor.param")
            cursor_field_in_response = (
                (cursor_block.get("from") or {}).get("ref")
                or cursor_block.get("response_field")
                or "next_cursor"
            )
            cursor_token: Optional[str] = None
            while True:
                params = dict(base_params)
                if cursor_token:
                    params[cursor_param] = cursor_token
                data, records = await self._request_payload(
                    full_url, method, params, records_ref
                )
                if not records:
                    return
                yield records
                cursor_token = _extract_next_cursor(data, cursor_field_in_response)
                if not cursor_token:
                    return

        elif p_type == "keyset":
            keyset_block = pagination.get("keyset") or {}
            keyset_param = keyset_block.get("param")
            if not keyset_param:
                raise ReadError("keyset pagination requires keyset.param")
            key_field_in_record = keyset_block.get("from_record")
            if not key_field_in_record:
                raise ReadError(
                    "keyset pagination requires keyset.from_record (record field)"
                )
            last_key: Optional[str] = None
            while True:
                params = dict(base_params)
                if last_key:
                    params[keyset_param] = last_key
                records = await self._request_records(
                    full_url, method, params, records_ref
                )
                if not records:
                    return
                yield records
                last_key = records[-1].get(key_field_in_record)
                if not last_key or len(records) < batch_size:
                    return

        else:
            raise ReadError(f"Unsupported pagination type: {p_type!r}")

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _request_records(
        self,
        url: str,
        method: str,
        params: Dict[str, Any],
        records_ref: str,
    ) -> List[Dict[str, Any]]:
        _, records = await self._request_payload(url, method, params, records_ref)
        return records

    async def _request_payload(
        self,
        url: str,
        method: str,
        params: Dict[str, Any],
        records_ref: str,
    ) -> tuple[Any, List[Dict[str, Any]]]:
        if self.rate_limiter:
            await self.rate_limiter.acquire()
        logger.debug("API %s %s params=%s", method, url, params)
        async with self.session.request(method, url, params=params) as response:
            if response.status != 200:
                body = await response.text()
                body_snippet = body[:500]
                logger.error("API %d %s %s: %s", response.status, method, url, body_snippet)
                raise ReadError(
                    f"API request failed: {method} {url} -> status {response.status}; "
                    f"params={params}; body[:500]={body_snippet!r}"
                )
            data = await response.json()
        self.metrics["records_read"] += 0  # incremented below per page
        records = _extract_records(data, records_ref)
        if records:
            self.metrics["records_read"] += len(records)
            self.metrics["batches_read"] += 1
        return data, records

    # ------------------------------------------------------------------
    # Deduplication / checkpointing
    # ------------------------------------------------------------------

    def _deduplicate_records(
        self,
        batch: List[Dict[str, Any]],
        state: Dict[str, Any],
        cursor_field: Optional[str],
        tie_breaker_fields: List[str],
    ) -> List[Dict[str, Any]]:
        bookmarks = state.get("bookmarks") or []
        if not bookmarks or not cursor_field:
            return batch
        bookmark = bookmarks[0]
        return [
            r
            for r in batch
            if _is_record_new(r, bookmark, cursor_field, tie_breaker_fields or [])
        ]

    def _save_checkpoint(
        self,
        *,
        state_manager: StateManager,
        stream_name: str,
        partition: Dict[str, Any],
        last_record: Dict[str, Any],
        cursor_field: str,
        tie_breaker_fields: List[str],
        total_records: int,
        batch_count: int,
    ) -> None:
        cursor_value = last_record.get(cursor_field)
        if cursor_value is None:
            return
        cursor = StreamCursor(
            primary=CursorField(field=cursor_field, value=cursor_value, inclusive=True)
        )
        if tie_breaker_fields:
            tiebreakers = []
            for field_name in tie_breaker_fields:
                value = _get_nested_field(last_record, field_name)
                if value is not None:
                    tiebreakers.append(
                        CursorField(field=field_name, value=value, inclusive=True)
                    )
            if tiebreakers:
                cursor.tiebreakers = tiebreakers
        stats = StreamStats(
            records_synced=total_records,
            batches_written=batch_count,
            last_checkpoint_at=datetime.now(timezone.utc),
            errors_since_checkpoint=0,
        )
        state_manager.save_stream_checkpoint(
            stream_name=stream_name,
            partition=partition,
            cursor=asdict(cursor),
            hwm=cursor_value,
            stats=asdict(stats),
        )

    # ------------------------------------------------------------------
    # Base interface stubs
    # ------------------------------------------------------------------

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        raise NotImplementedError("Source connector is read-only")

    def supports_incremental_read(self) -> bool:
        return True

    def supports_upsert(self) -> bool:
        return True


# ----------------------------------------------------------------------
# Free helpers (pure functions, no connector state)
# ----------------------------------------------------------------------


def _extract_records(data: Any, records_ref: str) -> List[Dict[str, Any]]:
    """Pull records out of a response body according to ``records_ref``.

    The ref is expressed as ``response.body[.<dotted.path>]`` per the
    contract. ``response.body`` returns the body itself; any deeper path
    walks into the parsed JSON.
    """
    if not records_ref:
        records_ref = "response.body"
    body = data
    prefix = "response.body"
    if records_ref == prefix:
        cursor: Any = body
    elif records_ref.startswith(prefix + "."):
        cursor = body
        for segment in records_ref[len(prefix) + 1 :].split("."):
            if isinstance(cursor, dict) and segment in cursor:
                cursor = cursor[segment]
            else:
                cursor = None
                break
    else:
        cursor = None
    if isinstance(cursor, list):
        return [r for r in cursor if isinstance(r, dict)]
    if isinstance(cursor, dict):
        return [cursor]
    return []


def _extract_next_cursor(data: Any, response_field_ref: str) -> Optional[str]:
    """Walk a ``response.body[.<dotted.path>]`` ref to a string cursor."""
    if not isinstance(data, dict):
        return None
    prefix = "response.body"
    if response_field_ref == prefix:
        return None
    if response_field_ref.startswith(prefix + "."):
        segments = response_field_ref[len(prefix) + 1 :].split(".")
    else:
        segments = response_field_ref.split(".")
    cursor: Any = data
    for segment in segments:
        if isinstance(cursor, dict) and segment in cursor:
            cursor = cursor[segment]
        else:
            return None
    if cursor in (None, ""):
        return None
    return str(cursor)


def _get_nested_field(record: Dict[str, Any], field_path: str) -> Any:
    value: Any = record
    for segment in field_path.split("."):
        if isinstance(value, dict) and segment in value:
            value = value[segment]
        else:
            return None
    return value


def _is_record_new(
    record: Dict[str, Any],
    bookmark: Dict[str, Any],
    cursor_field: str,
    tie_breaker_fields: List[str],
) -> bool:
    """Decide whether ``record`` is past the stored bookmark.

    Cursor values are compared as ISO timestamps when parseable, else as
    strings. Equal cursors fall through to tie-breaker comparison; in
    inclusive mode an exact tie-breaker match is treated as a duplicate.
    """
    from dateutil.parser import isoparse

    record_cursor_value = record.get(cursor_field)
    if record_cursor_value is None:
        return True
    stored_cursor = bookmark.get("cursor")
    if stored_cursor is None:
        return True
    try:
        rdt = isoparse(str(record_cursor_value))
        sdt = isoparse(str(stored_cursor))
        if rdt.tzinfo is None:
            rdt = rdt.replace(tzinfo=timezone.utc)
        if sdt.tzinfo is None:
            sdt = sdt.replace(tzinfo=timezone.utc)
        if rdt > sdt:
            return True
        if rdt < sdt:
            return False
    except (ValueError, TypeError):
        if str(record_cursor_value) > str(stored_cursor):
            return True
        if str(record_cursor_value) < str(stored_cursor):
            return False
    # Tie on cursor value: compare tie-breakers (inclusive mode keeps
    # exact matches out).
    aux = bookmark.get("aux") or {}
    stored_tiebreakers = aux.get("tiebreakers") or []
    for i, field_name in enumerate(tie_breaker_fields):
        if i >= len(stored_tiebreakers):
            return True
        record_value = _get_nested_field(record, field_name)
        stored_value = stored_tiebreakers[i].get("value")
        if record_value is None:
            return True
        try:
            if (
                isinstance(stored_value, str)
                and str(record_value).isdigit()
                and stored_value.isdigit()
            ):
                if int(record_value) > int(stored_value):
                    return True
                if int(record_value) < int(stored_value):
                    return False
                continue
        except (ValueError, TypeError):
            # Non-integer digit-strings (e.g. Unicode digits) fall through
            # to the lexicographic compare below.
            pass
        if str(record_value) > str(stored_value):
            return True
        if str(record_value) < str(stored_value):
            return False
    return False
