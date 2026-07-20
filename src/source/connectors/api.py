"""Contract-native REST API source connector.

This connector consumes the published API-endpoint contract directly:

* ``operations.read.request.{method, path}`` — URL + HTTP verb.
* ``operations.read.request.body`` — optional JSON body; built per page
  request: ``{"from_param": ...}`` nodes bind the page's param values
  (including pagination-controlled ones), then the body is deep-resolved
  through the value-expression grammar. Params declared ``in: body``
  stay out of the query string.
* ``operations.read.params.<name>`` — declared params with optional
  ``default`` value expressions (``literal``/``ref``/``template``/
  ``function``) resolved against the connection scopes
  (``connection.parameters``/``selections``/``discovered``) and runtime
  scopes (``runtime.batch_size``).
* ``operations.read.pagination`` — the five contract strategies
  (``offset``, ``page``, ``cursor``, ``link``, ``keyset``). Each declares
  how to reach the next page and a required ``stop_when`` predicate that
  decides when the read is done; the engine contributes no termination
  heuristic of its own.
* ``operations.read.response.records`` — record extraction path
  (e.g. ``response.body`` or ``response.body.<field>``).
* ``operations.read.response.metadata`` — named extractions published
  into the ``response`` resolution scope alongside ``body`` / ``records``
  / ``record_count`` / ``status`` / ``headers``, so pagination
  expressions and stop predicates can address them.
* ``operations.read.replication.cursor_mappings`` — cursor-field ↔
  query-param map used to build incremental ``WHERE`` filters.

The source-config dict carries the resolved contract documents and
per-stream overrides; nothing else.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import timezone
from decimal import Decimal
from typing import Any, NamedTuple

import aiohttp
import pyarrow as pa

from cdk.connection_runtime import ConnectionRuntime
from cdk.exceptions import TransportSpecError
from cdk.predicate import evaluate_predicate
from cdk.rate_limiter import RateLimiter
from cdk.request_binding import bind_param_refs, resolve_param_defaults
from cdk.resolver import Resolver
from cdk.schema_contract import SchemaContract
from cdk.type_map import TypeMapper, UnmappedTypeError
from cdk.types import CheckpointStore, EndpointScope

from ...models.state import ReplicationConfig
from ...shared.dict_path import walk_path
from ...shared.http_utils import join_url
from .base import BaseConnector, ConnectionError, ReadError, TransientReadError

logger = logging.getLogger(__name__)

# HTTP statuses retrying can heal: request timeout, rate limit, upstream
# outages. Everything else non-200 is a deterministic contract/config error.
_TRANSIENT_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})

# Lookback subtracted from the stored cursor on an incremental read when the
# stream has a prior cursor but declares no safety window of its own. It is an
# operational safety default, not a per-connector attribute, so connectors
# never declare it. Sourced from ReplicationConfig; SourceConfig carries the
# same default independently (models/state.py), so the two move separately.
_DEFAULT_SAFETY_WINDOW_SECONDS: int = ReplicationConfig.safety_window_seconds


def _loads_preserving_decimals(payload: str) -> Any:
    """Decode a JSON response body without flattening decimals to float.

    The stdlib default parses every floating-point token as a double,
    discarding digits before Arrow ever sees the value, so a Decimal-typed
    column lands a rounded number. Parsing those tokens as ``Decimal`` keeps the
    exact source digits; the schema contract then renders each value per its
    declared Arrow type (Decimal columns stay exact, Float columns narrow to
    double on purpose). Integer tokens are untouched -- the default already
    parses them as arbitrary-precision ``int``.
    """
    return json.loads(payload, parse_float=Decimal)


@dataclass(frozen=True)
class _Page:
    """One page response, in the shape the ``response`` scope is built from.

    Carries more than the records because pagination expressions and stop
    predicates address the whole contract response vocabulary — a next-page
    link often lives in a header, a stop condition often in a body field
    that sits beside the records rather than in them.
    """

    body: Any
    records: list[dict[str, Any]]
    status: int
    headers: dict[str, str]


class _NextRequest(NamedTuple):
    """Where the following page comes from.

    ``send_params`` is False only for ``link`` pagination, whose next URL
    already carries the provider's own query string; re-appending the
    declared params there would duplicate keys.
    """

    url: str
    params: dict[str, Any]
    send_params: bool


class APIConnector(BaseConnector):
    """Modern API connector consuming the contract endpoint document."""

    def __init__(self, name: str = "APIConnector"):
        super().__init__(name)
        self._runtime: ConnectionRuntime | None = None
        self.session: aiohttp.ClientSession | None = None
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
            # Brief courtesy delay so aiohttp's transports finish closing. It
            # runs in read_batches' ``finally``, so a cancellation landing here
            # must not replace a read error already propagating through the
            # teardown; absorb it rather than let the drain delay become the
            # surfaced exception.
            try:
                await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                # Intentionally ignored: the runtime is already closed above, so
                # the only thing cancelled is the drain delay. Absorbing it keeps
                # a propagating read error (or task cancellation) intact.
                pass
        self.session = None
        self.is_connected = False

    # ------------------------------------------------------------------
    # Read loop
    # ------------------------------------------------------------------

    async def read_batches(
        self,
        runtime: ConnectionRuntime,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Read upstream records as Arrow batches.

        ``runtime`` is the only connection input (the ``Readable`` contract):
        this connector opens the session on entry and closes it on exit, so no
        prior ``connect()`` is required. ``checkpoint`` is the minimal
        ``CheckpointStore`` Protocol — the same ``{"cursor": value}`` round
        trip every source connector uses, so the worker's relay facade and the
        engine's ``StateManager`` are interchangeable here. Each yielded batch
        corresponds to one upstream page; the destination realigns to its
        declared schema via :meth:`SchemaContract.cast_arrow_batch`.

        ``connect()`` runs inside the ``try`` so a connect/materialize failure
        still reaches ``disconnect()`` in the ``finally`` and releases the
        runtime reference it acquired — the lifecycle is balanced on every exit.
        """
        try:
            await self.connect(runtime)
            async for batch in self._read_batches_impl(
                config,
                checkpoint=checkpoint,
                stream_name=stream_name,
                partition=partition,
                batch_size=batch_size,
            ):
                yield batch
        finally:
            await self.disconnect()

    async def _read_batches_impl(
        self,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        if partition is None:
            partition = {}

        if self._runtime is None or self.base_url is None:
            raise ReadError(
                "APIConnector: read attempted before connect() materialized "
                "the runtime"
            )

        endpoint_doc = config.get("endpoint_document")
        if not endpoint_doc:
            raise ReadError("APIConnector: source config missing 'endpoint_document'")
        stream_source = config.get("stream_source") or {}
        endpoint_ref = stream_source.get("endpoint_ref")
        if not endpoint_ref:
            raise ReadError(
                "APIConnector: stream_source missing 'endpoint_ref'; "
                "the source contract requires it to declare per-field types"
            )
        read_spec = (endpoint_doc.get("operations") or {}).get("read") or {}
        records_items_schema = self._resolve_records_items_schema(
            endpoint_doc,
            read_spec,
        )
        self._apply_read_type_map(records_items_schema, endpoint_ref)
        schema_contract = SchemaContract(records_items_schema)
        request = read_spec.get("request") or {}
        path = request.get("path")
        method = (request.get("method") or "GET").upper()
        if not isinstance(path, str) or not path:
            raise ReadError(
                f"endpoint {endpoint_doc.get('endpoint_id')!r}: "
                f"operations.read.request.path is required"
            )
        full_url = join_url(self.base_url, path)

        # One resolver covers everything this read materializes per request:
        # declared param defaults and the optional request body. Expression
        # nodes that do not resolve are omitted (with a warning) instead of
        # being serialized verbatim onto the wire. ``runtime.batch_size`` is
        # the effective page size driving the pagination loops.
        resolver = self._runtime.request_resolver(
            runtime_values={"batch_size": batch_size}
        )

        replication_block = stream_source.get("replication") or {}
        replication_method = replication_block.get("method", "full_refresh")
        # cursor_field is a contract string|null (validated upstream), so no
        # list normalization is needed.
        cursor_field = replication_block.get("cursor_field")
        safety_window = replication_block.get("safety_window_seconds")

        # Build the param value table from the declared ``params`` block:
        # defaults via value-expression resolution, then stream-level filter
        # overrides. ``controlled_by: pagination|replication`` params are
        # filled by their respective loops.
        param_values, param_placements = self._build_base_params(
            read_spec, resolver, stream_source.get("filters") or []
        )

        # Set up incremental replication: subtract safety window from the
        # stored cursor, then write the value into the cursor's mapped
        # param.
        if replication_method == "incremental":
            await self._apply_incremental_replication(
                param_values,
                read_spec,
                checkpoint,
                stream_name,
                partition,
                cursor_field,
                safety_window,
            )

        # Each page request materializes from the full per-page param
        # table (declared defaults + filters + replication + the values
        # the pagination loop sets): the query string carries params not
        # declared ``in: body``; the body binds ``{"from_param": ...}``
        # nodes against the same table and resolves expressions. Built
        # per page so controlled params (limit, offset, cursor) reach a
        # body-paginated endpoint instead of freezing at their initial
        # values.
        raw_body = request.get("body")

        def build_request(
            page_params: dict[str, Any],
        ) -> tuple[dict[str, Any], Any]:
            # A fractional value from the lossless JSON parse (e.g. a keyset
            # key) arrives as a Decimal, which neither sink takes as-is, so each
            # placement converts it to the form its serializer needs; int/str
            # stay native.
            query = {
                # yarl truncates a Decimal in the query string; stringify it
                # (full precision).
                name: str(value) if isinstance(value, Decimal) else value
                for name, value in page_params.items()
                if param_placements.get(name) != "body"
            }
            if raw_body is not None:
                # aiohttp serializes the body via stdlib json.dumps, which
                # cannot encode a Decimal. Narrow body Decimals to float so the
                # value stays a JSON number a numeric body schema accepts --
                # the same float the body carried before the lossless parse.
                body_params = {
                    name: float(value) if isinstance(value, Decimal) else value
                    for name, value in page_params.items()
                }
                body = resolver.resolve_for_request(
                    bind_param_refs(raw_body, body_params)
                )
            else:
                body = None
            return query, body

        records_ref = ((read_spec.get("response") or {}).get("records") or {}).get(
            "ref", "response.body"
        )

        batch_count = 0
        total_records = 0
        async for batch in self._iterate_pages(
            full_url=full_url,
            method=method,
            base_params=param_values,
            pagination=read_spec.get("pagination") or {},
            batch_size=batch_size,
            records_ref=records_ref,
            metadata_spec=(read_spec.get("response") or {}).get("metadata") or {},
            resolver=resolver,
            build_request=build_request,
        ):
            if not batch:
                continue
            yield schema_contract.from_pylist(batch)

            batch_count += 1
            total_records += len(batch)
            if cursor_field:
                cursor_value = batch[-1].get(cursor_field)
                if cursor_value is not None:
                    await checkpoint.save_cursor(
                        stream_name, partition, {"cursor": cursor_value}
                    )
                else:
                    # Safe under at-least-once + upsert (resume re-reads),
                    # but visible: an author debugging "incremental stream
                    # keeps re-reading its tail" needs this signal.
                    logger.debug(
                        "stream %r: last record has no %r value; "
                        "cursor not advanced for batch %d",
                        stream_name,
                        cursor_field,
                        batch_count,
                    )

    @staticmethod
    def _resolve_records_items_schema(
        endpoint_doc: dict[str, Any],
        read_spec: dict[str, Any],
    ) -> dict[str, Any]:
        """Walk operations.read.response.schema via records.ref to per-record items.

        Accepted records.ref forms: ``response.body`` and
        ``response.body.<field>[.<field>...]``.
        """
        endpoint_id = endpoint_doc.get("endpoint_id")
        response_block = read_spec.get("response") or {}
        response_schema = response_block.get("schema") or {}
        records_ref = (response_block.get("records") or {}).get("ref", "response.body")

        if records_ref == "response.body":
            node = response_schema
        elif records_ref.startswith("response.body."):
            node = response_schema
            for field in records_ref[len("response.body.") :].split("."):
                properties = node.get("properties") if isinstance(node, dict) else None
                if not isinstance(properties, dict) or field not in properties:
                    available = (
                        sorted(properties.keys())
                        if isinstance(properties, dict)
                        else []
                    )
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

    def _apply_read_type_map(
        self, items_schema: dict[str, Any], endpoint_ref: dict[str, Any]
    ) -> None:
        """Resolve each record field's ``arrow_type`` from the read type-map.

        API endpoints declare per-field JSON ``type``/``format`` and ship a
        ``type-map-read.json`` mapping those to Arrow types - the same read
        type-map the database source path consumes (see ``cdk/sql/discovery.py``).
        ``SchemaContract`` requires an explicit ``arrow_type`` per field and
        recurses into ``Object``/``List`` children, so resolution walks nested
        ``properties``/``items`` too. A field that already declares
        ``arrow_type`` keeps it, so hand-annotated connectors stay valid and the
        mapper is only consulted when a field needs it; an unmapped JSON type
        fails loud, naming the field.

        The mapper is chosen by the endpoint's scope so a connection-scoped
        endpoint's ``type-map-read.json`` composes over the connector defaults,
        matching the database path
        (``GenericSQLConnector._type_mapper_for_stream``). A missing or invalid
        type-map is a deterministic config defect, so it surfaces as
        ``ReadError`` (fail fast) rather than the raw ``RuntimeError`` the worker
        would otherwise classify as retryable.
        """
        runtime = self._runtime
        if runtime is None:
            raise ReadError(
                "APIConnector: type-map resolution attempted before connect()"
            )
        scope = endpoint_ref.get("scope")
        if not scope:
            raise ReadError(
                "APIConnector: stream_source endpoint_ref has no 'scope'; "
                f"expected one of {[s.value for s in EndpointScope]}"
            )

        mapper: TypeMapper | None = None

        def get_mapper() -> TypeMapper:
            # Resolved lazily: an endpoint that hand-annotates every field never
            # needs a type-map. A missing/unknown mapper is a config defect, so
            # surface it as a deterministic ReadError, not a retryable error.
            nonlocal mapper
            if mapper is None:
                try:
                    mapper = runtime.type_mapper_for(scope=EndpointScope(scope))
                except (RuntimeError, ValueError) as err:
                    raise ReadError(
                        f"APIConnector: no usable read type-map for "
                        f"{scope!r}-scoped endpoint; a field needs arrow_type "
                        f"resolution but the type-map is absent or invalid"
                    ) from err
            return mapper

        for name, prop in (items_schema.get("properties") or {}).items():
            if isinstance(prop, dict):
                self._resolve_field_arrow_type(prop, name, get_mapper)

    def _resolve_field_arrow_type(
        self,
        field: dict[str, Any],
        name: str,
        get_mapper: Callable[[], TypeMapper],
    ) -> None:
        """Fill ``field['arrow_type']`` from the type-map if absent, then recurse.

        Recursion is gated to the resolved ``arrow_type`` exactly as
        ``SchemaContract.resolve_arrow_type`` does: it descends into
        ``properties`` only for ``Object`` and into ``items`` only for ``List``,
        and treats everything else (including a ``Json`` blob that keeps
        ``properties``/``items`` for documentation, and every scalar) as a leaf.
        A nested child authored with only JSON ``type``/``format`` under a real
        ``Object``/``List`` must be resolved here too, or the schema build
        fails; but descending into a ``Json`` blob's documentary children would
        wrongly fail a read on a child type the schema build never consults.
        Recursion runs even when a container already carries an ``arrow_type``,
        because a hand-annotated ``Object``/``List`` can still hold children
        that do not.
        """
        if not field.get("arrow_type"):
            json_type = field.get("type")
            if isinstance(json_type, list):
                json_type = next((t for t in json_type if t != "null"), None)
            if isinstance(json_type, str):
                fmt = field.get("format")
                native = (
                    f"{json_type}:{fmt}" if isinstance(fmt, str) and fmt else json_type
                )
                try:
                    field["arrow_type"] = get_mapper().to_arrow_type(native)
                except UnmappedTypeError as err:
                    raise ReadError(
                        f"field {name!r}: JSON type {native!r} has no rule in "
                        f"the connector's read type-map"
                    ) from err
        arrow_type = field.get("arrow_type")
        if arrow_type == "Object":
            nested = field.get("properties")
            if isinstance(nested, dict):
                for child_name, child in nested.items():
                    if isinstance(child, dict):
                        self._resolve_field_arrow_type(
                            child, f"{name}.{child_name}", get_mapper
                        )
        elif arrow_type == "List":
            items = field.get("items")
            if isinstance(items, dict):
                self._resolve_field_arrow_type(items, f"{name}[]", get_mapper)

    def _build_base_params(
        self,
        read_spec: dict[str, Any],
        resolver: Resolver,
        stream_filters: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Resolve declared param defaults and apply stream filter overrides.

        Defaults resolve through the full value-expression grammar
        (``literal`` / ``ref`` / ``template`` / ``function``); a default
        that does not resolve omits the parameter with a warning rather
        than sending the raw expression structure upstream.

        Returns ``(param_values, placements)``: ``param_values`` is the
        single value table (body ``from_param`` binding reads it whole;
        the caller filters ``in: body`` params out of the query string via
        ``placements``), and ``placements`` maps param name to its declared
        ``in`` location. Params declared ``controlled_by`` pagination or
        replication are left out of the defaults so their loops set them.
        """
        placements: dict[str, str] = {}
        declared = read_spec.get("params") or {}
        for name, decl in declared.items():
            if not isinstance(decl, dict):
                continue
            placements[name] = decl.get("in") or "query"

        uncontrolled = {
            n: d
            for n, d in declared.items()
            if isinstance(d, dict) and not d.get("controlled_by")
        }
        param_values = resolve_param_defaults(uncontrolled, resolver)

        for f in stream_filters:
            target = f.get("field")
            if not target:
                continue
            value = f.get("value")
            if value is not None:
                param_values[target] = value
        return param_values, placements

    # ------------------------------------------------------------------
    # Incremental replication
    # ------------------------------------------------------------------

    async def _apply_incremental_replication(
        self,
        params: dict[str, Any],
        read_spec: dict[str, Any],
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any],
        cursor_field: str | None,
        safety_window_seconds: int | None,
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
        cursor_state = await checkpoint.get_cursor(stream_name, partition)
        cursor_value = (cursor_state or {}).get("cursor")
        if not cursor_value:
            logger.info(
                "No prior cursor for stream %r; first run performs full replication",
                stream_name,
            )
            return
        if safety_window_seconds is None:
            safety_window_seconds = _DEFAULT_SAFETY_WINDOW_SECONDS
            logger.info(
                "stream %r: no safety_window_seconds in the replication block; "
                "applying engine default of %d seconds",
                stream_name,
                safety_window_seconds,
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
        from datetime import datetime, timedelta

        from dateutil.parser import isoparse

        cursor_str = str(cursor)
        try:
            cursor_dt: datetime = isoparse(cursor_str)
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
        base_params: dict[str, Any],
        pagination: dict[str, Any],
        batch_size: int,
        records_ref: str,
        metadata_spec: dict[str, Any],
        resolver: Resolver,
        build_request: Callable[[dict[str, Any]], tuple[dict[str, Any], Any]],
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Drive the pagination loop, building each page request.

        Each page request is built via ``build_request``: it maps the page's
        full param table to the ``(query_params, body)`` actually sent,
        applying declared param placement and per-page body binding.

        Every strategy runs the same loop: read a page, emit it, evaluate the
        contract's ``stop_when`` predicate against that page's ``response``
        scope, then ask the strategy where the next page comes from. The
        engine contributes exactly one termination rule of its own — a page
        with no records ends the read — because a strategy whose predicate
        never fires would otherwise request forever, and "the provider
        returned nothing" is the one signal that means done under every
        strategy.
        """
        p_type = pagination.get("type")
        if not p_type:
            query, body = build_request(dict(base_params))
            page = await self._request_page(
                full_url, method, query, records_ref, body=body
            )
            if page.records:
                yield page.records
            return

        stop_when = pagination.get("stop_when")
        if not isinstance(stop_when, Mapping) or not stop_when:
            raise ReadError(
                f"{p_type!r} pagination requires a stop_when predicate; "
                f"the endpoint declares none"
            )

        limit_block = pagination.get("limit") or {}
        effective_limit = self._effective_limit(limit_block, batch_size, resolver)
        limit_param = limit_block.get("param")
        if limit_param:
            base_params[limit_param] = effective_limit

        params = dict(base_params)
        advance = self._build_advance(
            p_type, pagination, params, effective_limit, resolver, full_url
        )

        url = full_url
        send_params = True
        while True:
            query, body = build_request(params)
            page = await self._request_page(
                url, method, query if send_params else {}, records_ref, body=body
            )
            if not page.records:
                return
            yield page.records

            page_resolver = resolver.with_context(
                replace(
                    resolver.context,
                    response=self._response_scope(page, metadata_spec, resolver),
                )
            )
            try:
                if evaluate_predicate(stop_when, page_resolver):
                    return
            except TransportSpecError as err:
                raise ReadError(
                    f"{p_type!r} pagination: stop_when predicate is not "
                    f"evaluable against this response: {err}"
                ) from err

            following = advance(page, page_resolver)
            if following is None:
                return
            url, params, send_params = following

    def _build_advance(
        self,
        p_type: str,
        pagination: dict[str, Any],
        params: dict[str, Any],
        effective_limit: int,
        resolver: Resolver,
        full_url: str,
    ) -> Callable[[_Page, Resolver], _NextRequest | None]:
        """Seed *params* for the first page and return the per-strategy step.

        The returned callable receives the page just read plus a resolver
        scoped to it, and answers where the following page comes from —
        ``None`` when the strategy itself has run out (an unresolvable next
        cursor / next link, a last record carrying no key). ``stop_when``
        remains the declared stop condition; this is only the mechanical
        "can I even build another request" question.
        """
        if p_type == "offset":
            block = pagination.get("offset") or {}
            param = _required_setting(block, "offset", "param")
            position = _int_setting(block, "offset", "initial", resolver, default=0)
            # The contract defines the offset step as the effective page size
            # unless the author overrides it, so a provider that counts in
            # records and one that counts in pages both stay expressible.
            step = _int_setting(
                block, "offset", "increment_by", resolver, default=effective_limit
            )
            params[param] = position

            def advance_offset(
                page: _Page, page_resolver: Resolver
            ) -> _NextRequest | None:
                nonlocal position
                position += step
                params[param] = position
                return _NextRequest(full_url, params, True)

            return advance_offset

        if p_type == "page":
            block = pagination.get("page") or {}
            param = _required_setting(block, "page", "param")
            number = _int_setting(block, "page", "initial", resolver, default=1)
            step = _int_setting(block, "page", "increment_by", resolver, default=1)
            params[param] = number

            def advance_page(
                page: _Page, page_resolver: Resolver
            ) -> _NextRequest | None:
                nonlocal number
                number += step
                params[param] = number
                return _NextRequest(full_url, params, True)

            return advance_page

        if p_type == "cursor":
            block = pagination.get("cursor") or {}
            param = _required_setting(block, "cursor", "param")
            next_cursor = block.get("next_cursor")
            if next_cursor is None:
                raise ReadError(
                    "cursor pagination requires cursor.next_cursor (a value "
                    "expression resolving to the next page's token)"
                )
            # No first-page cursor: the token only exists once a response has
            # produced one. The param stays absent rather than being sent empty.
            params.pop(param, None)

            def advance_cursor(
                page: _Page, page_resolver: Resolver
            ) -> _NextRequest | None:
                token = page_resolver.resolve_for_request(next_cursor)
                if _is_blank(token):
                    # The declared expression found no token, which is how a
                    # cursor provider says "last page". Note the difference
                    # from the old behaviour: the field consulted is the one
                    # the endpoint declared, never a hardcoded `next_cursor`.
                    logger.debug(
                        "cursor pagination: next_cursor expression yielded no "
                        "token; ending read"
                    )
                    return None
                params[param] = token
                return _NextRequest(full_url, params, True)

            return advance_cursor

        if p_type == "link":
            block = pagination.get("link") or {}
            next_url = block.get("next_url")
            if next_url is None:
                raise ReadError(
                    "link pagination requires link.next_url (a value "
                    "expression resolving to the next page's absolute URL)"
                )

            def advance_link(
                page: _Page, page_resolver: Resolver
            ) -> _NextRequest | None:
                target = page_resolver.resolve_for_request(next_url)
                if _is_blank(target):
                    logger.debug(
                        "link pagination: next_url expression yielded no link; "
                        "ending read"
                    )
                    return None
                if not isinstance(target, str):
                    raise ReadError(
                        f"link pagination: next_url resolved to "
                        f"{type(target).__name__}, expected a URL string"
                    )
                # The provider's link already carries the full query, so the
                # declared params must not be appended again — doing so
                # duplicates keys (`?limit=100&limit=100`). The body, which
                # the link cannot express, is still built and sent.
                return _NextRequest(target, params, False)

            return advance_link

        if p_type == "keyset":
            block = pagination.get("keyset") or {}
            param = _required_setting(block, "keyset", "param")
            order_by_field = block.get("order_by_field")
            if not order_by_field or not isinstance(order_by_field, str):
                raise ReadError(
                    "keyset pagination requires keyset.order_by_field (the "
                    "dotted record field path the pages are ordered by)"
                )
            field_path = order_by_field.split(".")
            initial = block.get("initial")
            if initial is not None:
                params[param] = resolver.resolve_for_request(initial)
            else:
                params.pop(param, None)

            def advance_keyset(
                page: _Page, page_resolver: Resolver
            ) -> _NextRequest | None:
                # The raw record value (int/str/Decimal) feeds both the query
                # string and any body binding through build_request, so it
                # stays native here; build_request stringifies Decimals only
                # for the query (where yarl would truncate them) and keeps the
                # body numeric.
                last_key = walk_path(page.records[-1], field_path)
                if last_key is None:
                    logger.debug(
                        "keyset pagination: last record has no %r value; "
                        "ending read",
                        order_by_field,
                    )
                    return None
                params[param] = last_key
                return _NextRequest(full_url, params, True)

            return advance_keyset

        raise ReadError(f"Unsupported pagination type: {p_type!r}")

    def _effective_limit(
        self, limit_block: dict[str, Any], batch_size: int, resolver: Resolver
    ) -> int:
        """Page size for this read: the declared default, clamped to the max.

        ``limit.default`` is a value expression (commonly
        ``{"ref": "runtime.batch_size"}``), so the author decides whether the
        provider page size tracks the engine's batch size or is fixed. With no
        default declared the engine's batch size stands in. ``limit.max`` is
        the provider's ceiling and always wins.
        """
        declared = limit_block.get("default")
        value: Any = batch_size
        if declared is not None:
            resolved = resolver.resolve_for_request(declared)
            if resolved is None:
                logger.warning(
                    "pagination.limit.default did not resolve; falling back to "
                    "the engine batch size of %d",
                    batch_size,
                )
            else:
                value = resolved
        limit = _as_positive_int(value, "pagination.limit.default")

        max_limit = limit_block.get("max")
        if max_limit is not None:
            limit = min(limit, _as_positive_int(max_limit, "pagination.limit.max"))
        return limit

    @staticmethod
    def _response_scope(
        page: _Page, metadata_spec: dict[str, Any], resolver: Resolver
    ) -> dict[str, Any]:
        """Build the ``response`` resolution scope for one page.

        The key set is the contract's reserved response vocabulary, so a
        ``stop_when`` predicate or a ``next_cursor`` / ``next_url`` expression
        can address any of it. ``metadata`` resolves last, against the scope
        the other keys already form, because metadata expressions are
        themselves written in terms of ``response.body``.
        """
        scope: dict[str, Any] = {
            "body": page.body,
            "records": page.records,
            "record_count": len(page.records),
            "status": page.status,
            "headers": page.headers,
        }
        if metadata_spec:
            metadata_resolver = resolver.with_context(
                replace(resolver.context, response=scope)
            )
            scope["metadata"] = {
                name: metadata_resolver.resolve_for_request(expression)
                for name, expression in metadata_spec.items()
            }
        return scope

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _request_page(
        self,
        url: str,
        method: str,
        params: dict[str, Any],
        records_ref: str,
        *,
        body: Any = None,
    ) -> _Page:
        if self.session is None:
            raise ReadError(
                "APIConnector: HTTP request attempted before connect() opened "
                "the session"
            )
        if self.rate_limiter:
            await self.rate_limiter.acquire()
        logger.debug("API %s %s params=%s", method, url, params)
        request_kwargs: dict[str, Any] = {"params": params}
        if body is not None:
            # Resolved ``operations.read.request.body``; only sent when the
            # endpoint declares one, so plain GET reads stay body-less.
            request_kwargs["json"] = body
        async with self.session.request(method, url, **request_kwargs) as response:
            if response.status != 200:
                error_text = await response.text()
                body_snippet = error_text[:500]
                logger.error(
                    "API %d %s %s: %s", response.status, method, url, body_snippet
                )
                detail = (
                    f"API request failed: {method} {url} -> status {response.status}; "
                    f"params={params}; body[:500]={body_snippet!r}"
                )
                # Rate limits and upstream outages heal on retry; other
                # statuses (bad request, auth, missing endpoint) do not.
                if response.status in _TRANSIENT_HTTP_STATUSES:
                    raise TransientReadError(detail)
                raise ReadError(detail)
            data = await response.json(loads=_loads_preserving_decimals)
            # Read inside the context manager: the headers proxy is tied to the
            # live response, and pagination expressions address it as
            # `response.headers.<name>` (a `Link` header, typically).
            headers = dict(response.headers)
            status = response.status
        records = _extract_records(data, records_ref)
        if records:
            self.metrics["records_read"] += len(records)
            self.metrics["batches_read"] += 1
        return _Page(body=data, records=records, status=status, headers=headers)

    # ------------------------------------------------------------------
    # Base interface stubs
    # ------------------------------------------------------------------

    async def write_batch(
        self, batch: list[dict[str, Any]], config: dict[str, Any]
    ) -> None:
        raise NotImplementedError("Source connector is read-only")

    def supports_incremental_read(self) -> bool:
        return True

    def supports_upsert(self) -> bool:
        return True


# ----------------------------------------------------------------------
# Free helpers (pure functions, no connector state)
# ----------------------------------------------------------------------


def _extract_records(data: Any, records_ref: str) -> list[dict[str, Any]]:
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
        cursor = walk_path(body, records_ref[len(prefix) + 1 :].split("."))
    else:
        cursor = None
    if isinstance(cursor, list):
        return [r for r in cursor if isinstance(r, dict)]
    if isinstance(cursor, dict):
        return [cursor]
    return []


def _is_blank(value: Any) -> bool:
    """Report whether a resolved next-page token / link carries nothing usable.

    Absent, or a string that is empty or all whitespace — which is how a
    provider signals "no further pages". Deliberately not a plain falsiness
    test: a numeric cursor of ``0`` is a real token.
    """
    return value is None or (isinstance(value, str) and not value.strip())


def _required_setting(block: dict[str, Any], strategy: str, key: str) -> str:
    """Read a required non-empty string setting off a pagination strategy block."""
    value = block.get(key)
    if not isinstance(value, str) or not value:
        raise ReadError(f"{strategy} pagination requires {strategy}.{key}")
    return value


def _int_setting(
    block: dict[str, Any],
    strategy: str,
    key: str,
    resolver: Resolver,
    *,
    default: int,
) -> int:
    """Read an integer pagination setting that may be authored as an expression.

    The contract types these ``Any`` because ``{"ref": "..."}`` is as valid an
    initial offset as ``0``. An authored value that resolves to something
    non-integral is a defect worth naming rather than coercing.
    """
    declared = block.get(key)
    if declared is None:
        return default
    resolved = resolver.resolve_for_request(declared)
    if resolved is None:
        logger.warning(
            "pagination.%s.%s did not resolve; falling back to %d",
            strategy,
            key,
            default,
        )
        return default
    return _as_int(resolved, f"pagination.{strategy}.{key}")


def _as_int(value: Any, label: str) -> int:
    """Coerce a resolved pagination setting to ``int``, or fail naming it."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal, str)):
        raise ReadError(
            f"{label} must be an integer, got {type(value).__name__}: {value!r}"
        )
    try:
        return int(value)
    except (TypeError, ValueError) as err:
        raise ReadError(f"{label} must be an integer, got {value!r}") from err


def _as_positive_int(value: Any, label: str) -> int:
    """Coerce a resolved page-size setting to a positive ``int``.

    A non-positive page size would either send ``limit=0`` upstream or make
    the offset loop never advance, so it fails here rather than at the
    provider (or not at all).
    """
    number = _as_int(value, label)
    if number < 1:
        raise ReadError(f"{label} must be >= 1, got {number}")
    return number
