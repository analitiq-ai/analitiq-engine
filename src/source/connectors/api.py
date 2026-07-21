"""Contract-native REST API source connector.

This connector consumes the published API-endpoint contract as its typed
model: the ``endpoint_document`` from the source config is re-validated
into :class:`~analitiq.contracts.endpoints.ApiEndpointDoc` once per read
(issue #349), and every declaration is read as a model attribute:

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
* ``operations.read.pagination`` — all five contract strategies (offset /
  page / cursor / link / keyset) driven by their declared params, with the
  ``next_cursor`` / ``next_url`` value expressions and the strategy's
  ``stop_when`` predicate evaluated against each page's response.
* ``operations.read.response.records`` — record extraction path
  (e.g. ``response.body`` or ``response.body.<field>``).
* ``operations.read.replication.cursor_mappings`` — cursor-field ↔
  query-param map used to build incremental ``WHERE`` filters.

The source-config dict carries the resolved contract documents and
per-stream overrides; nothing else.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable
from datetime import timezone
from decimal import Decimal
from typing import Any
from urllib.parse import SplitResult, urlsplit

import aiohttp
import pyarrow as pa
from analitiq.contracts.endpoints import (
    ApiEndpointDoc,
    CursorPagination,
    LinkPagination,
    OffsetPagination,
    PagePagination,
    Pagination,
    Predicate,
    ReadOperation,
    Replication,
    ResponseExtraction,
    SingleCursorMapping,
)
from pydantic import ValidationError

from cdk.connection_runtime import ConnectionRuntime
from cdk.exceptions import TransportSpecError
from cdk.rate_limiter import RateLimiter
from cdk.request_binding import bind_param_refs, resolve_param_defaults
from cdk.resolver import Resolver
from cdk.schema_contract import SchemaContract
from cdk.type_map import TypeMapper, UnmappedTypeError
from cdk.types import CheckpointStore, EndpointScope

from ...models.state import ReplicationConfig
from ...shared.dict_path import walk_path
from ...shared.http_utils import join_url
from .base import BaseConnector, ConnectorConnectionError, ReadError, TransientReadError
from .response_expr import evaluate_predicate, resolve_response_expr

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
            raise ConnectorConnectionError(f"API connection failed: {e}") from e

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

    # Complexity is inherent to the single read pass (validate, resolve
    # params, replication, pagination); the pre-#349 dict version carried the
    # same branching and is a tolerated occurrence on the default branch.
    async def _read_batches_impl(  # skipcq: PY-R1000
        self,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Drive the contract-typed read: validate, page, and yield batches."""
        if partition is None:
            partition = {}

        if self._runtime is None or self.base_url is None:
            raise ReadError(
                "APIConnector: read attempted before connect() materialized "
                "the runtime"
            )

        raw_endpoint = config.get("endpoint_document")
        if not raw_endpoint:
            raise ReadError("APIConnector: source config missing 'endpoint_document'")
        try:
            endpoint_doc = ApiEndpointDoc.model_validate(raw_endpoint)
        except ValidationError as err:
            # A document that fails the contract is a deterministic config
            # defect: fail fast instead of hand-parsing an unknown shape.
            raise ReadError(
                f"APIConnector: endpoint document failed api-endpoint "
                f"contract validation: {err}"
            ) from err
        stream_source = config.get("stream_source") or {}
        endpoint_ref = stream_source.get("endpoint_ref")
        if not endpoint_ref:
            raise ReadError(
                "APIConnector: stream_source missing 'endpoint_ref'; "
                "the source contract requires it to declare per-field types"
            )
        read = endpoint_doc.operations.read
        if read is None:
            raise ReadError(
                f"endpoint {endpoint_doc.endpoint_id!r}: operations.read is "
                f"required to read this endpoint as a source"
            )
        records_items_schema = self._resolve_records_items_schema(
            endpoint_doc.endpoint_id,
            read.response,
        )
        self._apply_read_type_map(records_items_schema, endpoint_ref)
        schema_contract = SchemaContract(records_items_schema)
        full_url = join_url(self.base_url, read.request.path)
        method = read.request.method

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
            read, resolver, stream_source.get("filters") or []
        )

        # Set up incremental replication: subtract safety window from the
        # stored cursor, then write the value into the cursor's mapped
        # param.
        if replication_method == "incremental":
            await self._apply_incremental_replication(
                param_values,
                read.replication,
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
        raw_body = read.request.body

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

        records_ref = read.response.records.ref

        batch_count = 0
        total_records = 0
        async for batch in self._iterate_pages(
            full_url=full_url,
            method=method,
            base_params=param_values,
            pagination=read.pagination,
            batch_size=batch_size,
            records_ref=records_ref,
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
        endpoint_id: str,
        response: ResponseExtraction,
    ) -> dict[str, Any]:
        """Walk operations.read.response.schema via records.ref to per-record items.

        Accepted records.ref forms: ``response.body`` and
        ``response.body.<field>[.<field>...]``. The response schema itself
        is free-form JSON Schema in the contract (``dict[str, Any]``), so
        the walk below stays dict-shaped.
        """
        response_schema = response.schema_
        records_ref = response.records.ref

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
        read: ReadOperation,
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
        declared = read.params or {}
        placements: dict[str, str] = {
            name: decl.location for name, decl in declared.items()
        }

        # ``resolve_param_defaults`` is a CDK helper and consumes the
        # authored JSON shape, so the uncontrolled params are dumped back
        # to dicts at this boundary.
        uncontrolled = {
            n: d.model_dump(mode="json", by_alias=True, exclude_unset=True)
            for n, d in declared.items()
            if not d.controlled_by
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
        replication: Replication | None,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any],
        cursor_field: str | None,
        safety_window_seconds: int | None,
    ) -> None:
        """Write the stored cursor (minus safety window) into its mapped param."""
        if not cursor_field:
            return
        mappings = replication.cursor_mappings if replication is not None else []
        # Only single-param mappings drive the incremental filter. A window
        # mapping (start/end params) matching the cursor field is not
        # implemented as a filter source; it falls through to the loud
        # full-replication warning below rather than half-binding one side.
        param_name = next(
            (
                m.param
                for m in mappings
                if isinstance(m, SingleCursorMapping) and m.cursor_field == cursor_field
            ),
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

    @staticmethod
    def _stop_requested(stop_when: Predicate, page_resolver: Resolver) -> bool:
        """Evaluate the strategy's declared ``stop_when`` against a page."""
        try:
            return evaluate_predicate(stop_when, page_resolver)
        except (ValueError, KeyError, TransportSpecError) as err:
            raise ReadError(f"pagination stop_when failed to evaluate: {err}") from err

    @staticmethod
    def _resolve_response_value(
        expr: Any, page_resolver: Resolver, *, context: str
    ) -> Any:
        """Resolve a response-scope value expression, failing deterministically."""
        try:
            return resolve_response_expr(expr, page_resolver)
        except (ValueError, KeyError, TransportSpecError) as err:
            raise ReadError(f"pagination {context} failed to resolve: {err}") from err

    @staticmethod
    def _same_origin(base: SplitResult, target: SplitResult) -> bool:
        """Whether two split URLs share scheme, host, and effective port.

        Compares normalized parts (case-insensitive scheme/host, default
        ports made explicit) so ``https://api.example.test:443`` and
        ``https://API.example.test`` count as the origin they are.
        """
        default_ports = {"http": 80, "https": 443}
        base_scheme = base.scheme.lower()
        target_scheme = target.scheme.lower()
        return (
            base_scheme == target_scheme
            and (base.hostname or "").lower() == (target.hostname or "").lower()
            and (base.port or default_ports.get(base_scheme))
            == (target.port or default_ports.get(target_scheme))
        )

    @staticmethod
    def _positive_step(value: Any, *, context: str) -> int:
        """Parse an authored ``increment_by`` into a positive int step.

        A step of zero or less can never advance its loop — the same
        request would repeat unbounded — and a non-numeric one is an
        authoring defect; both fail deterministically before any request.
        """
        try:
            step = int(value)
        except (TypeError, ValueError) as err:
            raise ReadError(
                f"pagination {context} must be an integer, got {value!r}"
            ) from err
        if step <= 0:
            raise ReadError(f"pagination {context} must be positive, got {step}")
        return step

    # One loop per pagination strategy; the pre-#346 version carried the same
    # branching and is a tolerated occurrence on the default branch.
    async def _iterate_pages(  # skipcq: PY-R1000
        self,
        *,
        full_url: str,
        method: str,
        base_params: dict[str, Any],
        pagination: Pagination | None,
        batch_size: int,
        records_ref: str,
        resolver: Resolver,
        build_request: Callable[[dict[str, Any]], tuple[dict[str, Any], Any]],
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Drive the pagination loop, building each page request.

        Each page request is built via ``build_request``: it maps the page's
        full param table to the ``(query_params, body)`` actually sent,
        applying declared param placement and per-page body binding.
        ``resolver`` gains the page's body as its ``response`` scope for the
        per-page expressions (``stop_when``, ``next_cursor``, ``next_url``).

        Every strategy stops when its declared ``stop_when`` predicate holds
        for the page's response. An empty page always stops (there is nothing
        left to yield), and the count-driven strategies (offset, page, keyset)
        also stop on a short page, which no non-empty page can follow. Each
        page's continuation — the stop decision and the value it advances
        on — is computed BEFORE the page is yielded, so a page that cannot
        be followed correctly fails before the engine can commit it.
        """
        if pagination is None:
            query, body = build_request(dict(base_params))
            records = await self._request_records(
                full_url, method, query, records_ref, body=body
            )
            if records:
                yield records
            return

        if isinstance(pagination, LinkPagination):
            async for records in self._iterate_link_pages(
                full_url=full_url,
                method=method,
                base_params=base_params,
                pagination=pagination,
                records_ref=records_ref,
                resolver=resolver,
                build_request=build_request,
            ):
                yield records
            return

        if pagination.limit is not None and pagination.limit.param:
            base_params[pagination.limit.param] = batch_size

        if isinstance(pagination, OffsetPagination):
            offset_param = pagination.offset.param
            offset = int(pagination.offset.initial)
            # An authored ``increment_by`` fixes the step; otherwise the
            # offset advances by the page size actually requested.
            offset_step = (
                self._positive_step(
                    pagination.offset.increment_by, context="offset.increment_by"
                )
                if pagination.offset.increment_by is not None
                else batch_size
            )
            while True:
                params = dict(base_params)
                params[offset_param] = offset
                query, body = build_request(params)
                data, records = await self._request_payload(
                    full_url, method, query, records_ref, body=body
                )
                if not records:
                    return
                page_resolver = resolver.with_response({"body": data})
                stop = self._stop_requested(pagination.stop_when, page_resolver)
                short = len(records) < batch_size
                yield records
                if stop or short:
                    return
                offset += offset_step

        elif isinstance(pagination, PagePagination):
            page_param = pagination.page.param
            page = int(pagination.page.initial)
            # An authored ``increment_by`` fixes the step; page numbers
            # advance by one otherwise.
            page_step = (
                self._positive_step(
                    pagination.page.increment_by, context="page.increment_by"
                )
                if pagination.page.increment_by is not None
                else 1
            )
            while True:
                params = dict(base_params)
                params[page_param] = page
                query, body = build_request(params)
                data, records = await self._request_payload(
                    full_url, method, query, records_ref, body=body
                )
                if not records:
                    return
                page_resolver = resolver.with_response({"body": data})
                stop = self._stop_requested(pagination.stop_when, page_resolver)
                short = len(records) < batch_size
                yield records
                if stop or short:
                    return
                page += page_step

        elif isinstance(pagination, CursorPagination):
            cursor_param = pagination.cursor.param
            # The declared ``cursor.next_cursor`` value expression names the
            # next-page token in the response; a token that resolves to
            # nothing ends the loop. The token keeps its native type — the
            # query/body sinks in ``build_request`` own serialization.
            cursor_token: Any = None
            while True:
                params = dict(base_params)
                if cursor_token is not None:
                    params[cursor_param] = cursor_token
                query, body = build_request(params)
                data, records = await self._request_payload(
                    full_url, method, query, records_ref, body=body
                )
                if not records:
                    return
                page_resolver = resolver.with_response({"body": data})
                stop = self._stop_requested(pagination.stop_when, page_resolver)
                cursor_token = None
                if not stop:
                    cursor_token = self._resolve_response_value(
                        pagination.cursor.next_cursor,
                        page_resolver,
                        context="cursor.next_cursor",
                    )
                yield records
                if stop or cursor_token is None or cursor_token == "":  # nosec B105
                    return

        else:
            # KeysetPagination by narrowing: the pinned contract's pagination
            # union is closed and every other strategy is dispatched above.
            keyset_param = pagination.keyset.param
            order_by_field = pagination.keyset.order_by_field
            # The declared ``initial`` seeds the first request when authored;
            # afterwards the key advances from the last record of each page.
            # The key keeps its native type (int/str/Decimal) — the
            # query/body sinks in ``build_request`` own serialization.
            last_key: Any = pagination.keyset.initial
            # A short page ends the loop only when this read controls the
            # page size (a declared limit param was sent as batch_size).
            # Without one, the provider's own page size bounds every page,
            # so a short page says nothing about whether a next key exists —
            # only an empty page or stop_when may end the loop then.
            limit_sent = pagination.limit is not None and bool(pagination.limit.param)
            while True:
                params = dict(base_params)
                if last_key is not None:
                    params[keyset_param] = last_key
                query, body = build_request(params)
                data, records = await self._request_payload(
                    full_url, method, query, records_ref, body=body
                )
                if not records:
                    return
                page_resolver = resolver.with_response({"body": data})
                stop = self._stop_requested(pagination.stop_when, page_resolver)
                short = limit_sent and len(records) < batch_size
                if not stop and not short:
                    # Validate the continuation before yielding: a page the
                    # loop cannot advance from must fail before the engine
                    # can commit it downstream.
                    last_key = records[-1].get(order_by_field)
                    if last_key is None:
                        raise ReadError(
                            f"keyset pagination: the last record of a page "
                            f"has no {order_by_field!r} value to advance "
                            f"from; keyset.order_by_field must be present "
                            f"on every record"
                        )
                yield records
                if stop or short:
                    return

    async def _iterate_link_pages(
        self,
        *,
        full_url: str,
        method: str,
        base_params: dict[str, Any],
        pagination: LinkPagination,
        records_ref: str,
        resolver: Resolver,
        build_request: Callable[[dict[str, Any]], tuple[dict[str, Any], Any]],
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Follow ``link.next_url`` from each response to the next page.

        Per the contract, the resolved URL replaces the entire request:
        follow-up pages carry no query params and no body (spec: §Pagination
        Strategies — link, "no params traverse"). A relative next URL joins
        the connection's base URL; an absolute one must stay on the
        connection's origin — the shared session sends the connection's
        default headers (auth included) on every request, so following a
        response-supplied URL to another host would hand those credentials
        to it. The next URL is resolved and vetted BEFORE its page is
        yielded, so a malformed link fails before the engine can commit
        the page.
        """
        base_url = self.base_url
        if base_url is None:
            raise ReadError(
                "APIConnector: link pagination attempted before connect() "
                "materialized the base URL"
            )
        origin = urlsplit(base_url)
        query, body = build_request(dict(base_params))
        data, records = await self._request_payload(
            full_url, method, query, records_ref, body=body
        )
        while True:
            if not records:
                return
            page_resolver = resolver.with_response({"body": data})
            stop = self._stop_requested(pagination.stop_when, page_resolver)
            next_target: str | None = None
            if not stop:
                next_url = self._resolve_response_value(
                    pagination.link.next_url, page_resolver, context="link.next_url"
                )
                if next_url is not None and next_url != "":
                    if not isinstance(next_url, str):
                        raise ReadError(
                            f"link pagination: next_url resolved to a "
                            f"{type(next_url).__name__}, expected a URL string"
                        )
                    # Classify by parsing, not by string prefix: a URL
                    # carrying any scheme or authority is absolute (case
                    # included), and must then pass the origin check —
                    # which also rejects non-HTTP schemes and ambiguous
                    # protocol-relative URLs loudly.
                    target = urlsplit(next_url)
                    if target.scheme or target.netloc:
                        if not self._same_origin(origin, target):
                            raise ReadError(
                                f"link pagination: next_url {next_url!r} "
                                f"leaves the connection's origin "
                                f"{origin.scheme}://{origin.netloc}; refusing "
                                f"to send the connection's headers to "
                                f"another host"
                            )
                        next_target = next_url
                    else:
                        next_target = join_url(base_url, next_url)
            yield records
            if next_target is None:
                return
            data, records = await self._request_payload(
                next_target, method, {}, records_ref
            )

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _request_records(
        self,
        url: str,
        method: str,
        params: dict[str, Any],
        records_ref: str,
        *,
        body: Any = None,
    ) -> list[dict[str, Any]]:
        _, records = await self._request_payload(
            url, method, params, records_ref, body=body
        )
        return records

    async def _request_payload(
        self,
        url: str,
        method: str,
        params: dict[str, Any],
        records_ref: str,
        *,
        body: Any = None,
    ) -> tuple[Any, list[dict[str, Any]]]:
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
        self.metrics["records_read"] += 0  # incremented below per page
        records = _extract_records(data, records_ref)
        if records:
            self.metrics["records_read"] += len(records)
            self.metrics["batches_read"] += 1
        return data, records

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
