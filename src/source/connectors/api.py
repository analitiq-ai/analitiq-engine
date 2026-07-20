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
import hashlib
import json
import logging
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import timezone
from decimal import Decimal
from typing import Any, NamedTuple
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import aiohttp
import pyarrow as pa
from multidict import CIMultiDict

from cdk.connection_runtime import ConnectionRuntime
from cdk.exceptions import TransportSpecError, UnresolvedValueError
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

# Redirect statuses. Refused rather than followed -- see _request_page.
_REDIRECT_HTTP_STATUSES = frozenset({301, 302, 303, 307, 308})

# Lookback subtracted from the stored cursor on an incremental read when the
# stream has a prior cursor but declares no safety window of its own. It is an
# operational safety default, not a per-connector attribute, so connectors
# never declare it. Sourced from ReplicationConfig; SourceConfig carries the
# same default independently (models/state.py), so the two move separately.
_DEFAULT_SAFETY_WINDOW_SECONDS: int = ReplicationConfig.safety_window_seconds


@contextmanager
def _document_errors(context: str, suffix: str = "is not resolvable") -> Iterator[None]:
    """Classify any failure resolving a document expression as a config error.

    Everything the endpoint document declares -- param defaults, the request
    body, the page size, a strategy's setup values, the stop predicate, the
    next-page expression -- is resolved through the same grammar and fails the
    same two ways: ``TransportSpecError`` for a malformed node, and a plain
    ``KeyError`` from the resolver for an unknown scope name (``runtme``,
    ``respones``). Neither derives from anything the source worker treats as
    deterministic, so an unwrapped one is reported as an internal, retryable
    failure: the run is retried until it exhausts its attempts on a typo that
    cannot fix itself, and the log points at the engine rather than at the
    document.

    One guard rather than a ``try`` at each call site, so the set of places
    this applies is greppable and a new resolution point is an obvious
    omission.
    """
    try:
        yield
    except (TransportSpecError, KeyError) as err:
        raise ReadError(f"{context} {suffix}: {err}") from err


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
    headers: Mapping[str, str]


class _NextRequest(NamedTuple):
    """Where the following page comes from.

    ``send_params`` is False only for ``link`` pagination, whose next URL
    already carries the provider's own query string; re-appending the
    declared params there would duplicate keys.
    """

    url: str
    params: dict[str, Any]
    send_params: bool


@dataclass(frozen=True)
class _RequestState:
    """The request-building context a pagination strategy draws on.

    Bundled rather than passed as separate positional arguments because each
    strategy needs a different subset — a link strategy has no use for the base
    URL, a cursor strategy resolves against the page rather than this resolver
    — and a uniform signature would otherwise carry dead parameters into every
    builder.

    ``params`` is the live param table: a builder seeds it with its strategy's
    first-page values and owns it thereafter.
    """

    params: dict[str, Any]
    resolver: Resolver
    url: str


@dataclass(frozen=True)
class _ReadPlan:
    """What one read draws from its endpoint document, resolved once up front.

    Every field here is fixed for the whole read — the document never changes
    between pages — so it is validated before the first request rather than
    re-derived per page.
    """

    stream_source: dict[str, Any]
    read_spec: dict[str, Any]
    schema_contract: SchemaContract
    method: str
    url: str
    records_ref: str
    metadata_spec: dict[str, Any]
    raw_body: Any


# What a strategy answers each page: where the following request comes from,
# or None when the strategy itself has run out.
_Advance = Callable[[_Page, Resolver], "_NextRequest | None"]
# What builds one, given the strategy's own block plus the request state.
_AdvanceBuilder = Callable[[dict[str, Any], _RequestState], _Advance]
# One materialized request, reduced to a comparable identity.
_Wire = bytes
# Turns one page's param table into the (query, body) actually sent.
_RequestBuilder = Callable[[dict[str, Any]], "tuple[dict[str, Any], Any]"]


def _make_request_builder(
    param_placements: Mapping[str, str],
    raw_body: Any,
    resolver: Resolver,
) -> _RequestBuilder:
    """Build the per-page ``params -> (query, body)`` step for one read.

    Each page request materializes from the full per-page param table
    (declared defaults + filters + replication + whatever the pagination loop
    set): the query string carries every param not declared ``in: body``,
    while the body binds ``{"from_param": ...}`` nodes against that same
    table. Rebuilt per page so controlled params (limit, offset, cursor)
    reach a body-paginated endpoint instead of freezing at their first-page
    values.
    """

    def build_request(page_params: dict[str, Any]) -> tuple[dict[str, Any], Any]:
        # A fractional value from the lossless JSON parse (e.g. a keyset key)
        # arrives as a Decimal, which neither sink takes as-is, so each
        # placement converts it to the form its serializer needs; int/str stay
        # native.
        query = {
            # yarl truncates a Decimal in the query string; stringify it
            # (full precision).
            name: str(value) if isinstance(value, Decimal) else value
            for name, value in page_params.items()
            if param_placements.get(name) != "body"
        }
        if raw_body is None:
            return query, None
        # aiohttp serializes the body via stdlib json.dumps, which cannot
        # encode a Decimal. Narrow body Decimals to float so the value stays a
        # JSON number a numeric body schema accepts -- the same float the body
        # carried before the lossless parse.
        body_params = {
            name: float(value) if isinstance(value, Decimal) else value
            for name, value in page_params.items()
        }
        body = resolver.resolve_for_request(bind_param_refs(raw_body, body_params))
        return query, body

    return build_request


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

        plan = self._plan_read(config, self.base_url)
        read_spec = plan.read_spec
        stream_source = plan.stream_source

        # One resolver covers everything this read materializes per request:
        # declared param defaults, the optional request body, the stop
        # predicate and every pagination expression. Expression nodes that do
        # not resolve are omitted (with a warning) instead of being serialized
        # verbatim onto the wire.
        resolver = self._runtime.request_resolver(
            runtime_values={"batch_size": batch_size}
        )

        # `runtime.batch_size` means the page size actually requested, for the
        # whole read. `limit.default` resolves first, against the engine batch
        # size — that is what "track the engine batch size" means — and
        # `limit.max` may then clamp it below. Everything downstream sees the
        # clamped result: the stop predicate, the strategy expressions, and the
        # request body alike. Binding it once here rather than per consumer is
        # what keeps them from disagreeing — a body that asks for one page size
        # while the predicate judges against another silently mismatches what
        # the provider was told.
        pagination = read_spec.get("pagination") or {}
        with _document_errors("pagination.limit"):
            effective_limit = self._effective_limit(
                pagination.get("limit") or {}, batch_size, resolver
            )
        resolver = resolver.with_context(
            replace(
                resolver.context,
                runtime={**resolver.context.runtime, "batch_size": effective_limit},
            )
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

        build_request = _make_request_builder(param_placements, plan.raw_body, resolver)

        batch_count = 0
        async for batch in self._iterate_pages(
            full_url=plan.url,
            method=plan.method,
            base_params=param_values,
            pagination=pagination,
            effective_limit=effective_limit,
            records_ref=plan.records_ref,
            metadata_spec=plan.metadata_spec,
            resolver=resolver,
            build_request=build_request,
        ):
            batch_count += 1
            # Read before emitting: a record set carrying no `cursor_field` is
            # a document defect, knowable now, and nothing should reach the
            # destination on a stream that cannot run. The *save* stays after
            # the yield -- checkpointing a cursor for records that have not
            # been emitted would let a resume skip them.
            cursor_value = (
                self._cursor_value(batch, cursor_field, stream_name, batch_count)
                if cursor_field
                else None
            )

            yield plan.schema_contract.from_pylist(batch)

            if cursor_value is not None:
                await checkpoint.save_cursor(
                    stream_name, partition, {"cursor": cursor_value}
                )

    @staticmethod
    def _cursor_value(
        batch: list[dict[str, Any]],
        cursor_field: str,
        stream_name: str,
        batch_count: int,
    ) -> Any:
        """Cursor to checkpoint at the end of *batch*, or None to leave it be.

        Absent and null are opposite situations here, the same way they are
        for a keyset ordering field. A record that carries no ``cursor_field``
        at all means the stream declared a field its records do not have: the
        cursor would never advance, every run would re-read the whole stream
        from the beginning, and nothing would say so. A field that is present
        and null is a record the provider has no cursor value for -- unusual,
        but the stream is otherwise wired correctly.
        """
        last_record = batch[-1]
        if cursor_field not in last_record:
            raise ReadError(
                f"stream {stream_name!r}: records carry no {cursor_field!r} "
                f"field, so the incremental cursor can never advance and every "
                f"run would re-read the whole stream. Declared keys on the "
                f"record: {sorted(last_record)}"
            )
        cursor_value = last_record[cursor_field]
        if cursor_value is None:
            # Safe under at-least-once + upsert (a resume re-reads), but said
            # out loud: an author debugging "incremental stream keeps
            # re-reading its tail" needs this signal, and the worker logs at
            # INFO so debug would never reach them.
            logger.warning(
                "stream %r: last record has a null %r; "
                "cursor not advanced for batch %d",
                stream_name,
                cursor_field,
                batch_count,
            )
        return cursor_value

    def _plan_read(self, config: dict[str, Any], base_url: str) -> _ReadPlan:
        """Validate the endpoint document once and pull out what a read needs.

        Everything here is fixed for the whole read: which document, which
        record schema, which URL and method, where the records and metadata
        live in a response. Resolving it up front keeps the read loop about
        paging rather than about document shape, and means a malformed
        document fails before the first request goes out.
        """
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
            endpoint_doc, read_spec
        )
        self._apply_read_type_map(records_items_schema, endpoint_ref)

        request = read_spec.get("request") or {}
        path = request.get("path")
        if not isinstance(path, str) or not path:
            raise ReadError(
                f"endpoint {endpoint_doc.get('endpoint_id')!r}: "
                f"operations.read.request.path is required"
            )
        response_block = read_spec.get("response") or {}
        return _ReadPlan(
            stream_source=stream_source,
            read_spec=read_spec,
            schema_contract=SchemaContract(records_items_schema),
            method=(request.get("method") or "GET").upper(),
            url=join_url(base_url, path),
            records_ref=(response_block.get("records") or {}).get(
                "ref", "response.body"
            ),
            metadata_spec=response_block.get("metadata") or {},
            raw_body=request.get("body"),
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
        with _document_errors("operations.read.params"):
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
        effective_limit: int,
        records_ref: str,
        metadata_spec: dict[str, Any],
        resolver: Resolver,
        build_request: _RequestBuilder,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Drive the pagination loop: request, decide, emit, advance.

        Each page request is built via ``build_request``, which maps the
        page's full param table to the ``(query, body)`` actually sent.

        Every strategy runs this same loop. What differs between them is
        confined to ``advance``. What the *document* decides -- when the read
        is done -- is confined to ``_decide_page``. What the *engine* refuses
        to do regardless of the document -- repeat a request forever, continue
        past a page with no records -- is the two guards in the loop body.
        """
        p_type = pagination.get("type")
        if not p_type:
            async for batch in self._read_unpaginated(
                full_url, method, base_params, records_ref, build_request
            ):
                yield batch
            return

        stop_when, advance, params = self._setup_pagination(
            p_type, pagination, base_params, effective_limit, resolver, full_url
        )

        url = full_url
        send_params = True
        pages = 0
        records = 0
        # Every request already sent, not just the last one: a provider that
        # alternates between two next targets (A -> B -> A) passes a
        # compare-with-previous check on every page while making no progress at
        # all, re-emitting the same pages for as long as stop_when stays false.
        sent_wires: set[_Wire] = set()
        while True:
            with _document_errors(f"{p_type!r} pagination: the request body"):
                query, body = build_request(params)
            sent_query = query if send_params else {}
            wire = _canonical_wire(url, sent_query, body)
            self._require_progress(wire, sent_wires, p_type, pages, url)
            sent_wires.add(wire)

            page = await self._request_page(
                url, method, sent_query, records_ref, body=body
            )
            if not page.records:
                self._report_empty_page(p_type, page, records_ref, pages, records)
                return

            pages += 1
            records += len(page.records)
            # Decided in full before any of it is emitted: a yielded batch is
            # enqueued by the extract stage immediately, so emitting first
            # would let a stream with a deterministic document defect
            # transform and write its first page and only then fail.
            stop, following = self._decide_page(
                page, resolver, stop_when, metadata_spec, advance, p_type
            )

            yield page.records

            if stop:
                self._log_read_end(p_type, "stop_when", pages, records)
                return
            if following is None:
                self._log_read_end(p_type, "no further page", pages, records)
                return
            url, params, send_params = following

    async def _read_unpaginated(
        self,
        url: str,
        method: str,
        base_params: dict[str, Any],
        records_ref: str,
        build_request: _RequestBuilder,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Read an endpoint that declares no pagination: one request, one page."""
        with _document_errors("operations.read.request.body"):
            query, body = build_request(dict(base_params))
        page = await self._request_page(url, method, query, records_ref, body=body)
        if page.records:
            yield page.records

    def _setup_pagination(
        self,
        p_type: str,
        pagination: dict[str, Any],
        base_params: dict[str, Any],
        effective_limit: int,
        resolver: Resolver,
        full_url: str,
    ) -> tuple[Mapping[str, Any], _Advance, dict[str, Any]]:
        """Validate the pagination block and seed the first page's params.

        Everything here is settled before the first request goes out, so a
        malformed document fails without touching the provider.
        """
        stop_when = pagination.get("stop_when")
        if not isinstance(stop_when, Mapping) or not stop_when:
            raise ReadError(
                f"{p_type!r} pagination requires a stop_when predicate; "
                f"the endpoint declares none"
            )

        limit_param = (pagination.get("limit") or {}).get("param")
        if limit_param:
            base_params[limit_param] = effective_limit

        params = dict(base_params)
        # A malformed `offset.initial` / `increment_by` / `keyset.initial`
        # expression raises TransportSpecError from the builder. That type
        # derives from Exception alone, and the source worker's deterministic
        # tuple does not list it, so leaving it unwrapped reports a document
        # defect as retryable -- the run is retried until it exhausts its
        # attempts on an error that will never resolve itself.
        with _document_errors(f"{p_type!r} pagination: a setup expression"):
            advance = self._build_advance(
                p_type,
                pagination,
                _RequestState(params=params, resolver=resolver, url=full_url),
            )
        return stop_when, advance, params

    def _decide_page(
        self,
        page: _Page,
        resolver: Resolver,
        stop_when: Mapping[str, Any],
        metadata_spec: dict[str, Any],
        advance: _Advance,
        p_type: str,
    ) -> tuple[bool, _NextRequest | None]:
        """Ask the document what this page means: stop here, and if not, go where.

        `response.metadata`, the stop predicate and the next-page expression
        all resolve against the same page scope and are all authoring defects
        in the same document, so they classify identically. ``stage`` names
        which of them failed, so the message points at the actual culprit
        rather than always blaming ``stop_when``.
        """
        against = "is not evaluable against this response"
        with _document_errors(f"{p_type!r} pagination: response.metadata", against):
            page_resolver = resolver.with_context(
                replace(
                    resolver.context,
                    response=self._response_scope(page, metadata_spec, resolver),
                )
            )
        with _document_errors(
            f"{p_type!r} pagination: the stop_when predicate", against
        ):
            stop = evaluate_predicate(stop_when, page_resolver)
        if stop:
            return True, None
        with _document_errors(
            f"{p_type!r} pagination: the next-page expression", against
        ):
            return False, advance(page, page_resolver)

    @staticmethod
    def _require_progress(
        wire: _Wire, sent_wires: set[_Wire], p_type: str, pages: int, url: str
    ) -> None:
        """Refuse to send a request this read has already sent.

        Progress is judged on what actually goes out, not on the param table.
        A pagination param declared ``in: body`` whose ``from_param`` the body
        omits or misnames changes the params every page while the wire request
        stays byte-identical -- comparing param tables would call that progress
        and page forever, feeding the destination the same records without
        limit.
        """
        if wire not in sent_wires:
            return
        raise ReadError(
            f"{p_type!r} pagination: page {pages + 1} would send the "
            f"identical request again ({url!r}), so the read cannot "
            f"advance. Either the provider is cycling between next-page "
            f"values it has already given, or the value never reaches "
            f"the request -- check that a param declared `in: body` is "
            f"bound by a matching `from_param` in the body"
        )

    @staticmethod
    def _report_empty_page(
        p_type: str, page: _Page, records_ref: str, pages: int, records: int
    ) -> None:
        """Record why a read ended on a page with no records.

        A first page with nothing in it is not a quiet "nothing to sync": the
        endpoint declared where its records live and that address produced
        none, so a mis-declared ``records.ref`` and a genuinely empty source
        look identical from here. Name both so the difference is diagnosable
        from the log alone.
        """
        if pages:
            APIConnector._log_read_end(p_type, "empty page", pages, records)
            return
        logger.warning(
            "%r pagination: first page carried no records at %r "
            "(HTTP %d, body keys: %s); the stream reads empty",
            p_type,
            records_ref,
            page.status,
            (
                sorted(page.body)
                if isinstance(page.body, dict)
                else type(page.body).__name__
            ),
        )

    @staticmethod
    def _log_read_end(p_type: str, reason: str, pages: int, records: int) -> None:
        """Record why a paginated read stopped, and how much it read.

        Every exit from the loop reports itself. A read that ends early is
        otherwise indistinguishable from one that ends correctly — both just
        return — and "the source had more rows" is precisely the failure this
        change exists to remove.
        """
        logger.info(
            "%r pagination: ending read after %d page(s) / %d record(s) — %s",
            p_type,
            pages,
            records,
            reason,
        )

    @staticmethod
    def _build_advance(
        p_type: str, pagination: dict[str, Any], state: _RequestState
    ) -> _Advance:
        """Seed *params* for the first page and return the per-strategy step.

        The returned callable receives the page just read plus a resolver
        scoped to it, and answers where the following page comes from —
        ``None`` when the strategy itself has run out (an unresolvable next
        cursor / next link, a last record carrying no key). ``stop_when``
        remains the declared stop condition; this is only the mechanical
        "can I even build another request" question.
        """
        builder = _ADVANCE_BUILDERS.get(p_type)
        if builder is None:
            raise ReadError(f"Unsupported pagination type: {p_type!r}")
        return builder(pagination.get(p_type) or {}, state)

    @staticmethod
    def _effective_limit(
        limit_block: dict[str, Any], batch_size: int, resolver: Resolver
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
            resolved = _resolve_control(declared, resolver)
            if resolved is None:
                raise ReadError(
                    "pagination.limit.default is declared but did not resolve. "
                    "Falling back to the engine batch size would request a "
                    "page size the endpoint never declared"
                )
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
            # Always present, even with nothing declared, so the scope offers
            # the contract's reserved vocabulary uniformly.
            "metadata": {},
        }
        if metadata_spec:
            metadata_resolver = resolver.with_context(
                replace(resolver.context, response=scope)
            )
            # Per key, resolved strictly, because the two failure modes have
            # to stay apart. Lenient whole-dict resolution drops any entry
            # yielding None, so a provider field that is *present and null*
            # vanishes and `missing` answers True -- while the same `{ref}`
            # written straight into the predicate resolves to None and answers
            # False. Same data, opposite verdicts, depending only on whether
            # the author routed it through metadata. Strict resolution keeps a
            # real null as a present None and omits only a path that genuinely
            # does not resolve.
            resolved: dict[str, Any] = {}
            for name, expr in metadata_spec.items():
                try:
                    resolved[name] = metadata_resolver.resolve(expr)
                except UnresolvedValueError:
                    continue
            scope["metadata"] = resolved
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
        # Redirects are followed here rather than by aiohttp, which strips only
        # `Authorization` when a redirect crosses origins and forwards every
        # other default header. Connection auth is regularly a custom header
        # (`X-Api-Key`, `Private-Token`, a session `Cookie`), so a provider
        # answering 302 with a foreign Location would hand those credentials
        # over -- on any request, under any strategy. Checking a next_url
        # string is no defence against that; the redirect is the mechanism, so
        # the check belongs here.
        request_kwargs["allow_redirects"] = False
        async with self.session.request(method, url, **request_kwargs) as response:
            if response.status in _REDIRECT_HTTP_STATUSES:
                location = response.headers.get("Location")
                raise ReadError(
                    f"API request redirected: {method} {url} -> "
                    f"{response.status} {location!r}. Redirects are not "
                    f"followed: this request carries the connection's "
                    f"credentials, and a redirect can move it to another host. "
                    f"Point the endpoint at the final URL instead"
                )
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
            # Copied inside the context manager (the proxy is tied to the live
            # response) but kept case-insensitive: HTTP header names are
            # case-insensitive per RFC 9110, and pagination expressions address
            # them as `response.headers.<name>`. A plain dict would make
            # `response.headers.Link` miss a provider that sends `link:`,
            # ending a link-paginated read after one page with no error.
            headers = CIMultiDict(response.headers)
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


def _has_path(record: Any, path: list[str]) -> bool:
    """Report whether *path* is present in *record*, even when its value is null.

    ``walk_path`` returns ``None`` for both "key absent" and "key present and
    null", which for a keyset ordering field are opposite situations: the first
    is a mis-declared ``order_by_field`` that would truncate every read of the
    stream, the second is a provider that simply has no further key.
    """
    cursor = record
    for segment in path:
        if not isinstance(cursor, dict) or segment not in cursor:
            return False
        cursor = cursor[segment]
    return True


def _is_blank(value: Any) -> bool:
    """Report whether a resolved next-page token / link carries nothing usable.

    Absent, or a string that is empty or all whitespace — which is how a
    provider signals "no further pages". Deliberately not a plain falsiness
    test: a numeric cursor of ``0`` is a real token.
    """
    return value is None or (isinstance(value, str) and not value.strip())


def _resolve_control(expr: Any, resolver: Resolver) -> Any:
    """Resolve an expression that decides what to request, strictly.

    Two resolution policies, split by what the value governs. A request
    *param* resolves leniently: an unresolved node is dropped so a
    partly-known request still goes out. Anything that decides *what is
    requested or where the read starts* cannot use that policy -- the page
    size, the initial offset or page, the per-page step, the keyset seed, the
    next cursor, the next link. Lenient resolution renders
    `{"template": "p-${response.body.next}"}` as ``"p-"`` when the field is
    absent, which is not blank, so the engine would read from a position it
    invented out of configuration that was never there.

    Strict resolution answers the question actually being asked: did the
    declared next-page value resolve at all. A field that is present and null
    still comes back as ``None`` (and reads as blank); only a genuinely
    unreachable path raises, and that is returned as "no value". An unknown
    *scope* raises plain KeyError and is left to propagate -- that is a typo in
    the document, not a provider omission, and the caller classifies it.
    """
    try:
        return resolver.resolve(expr)
    except UnresolvedValueError:
        return None


def _require_scalar(value: Any, label: str) -> Any:
    """Reject a page-advance value that cannot be sent as a single param.

    A provider-supplied token or key reaches the query string, where yarl
    accepts only str/int/float. A dict or bool raises a bare ``TypeError``
    from inside the HTTP layer, and a *list* is silently expanded into
    repeated query values -- letting a provider inject extra values for a
    param the endpoint declared. Both are provider data crossing into a
    request, so both are named here rather than left to the transport.
    """
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ReadError(
            f"{label} resolved to {type(value).__name__} ({value!r}), which "
            f"cannot be sent as a single request parameter; expected a string "
            f"or number"
        )
    return value


def _canonical_wire(url: str, query: Mapping[str, Any], body: Any) -> _Wire:
    """Reduce a request to what the provider will actually receive.

    The same request target reaches the wire two ways: as a bare URL plus a
    declared param table, or as a URL that already carries those params in its
    query string. `link` pagination switches between them -- page 1 is
    ``(base, {"limit": 50})`` and a self-referential next link is
    ``(base?limit=50, {})``. Compared literally those look like progress, so a
    provider returning its own URL would get one duplicate page emitted before
    the repeat was noticed on the iteration after. Folding the query into the
    param table makes both spellings compare equal, so the repeat is caught
    before anything duplicate is emitted.

    Values are stringified because the query string only carries text: a
    declared ``{"limit": 50}`` and a link's ``limit=50`` are the same request.
    """
    parts = urlsplit(url)
    merged = {name: str(value) for name, value in parse_qsl(parts.query, True)}
    merged.update({name: str(value) for name, value in query.items()})
    target = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    # Digested rather than kept whole: a long read holds one entry per page,
    # and 16 bytes each keeps that bounded whatever the page count. `default`
    # covers Decimals from the lossless JSON parse.
    canonical = json.dumps([target, merged, body], sort_keys=True, default=str)
    return hashlib.blake2b(canonical.encode(), digest_size=16).digest()


_DEFAULT_PORTS = {"http": 80, "https": 443}


def _origin(url: str) -> str:
    """Scheme/host/port of *url*, normalized for same-origin comparison.

    Scheme and host are case-insensitive per RFC 3986, and an explicit
    default port names the same origin as an omitted one, so ``HTTPS://API.x``
    and ``https://api.x:443`` must not read as different hosts. A URL with no
    scheme or host has no origin and returns ``""``, which never matches a
    real one.
    """
    parts = urlsplit(url)
    if not parts.scheme or not parts.hostname:
        return ""
    if parts.username is not None or parts.password is not None:
        # Userinfo is not part of the origin, so a link like
        # `http://evil:pw@api.provider.com/x` would pass a same-origin check
        # and then be turned into an outgoing `Authorization: Basic ...` --
        # credentials the provider chose for a request the engine makes. No
        # origin at all rather than a match.
        return ""
    scheme = parts.scheme.lower()
    try:
        port = parts.port
    except ValueError:
        # Unparseable port: no origin rather than a spurious match.
        return ""
    return (
        f"{scheme}://{parts.hostname.lower()}:{port or _DEFAULT_PORTS.get(scheme, 0)}"
    )


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
    positive: bool = False,
) -> int:
    """Read an integer pagination setting that may be authored as an expression.

    The contract types these ``Any`` because ``{"ref": "..."}`` is as valid an
    initial offset as ``0``. An authored value that resolves to something
    non-integral is a defect worth naming rather than coercing.

    An *undeclared* setting takes *default*. A *declared* one that fails to
    resolve raises: these values anchor where a read starts and how far it
    steps, so quietly substituting a guess can skip the first page or stride
    past records — the difference is invisible in the output.
    """
    value = _optional_int_setting(block, strategy, key, resolver, positive=positive)
    return default if value is None else value


def _optional_int_setting(
    block: dict[str, Any],
    strategy: str,
    key: str,
    resolver: Resolver,
    *,
    positive: bool = False,
) -> int | None:
    """Resolve an integer setting, or ``None`` when the endpoint omits it.

    Lets a caller whose fallback is not a number — the offset step, which falls
    back to the record count of the page just read — distinguish "undeclared"
    without inventing a placeholder default it would never use.
    """
    declared = block.get(key)
    if declared is None:
        return None
    label = f"pagination.{strategy}.{key}"
    resolved = _resolve_control(declared, resolver)
    if resolved is None:
        raise ReadError(
            f"{label} is declared but did not resolve. It anchors where the "
            f"read starts or how far each page steps, so it cannot fall back "
            f"to a default without silently changing which records are read"
        )
    return _as_positive_int(resolved, label) if positive else _as_int(resolved, label)


def _as_int(value: Any, label: str) -> int:
    """Coerce a resolved pagination setting to ``int``, or fail naming it.

    A fractional value is rejected rather than truncated. These settings say
    where a read starts and how far each page steps, so turning ``10.9`` into
    ``10`` would quietly shift every subsequent page boundary — skipping or
    repeating records with nothing in the log to show for it.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal, str)):
        raise ReadError(
            f"{label} must be an integer, got {type(value).__name__}: {value!r}"
        )
    try:
        number = Decimal(value)
    except (ArithmeticError, TypeError, ValueError) as err:
        raise ReadError(f"{label} must be an integer, got {value!r}") from err
    # is_finite() first: NaN and Infinity both survive the integral check
    # below (Infinity is its own integral value) and only blow up at int().
    if not number.is_finite() or number != number.to_integral_value():
        raise ReadError(f"{label} must be a whole number, got {value!r}")
    return int(number)


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


# ----------------------------------------------------------------------
# Pagination strategies
# ----------------------------------------------------------------------
#
# One builder per contract strategy. Each seeds the first page's params and
# returns the step that answers "where does the following page come from".
# They share a signature so the dispatch table below stays a plain lookup;
# a builder ignores the arguments its strategy has no use for.


def _build_offset_advance(block: dict[str, Any], state: _RequestState) -> _Advance:
    """Offset/start-index pagination."""
    params, resolver, full_url = state.params, state.resolver, state.url
    param = _required_setting(block, "offset", "param")
    position = _int_setting(block, "offset", "initial", resolver, default=0)
    # An authored step wins (a provider that counts offsets in pages declares
    # 1). Otherwise the step is the number of records the page actually
    # returned — NOT the requested page size. A declared limit is a request,
    # not an agreement: with no limit param the provider picks the page size,
    # and with one it may clamp below it. Stepping by the requested size then
    # jumps over every record in between, losing records from the middle of a
    # stream with no error — worse than the truncation it replaces, because it
    # survives re-runs and passes a row-count glance.
    step = _optional_int_setting(
        block, "offset", "increment_by", resolver, positive=True
    )
    params[param] = position

    def advance(page: _Page, _page_resolver: Resolver) -> _NextRequest | None:
        nonlocal position
        position += step if step is not None else len(page.records)
        params[param] = position
        return _NextRequest(full_url, dict(params), True)

    return advance


def _build_page_advance(block: dict[str, Any], state: _RequestState) -> _Advance:
    """Page-number pagination."""
    params, resolver, full_url = state.params, state.resolver, state.url
    param = _required_setting(block, "page", "param")
    number = _int_setting(block, "page", "initial", resolver, default=1)
    # A zero step would re-request the same page forever.
    step = _int_setting(
        block, "page", "increment_by", resolver, default=1, positive=True
    )
    params[param] = number

    def advance(_page: _Page, _page_resolver: Resolver) -> _NextRequest | None:
        nonlocal number
        number += step
        params[param] = number
        return _NextRequest(full_url, dict(params), True)

    return advance


def _build_cursor_advance(block: dict[str, Any], state: _RequestState) -> _Advance:
    """Opaque-cursor pagination."""
    params, full_url = state.params, state.url
    param = _required_setting(block, "cursor", "param")
    next_cursor = block.get("next_cursor")
    if next_cursor is None:
        raise ReadError(
            "cursor pagination requires cursor.next_cursor (a value "
            "expression resolving to the next page's token)"
        )
    # No first-page cursor: the token only exists once a response has produced
    # one. The param stays absent rather than being sent empty.
    params.pop(param, None)

    def advance(_page: _Page, page_resolver: Resolver) -> _NextRequest | None:
        token = _resolve_control(next_cursor, page_resolver)
        if _is_blank(token):
            # `advance` only runs when stop_when evaluated false -- the document
            # said this is not the last page. A missing token then contradicts
            # it, and ending quietly would drop every remaining page while the
            # run reported success: the #346 failure shape. A cursor provider
            # that omits the token on its last page is authored by making
            # stop_when say so (`{"missing": {"ref": ...}}`), which fires before
            # this point.
            raise ReadError(
                "cursor pagination: stop_when did not fire, but next_cursor "
                "resolved to no token, so the next page cannot be requested. "
                "Declare the last page in stop_when (for example "
                '`{"missing": ...}` on the same field) if the provider omits '
                "the token when it is done"
            )
        params[param] = _require_scalar(token, "cursor pagination: next_cursor")
        return _NextRequest(full_url, dict(params), True)

    return advance


def _build_link_advance(block: dict[str, Any], state: _RequestState) -> _Advance:
    """Next-URL pagination."""
    params, origin = state.params, _origin(state.url)
    next_url = block.get("next_url")
    if next_url is None:
        raise ReadError(
            "link pagination requires link.next_url (a value expression "
            "resolving to the next page's absolute URL)"
        )

    def advance(_page: _Page, page_resolver: Resolver) -> _NextRequest | None:
        target = _resolve_control(next_url, page_resolver)
        if _is_blank(target):
            # Same reasoning as cursor: stop_when said this is not the last
            # page, so an absent link is a contradiction, not a quiet ending.
            raise ReadError(
                "link pagination: stop_when did not fire, but next_url "
                "resolved to no link, so the next page cannot be requested. "
                "Declare the last page in stop_when if the provider omits the "
                "link when it is done"
            )
        if not isinstance(target, str):
            raise ReadError(
                f"link pagination: next_url resolved to "
                f"{type(target).__name__}, expected a URL string"
            )
        # This is the one place a *response* decides where the next request
        # goes, and the session carries the connection's auth headers on every
        # request it makes. An absolute link to another host would therefore
        # hand the connector's credentials to whoever the provider named, so
        # the next page must come from the endpoint's own origin.
        target_origin = _origin(target)
        if not target_origin:
            raise ReadError(
                f"link pagination: next_url resolved to {target!r}, which is "
                f"not an absolute URL. The next page must be addressed "
                f"scheme-and-host so it can be checked against the endpoint "
                f"origin {origin!r}"
            )
        if target_origin != origin:
            raise ReadError(
                f"link pagination: next_url points at {target_origin!r}, "
                f"outside the endpoint origin {origin!r}. Refusing to follow "
                f"it — the request carries this connection's credentials"
            )
        # The provider's link already carries the full query, so the declared
        # params must not be appended again — doing so duplicates keys
        # (`?limit=100&limit=100`). The body, which the link cannot express, is
        # still built and sent.
        return _NextRequest(target, dict(params), False)

    return advance


def _build_keyset_advance(block: dict[str, Any], state: _RequestState) -> _Advance:
    """Keyset (advance-from-last-key) pagination."""
    params, resolver, full_url = state.params, state.resolver, state.url
    param = _required_setting(block, "keyset", "param")
    order_by_field = block.get("order_by_field")
    if not order_by_field or not isinstance(order_by_field, str):
        raise ReadError(
            "keyset pagination requires keyset.order_by_field (the dotted "
            "record field path the pages are ordered by)"
        )
    field_path = order_by_field.split(".")
    initial = block.get("initial")
    if initial is not None:
        seed = _resolve_control(initial, resolver)
        if seed is not None:
            seed = _require_scalar(seed, "keyset pagination: keyset.initial")
        if seed is None:
            raise ReadError(
                "keyset pagination: keyset.initial is declared but did not "
                "resolve. Sending it as an empty value would start the read "
                "from an arbitrary point; omit keyset.initial to start from "
                "the beginning"
            )
        params[param] = seed
    else:
        params.pop(param, None)

    def advance(page: _Page, _page_resolver: Resolver) -> _NextRequest | None:
        # The raw record value (int/str/Decimal) feeds both the query string
        # and any body binding through build_request, so it stays native here;
        # build_request stringifies Decimals only for the query (where yarl
        # would truncate them) and keeps the body numeric.
        last_record = page.records[-1]
        if not _has_path(last_record, field_path):
            # The ordering field is absent from the record entirely. Ending
            # here would silently truncate every read of this stream to one
            # page — the #346 failure shape — so a mis-declared order_by_field
            # fails loud instead.
            raise ReadError(
                f"keyset pagination: record carries no {order_by_field!r} "
                f"field, so the next page cannot be requested. Declared keys "
                f"on the record: {sorted(last_record)}"
            )
        last_key = walk_path(last_record, field_path)
        if last_key is None:
            # Present but null: the provider has no further key to advance
            # from. Visible, because a null ordering key in a keyset stream is
            # anomalous rather than routine.
            logger.warning(
                "keyset pagination: last record has a null %r; ending read "
                "after this page",
                order_by_field,
            )
            return None
        params[param] = _require_scalar(
            last_key, f"keyset pagination: record field {order_by_field!r}"
        )
        return _NextRequest(full_url, dict(params), True)

    return advance


# Strategy name -> builder. The key is also the name of the strategy's own
# block in the pagination document, which is what `_build_advance` passes in.
_ADVANCE_BUILDERS: dict[str, _AdvanceBuilder] = {
    "offset": _build_offset_advance,
    "page": _build_page_advance,
    "cursor": _build_cursor_advance,
    "link": _build_link_advance,
    "keyset": _build_keyset_advance,
}
