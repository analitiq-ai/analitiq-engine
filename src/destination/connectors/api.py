"""API destination handler for writing records to REST APIs.

This handler sends records to REST API endpoints with support for
rate limiting, retries, and contract-declared request batching.
"""

import asyncio
import base64
import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import aiohttp
import orjson
import pyarrow as pa
from aiohttp_retry import ExponentialRetry, RetryClient

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.connection_runtime import ConnectionRuntime
from cdk.json_utils import decode_json_fields
from cdk.rate_limiter import RateLimiter
from cdk.request_binding import (
    bind_param_refs,
    bind_record_inputs,
    collect_from_input_selectors,
    resolve_param_defaults,
)
from cdk.resolver import Resolver
from cdk.types import AckStatus, Cursor, RetrySemantics, RetryVerdict, SchemaSpec

from ...shared.http_utils import join_url

logger = logging.getLogger(__name__)


# Proto WriteMode int -> contract operations.write.<mode> key. The
# api-endpoint contract's ``operations.write`` is a closed map keyed by
# mode name (v1 keys: ``insert``, ``upsert``); the destination handler
# must dispatch to the block matching the stream's write_mode.
_API_WRITE_MODE_KEYS: dict[int, str] = {
    1: "insert",
    2: "upsert",
}


def _endpoint_write_mode_block(
    endpoint_doc: Mapping[str, Any], mode_key: str
) -> dict[str, Any] | None:
    """Return the ``operations.write.<mode_key>`` block, or ``None``.

    The block is returned when it is a usable request definition.
    A block is usable when it is a dict carrying a truthy ``request.path``.
    This is the single acceptance predicate for an API write mode:
    ``supports_upsert`` (capability advertisement) and ``configure_schema``
    (per-stream dispatch) both apply it, so the two cannot disagree on
    whether a given write-mode block is usable.

    Tolerant of malformed contract documents: any level that is not the
    expected mapping yields ``None`` rather than raising, so capability
    advertisement over arbitrary endpoint docs never crashes.
    """
    operations = endpoint_doc.get("operations")
    write = operations.get("write") if isinstance(operations, Mapping) else None
    mode_block = write.get(mode_key) if isinstance(write, Mapping) else None
    if not isinstance(mode_block, Mapping):
        return None
    request = mode_block.get("request")
    if not isinstance(request, Mapping) or not request.get("path"):
        return None
    return dict(mode_block)


def _batching_max_records(batching: Any) -> int | None:
    """``max_records`` from a contract-valid ``batching`` block, else ``None``.

    The api-endpoint contract's Batching shape is
    ``{"max_records": <int >= 2>}`` — required and closed, so unknown
    sibling keys (a typo, or the pre-contract ``mode``/``size`` blended
    in) make the block non-contract and must not be waved through. This
    is the single acceptance predicate for a batching block:
    ``supports_bulk_load`` (capability advertisement) and
    ``configure_schema`` (per-stream acceptance) both apply it, so the
    two cannot disagree on whether a given block enables multi-record
    requests.
    """
    if not isinstance(batching, Mapping) or set(batching) != {"max_records"}:
        return None
    value = batching.get("max_records")
    if isinstance(value, bool) or not isinstance(value, int) or value < 2:
        return None
    return value


def _idempotency_config_problem(
    idempotency: Any,
    batching: Any,
    state: "_StreamState",
    *,
    reserved_headers: frozenset[str] | set[str],
    declared_input_fields: set[str],
) -> str | None:
    """Why this ``idempotency`` block cannot work for the stream, or ``None``.

    Mirrors the api-endpoint schema's own constraints (infra#890) so a
    document that slipped past contract validation still fails loud at
    configure time instead of writing without the key it promised. The
    contract has no batching mode: a present ``batching`` block IS the
    multi-record case, so the schema's exclusion — and this mirror — key
    on its presence. ``reserved_headers`` are the lowercased engine- and
    connection-owned request headers; ``declared_input_fields`` are the
    write input schema's field names, which shape the pass-through body
    when no body template is declared.
    """
    if not isinstance(idempotency, Mapping):
        return f"idempotency must be an object, got {type(idempotency).__name__}"
    target = idempotency.get("in")
    name = idempotency.get("name")
    if target not in ("header", "body"):
        return f"idempotency.in must be 'header' or 'body', got {target!r}"
    if not isinstance(name, str) or not name:
        return f"idempotency.name must be a non-empty string, got {name!r}"
    if target == "header" and name.lower() in reserved_headers:
        # Same rule as the body reserved-field check: these headers are
        # engine-owned (Content-Type) or carry the connection's own
        # values (auth and friends); layering the key over one would
        # silently break every request — or worse, send the record id as
        # the credential — instead of rejecting the document.
        return (
            f"idempotency.name {name!r} collides with an engine- or "
            f"connection-owned request header; pick a header the "
            f"connection does not already send"
        )
    if batching:
        return (
            "idempotency cannot be combined with a batching block: a "
            "restart re-batches records, so a per-request key over several "
            "records cannot dedup (issue #286); the api-endpoint schema "
            "forbids the combination (infra#890)"
        )
    if target == "body":
        if state.body_spec is not None and not isinstance(state.body_spec, Mapping):
            return (
                f"idempotency.in='body' needs a JSON-object request body; "
                f"the declared request.body is a "
                f"{type(state.body_spec).__name__}"
            )
        if isinstance(state.body_spec, Mapping) and name in state.body_spec:
            return (
                f"request.body already declares the field {name!r} that "
                f"idempotency.name reserves for the engine-owned key"
            )
        if state.body_spec is None and name in declared_input_fields:
            # No body template: the record itself is the body, shaped by
            # the write input schema — a declared field with the reserved
            # name would collide on every record at write time, after the
            # ack already promised exactly-once.
            return (
                f"the write input schema already declares the field "
                f"{name!r} that idempotency.name reserves for the "
                f"engine-owned key on the pass-through body"
            )
    return None


def _retry_verdict(mode_key: str, state: "_StreamState") -> RetryVerdict:
    """Retry-safety verdict for one configured API stream (issue #286).

    Upsert dedups on the endpoint's conflict keys regardless of a declared
    idempotency key; insert is exactly-once only when the endpoint declares
    where the engine-owned key lands, otherwise a same-run restart re-sends
    already-delivered records.
    """
    if mode_key == "upsert":
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE,
            reason=(
                "upsert dedups on the endpoint's conflict keys; a re-sent "
                "record updates in place"
            ),
        )
    if state.idempotency_in is not None:
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE,
            reason=(
                f"each request carries the record's identity hash as an "
                f"idempotency key ({state.idempotency_in} "
                f"{state.idempotency_name!r}); dedup holds within the "
                f"provider's replay window, with SQL insert-mode identity "
                f"semantics (first occurrence of a key wins)"
            ),
        )
    return RetryVerdict(
        semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
        reason=(
            "insert mode with no declared idempotency key; a same-run "
            "restart re-sends already-delivered records"
        ),
    )


def _body_with_idempotency_key(
    state: "_StreamState", body: Any, record_id: str
) -> dict[str, Any]:
    """Return the request body with the engine-owned idempotency key added.

    Configure time already rejected a declared non-object body spec; this
    guards the remaining runtime shapes (a spec-less record body, or a spec
    that resolved away its object shape). A body that already carries the
    reserved field is a collision the engine must not silently overwrite.
    """
    if not isinstance(body, dict):
        raise ValueError(
            f"idempotency.in='body' for endpoint {state.endpoint!r} needs a "
            f"JSON-object request body, got {type(body).__name__}"
        )
    if state.idempotency_name in body:
        raise ValueError(
            f"request body already carries the field "
            f"{state.idempotency_name!r}, which idempotency.name reserves "
            f"for the engine-owned key; rename the record field or the key"
        )
    return {**body, state.idempotency_name: record_id}


def _orjson_default(obj: Any) -> Any:
    """Serialise types that orjson does not natively handle (default-hook).

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


def _collect_json_fields(mode_block: Mapping[str, Any]) -> set[str]:
    """Body field names declared with ``arrow_type: "Json"``.

    Handles both shapes the api-endpoint contract permits:
    JSON-Schema-style ``input.schema.properties`` (most common) and the
    flat ``input.schema.columns`` array.
    """
    schema = (mode_block.get("input") or {}).get("schema") or {}
    names: set[str] = set()
    for name, prop in (schema.get("properties") or {}).items():
        if isinstance(prop, dict) and prop.get("arrow_type") == "Json":
            names.add(name)
    for col in schema.get("columns") or []:
        if isinstance(col, dict) and col.get("arrow_type") == "Json":
            col_name = col.get("name")
            if col_name:
                names.add(col_name)
    return names


def _collect_input_field_names(mode_block: Mapping[str, Any]) -> set[str]:
    """Every field name the write input schema declares (both shapes)."""
    schema = (mode_block.get("input") or {}).get("schema") or {}
    names: set[str] = {
        name for name in (schema.get("properties") or {}) if isinstance(name, str)
    }
    for col in schema.get("columns") or []:
        if isinstance(col, dict) and col.get("name"):
            names.add(col["name"])
    return names


def _classify_http_error(
    exc: aiohttp.ClientError | asyncio.TimeoutError,
) -> "AckStatus":
    """FATAL for deterministic 4xx rejections (not 408/429); RETRYABLE otherwise.

    A ClientResponseError with a 4xx status other than 408 and 429 is a
    deterministic rejection — the API refused the payload, and retrying burns
    retries identically. 408 (Request Timeout) and 429 (Too Many Requests) are
    transient 4xx that may recover, so they stay RETRYABLE, matching the source
    connector's ``_TRANSIENT_HTTP_STATUSES``. 5xx, connection errors, and
    timeouts stay RETRYABLE too (subject to the nothing-landed invariant on
    multi-record paths).
    """
    if (
        isinstance(exc, aiohttp.ClientResponseError)
        and 400 <= exc.status < 500
        and exc.status not in (408, 429)
    ):
        return AckStatus.ACK_STATUS_FATAL_FAILURE
    return AckStatus.ACK_STATUS_RETRYABLE_FAILURE


def _content_idempotency_key(record: Mapping[str, Any]) -> str:
    """Full-content hash used as the idempotency key in upsert mode.

    Upsert exists to reconcile changed rows, so its key must change when
    the content changes: a stable identity key would make the provider's
    replay cache swallow a legitimate update to the same entity within
    its dedup window. The canonicalisation mirrors the SQL destination's
    ``_record_hash`` (sorted-key JSON, ``default=str``), so an identical
    replay dedups and a changed row gets a new key — SQL upsert
    semantics, provider-side.
    """
    canonical = json.dumps(dict(record), sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


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
    # ``operations.write.<mode>.batching.max_records`` — the provider's
    # maximum records per request. ``None`` means the endpoint declares
    # no batching block, so every record is sent as its own request;
    # otherwise records are sent in chunks of at most this many.
    max_records: int | None = None
    # Names of body fields declared with ``arrow_type: "Json"`` in the
    # endpoint's write input schema. The wire carries them as
    # JSON-encoded strings (so they fit a ``pa.large_string`` column);
    # the handler must ``json.loads`` them before aiohttp serializes the
    # body, otherwise the API receives a quoted string instead of a
    # nested object.
    json_fields: set[str] = field(default_factory=set)
    # ``operations.write.<mode>.request.body`` from the endpoint document,
    # or ``None`` when the endpoint declares no body template. When set,
    # the request body is built by binding ``from_param`` nodes to the
    # declared write params and ``from_input`` nodes to the in-flight
    # record(s), then resolving value expressions; when absent the
    # record(s) are the body, unchanged.
    body_spec: Any | None = None
    # ``operations.write.<mode>.params`` — declared write params whose
    # resolved defaults feed body ``{"from_param": ...}`` bindings.
    params_spec: dict[str, Any] = field(default_factory=dict)
    # ``operations.write.<mode>.idempotency`` (infra#890): where the
    # engine-owned per-record idempotency key lands ("header" or "body")
    # and the header/field name it lands under. ``None`` means the
    # endpoint declares no key. The key VALUE is always the record's
    # identity-derived ``record_id`` (primary-key fields when the source
    # declares them, else the full content) — the author declares
    # placement only, and dedup follows SQL insert-mode identity
    # semantics: the first occurrence of an identity wins.
    idempotency_in: str | None = None
    idempotency_name: str = ""
    # The stream's write mode key ("insert" / "upsert"). Insert keys on
    # the engine's identity-derived record_id (SQL insert parity: first
    # occurrence of an identity wins); upsert keys on the full record
    # content so a changed row gets a new key and the provider applies
    # the update instead of replaying the cached response.
    write_mode_key: str = "insert"
    # Retry-safety verdict computed at configure time (issue #286).
    retry_verdict: RetryVerdict | None = None


class ApiDestinationHandler(BaseDestinationHandler):
    """
    Destination handler that writes records to REST APIs.

    Supports:
    - Contract-declared batching (``operations.write.<mode>.batching``):
      absent means one request per record; present means chunked requests
      of at most ``max_records`` records each
    - Rate limiting with configurable limits
    - Retry with exponential backoff
    - Multiple authentication methods
    - Per-record idempotency keys (``operations.write.<mode>.idempotency``,
      infra#890): single-record streams only; the engine-owned
      identity-derived record id is sent as a header or a body field so a
      same-run restart cannot double-write (issue #286)

    Configuration (connection config):
    - host: Base URL for the API
    - headers: HTTP headers (including auth headers)
    - timeout: Request timeout in seconds (default: 30)
    - rate_limit: Optional rate limiting config
      - max_requests: Max requests per time window
      - time_window: Time window in seconds

    Per-stream endpoint settings (path, method, batching, optional body
    template) are read from the preloaded contract API endpoint document
    at ``configure_schema`` time, keyed by ``operations.write.<mode>``.
    The SchemaSpec off the wire only carries ``stream_id``, ``version``,
    and ``write_mode``.
    """

    def __init__(self) -> None:
        """Initialize the API handler."""
        self._runtime: ConnectionRuntime | None = None
        self._session: RetryClient | None = None
        self._config: dict[str, Any] = {}
        self._connected: bool = False

        # Connection settings
        self._base_url: str = ""
        self._timeout: int = 30

        # Rate limiter
        self._rate_limiter: RateLimiter | None = None

        # Per-request value-expression resolver, built from the connection
        # runtime at connect() time. Used only when a stream declares a
        # request body spec.
        self._request_resolver: Resolver | None = None

        # Per-stream endpoint settings derived from the contract document
        # at configure_schema() time. Keyed by stream_id so concurrent
        # streams sharing this handler do not race shared state.
        self._streams: dict[str, _StreamState] = {}

        # stream_id -> contract API endpoint document. Populated by
        # set_stream_endpoints() at startup so configure_schema() can read
        # operations.write.<mode>.* directly from the contract.
        self._stream_endpoints: dict[str, dict[str, Any]] = {}

        # HTTP statuses that trigger retry with exponential backoff; the
        # attempt count comes from ``runtime.raw_config["max_retries"]`` at
        # connect() time. 4xx other than 429 are single-attempt and
        # classified FATAL by _classify_http_error; 5xx outside this set
        # (e.g. 502) are single-attempt but remain RETRYABLE.
        self._retry_statuses: set = {429, 500, 503, 504}

        # Lowercased default-header names the connection's session sends on
        # every request (auth and friends), captured at connect() time. An
        # idempotency header colliding with one of these is rejected at
        # configure time — layering the key over it would shadow the
        # connection's own value on every write.
        self._session_header_names: set[str] = set()

        # The last configure_schema rejection reason. The servicer reads
        # this into the SchemaAck message (issue #231) so the engine ack
        # carries the real reason, not the generic fallback.
        self.last_schema_rejection: str | None = None

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Register stream_id to its contract API endpoint document.

        The handler reads ``operations.write.<mode>.request.{path,method}``
        and optional ``operations.write.<mode>.batching`` from the document
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
        """Report that this connector does not support transactions."""
        return False

    @property
    def supports_upsert(self) -> bool:
        """Report whether any registered endpoint declares an upsert write.

        The capability is declared via ``operations.write.upsert`` in the
        endpoint's contract.
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
        """Report whether any registered endpoint declares write batching.

        Contract-driven like ``supports_upsert``: multi-record requests
        exist only where an endpoint's ``operations.write.<mode>.batching``
        block declares the provider's per-request cap; without one every
        write is one request per record, so advertising bulk load would
        promise a capability no configured stream can use. The block must
        pass the same acceptance predicate ``configure_schema`` applies —
        a block configure would reject enables nothing.
        """
        return any(
            _batching_max_records(block.get("batching")) is not None
            for doc in self._stream_endpoints.values()
            for mode_key in _API_WRITE_MODE_KEYS.values()
            if (block := _endpoint_write_mode_block(doc, mode_key)) is not None
        )

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Per-stream verdict computed at configure time (issue #286)."""
        state = self._streams.get(stream_id)
        if state is None or state.retry_verdict is None:
            return super().retry_semantics(stream_id)
        return state.retry_verdict

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
        # Resolves value expressions in declared body specs at write time
        # (connection parameters/selections/discovered; no secrets — those
        # are consumed engine-side at transport materialization).
        self._request_resolver = runtime.request_resolver()

        # Wrap plain session with retry (destination-specific)
        retry_options = ExponentialRetry(
            attempts=self._max_retries,
            statuses=self._retry_statuses,
        )
        self._session = RetryClient(
            client_session=runtime.session,
            retry_options=retry_options,
        )
        self._session_header_names = {k.lower() for k in runtime.session.headers}

        self._connected = True
        logger.info(f"ApiDestinationHandler connected to {self._base_url}")

    async def disconnect(self) -> None:
        """Close API connection."""
        if self._session:
            await self._session.close()
        if self._runtime:
            # runtime.close() is idempotent — safe even if RetryClient
            # already closed the session
            await self._runtime.close()
        self._connected = False
        logger.info("ApiDestinationHandler disconnected")

    def _reject_schema(self, stream_id: str, reason: str) -> bool:
        """Log one configure-time rejection and record it for the ack.

        The servicer surfaces ``last_schema_rejection`` in the SchemaAck
        (issue #231 channel), so the engine-side operator sees the real
        reason instead of the generic "Schema configuration failed" that
        otherwise leaves only the sidecar log to dig through.
        """
        logger.error("Schema rejected for stream %r: %s", stream_id, reason)
        self.last_schema_rejection = reason
        return False

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        """Configure the API endpoint from the preloaded contract document.

        ``operations.write`` is a closed map keyed by write mode in the
        api-endpoint contract; dispatch into the block matching the
        stream's ``write.mode`` and read its ``request.{path,method}``
        plus optional ``batching`` block.
        """
        stream_id = schema_spec.stream_id
        # The servicer reads last_schema_rejection right after this call.
        # The handler is shared across concurrent streams, so the reset ->
        # read window is race-free only while this method stays await-free.
        self.last_schema_rejection = None
        endpoint_doc = self._stream_endpoints.get(stream_id)
        if endpoint_doc is None:
            return self._reject_schema(
                stream_id,
                f"no preloaded endpoint document for stream_id={stream_id!r}; "
                f"call set_stream_endpoints() before the gRPC server starts",
            )

        mode_key = _API_WRITE_MODE_KEYS.get(schema_spec.write_mode)
        if mode_key is None:
            return self._reject_schema(
                stream_id,
                f"API destination does not support "
                f"write_mode={schema_spec.write_mode}; valid api-endpoint "
                f"modes are {sorted(_API_WRITE_MODE_KEYS.values())}",
            )

        mode_block = _endpoint_write_mode_block(endpoint_doc, mode_key)
        if mode_block is None:
            operations = endpoint_doc.get("operations")
            write = operations.get("write") if isinstance(operations, Mapping) else None
            available = sorted(write.keys()) if isinstance(write, Mapping) else None
            return self._reject_schema(
                stream_id,
                f"endpoint document does not define a usable "
                f"operations.write.{mode_key} block (needs a dict with a "
                f"truthy request.path); write modes present: {available}",
            )

        request = mode_block.get("request") or {}
        state = _StreamState(
            endpoint=request["path"],
            method=(request.get("method") or "POST").upper(),
            json_fields=_collect_json_fields(mode_block),
            body_spec=request.get("body"),
            params_spec=dict(mode_block.get("params") or {}),
        )

        # Contract shape (infra#890): a present ``batching`` block IS the
        # multi-record case and must carry ``max_records`` (int >= 2);
        # absence/null means one request per record. Any other shape is a
        # non-contract document — fail the stream loud instead of silently
        # running single-record (issue #305).
        batching = mode_block.get("batching")
        if batching is not None:
            max_records = _batching_max_records(batching)
            if max_records is None:
                return self._reject_schema(
                    stream_id,
                    f'batching must be {{"max_records": <int >= 2>}} per '
                    f"the api-endpoint contract, got {batching!r}",
                )
            state.max_records = max_records

        # A body spec whose from_input selectors contradict the batching
        # declaration can never build a valid request — reject the stream
        # now rather than failing every record at write time.
        if state.body_spec is not None:
            selectors = collect_from_input_selectors(state.body_spec)
            is_single = state.max_records is None
            wants_batch = "records" in selectors
            wants_single = any(
                s == "record" or s.startswith("record.") for s in selectors
            )
            if (wants_batch and is_single) or (wants_single and not is_single):
                return self._reject_schema(
                    stream_id,
                    f"request.body from_input selectors {sorted(selectors)} "
                    f"do not match the batching declaration ('records' "
                    f"needs a batching block; 'record' needs none)",
                )

        state.write_mode_key = mode_key

        idempotency = mode_block.get("idempotency")
        if idempotency is not None:
            problem = _idempotency_config_problem(
                idempotency,
                mode_block.get("batching"),
                state,
                reserved_headers={"content-type"} | self._session_header_names,
                declared_input_fields=_collect_input_field_names(mode_block),
            )
            if problem is not None:
                return self._reject_schema(stream_id, problem)
            state.idempotency_in = idempotency["in"]
            state.idempotency_name = idempotency["name"]

        state.retry_verdict = _retry_verdict(mode_key, state)

        self._streams[stream_id] = state
        logger.info(
            "API schema configured for stream %r: %s %s, %s",
            stream_id,
            state.method,
            state.endpoint,
            "single-record"
            if state.max_records is None
            else f"batched (max_records={state.max_records})",
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
            decode_json_fields(records, state.json_fields)
            if state.max_records is None:
                written, failed_ids, failure_detail = await self._write_single_mode(
                    state, records, record_ids
                )
            else:
                written, failed_ids, failure_detail = await self._write_chunked_mode(
                    state, records, record_ids
                )

            logger.info(
                f"API wrote batch {batch_seq}: {written}/{len(records)} records"
            )
            return self._build_write_result(
                written=written,
                failed_record_ids=failed_ids,
                total=len(records),
                cursor=cursor,
                failure_detail=failure_detail,
            )

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            status = _classify_http_error(e)
            logger.error("Transport error writing to API: %s", e, exc_info=True)
            return BatchWriteResult(
                status=status,
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

    def _resolve_write_params(self, state: _StreamState) -> dict[str, Any]:
        """Resolve declared write-param defaults into a value table.

        The table feeds body ``{"from_param": ...}`` bindings.
        Write params are request-construction inputs, not stream filters;
        defaults resolve through the request-time grammar and an
        unresolved default simply leaves the param out of the table (its
        ``from_param`` node then binds ``None`` and is dropped).
        """
        return resolve_param_defaults(
            state.params_spec, self._request_resolver, context="write param"
        )

    def _build_body(
        self,
        state: _StreamState,
        *,
        record: dict[str, Any] | None = None,
        records: list[dict[str, Any]] | None = None,
    ) -> Any:
        """Build one request body for the in-flight record(s).

        No declared body spec: the record(s) are the body, unchanged.
        With a spec: bind ``from_param`` nodes to the declared write
        params and ``from_input`` nodes to the record data, then resolve
        value expressions (``literal``/``ref``/``template``/``function``)
        per the request-time contract — an unresolved expression omits
        its field rather than going onto the wire raw. A spec whose
        entire body resolves away is an authoring error: posting ``null``
        per record would silently write nothing.
        """
        if state.body_spec is None:
            return record if record is not None else records
        if self._request_resolver is None:
            raise RuntimeError("Handler not connected: no request resolver")
        bound = bind_param_refs(state.body_spec, self._resolve_write_params(state))
        bound = bind_record_inputs(bound, record=record, records=records)
        body = self._request_resolver.resolve_for_request(bound)
        if body is None:
            raise ValueError(
                f"request body spec for endpoint {state.endpoint!r} resolved "
                f"to nothing; check the endpoint's request.body expressions"
            )
        return body

    def _build_write_result(
        self,
        *,
        written: int,
        failed_record_ids: list[str],
        total: int,
        cursor: Cursor | None,
        failure_detail: str = "",
    ) -> BatchWriteResult:
        """One partial-failure verdict shared by every write mode.

        Full success advances the cursor. Any shortfall is FATAL (not
        RETRYABLE) and carries the failed record ids: the records that did
        land are already written, so retrying the whole batch would duplicate
        them. A failure result carries no cursor, so the checkpoint never
        advances past records that were never written. ``failure_detail``
        carries the first per-record failure reason into the summary — the
        bare count crosses the wire but the reason otherwise stays in this
        container's logs, leaving the engine-side operator nothing to act on.
        """
        if written == total and not failed_record_ids:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=written,
                committed_cursor=cursor,
            )
        failed_count = len(failed_record_ids) or (total - written)
        summary = f"{failed_count}/{total} records failed to write to API"
        if failure_detail:
            summary = f"{summary}; first failure: {failure_detail}"
        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=written,
            failed_record_ids=tuple(failed_record_ids),
            failure_summary=summary,
        )

    async def _write_single_mode(
        self,
        state: _StreamState,
        records: list[dict[str, Any]],
        record_ids: list[str],
    ) -> tuple[int, list[str], str]:
        """Write records one at a time.

        Returns ``(written, failed_record_ids, first_failure)`` — the first
        per-record failure reason rides the engine-facing failure summary.
        Body construction is data-dependent (a record field can feed a
        derived function) and is caught per record so a bad record fails
        just itself. Authoring and programming errors (TransportSpecError,
        RuntimeError, KeyError) propagate to write_batch and become FATAL
        for the whole batch.

        Transport error handling applies ``_classify_http_error`` per record:
        - RETRYABLE (429, 408, connection errors, timeouts): re-raises
          immediately. ``write_batch``'s outer catch has no access to the
          local ``written`` counter, so it always reports ``records_written=0``
          and no committed cursor; the engine retries the full batch. Records
          that already landed before the re-raise will be re-sent (possible
          duplication). Streams with a declared idempotency key are fully
          protected; insert-mode streams without one (classified AT_LEAST_ONCE
          by ``_retry_verdict``) should account for this in their retry window.
        - FATAL (4xx non-429/408): deterministic rejection — the record is
          added to ``failed_ids`` and the loop continues to the next record.
        """
        written = 0
        failed_ids: list[str] = []
        first_failure = ""

        for i, record in enumerate(records):
            # Insert keys on the identity-derived record_id (first
            # occurrence of an identity wins, matching the SQL insert
            # anti-join); upsert keys on the full record content so a
            # changed row gets a new key and the provider applies the
            # update instead of replaying its cached response.
            idempotency_key = (
                None
                if state.idempotency_in is None
                else (
                    record_ids[i]
                    if state.write_mode_key == "insert"
                    else _content_idempotency_key(record)
                )
            )
            try:
                body = self._build_body(state, record=record)
                if state.idempotency_in == "body" and idempotency_key is not None:
                    body = _body_with_idempotency_key(state, body, idempotency_key)
            except (TypeError, ValueError) as e:
                logger.warning(
                    "Failed to build body for record %s: %s: %s",
                    record_ids[i],
                    type(e).__name__,
                    e,
                )
                failed_ids.append(record_ids[i])
                first_failure = first_failure or f"{type(e).__name__}: {e}"
                continue
            idempotency_header = (
                {state.idempotency_name: idempotency_key}
                if state.idempotency_in == "header" and idempotency_key is not None
                else None
            )
            try:
                await self._send_request(state, body, extra_headers=idempotency_header)
                written += 1
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if _classify_http_error(e) == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
                    logger.warning(
                        "RETRYABLE error on record %s (index %d, %d already written)"
                        " — aborting batch: %s: %s",
                        record_ids[i],
                        i,
                        written,
                        type(e).__name__,
                        e,
                    )
                    raise
                logger.warning(
                    "Failed to write record %s: %s: %s",
                    record_ids[i],
                    type(e).__name__,
                    e,
                )
                failed_ids.append(record_ids[i])
                first_failure = first_failure or f"{type(e).__name__}: {e}"

        if failed_ids:
            logger.warning(
                f"Failed to write {len(failed_ids)} records: {failed_ids[:5]}..."
            )

        return written, failed_ids, first_failure

    async def _write_chunked_mode(
        self,
        state: _StreamState,
        records: list[dict[str, Any]],
        record_ids: list[str],
    ) -> tuple[int, list[str], str]:
        """Write records in chunks of at most ``max_records``.

        Returns ``(written, failed_record_ids, first_failure)``, matching
        single mode. The written count tracks records actually sent so a
        mid-loop chunk failure reports the true count instead of 0 —
        reporting 0 (and RETRYABLE) would re-send the chunks that already
        landed and duplicate them.

        Per-item partial failure inside a 2xx response body is NOT
        inspected: the engine is connector-agnostic and no endpoint
        contract declares where a per-item error array lives, so the
        response shape is opaque. A 2xx means the API accepted the whole
        chunk.

        Any chunk failure stops the loop: the batch verdict is already
        FATAL (written can no longer reach total), the engine DLQs the
        whole batch and fails the stream on FATAL, and the uncheckpointed
        batch gets replayed on a restart — so every record sent past the
        first failed chunk would land only to be re-sent by that replay.
        Chunked streams can never carry an idempotency key (the contract
        excludes it with batching), so that duplication is unmitigated.
        The failed chunk and the unsent tail are attributed as failed.

        A RETRYABLE transport failure before any chunk landed
        (``written == 0``) re-raises instead, so write_batch classifies it
        RETRYABLE — nothing was written, a retry cannot duplicate. A FATAL
        transport failure (4xx non-429) does not re-raise even at
        ``written == 0``: the API rejected the payload deterministically, and
        retrying wastes retries identically. A build failure never re-raises:
        it is deterministic, so a retry cannot help and FATAL with the ids
        attributed is the honest verdict even at ``written == 0``.
        """
        chunk_size = state.max_records
        if chunk_size is None:
            raise RuntimeError(
                "chunked write dispatched for a stream with no batching "
                "declaration; write_batch routes those to single mode"
            )
        written = 0

        for i in range(0, len(records), chunk_size):
            batch = records[i : i + chunk_size]
            try:
                body = self._build_body(state, records=batch)
            except (TypeError, ValueError) as e:
                logger.warning(
                    "Failed to build body for chunk at offset %d (%d records "
                    "%s...): %s: %s",
                    i,
                    len(batch),
                    record_ids[i : i + 3],
                    type(e).__name__,
                    e,
                )
                return written, list(record_ids[i:]), f"{type(e).__name__}: {e}"
            try:
                await self._send_request(state, body)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if (
                    written == 0
                    and _classify_http_error(e)
                    == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
                ):
                    # No chunk landed and the error is transient — safe to
                    # retry the whole batch.
                    raise
                logger.warning(
                    "Failed to write batch chunk at offset %d (%d records): %s: %s",
                    i,
                    len(batch),
                    type(e).__name__,
                    e,
                    exc_info=True,
                )
                return written, list(record_ids[i:]), f"{type(e).__name__}: {e}"
            written += len(batch)

        return written, [], ""

    async def _send_request(
        self,
        state: _StreamState,
        data: Any,
        extra_headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Send HTTP request with rate limiting.

        RetryClient transport-level retries (configured in connect()):
        - 429, 500, 503, 504: retry up to max_retries with exponential backoff
        - all other statuses (4xx except 429, 502, etc.): single attempt

        Engine-level ack classification (via _classify_http_error):
        - 4xx except 429: ACK_STATUS_FATAL_FAILURE — deterministic rejection
        - 429, 5xx, connection errors, timeouts: ACK_STATUS_RETRYABLE_FAILURE

        Args:
            data: Request body (single record or list of records)
            extra_headers: Per-request headers layered over the defaults
                (single mode passes the idempotency key header here)

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

        url = join_url(self._base_url, state.endpoint)

        # ``aiohttp.request(json=...)`` would call stdlib ``json.dumps``
        # which doesn't understand ``datetime`` / ``Decimal``. orjson is
        # C-based, handles ``datetime`` natively, and falls through to
        # the explicit default-hook only for ``Decimal`` / ``bytes`` —
        # one C-speed pass instead of a separate Python or Arrow cast.
        body = orjson.dumps(data, default=_orjson_default)

        headers = {"Content-Type": "application/json"}
        if extra_headers:
            headers.update(extra_headers)

        async with self._session.request(
            method=state.method,
            url=url,
            data=body,
            headers=headers,
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
                payload: dict[str, Any] = await response.json(content_type=None)
                return payload
            except (
                json.JSONDecodeError,
                aiohttp.ContentTypeError,
                UnicodeDecodeError,
            ) as err:
                body_snippet = (await response.text())[:500]
                raise aiohttp.ClientPayloadError(
                    f"API returned status {response.status} with non-JSON body: "
                    f"{err}; body[:500]={body_snippet!r}"
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
                type(e).__name__,
                e,
                exc_info=True,
            )
            return False
