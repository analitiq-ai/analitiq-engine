"""Per-stream write configuration, built once at the schema handshake.

The direct mirror of ``cdk.sql.write_plan``: everything a stream's writes
need -- the path, the method, the batching cap, the body template, the
idempotency placement, the retry verdict -- is settled here, so the write
loop reads a plan instead of re-deriving the document per batch.

The document arrives parsed, so every read here is a model attribute, and
on two fields that attribute is spelled differently from the key the
author wrote: ``in`` is ``location``, ``schema`` is ``schema_``. Naming
the authored key instead now raises on the first read rather than
answering nothing, which is most of what the parse buys: every site here
reads nothing as "the author declared nothing", and for idempotency that
means promising exactly-once while never sending the key.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from analitiq.contracts.endpoints import (
    ApiEndpointDoc,
    Batching,
    Idempotency,
    WriteMode,
    WriteOperation,
    WriteResponse,
)

from ..exceptions import TransportSpecError
from ..record_identity import record_digest
from ..resolver import Resolver, scope_paths
from ..transport_factory import require_wire_safe_header_name
from ..types import RetrySemantics, RetryVerdict, SchemaSpec
from .body import FORM_CONTENT_TYPE, media_type
from .exceptions import RequestSpecError
from .query_style import declared_query_styles
from .request import (
    ParamTable,
    bind_query_and_headers,
    bind_request_values,
    request_block_problem,
    substitute_path,
)
from .write_response import WRITE_SCOPE_KEYS

__all__ = [
    "WRITE_MODE_KEYS",
    "StreamWritePlan",
    "body_with_idempotency_key",
    "build_write_plan",
    "collect_input_field_names",
    "collect_json_fields",
    "content_idempotency_key",
    "idempotency_config_problem",
    "reserved_header_names",
    "retry_verdict",
    "write_mode_block",
]

#: Proto write-mode int -> the ``operations.write.<key>`` block it selects.
#: The contract's write map is closed and keyed by mode name, and the names
#: are the contract's own ``WriteMode``: ``operations.write`` is keyed by it,
#: so a mode the contract renames or drops stops type-checking here instead
#: of quietly selecting nothing.
WRITE_MODE_KEYS: dict[int, WriteMode] = {1: "insert", 2: "upsert"}


@dataclass
class StreamWritePlan:
    """How one configured stream writes.

    The connector instance is shared across every stream writing to this
    destination, so per-stream values live here keyed by ``stream_id`` and
    concurrent schema handshakes cannot clobber each other.
    """

    #: ``request.path`` with its ``{name}`` placeholders already substituted
    #: -- the path every request for this stream goes to.
    endpoint: str = ""
    method: str = "POST"
    #: ``request.transport_ref`` -- the transport this stream's writes
    #: dispatch through, or ``None`` for the connection's default. The
    #: path above is relative to THAT transport's base URL, so the two
    #: travel together: a plan carrying one without the other addresses a
    #: resource on the wrong origin.
    transport_ref: str | None = None
    #: ``request.headers`` and ``request.query``, resolved once: write
    #: params read only what ``request_resolver`` supplies (the connection
    #: subtrees and the runtime values -- never secrets, which resolve
    #: engine-side at materialization), so nothing in either map can vary
    #: per record.
    headers: dict[str, str] = field(default_factory=dict)
    query: dict[str, Any] = field(default_factory=dict)
    #: The declared write params' resolved values, feeding the body's
    #: ``from_param`` bindings.
    params: dict[str, Any] = field(default_factory=dict)
    #: ``batching.max_records`` -- the provider's per-request maximum.
    #: ``None`` means the endpoint declares no batching block, so every
    #: record is sent as its own request.
    max_records: int | None = None
    #: Body fields declared ``arrow_type: "Json"``. The wire carries them as
    #: JSON-encoded strings, so they are decoded before the body is
    #: serialised -- otherwise the provider receives a quoted string where a
    #: nested object was declared.
    json_fields: set[str] = field(default_factory=set)
    #: ``request.body``, or ``None`` when the endpoint declares no template
    #: and the record itself is the body.
    body_spec: Any | None = None
    #: ``request.content_type``. Selects the body encoding and is sent as
    #: the header; ``None`` means the endpoint declared none, so JSON.
    content_type: str | None = None
    #: Where the engine-owned per-record idempotency key lands ("header" or
    #: "body") and the name it lands under. ``None`` means the endpoint
    #: declares no key. The VALUE is always engine-owned -- the author
    #: declares placement only.
    idempotency_in: str | None = None
    idempotency_name: str = ""
    #: The stream's write mode key. Insert keys on the engine's
    #: identity-derived record id (SQL insert parity: the first occurrence
    #: of an identity wins); upsert keys on the full record content so a
    #: changed row gets a new key.
    write_mode_key: str = "insert"
    #: The endpoint's ``conflict_keys`` -- the provider-defined natural key
    #: an upsert matches on, each a top-level field of the write input
    #: schema. The request block addresses the record through
    #: ``from_input``, so the keys reach the wire inside the record: every
    #: record this stream sends must carry a value for each. Empty for
    #: insert, where the contract forbids the field. The endpoint is the
    #: only source: the contract gives an API stream's write block no
    #: ``conflict_keys`` of its own.
    conflict_keys: list[str] = field(default_factory=list)
    #: Retry-safety verdict, computed at configure time.
    retry_verdict: RetryVerdict | None = None
    #: The declared ``response`` block: how the provider's answer says
    #: whether the records landed. ``None`` means the endpoint declares
    #: none and a success status is the whole verdict.
    response: WriteResponse | None = None


_RESPONSE_PREFIX = "response."


def declared_expressions(declared: WriteResponse) -> list[Any]:
    """Every expression the block declares, for a check that reads them all.

    The configure-time check in :func:`build_write_plan` is the caller.
    One expression at a time, never the block: a scanner handed a map
    reads a key named ``ref``, ``template`` or ``literal`` as an
    expression marker and stops seeing its siblings
    (:func:`~cdk.api.request._declared_expressions` documents the case).
    """
    nodes: list[Any] = [
        declared.success_when,
        declared.affected_records,
        declared.generated_keys,
    ]
    if declared.error is not None:
        nodes += [declared.error.code, declared.error.message, declared.error.details]
    if declared.metadata is not None:
        nodes += list(declared.metadata.values())
    return [node for node in nodes if node is not None]


def response_scope_problem(declared: WriteResponse) -> str | None:
    """Why the block reads what no write response carries, or ``None``.

    The contract resolves ``response.body`` paths against the declared
    response schema and leaves the other sub-scopes to their engine-side
    owner; on the write path that owner is
    :func:`~cdk.api.write_response.write_response_scope`. A reference
    outside it resolves to nothing on every response, so ``success_when``
    holds false on every batch and each is reported as a provider
    rejection for what is an authoring defect. Judged here, before the
    first write, through the same walker the read-path kit uses.
    """
    for lookup in dict.fromkeys(scope_paths(declared_expressions(declared))):
        if not lookup.startswith(_RESPONSE_PREFIX):
            continue
        scope = lookup[len(_RESPONSE_PREFIX) :].split(".")[0]
        if scope not in WRITE_SCOPE_KEYS:
            return (
                f"reads {lookup!r}, but a write response carries only "
                f"{', '.join(repr(k) for k in WRITE_SCOPE_KEYS)} under "
                f"'response'; this resolves to nothing on every response"
            )
    return None


def write_mode_block(doc: ApiEndpointDoc, mode_key: WriteMode) -> WriteOperation | None:
    """Return the ``operations.write.<mode_key>`` block, or ``None``.

    Presence is the whole acceptance predicate: the validated document
    guarantees a present block is usable. Capability advertisement and
    per-stream dispatch both go through here, so the two cannot disagree on
    whether a write mode is offered.
    """
    return (doc.operations.write or {}).get(mode_key)


def collect_json_fields(mode_block: WriteOperation) -> set[str]:
    """Body field names declared with ``arrow_type: "Json"``.

    The write input schema is free-form JSON Schema in the contract, so
    both shapes it permits are walked: JSON-Schema ``properties`` and the
    flat ``columns`` array.
    """
    schema = mode_block.input.schema_
    names: set[str] = set()
    for name, prop in (schema.get("properties") or {}).items():
        if isinstance(prop, Mapping) and prop.get("arrow_type") == "Json":
            names.add(name)
    for col in schema.get("columns") or []:
        if isinstance(col, Mapping) and col.get("arrow_type") == "Json":
            col_name = col.get("name")
            if col_name:
                names.add(col_name)
    return names


def collect_input_field_names(mode_block: WriteOperation) -> set[str]:
    """Every field name the write input schema declares, in both shapes."""
    schema = mode_block.input.schema_
    names: set[str] = {
        name for name in (schema.get("properties") or {}) if isinstance(name, str)
    }
    for col in schema.get("columns") or []:
        if isinstance(col, Mapping) and col.get("name"):
            names.add(col["name"])
    return names


def reserved_header_names(transport_header_names: Iterable[str]) -> frozenset[str]:
    """Header names an endpoint may not declare: the CONNECTION's own.

    One set, read by both things that land in the same header map -- the
    endpoint's declared headers and the engine-owned idempotency key -- so
    the two cannot disagree on what is already taken.

    The engine-owned names are no longer in it. ``Content-Length`` and
    ``Content-Type`` are static facts about a document, and since contract
    1.0.0rc23 it refuses them in every block that names a header
    (RULE-HTTP-002 / RULE-HTTP-003) -- this one, an ``idempotency.name``,
    included. What is left is what no document can know: the header names
    THIS connection's transport resolved to something.

    Which transport: the one the operation being judged dispatches
    through. A connector may own ``Authorization`` on one and nothing on
    another, so a set taken from the default would let an endpoint shadow
    a named transport's credential while refusing a header the transport
    it actually uses never sends.
    """
    return frozenset(name.lower() for name in transport_header_names)


def idempotency_config_problem(
    idempotency: Idempotency,
    batching: Batching | None,
    plan: StreamWritePlan,
    *,
    reserved_headers: frozenset[str] | set[str],
    declared_input_fields: set[str],
) -> str | None:
    """Why this ``idempotency`` block cannot work for the stream, or ``None``.

    Mirrors the api-endpoint schema's cross-block constraints that
    per-model validation cannot express: the header namespace this
    connection owns, the batching exclusion, and body-field collisions. The
    block's own shape is contract-guaranteed. The contract has no batching
    mode -- a present ``batching`` block IS the multi-record case, so the
    exclusion keys on its presence.
    """
    target = idempotency.location
    name = idempotency.name
    if target == "header":
        # The key lands in the same header map an endpoint's own headers do,
        # by a different route, so it answers to the same wire rules: a name
        # that is not an HTTP token dies in the client on every request. The
        # name alone is judged, and by the function that judges names -- the
        # value here is a per-record digest the engine computes, so there is
        # no declared one to hand over.
        try:
            require_wire_safe_header_name(name)
        except TransportSpecError as err:
            return f"idempotency.name is unusable as a header: {err}"
    if target == "header" and name.lower() in reserved_headers:
        # Same rule as the body reserved-field check: these headers are
        # engine-owned (Content-Type) or carry the connection's own values
        # (auth and friends). Layering the key over one would silently
        # break every request -- or send the record id as the credential.
        return (
            f"idempotency.name {name!r} collides with an engine- or "
            f"connection-owned request header; pick a header the connection "
            f"does not already send"
        )
    if batching is not None:
        return (
            "idempotency cannot be combined with a batching block: a restart "
            "re-batches records, so a per-request key over several records "
            "cannot dedup (issue #286); the api-endpoint schema forbids the "
            "combination"
        )
    if target == "body":
        if plan.body_spec is not None and not isinstance(plan.body_spec, Mapping):
            return (
                f"idempotency.in='body' needs a JSON-object request body; the "
                f"declared request.body is a {type(plan.body_spec).__name__}"
            )
        if isinstance(plan.body_spec, Mapping) and name in plan.body_spec:
            return (
                f"request.body already declares the field {name!r} that "
                f"idempotency.name reserves for the engine-owned key"
            )
        if plan.body_spec is None and name in declared_input_fields:
            # No body template: the record itself is the body, shaped by the
            # write input schema -- a declared field with the reserved name
            # would collide on every record at write time, after the ack
            # already promised exactly-once.
            return (
                f"the write input schema already declares the field {name!r} "
                f"that idempotency.name reserves for the engine-owned key on "
                f"the pass-through body"
            )
    return None


def retry_verdict(mode_key: str, plan: StreamWritePlan) -> RetryVerdict:
    """Retry-safety verdict for one configured stream (issue #286).

    Upsert dedups on the endpoint's conflict keys whether or not an
    idempotency key is declared; insert is exactly-once only when the
    endpoint declares where the engine-owned key lands, otherwise a
    same-run restart re-sends already-delivered records.
    """
    if mode_key == "upsert":
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE,
            reason=(
                f"upsert dedups on the endpoint's conflict keys "
                f"{plan.conflict_keys}; a re-sent record updates in place"
            ),
        )
    if plan.idempotency_in is not None:
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE,
            reason=(
                f"each request carries the record's identity hash as an "
                f"idempotency key ({plan.idempotency_in} "
                f"{plan.idempotency_name!r}); dedup holds within the "
                f"provider's replay window, with SQL insert-mode identity "
                f"semantics (first occurrence of a key wins)"
            ),
        )
    return RetryVerdict(
        semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
        reason=(
            "insert mode with no declared idempotency key; a same-run restart "
            "re-sends already-delivered records"
        ),
    )


def body_with_idempotency_key(
    plan: StreamWritePlan, body: Any, record_id: str
) -> dict[str, Any]:
    """Return the request body with the engine-owned idempotency key added.

    Configure time already rejected a declared non-object body spec; this
    guards the remaining runtime shapes (a spec-less record body, or a spec
    that resolved away its object shape). A body already carrying the
    reserved field is a collision the engine must not silently overwrite.
    """
    if not isinstance(body, dict):
        raise ValueError(
            f"idempotency.in='body' for endpoint {plan.endpoint!r} needs a "
            f"JSON-object request body, got {type(body).__name__}"
        )
    if plan.idempotency_name in body:
        raise ValueError(
            f"request body already carries the field {plan.idempotency_name!r}, "
            f"which idempotency.name reserves for the engine-owned key; rename "
            f"the record field or the key"
        )
    return {**body, plan.idempotency_name: record_id}


def content_idempotency_key(record: Mapping[str, Any]) -> str:
    """Full-content hash used as the idempotency key in upsert mode.

    Upsert exists to reconcile changed rows, so its key must change when
    the content changes: a stable identity key would make the provider's
    replay cache swallow a legitimate update to the same entity inside its
    dedup window. The canonicalisation is the shared
    :func:`~cdk.record_identity.record_digest`, so an identical replay
    dedups and a changed row gets a new key.

    The record is hashed as sent: declared JSON columns are already decoded
    to objects by this point, so the key covers what the provider receives
    rather than the wire encoding of it.
    """
    return record_digest(dict(record))


def _selected_mode(
    doc: ApiEndpointDoc, schema_spec: SchemaSpec
) -> tuple[WriteMode, WriteOperation] | str:
    """Return the write block this schema selects, or why it cannot be served.

    Two ways a stream asks for something the api path does not have: a
    write mode outside the contract's closed map, and a mode the document
    declares no block for. Both are refusals the ack carries rather than
    exceptions, so they answer in the same currency as the rest of the
    handshake.
    """
    mode_key = WRITE_MODE_KEYS.get(schema_spec.write_mode)
    if mode_key is None:
        return (
            f"API destination does not support write_mode="
            f"{schema_spec.write_mode}; valid api-endpoint modes are "
            f"{sorted(WRITE_MODE_KEYS.values())}"
        )
    mode_block = write_mode_block(doc, mode_key)
    if mode_block is None:
        write = doc.operations.write
        available = sorted(write) if write else None
        return (
            f"endpoint document does not define an operations.write.{mode_key} "
            f"block; write modes present: {available}"
        )
    return mode_key, mode_block


def _batching_problem(plan: StreamWritePlan) -> str | None:
    """Why this stream cannot batch as declared, or ``None``.

    A batched body binds ``records``, which is a list, and a form carries
    flat name/value pairs -- so every chunk this stream ever builds fails
    encoding, deterministically, before anything is sent. The two are
    incompatible by shape rather than by data, so the handshake refuses
    the stream instead of advertising one that cannot write a batch.
    """
    if media_type(plan.content_type) != FORM_CONTENT_TYPE:
        return None
    return (
        f"operations.write declares batching and content_type "
        f"{plan.content_type!r}: a batched body sends the records as a "
        f"list, and {FORM_CONTENT_TYPE} carries only flat name/value "
        f"pairs, so no chunk of this stream could ever be encoded. Drop "
        f"the batching block to send one record per request, or declare a "
        f"content_type that carries structure"
    )


def _apply_idempotency(
    plan: StreamWritePlan,
    mode_block: WriteOperation,
    batching: Batching | None,
    *,
    reserved: frozenset[str],
) -> str | None:
    """Record where the engine-owned idempotency key lands, or why it cannot.

    The author declares placement only -- the VALUE is always the
    engine's -- so what can go wrong is where it would land: a header the
    connection or the endpoint already sends, a name the client cannot
    put on the wire, a body field the record already carries.
    """
    idempotency = mode_block.idempotency
    if idempotency is None:
        return None
    problem = idempotency_config_problem(
        idempotency,
        batching,
        plan,
        # The endpoint's own headers join the reserved set: the
        # engine-owned key must not be layered over a header this
        # endpoint declares either.
        reserved_headers=reserved | {name.lower() for name in plan.headers},
        declared_input_fields=collect_input_field_names(mode_block),
    )
    if problem is not None:
        return problem
    plan.idempotency_in = idempotency.location
    plan.idempotency_name = idempotency.name
    return None


def build_write_plan(
    doc: ApiEndpointDoc,
    schema_spec: SchemaSpec,
    *,
    header_names_for: Callable[[str | None], Iterable[str]],
    transport_problem: Callable[[str | None], str | None],
    resolver: Resolver,
) -> StreamWritePlan | str:
    """Build the plan for a stream, or return why the schema is refused.

    A string return is the rejection reason the ack carries. Every
    rejection is a defect in the endpoint document or the stream's write
    config, which is what lets the caller declare one failure category for
    all of them.

    ``header_names_for`` and ``transport_problem`` are asked, not told:
    only this function knows which mode block the schema selects, and so
    which ``transport_ref`` the stream's writes dispatch through. A caller
    passing one answer for the whole document would be passing the default
    transport's, which is the wrong one for a stream that writes somewhere
    else.
    """
    selected = _selected_mode(doc, schema_spec)
    if isinstance(selected, str):
        return selected
    mode_key, mode_block = selected

    request = mode_block.request
    endpoint_id = doc.endpoint_id
    # Refused HERE, not when the first non-empty batch tries to send: a
    # transport that cannot be opened is an authoring defect, and
    # accepting the schema for it turns that defect into a fatal batch
    # after the engine was told the stream was ready to write.
    problem = transport_problem(request.transport_ref)
    if problem is not None:
        return problem
    reserved = reserved_header_names(header_names_for(request.transport_ref))
    if mode_block.response is not None:
        # Decidable from the document alone, so refused here: found on the
        # first write instead, the record may have landed by the time the
        # answer cannot be read, and it is reported failed either way.
        problem = resolver.unknown_function_problem(
            declared_expressions(mode_block.response)
        ) or response_scope_problem(mode_block.response)
        if problem is not None:
            return f"response block on endpoint {endpoint_id!r}: {problem}"
    try:
        table = ParamTable.for_write(mode_block.params, resolver, endpoint=endpoint_id)
        problem = request_block_problem(
            request,
            reserved_headers=reserved,
            resolver=resolver,
            controlled_by=table.controlled_by,
            declared_params=mode_block.params,
            endpoint=endpoint_id,
        )
        if problem is not None:
            return problem
        # After the block walk, not before it: a default reading a scope that
        # write-time resolution never fills is the same omission with a far
        # better answer -- it names the unfillable path -- and the walk above
        # is where that answer lives. This one takes what is left: a param
        # that resolved to nothing for any other reason, and a resolved value
        # the declaration does not admit. Checked once here rather than per
        # record: a write's params are the declared defaults and nothing else,
        # so this table is the table every record is written with.
        table.checker.check(table.values)

        plan = StreamWritePlan(
            method=request.method,
            transport_ref=request.transport_ref,
            json_fields=collect_json_fields(mode_block),
            body_spec=request.body,
            content_type=request.content_type,
            params=table.values,
            write_mode_key=mode_key,
            response=mode_block.response,
            conflict_keys=list(mode_block.conflict_keys or []),
        )
        plan.endpoint = substitute_path(
            request.path,
            bind_request_values(
                request.path_params,
                params=table.values,
                resolver=resolver,
                block="path_params",
                endpoint=endpoint_id,
            ),
            endpoint=endpoint_id,
        )
        # The same binder the read role's pages go through, so a declared
        # query key or header reaches the wire whichever role sends it.
        plan.query, plan.headers = bind_query_and_headers(
            params=table.values,
            declared_query=request.query,
            declared_headers=request.headers,
            resolver=resolver,
            endpoint=endpoint_id,
            query_styles=declared_query_styles(request.query, mode_block.params),
        )
    except RequestSpecError as err:
        # An unbound placeholder, a param default reading a scope nothing
        # fills, a malformed binding map: the stream can never send a
        # correct request, so the schema handshake refuses it with a reason
        # the ack carries. Letting one of these out instead would tear down
        # the whole gRPC stream over one stream's defective document.
        return str(err)

    # A present batching block IS the multi-record case and carries
    # ``max_records`` (>= 2, contract-guaranteed); absence means one request
    # per record.
    batching = mode_block.batching
    if batching is not None:
        problem = _batching_problem(plan)
        if problem is not None:
            return problem
        plan.max_records = batching.max_records

    problem = _apply_idempotency(plan, mode_block, batching, reserved=reserved)
    if problem is not None:
        return problem

    plan.retry_verdict = retry_verdict(mode_key, plan)
    return plan
