"""Per-stream write configuration, built once at the schema handshake.

The direct mirror of ``cdk.sql.write_plan``: everything a stream's writes
need -- the path, the method, the batching cap, the body template, the
idempotency placement, the retry verdict -- is settled here, so the write
loop reads a plan instead of re-deriving the document per batch.

Every read is a RAW contract key. The document is already validated, and
the model's Python attribute names differ from the keys it serialises:
``in`` is spelled ``location`` on the model and ``schema`` is spelled
``schema_``. Reading by the attribute name finds nothing, and every site
here treats nothing as "the author declared nothing" -- which for
idempotency means promising exactly-once while never sending the key.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from ..exceptions import TransportSpecError
from ..record_identity import record_digest
from ..resolver import Resolver
from ..transport_factory import require_wire_safe_header_name
from ..types import RetrySemantics, RetryVerdict, SchemaSpec
from .exceptions import RequestSpecError
from .request import (
    ParamTable,
    bind_query_and_headers,
    bind_request_values,
    request_block_problem,
    substitute_path,
)

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
#: The contract's write map is closed and keyed by mode name.
WRITE_MODE_KEYS: dict[int, Literal["insert", "upsert"]] = {1: "insert", 2: "upsert"}


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
    #: Retry-safety verdict, computed at configure time.
    retry_verdict: RetryVerdict | None = None


def write_mode_block(doc: Mapping[str, Any], mode_key: str) -> Mapping[str, Any] | None:
    """Return the ``operations.write.<mode_key>`` block, or ``None``.

    Presence is the whole acceptance predicate: the validated document
    guarantees a present block is usable. Capability advertisement and
    per-stream dispatch both go through here, so the two cannot disagree on
    whether a write mode is offered.
    """
    write = (doc.get("operations") or {}).get("write") or {}
    block = write.get(mode_key)
    return block if isinstance(block, Mapping) else None


def collect_json_fields(mode_block: Mapping[str, Any]) -> set[str]:
    """Body field names declared with ``arrow_type: "Json"``.

    The write input schema is free-form JSON Schema in the contract, so
    both shapes it permits are walked: JSON-Schema ``properties`` and the
    flat ``columns`` array.
    """
    schema = (mode_block.get("input") or {}).get("schema") or {}
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


def collect_input_field_names(mode_block: Mapping[str, Any]) -> set[str]:
    """Every field name the write input schema declares, in both shapes."""
    schema = (mode_block.get("input") or {}).get("schema") or {}
    names: set[str] = {
        name for name in (schema.get("properties") or {}) if isinstance(name, str)
    }
    for col in schema.get("columns") or []:
        if isinstance(col, Mapping) and col.get("name"):
            names.add(col["name"])
    return names


def reserved_header_names(session_header_names: Iterable[str]) -> frozenset[str]:
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
    """
    return frozenset(name.lower() for name in session_header_names)


def idempotency_config_problem(
    idempotency: Mapping[str, Any],
    batching: Mapping[str, Any] | None,
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
    target = idempotency.get("in")
    name = str(idempotency.get("name") or "")
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
                "upsert dedups on the endpoint's conflict keys; a re-sent "
                "record updates in place"
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


def build_write_plan(
    doc: Mapping[str, Any],
    schema_spec: SchemaSpec,
    *,
    session_header_names: set[str],
    resolver: Resolver,
) -> StreamWritePlan | str:
    """Build the plan for a stream, or return why the schema is refused.

    A string return is the rejection reason the ack carries. Every
    rejection is a defect in the endpoint document or the stream's write
    config, which is what lets the caller declare one failure category for
    all of them.
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
        write = (doc.get("operations") or {}).get("write")
        available = sorted(write) if write else None
        return (
            f"endpoint document does not define an operations.write.{mode_key} "
            f"block; write modes present: {available}"
        )

    request = mode_block.get("request") or {}
    endpoint_id = str(doc.get("endpoint_id", "<unnamed>"))
    reserved = reserved_header_names(session_header_names)
    try:
        table = ParamTable.for_write(mode_block.get("params") or {}, resolver)
        problem = request_block_problem(
            request,
            reserved_headers=reserved,
            resolver=resolver,
            controlled_by=table.controlled_by,
            declared_params=mode_block.get("params") or {},
        )
        if problem is not None:
            return problem

        plan = StreamWritePlan(
            method=request.get("method", "POST"),
            json_fields=collect_json_fields(mode_block),
            body_spec=request.get("body"),
            content_type=request.get("content_type"),
            params=table.values,
            write_mode_key=mode_key,
        )
        plan.endpoint = substitute_path(
            request.get("path", ""),
            bind_request_values(
                request.get("path_params"),
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
            declared_query=request.get("query"),
            declared_headers=request.get("headers"),
            resolver=resolver,
            endpoint=endpoint_id,
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
    batching = mode_block.get("batching")
    if batching is not None:
        plan.max_records = batching.get("max_records")

    idempotency = mode_block.get("idempotency")
    if idempotency is not None:
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
        plan.idempotency_in = idempotency.get("in")
        plan.idempotency_name = idempotency.get("name", "")

    plan.retry_verdict = retry_verdict(mode_key, plan)
    return plan
