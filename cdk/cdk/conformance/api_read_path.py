"""The api read path, driven from the endpoint document with no network.

The database tier does no I/O either: it builds a runtime from the
definition, derives the driver, and *compiles* the read through
``QueryBuilder``. SQL is text, so a read shape is certifiable without a
database. The api analogue is that a read compiles into a
:class:`~cdk.api.page_loop.PageRequest`: ``strategy.first()`` answers a URL
and a bound param table with nothing sent, ``strategy.advance(page)``
answers the request after a page that is handed to it rather than fetched,
and the author's ``stop_when`` is a predicate evaluated against a page
scope. None of the three needs an HTTP client, which is why they live in
modules the ``conformance`` extra can import.

Every check here runs the engine's own functions -- ``build_strategy``,
``resolve_page_size``, ``ParamTable.for_read``, ``evaluate_predicate``,
``page_resolver``, ``follow_url``, ``records_items_schema`` -- against the
connector's real declarations. Nothing restates what a paging scheme ought
to do; the schemes are asked, and what they answer is the finding. That is
the whole difference from the enumeration this replaces: a table of "offset
pagination should increment by ..." certifies the table, and drifts from
the loop the day the loop changes.

Three substitutions are the kit's own, and each is named in the message
when it bites:

* the page a scheme advances from is *scripted*, not fetched. Its body
  carries, at every path the pagination block reads, a value of the type
  the endpoint's own response schema declares there -- so a comparison in
  ``stop_when`` sees the operand the connector said it would, and a next
  link the author pointed at the object containing it is handed that
  object. A path the schema does not declare is a document defect the
  contract refuses at parse (RULE-ENDP-023) rather than a type the kit
  invents: an invented type is what decides whether an ordering comparison
  raises, so inventing one would make the verdict the kit's rather than
  the connector's. Its records are shaped the same way, plus the keyset
  ordering field, which is planted because the engine walks the provider's
  raw record and not the declared schema.
* the origin the link guard is armed with is the default transport's
  literal ``base_url``, or a stand-in when the definition expresses it as
  a reference the connection document supplies. What the guard certifies
  -- that a link handed to the traversal is either refused or resolved
  back onto that origin -- holds for either.
* a path placeholder whose value the connection, a stream's filters or the
  replication cursor supplies gets a stand-in segment. The engine builds
  its param table from all three and substitutes the path after the
  incremental filter binds; a definition-only run has none of them, so
  demanding a value here would fail a connector the engine reads
  correctly. Only a placeholder nothing could ever bind is a finding: one
  with no binding at all, one bound to a param the endpoint does not
  declare, and one bound to an expression no run fills -- either it reads
  no scope at all, or it reads ``secrets``/``auth``, which request-time
  resolution never supplies. Each fails for every connection and every
  stream.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlsplit

from cdk.api.exceptions import RequestSpecError
from cdk.api.page_loop import Page, PageRequest, PaginationStrategy
from cdk.api.read_setup import build_read_strategy, stop_condition
from cdk.api.records import split_records_ref
from cdk.api.request import (
    ParamTable,
    PreparedRequest,
    RequestBuilder,
    bind_request_values,
    path_placeholders,
    request_block_problem,
    substitute_path,
)
from cdk.api.response_schema import records_items_schema, resolve_field_arrow_type
from cdk.api.strategies import KEYSET_REFUSAL_MARKER
from cdk.api.urls import ORIGIN_REFUSAL_MARKER, join_url, same_origin
from cdk.api.write_plan import reserved_header_names
from cdk.exceptions import ReadError
from cdk.resolver import Resolver, scope_paths
from cdk.schema_contract import SchemaContract
from cdk.type_map import TypeMapper

from .api_surface import (
    api_base_url,
    definition_resolver,
    expression_grammar_problem,
    fillable_at_request_time,
    read_operations,
)
from .target import ConformanceTarget
from .violations import Violation

__all__ = [
    "check_api_read_advances",
    "check_api_read_compiles",
    "check_api_read_stop_condition",
    "check_api_record_schema",
]

COMPILE_CHECK = "api-read-compiles"
ADVANCE_CHECK = "api-read-advances"
STOP_CHECK = "api-read-stop-condition"
RECORDS_CHECK = "api-record-schema"

#: What compiling a read raises: ``RequestSpecError`` from every
#: declaration the request build resolves, and ``ReadError`` from the
#: probe's own refusals (a missing request block, a placeholder nothing
#: could bind) and from ``build_read_strategy``. Two, not five: the engine
#: classifies at the boundary now, so the kit no longer has to enumerate
#: the resolver's exception vocabulary -- an enumeration it could only ever
#: get wrong in one direction, by letting a defect out as a raw traceback
#: that takes every later probe down with it.
_COMPILE_FAILURES = (ReadError, RequestSpecError)

#: What ``advance`` raises: ``ValueError`` from the strategies' own refusals
#: (a keyset page with no ordering value, a link off the origin, a next link
#: that is not a URL string) and ``ReadError`` from the page expression
#: resolver, which classifies everything it wraps.
_ADVANCE_FAILURES = (ReadError, ValueError)

#: What building a record schema raises: ``ReadError`` from the records ref
#: and the arrow-type resolution, ``ValueError`` from ``SchemaContract``.
_RECORD_FAILURES = (ReadError, ValueError)

#: The page size the probe reads with. Any positive integer works; a value
#: unlike the contract's own bounds makes it obvious in a message that the
#: number came from the kit rather than from the connector.
_PROBE_BATCH_SIZE = 37

#: How many records the scripted page carries: a full one. A stop condition
#: comparing the record count against the page size is asking "was this page
#: short", and a short probe page would answer "the stream ended here" for
#: every connector that asks.
_PROBE_RECORDS = _PROBE_BATCH_SIZE

#: The value a scripted record carries for a declared field, and the seed
#: every other scripted value is derived from. Distinctive on purpose: a
#: keyset scheme advances to the last record's ordering value, and a value
#: that happened to equal the declared ``initial`` would read as a
#: traversal that never moved.
_PROBE_KEY_VALUE = 9901

#: The pages a traversal is driven through, each as the name a message
#: gives it and the seed it is scripted from. Consecutive pages of a real
#: read differ in what the provider hands back -- a different token, a
#: different next link, a different last ordering value -- and two pages
#: scripted from the same seed would be one page served twice, which every
#: scheme that continues from the page would answer with the same request.
#: A drive comparing those requests would then read every correct cursor,
#: link and keyset connector as one that cannot move.
_DRIVEN_PAGES = (("first", _PROBE_KEY_VALUE), ("second", _PROBE_KEY_VALUE + 1))

#: Stands in for a ``base_url`` the definition expresses as a reference.
_STAND_IN_ORIGIN = "https://conformance.invalid"

#: Stands in for a path segment only a connection, a stream's filters or the
#: replication cursor supplies. Distinctive on purpose: it appears verbatim
#: in the compiled URL, so a message quoting that URL says where it came
#: from.
_STAND_IN_PATH_SEGMENT = "conformance-path-value"

#: A next link on another host, for arming the origin guard.
_OFF_ORIGIN_URL = "https://elsewhere.invalid/page/2"

#: The page scope, and the part of it a value can be planted under.
_RESPONSE_PREFIX = "response."
_BODY_PREFIX = "response.body."

#: What ``cdk.api.records.page_scope`` puts in the response scope. Nothing
#: else is in it: the contract recognises ``headers``/``status``/``metadata``
#: as reserved, engine-owned sub-scopes -- RULE-ENDP-023 resolves only
#: ``response.body`` paths -- and the read path does not put them in the
#: page scope, so their availability is this module's fact to state.
_PAGE_SCOPE_KEYS = ("body", "record_count")

#: "take the type the response schema declares" -- distinct from any value
#: a drive could legitimately want planted, ``None`` included.
_DECLARED = object()

#: "the response schema names no type here", so nothing can be scripted at
#: this path. Distinct from ``None``, which is a value a field can hold.
_UNTYPED = object()

#: Where the compiled probes are cached on the target. A conformance target
#: carries unhashable fields, so it cannot key a mapping; it does have a
#: ``__dict__``, which is the slot ``functools.cached_property`` writes into
#: and the same one used here.
_PROBE_CACHE = "_api_read_probes"

#: Markers from the CDK's own raise sites, imported from them, so a drive
#: can tell the refusal it armed from some other failure that happened to
#: raise first -- and a rewording at the raise site cannot silently stop
#: the drive from recognizing it.
_KEYSET_REFUSAL = KEYSET_REFUSAL_MARKER
_ORIGIN_REFUSAL = ORIGIN_REFUSAL_MARKER

#: A continuation value shaped for the scheme that reads it: a cursor token
#: is opaque text and a next link is a relative URL (so it resolves against
#: the page it came from whatever the origin is). Not a statement about how
#: a scheme paginates -- only about what type its declared field holds. A
#: scheme with no entry here continues from a number, and gets the page key
#: itself, which is a whole positive one.
#:
#: Every value carries the key of the page it was planted on, so two pages
#: hand back two different continuations the way a provider does.
#:
#: Keyset has no entry for a second reason: it continues from the last
#: *record's* ordering field, and ``_Keyset`` reads only ``param``,
#: ``order_by_field`` and ``initial`` off its block, so a keyset read
#: declares no continuation path at all.
_CONTINUATION_TEMPLATES: dict[str, str] = {
    "cursor": "conformance-next-page-token-{key}",  # nosec B105 - not a credential
    "link": "?conformance-page={key}",
}


@dataclass(frozen=True)
class _ReadProbe:
    """One endpoint's read, compiled as far as the first request.

    Holds the pieces rather than a built strategy because every scheme is
    stateful and single-use -- a check that drives one page must start from
    a fresh traversal, so :meth:`strategy` builds a new adapter per drive
    through the read's own setup.

    ``first`` and ``first_sent`` are two different objects and a check has
    to pick the one its question is about. ``first.params`` is the param
    table the traversal carries; ``first_sent`` is what the request builder
    made of it -- the query keys, the headers and the body that go on the
    wire.
    """

    label: str
    read: dict[str, Any]
    request: Mapping[str, Any]
    pagination: dict[str, Any] | None
    url: str
    origin: str
    table: ParamTable
    resolver: Resolver
    first: PageRequest
    first_sent: PreparedRequest

    def strategy(self) -> PaginationStrategy:
        """Build a fresh adapter for this read, exactly as the engine does."""
        return build_read_strategy(
            self.pagination,
            table=self.table,
            resolver=self.resolver,
            url=self.url,
            base_url=self.origin,
            batch_size=_PROBE_BATCH_SIZE,
        )


def _compile_read(
    target: ConformanceTarget, label: str, read: dict[str, Any]
) -> _ReadProbe:
    """Compile one read to its first request, raising on any authoring defect.

    The ordering is the read's own: one resolver per read carrying the
    engine's page size, the param table built from it, the request block
    judged against what the path can send, the path substituted
    (:func:`_path_values`), and then
    :func:`~cdk.api.read_setup.build_read_strategy` -- the same function
    the read calls, so the page size lands where the read puts it and the
    origin guard is armed the way the read arms it. A second resolver built
    here would leave ``runtime.batch_size`` unresolvable and probe at the
    wrong page size.
    """
    resolver = definition_resolver(
        target, runtime_values={"batch_size": _PROBE_BATCH_SIZE}
    )
    request_block = read.get("request")
    if not isinstance(request_block, Mapping):
        raise ReadError("operations.read declares no request block")
    declared_path = request_block.get("path")
    if not isinstance(declared_path, str) or not declared_path:
        raise ReadError("operations.read.request declares no path")

    declared_params = read.get("params") or {}
    table = ParamTable.for_read(declared_params, resolver)
    problem = request_block_problem(
        request_block,
        reserved_headers=_transport_header_names(target),
        resolver=resolver,
        params=table.values,
        controlled_by=table.controlled_by,
        declared_params=declared_params,
        pagination=read.get("pagination"),
    )
    if problem is not None:
        raise ReadError(problem)
    path = substitute_path(
        declared_path,
        _path_values(
            declared_path,
            request_block,
            table=table,
            declared_params=declared_params,
            resolver=resolver,
        ),
        endpoint=declared_path,
    )

    origin = api_base_url(target) or _STAND_IN_ORIGIN
    pagination = read.get("pagination")
    if isinstance(pagination, dict):
        scope_problem = _page_scope_problem(pagination)
        if scope_problem is not None:
            raise ReadError(scope_problem)
    probe = _ReadProbe(
        label=label,
        read=read,
        request=request_block,
        pagination=pagination if isinstance(pagination, dict) else None,
        url=join_url(origin, path),
        origin=origin,
        table=table,
        resolver=resolver,
        first=PageRequest(""),
        first_sent=PreparedRequest(),
    )
    first = probe.strategy().first()
    return replace(
        probe, first=first, first_sent=_materialize_first_request(probe, first)
    )


def _page_scope_problem(pagination: Mapping[str, Any]) -> str | None:
    """Why a pagination reference addresses what no page carries, or ``None``.

    The half of the retired reference check the contract did not take:
    RULE-ENDP-023 resolves ``response.body`` paths against the declared
    response schema and leaves the reserved sub-scopes (``headers``,
    ``status``, ``metadata``) to their engine-side owner. On the read path
    that owner is ``page_scope``, which carries ``body`` and
    ``record_count`` and nothing else -- so a pagination value or stop
    condition on ``response.headers`` resolves to nothing on every page
    ever served. Absent is not neutral: a ``missing`` or ``empty``
    condition on it holds at page one and the stream stops there reporting
    success; an ``exists`` condition never holds and the read runs to
    exhaustion; a next cursor or link resolves to nothing and the
    traversal ends after one page.
    """
    for lookup in dict.fromkeys(scope_paths(pagination)):
        if not lookup.startswith(_RESPONSE_PREFIX):
            continue
        scope = lookup[len(_RESPONSE_PREFIX) :].split(".")[0]
        if scope not in _PAGE_SCOPE_KEYS:
            return (
                f"pagination reads {lookup!r}, but a read's page carries "
                f"only {', '.join(repr(k) for k in _PAGE_SCOPE_KEYS)} under "
                f"'response' -- the contract reserves the other response "
                f"sub-scopes for the engine, and the read path does not put "
                f"them in the page scope. This resolves to nothing on every "
                f"page."
            )
    return None


def _path_values(
    declared_path: str,
    request_block: Mapping[str, Any],
    *,
    table: ParamTable,
    declared_params: Mapping[str, Any],
    resolver: Resolver,
) -> dict[str, Any]:
    """Bind the path placeholders, standing in for what only a run supplies.

    The engine builds its param table from the declared defaults, the
    stream's filters and the replication cursor, and substitutes the path
    after the incremental filter has bound -- its own comment says
    substituting earlier "would refuse a read that works". A definition-only
    run has the defaults and nothing else, so a placeholder left unbound
    here is usually a value the run supplies rather than a defect, and
    reporting it would fail a connector the engine reads correctly.

    So this applies the rule the rest of the module applies to a body and to
    a base URL: defer what a definition cannot supply, refuse only what
    nothing could ever bind (:func:`_binds_at_run_time`). A deferred
    placeholder gets :data:`_STAND_IN_PATH_SEGMENT`, which is enough for
    every drive after this one -- they certify how the traversal moves, and
    the value inside one path segment is the same on every page.
    """
    bindings = request_block.get("path_params")
    bound = bind_request_values(
        bindings,
        params=table.values,
        resolver=resolver,
        block="path_params",
        endpoint=declared_path,
    )
    declared_bindings = bindings if isinstance(bindings, Mapping) else {}
    for name in path_placeholders(declared_path):
        value = bound.get(name)
        if value is not None and str(value):
            continue
        binding = declared_bindings.get(name)
        if _binds_at_run_time(binding, declared_params):
            bound[name] = _STAND_IN_PATH_SEGMENT
            continue
        raise ReadError(
            f"path {declared_path!r} leaves the placeholder {{{name}}} "
            f"unbound, and nothing a connection, a stream's filters or the "
            f"replication cursor supplies could fill it: "
            f"{_unbindable_reason(name, binding)}. Every read of this "
            f"endpoint fails on the URL it builds."
        )
    return bound


def _binds_at_run_time(binding: Any, declared_params: Mapping[str, Any]) -> bool:
    """Whether something a definition-only run lacks could still fill *binding*.

    A ``{from_param}`` naming a declared param is one: the value arrives
    from that param's default resolved against a real connection, from a
    stream's filters, or from the replication cursor. Any other expression
    defers only when a run's request resolution could fill it -- when
    everything it reads is ``connection.``-scoped. The path is substituted
    at request time, where secrets and auth are never in scope (they are
    resolved once, engine-side, at transport materialization), so a binding
    reading them is refused: no run ever fills it.
    """
    if isinstance(binding, Mapping) and "from_param" in binding:
        return binding["from_param"] in declared_params
    return fillable_at_request_time(binding)


def _unbindable_reason(name: str, binding: Any) -> str:
    """Say what the declaration does that leaves *name* with no possible value."""
    if binding is None:
        return f"request.path_params declares no binding for {{{name}}}"
    if isinstance(binding, Mapping) and "from_param" in binding:
        return (
            f"request.path_params binds it to the param "
            f"{binding['from_param']!r}, which operations.read.params does "
            f"not declare"
        )
    # A binding reading `secrets.*`/`auth.*` never reaches here: the shared
    # `request_block_problem` refuses the whole request block first, naming
    # the phase fact for every slot at once.
    return (
        f"request.path_params binds it to {binding!r}, which resolves to "
        f"nothing and reads no scope request-time resolution supplies"
    )


def _transport_header_names(target: ConformanceTarget) -> frozenset[str]:
    """Name the headers an endpoint of this connector may not declare.

    The engine reserves the names its session carries, which is what the
    transport declares once each value has resolved: a header resolving to
    nothing is dropped. A definition-only run cannot resolve those values,
    so it reserves every declared name -- including one this connection
    would drop.

    That is deliberate rather than approximate. The request build never sees
    a session header's value, only its name, so an endpoint re-declaring one
    can only shadow it; and which connections drop it is a fact about
    connection documents, not about the connector being certified. Letting
    the endpoint keep its copy because *some* connection leaves the header
    empty would make a credential-shadowing defect appear only for the
    connections that fill it in.
    """
    ref = target.definition.get("default_transport")
    block = target.declared_transports().get(ref) if isinstance(ref, str) else None
    declared = block.get("headers") if isinstance(block, Mapping) else None
    names = declared if isinstance(declared, Mapping) else {}
    # The engine's own set builder, so the engine-owned names it adds are
    # reserved here too rather than named a second time.
    return reserved_header_names(str(name) for name in names)


def _request_builder(probe: _ReadProbe) -> RequestBuilder:
    """Build the request builder the read itself constructs for this endpoint.

    Same arguments the engine passes, declared query and headers included:
    a builder given less would certify a request the engine does not build.
    """
    return RequestBuilder(
        probe.table,
        raw_body=_drivable_body(probe),
        resolver=probe.resolver,
        endpoint=str(probe.request.get("path")),
        declared_query=probe.request.get("query"),
        declared_headers=probe.request.get("headers"),
    )


def _drivable_body(probe: _ReadProbe) -> Any:
    """Return the declared body, or ``None`` where a definition cannot resolve it.

    Withheld only when the body is *itself* one expression reading a scope
    a connection supplies. That one resolves to nothing here and would be
    refused for the single reason that says nothing about the connector. A
    connection-scoped expression nested inside the body is kept:
    request-time resolution omits an unresolved field rather than failing,
    so the rest of the body still binds -- and a malformed branch beside it
    still has to be caught.
    """
    body = probe.request.get("body")
    return None if body is None or _is_connection_expression(body) else body


def _materialize_first_request(
    probe: _ReadProbe, first: PageRequest
) -> PreparedRequest:
    """Build the first request's query, headers and body, as the fetch does.

    ``strategy.first()`` answers where the request goes; the read then runs
    ``RequestBuilder.for_page`` to turn the param table into what is
    actually sent. A body-paginated read whose declared body is malformed
    -- a bad ``from_param`` node, a page value too wide for a JSON number
    -- gets that far and no further, so stopping at the ``PageRequest``
    would certify a read that cannot issue its first request.

    The body's *grammar* is judged even where its resolution is deferred:
    a node the connection supplies is still a node, and ``{"ref":
    "connection.x", "extra": 1}`` is an authoring defect the engine raises
    on the first time the stream runs.

    The built request is kept rather than thrown away: it is the only
    record of what page one actually sends, and the advance drive compares
    page two against it.
    """
    body = probe.request.get("body")
    problem = None if body is None else expression_grammar_problem(body)
    if problem is not None:
        raise ReadError(problem)
    return _request_builder(probe).for_page(
        first.params, sends_declared_body=first.sends_declared_body
    )


def _is_connection_expression(node: Any) -> bool:
    """Whether *node* is one expression a run's request resolution will fill.

    Deliberately not "reads one anywhere": a nested unresolved expression
    omits its own field and the body still builds, so skipping the whole
    materialization over one would hide every other defect beside it. A
    body reading ``secrets.*`` or ``auth.*`` never reaches this deferral:
    the shared ``request_block_problem`` refuses the request block first.
    """
    return Resolver.is_expression_node(node) and fillable_at_request_time(node)


def _probes(
    target: ConformanceTarget,
) -> tuple[tuple[_ReadProbe, ...], tuple[Violation, ...]]:
    """Compile every read, splitting what compiled from what did not.

    A read that does not compile has no ``advance`` and no ``stop_when`` to
    drive. Only :func:`check_api_read_compiles` reports why -- repeating
    the same defect from four checks would bury the one message that says
    what to change -- but every other check still says it did not drive
    that endpoint (:func:`_undriven`). Each check is exported on its own
    and a repo may wire one into a harness of its own, so "returned
    nothing" must never be how a check reports "ran against nothing".

    Compiled once per target: the four checks that need probes would
    otherwise each rebuild every read, and a compile is the most expensive
    thing the api tier does.

    Shared state is handed out as tuples for that reason. A list would be
    the same object in all four callers, so one of them appending its own
    finding to it -- which the compile check has every reason to do -- would
    put that finding in the other three, and would grow the cache by one
    more copy on every call.
    """
    cached: tuple[
        tuple[_ReadProbe, ...], tuple[Violation, ...]
    ] | None = target.__dict__.get(_PROBE_CACHE)
    if cached is not None:
        return cached
    probes: list[_ReadProbe] = []
    violations: list[Violation] = []
    for label, read in read_operations(target):
        try:
            probes.append(_compile_read(target, label, read))
        except _COMPILE_FAILURES as err:
            violations.append(
                Violation(
                    COMPILE_CHECK,
                    f"endpoint {label!r}: the read does not compile into a "
                    f"first request: {err}. Every page this endpoint would "
                    f"serve fails here, before anything is sent.",
                )
            )
    compiled = (tuple(probes), tuple(violations))
    target.__dict__[_PROBE_CACHE] = compiled
    return compiled


def _undriven(check: str, violations: Sequence[Violation]) -> list[Violation]:
    """Say which endpoints a dependent check could not drive, if any."""
    if not violations:
        return []
    return [
        Violation(
            check,
            f"{len(violations)} endpoint(s) were not driven because their "
            f"read does not compile into a first request; see the "
            f"{COMPILE_CHECK!r} check for what to fix. Nothing here says "
            f"anything about them.",
        )
    ]


def _plant(body: dict[str, Any], path: list[str], value: Any) -> None:
    """Put *value* at *path* in *body*, creating the objects along the way."""
    node = body
    for key in path[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    node[path[-1]] = value


def _body_paths(node: Any) -> list[list[str]]:
    """Return the ``response.body`` field paths *node* reads."""
    return [
        lookup[len(_BODY_PREFIX) :].split(".")
        for lookup in scope_paths(node)
        if lookup.startswith(_BODY_PREFIX)
    ]


def _response_schema(probe: _ReadProbe) -> Any:
    """Return the endpoint's declared response schema, or ``None``."""
    response = probe.read.get("response")
    return response.get("schema") if isinstance(response, Mapping) else None


def _declared_schema(schema: Any, path: list[str]) -> Any | None:
    """Return the declared sub-schema at *path*, or ``None`` for none."""
    node = schema
    for key in path:
        properties = node.get("properties") if isinstance(node, Mapping) else None
        if not isinstance(properties, Mapping) or key not in properties:
            return None
        node = properties[key]
    return node


def declared_type(node: Any) -> str | None:
    """Return the JSON type *node* declares, or ``None`` when it declares none.

    ``None`` means "not scriptable", never "string": the type is what
    decides whether an ordering comparison raises, so guessing one would
    make the verdict the kit's rather than the connector's.

    A node the schema does reach but types only through composition
    (``allOf``, ``anyOf``, ``$ref`` -- all permitted by the contract's
    property-node definition) answers ``None`` and is therefore left
    unevaluated rather than reported. Walking a composed node needs the
    single path-resolution algorithm the contract does not yet specify;
    emitting a finding on one meanwhile would fail contract-valid
    connectors.
    """
    if not isinstance(node, Mapping):
        return None
    declared = node.get("type")
    if isinstance(declared, list):
        declared = next((item for item in declared if item != "null"), None)
    return declared if isinstance(declared, str) and declared else None


def _continuation_paths(probe: _ReadProbe) -> list[list[str]]:
    """Return the body paths the scheme itself continues from.

    Only these get a scheme-shaped value. A ``stop_when`` operand lives in
    the same block but is not a continuation, and giving it a cursor token
    would type the author's comparison by the paging scheme rather than by
    what the connector declared the field to be.
    """
    block = probe.pagination or {}
    scheme = block.get(str(block.get("type", "")))
    return _body_paths(scheme) if scheme is not None else []


def _scripted_page(
    probe: _ReadProbe,
    *,
    records: list[dict[str, Any]],
    continuation: Any = _DECLARED,
    key: int = _PROBE_KEY_VALUE,
) -> Page:
    """Build the page a scheme advances from, with nothing fetched.

    Every body path the pagination block reads gets a value of the type the
    endpoint's own response schema declares for it -- the continuation paths
    included. Letting the declaration win there is what catches a next link
    the author pointed at the object *containing* it: the scheme is handed
    the dict the provider would send and refuses, rather than being handed a
    URL string the kit invented and succeeding.

    A path the schema names no type for gets a scheme-shaped value instead,
    so the traversal still runs; the contract (RULE-ENDP-023) refuses the
    document whose schema does not reach the path, which is the finding
    worth acting on.
    ``continuation`` overrides the continuation paths outright, which is how
    a drive arms the origin guard with a link the connector would never have
    declared.

    ``key`` seeds every scripted value, so the pages of one driven traversal
    differ from each other exactly where a provider's own consecutive pages
    do (:data:`_DRIVEN_PAGES`).

    The records land at the declared ``records.ref``, so a stop condition
    written against the records array sees the page the loop would hand it.
    """
    scheme = str((probe.pagination or {}).get("type", ""))
    schema = _response_schema(probe)
    payload: dict[str, Any] = {}
    for path in _body_paths(probe.pagination):
        sample = _sample_value(_declared_schema(schema, path), key=key)
        if sample is not _UNTYPED:
            _plant(payload, path, sample)
    for path in _continuation_paths(probe):
        if continuation is not _DECLARED:
            _plant(payload, path, continuation)
        elif declared_type(_declared_schema(schema, path)) is None:
            _plant(payload, path, _continuation_value(scheme, key))
    records_ref = ((probe.read.get("response") or {}).get("records") or {}).get("ref")
    try:
        records_path = split_records_ref(records_ref)
    except ReadError:
        # Reported by the record-schema check; the page is still drivable.
        records_path = []
    if records_path:
        _plant(payload, records_path, records)
    return Page(records=records, payload=payload or records)


def _continuation_value(scheme: str, key: int) -> Any:
    """Return what the page seeded with *key* hands *scheme* to continue from."""
    template = _CONTINUATION_TEMPLATES.get(scheme)
    return key if template is None else template.format(key=key)


def _sample_value(schema: Any, *, key: int) -> Any:
    """One value of the JSON type *schema* declares, or :data:`_UNTYPED`.

    Never ``None``: a record field the provider serves is a value, and
    ``None`` is the answer a field walk gives for a field that is not
    there, so the two must not be confused when a scheme asks a record for
    its ordering value. And never a guess -- a node declaring no type gets
    no sample, because the type it would have been given is exactly what
    the connector is being judged on.

    ``key`` carries into the value itself, scalar by scalar, so two pages
    scripted from two keys differ at every field either of them declares.
    """
    kind = declared_type(schema)
    if kind == "object":
        return _sample_object(schema, key=key)
    if kind == "array":
        item = _sample_value(schema.get("items"), key=key)
        return [] if item is _UNTYPED else [item]
    if kind in ("integer", "number"):
        return key
    if kind == "boolean":
        # The one type with no room for a second value. Nothing continues a
        # traversal from a boolean, so no drive reads one back.
        return True
    if kind == "string":
        return f"conformance-{key}"
    return _UNTYPED


def _sample_object(schema: Mapping[str, Any], *, key: int) -> dict[str, Any]:
    """Build an object carrying the properties *schema* declares a type for."""
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return {}
    sampled = {name: _sample_value(prop, key=key) for name, prop in properties.items()}
    return {name: value for name, value in sampled.items() if value is not _UNTYPED}


def _declared_record(probe: _ReadProbe, *, key: int) -> dict[str, Any] | None:
    """Build a record shaped like the endpoint's own declared record schema.

    ``None`` when that schema does not resolve, which the record-schema
    check reports on its own.
    """
    response = probe.read.get("response")
    if not isinstance(response, Mapping):
        return None
    try:
        return _sample_object(records_items_schema(probe.label, response), key=key)
    except ReadError:
        # The only failure ``records_items_schema`` raises: a ref that is
        # not anchored, does not resolve, or reaches something carrying no
        # records.
        return None


def _probe_records(
    probe: _ReadProbe, *, declared: bool = True, key: int = _PROBE_KEY_VALUE
) -> list[dict[str, Any]]:
    """Build the records the scripted page carries.

    Shaped like the endpoint's own record schema, plus the keyset ordering
    field. The field is planted rather than taken from the schema because
    the engine walks the *provider's* record: ``extract_records`` hands the
    strategy the raw response objects, so ordering by a field the provider
    sends and the schema does not declare reads perfectly well. Asserting
    otherwise would fail a working connector.

    ``key`` is the page's seed, and the ordering field carries it: a keyset
    traversal continues from the last record's value, so two pages whose
    records held the same one would read as a scheme going nowhere.

    ``declared=False`` builds records carrying nothing, which is how the
    keyset refusal is armed.
    """
    if not declared:
        return [{} for _ in range(_PROBE_RECORDS)]
    template = _declared_record(probe, key=key) or {}
    ordering = _keyset_field(probe)
    if ordering:
        _plant(template, ordering.split("."), key)
    return [dict(template) for _ in range(_PROBE_RECORDS)]


def _keyset_field(probe: _ReadProbe) -> str | None:
    """Return the keyset scheme's ordering field, ``None`` for the other four."""
    block = probe.pagination or {}
    if block.get("type") != "keyset":
        return None
    field = (block.get("keyset") or {}).get("order_by_field")
    return field if isinstance(field, str) and field else None


def check_api_read_compiles(target: ConformanceTarget) -> list[Violation]:
    """Certify that every read builds its first request.

    Resolving the declared params, placing the page size under the declared
    limit param and building the paging adapter are the whole of what the
    engine settles before it sends anything. A defect in any of them --
    an unknown ``pagination.type``, a step that cannot advance, a page size
    expression that resolves to nothing usable -- fails every page of the
    stream, so it is worth reporting from a definition alone.

    What the compiled request itself has to show is read back off it: a
    cursor scheme's continuation token must be *absent* from the first
    request. A param the loop owns that also carries a declared default
    is resolved into the table before the loop touches it, and for the four
    schemes that set their param on the first request the default is simply
    overwritten -- but a cursor sends no token on the first request, so
    there the stale default survives onto the wire.

    This one reads the param TABLE rather than the prepared request, and
    deliberately: the contract binds every declared param in exactly one
    map, so a value sitting in the table is a value that goes out -- under
    the wire name of whichever map binds it, which may be a header or a
    body field rather than a query key. A check phrased against the query
    string would go blind on those two.
    """
    probes, compile_violations = _probes(target)
    violations = list(compile_violations)
    for probe in probes:
        cursor_param = ((probe.pagination or {}).get("cursor") or {}).get("param")
        if cursor_param and cursor_param in probe.first.params:
            violations.append(
                Violation(
                    COMPILE_CHECK,
                    f"endpoint {probe.label!r}: the first request already "
                    f"carries the continuation token {cursor_param!r} "
                    f"({probe.first.params[cursor_param]!r}). There is nothing "
                    f"to continue from yet, so this asks the provider to "
                    f"resume from a position it never issued. The param is "
                    f"declared with a default the pagination loop then owns; "
                    f"mark it controlled_by 'pagination'.",
                )
            )
    return violations


def check_api_read_advances(target: ConformanceTarget) -> list[Violation]:
    """Certify that each read can work out the request after a page.

    ``advance`` runs before the loop yields, so what it answers decides
    whether a stream reads past its first page at all. Four things are
    driven:

    * the traversal must keep moving. Two pages go through one strategy and
      every request is compared with the one before it, the first request
      included; a scheme answering ``None`` while the page still carries a
      continuation reads one page and reports success
      (:func:`_advance_violations`);
    * a read declaring no pagination must answer ``None`` -- the single
      page is the whole stream, and a scheme that kept going would re-read
      it forever;
    * the request after a page must build. A body derived from the
      continuation binds nothing on page one and can still be unbuildable
      once the loop supplies a value, which no other drive reaches;
    * a keyset read must refuse a page whose last record carries no
      ordering value, and a link read handed a next URL on another host
      must either refuse it or resolve it back onto the connection's own
      origin. Both fire before the yield, which is what keeps them from
      landing records the read cannot continue past, and each is armed on a
      fresh traversal of its own.
    """
    probes, compile_violations = _probes(target)
    violations: list[Violation] = _undriven(ADVANCE_CHECK, compile_violations)
    for probe in probes:
        violations.extend(_advance_violations(probe))
        violations.extend(_refusal_violations(probe))
    return violations


def _advance_violations(probe: _ReadProbe) -> list[Violation]:
    """Drive a traversal page by page and report where it stopped moving.

    Two pages, through one strategy, because "the request after a page is
    the request before it" is the whole of what stops a read moving -- and
    which page shows it depends on the scheme. A pagination param that
    reaches no request binding shows on the first: the offset counts on and
    the built request does not change. A scheme that keeps no position of
    its own -- a cursor continuing from a literal, a link whose next URL
    comes from the connection rather than from the page -- builds a request
    that differs from page one's and then repeats *that* one forever, so
    nothing before the second page tells it from a read that moves.

    Driving it is what lets the invariant be stated once for all five
    schemes. A table naming which of them keeps a position of its own, and
    which field each of the others continues from, states the same thing
    about ``strategies.py`` from outside it, and is wrong the day a field is
    renamed.

    The pages differ everywhere a provider's own consecutive pages differ
    (:data:`_DRIVEN_PAGES`), which is what makes a repeated request mean
    something rather than being the same page served twice.
    """
    scheme = str((probe.pagination or {}).get("type", ""))
    strategy = probe.strategy()
    builder = _request_builder(probe)
    # What moves a traversal is what is SENT, and a param reaches the wire
    # only through a request binding that names it. Comparing the param
    # tables instead passes an endpoint that advances its table and binds
    # none of it -- one identical URL and query per page, forever -- as a
    # read that moves. The whole prepared request is compared because a
    # body-paginated read moves in its body while its URL and query stay
    # put, and that is still a read that moves.
    sent = (probe.first.url, probe.first_sent)
    for word, key in _DRIVEN_PAGES:
        page = _scripted_page(probe, records=_probe_records(probe, key=key), key=key)
        try:
            following = strategy.advance(page)
        except _ADVANCE_FAILURES as err:
            return [
                Violation(
                    ADVANCE_CHECK,
                    f"endpoint {probe.label!r}: advancing past the {word} page "
                    f"raised {err}. The read fails there having already been "
                    f"handed the page's records.",
                )
            ]
        if probe.pagination is None:
            if following is None:
                return []
            return [
                Violation(
                    ADVANCE_CHECK,
                    f"endpoint {probe.label!r}: the read declares no "
                    f"pagination, so its one page is the whole stream, but "
                    f"the traversal asked for {following.url!r} next.",
                )
            ]
        if following is None:
            return [
                Violation(
                    ADVANCE_CHECK,
                    f"endpoint {probe.label!r}: pagination.type {scheme!r} has "
                    f"nowhere to go after a page carrying a value at every "
                    f"response path it declares. This stream stops after the "
                    f"{word} page and reports success.",
                )
            ]
        try:
            prepared = builder.for_page(
                following.params, sends_declared_body=following.sends_declared_body
            )
        except RequestSpecError as err:
            return [
                Violation(
                    ADVANCE_CHECK,
                    f"endpoint {probe.label!r}: the request after the {word} "
                    f"page could not be built: {err}. The first request "
                    f"builds, so this read passes every check that stops there "
                    f"and then fails mid-traversal, with the records it has "
                    f"already handed over.",
                )
            ]
        if (following.url, prepared) == sent:
            return [
                Violation(
                    ADVANCE_CHECK,
                    f"endpoint {probe.label!r}: the request after the {word} "
                    f"page is the request before it again ({following.url!r}, "
                    f"query {prepared.query!r}, body {prepared.body!r}). The "
                    f"read would fetch that page forever. A pagination param "
                    f"moves the traversal only where request.query, "
                    f"request.headers or request.body binds it, and a scheme "
                    f"that continues from what the last page handed back moves "
                    f"only where its declared continuation reads a value off "
                    f"that page.",
                )
            ]
        sent = (following.url, prepared)
    return []


def _refusal_violations(probe: _ReadProbe) -> list[Violation]:
    """Arm the two rules that have to hold before a page is yielded."""
    scheme = str((probe.pagination or {}).get("type", ""))
    if scheme == "keyset":
        return _refuses(
            probe,
            _scripted_page(probe, records=_probe_records(probe, declared=False)),
            marker=_KEYSET_REFUSAL,
            expected=(
                f"keyset pagination continues from the last record's "
                f"{_keyset_field(probe)!r}, so a page without one has no next "
                f"request; accepting it would land records the read cannot "
                f"continue past"
            ),
        )
    if scheme == "link":
        return _origin_violations(probe)
    return []


def _origin_violations(probe: _ReadProbe) -> list[Violation]:
    """Plant an off-origin link and require the engine's own guard to hold.

    Every link-paginated read gets this drive, whatever shape the
    declaration is. What is asserted is the invariant rather than the
    mechanism: handed a next link on another host, the traversal either
    refuses it or answers a URL still on the connection's origin --
    ``cdk.api.urls.same_origin`` being the judge, the same function
    ``follow_url`` uses. A declaration that writes the URL around the
    provider's value (``{"template": "/v1/events?after=${...}"}``) lands in
    the second arm: the result is relative, resolves against the page it
    came from, and stays put. Classifying the declaration instead -- by
    whether it opens with a placeholder, say -- is the string-prefix
    reasoning ``follow_url`` exists to replace, and it leaves whole shapes
    with the guard never armed at all.
    """
    page = _scripted_page(
        probe, records=_probe_records(probe), continuation=_OFF_ORIGIN_URL
    )
    expected = (
        f"a next link on another host must be refused: the session sends the "
        f"connection's headers, credentials included, on every request, and "
        f"{probe.origin!r} is the only origin they belong to"
    )
    try:
        following = probe.strategy().advance(page)
    except _ADVANCE_FAILURES as err:
        if _ORIGIN_REFUSAL in str(err):
            return []
        return [
            Violation(
                ADVANCE_CHECK,
                f"endpoint {probe.label!r}: advancing raised {err}, which is "
                f"not the refusal this page arms. {expected}. Fix what it did "
                f"raise about first -- until then nothing certifies the "
                f"refusal itself.",
            )
        ]
    if following is None:
        # A declaration that cannot turn any provider value into a next
        # request is already reported by the advance drive; saying it twice
        # buries the message that says what to change.
        return []
    if same_origin(urlsplit(probe.origin), urlsplit(following.url)):
        return []
    return [
        Violation(
            ADVANCE_CHECK,
            f"endpoint {probe.label!r}: advancing answered {following.url!r}, "
            f"which is not on {probe.origin!r}. {expected}.",
        )
    ]


def _refuses(
    probe: _ReadProbe, page: Page, *, marker: str, expected: str
) -> list[Violation]:
    """Report when ``advance`` did not refuse a page for the stated reason.

    The refusal has to be *the* refusal. Reading "it raised something" as
    "it refused correctly" is how a connector whose next-page value has the
    wrong shape passes: the strategy raises about the shape, the kit counts
    that as the refusal firing, and the rule is never exercised at all. So
    the message is checked for the marker the CDK's own raise site carries.
    """
    try:
        following = probe.strategy().advance(page)
    except _ADVANCE_FAILURES as err:
        if marker in str(err):
            return []
        return [
            Violation(
                ADVANCE_CHECK,
                f"endpoint {probe.label!r}: advancing raised {err}, which is "
                f"not the refusal this page arms. {expected}. Fix what it did "
                f"raise about first -- until then nothing certifies the "
                f"refusal itself.",
            )
        ]
    return [
        Violation(
            ADVANCE_CHECK,
            f"endpoint {probe.label!r}: advancing answered "
            f"{following.url if following else None!r} instead of refusing. "
            f"{expected}.",
        )
    ]


def check_api_read_stop_condition(target: ConformanceTarget) -> list[Violation]:
    """Certify that each paginated read's ``stop_when`` decides against a page.

    The loop evaluates it before every yield, so it is the one declaration
    that ends a traversal on the provider's own signal. Two things are
    driven:

    * it evaluates. An operator this build cannot read, a malformed
      predicate, or a comparison between types that do not order raises
      mid-read -- after records have already been handed over;
    * it reads something the page carries. A condition referencing nothing
      under ``response`` answers the same thing on every page, so it either
      ends the stream at page one or never ends it, and no page the provider
      serves can change its mind.
    """
    probes, compile_violations = _probes(target)
    violations: list[Violation] = _undriven(STOP_CHECK, compile_violations)
    for probe in probes:
        if probe.pagination is None:
            continue
        declared = probe.pagination.get("stop_when")
        if declared is None:
            violations.append(
                Violation(
                    STOP_CHECK,
                    f"endpoint {probe.label!r}: pagination declares no "
                    f"stop_when. The loop then ends only on an empty page or "
                    f"on the scheme running out, and a provider that serves "
                    f"a last page without either is read forever.",
                )
            )
            continue
        violations.extend(_stop_condition_violations(probe, declared))
    return violations


def _stop_condition_violations(probe: _ReadProbe, declared: Any) -> list[Violation]:
    """Evaluate one declared stop condition against a scripted page."""
    violations: list[Violation] = []
    page = _scripted_page(probe, records=_probe_records(probe))
    if _operands_are_declared(probe, declared):
        try:
            stops = stop_condition(declared, probe.resolver)(page)
        except ReadError as err:
            violations.append(
                Violation(
                    STOP_CHECK,
                    f"endpoint {probe.label!r}: stop_when raised {err} against "
                    f"a page. The loop evaluates it before every yield, so the "
                    f"read fails there rather than ending.",
                )
            )
        else:
            violations.extend(_premature_stop(probe, declared, stops))
    if not any(lookup.startswith(_RESPONSE_PREFIX) for lookup in scope_paths(declared)):
        violations.append(
            Violation(
                STOP_CHECK,
                f"endpoint {probe.label!r}: stop_when {declared!r} reads "
                f"nothing under 'response', which is the only scope that "
                f"differs from page to page. Its verdict is the same on every "
                f"page, so it either ends the stream at page one or never "
                f"ends it.",
            )
        )
    return violations


def _premature_stop(probe: _ReadProbe, declared: Any, stops: bool) -> list[Violation]:
    """Report a condition that ends the traversal on a plainly-full page.

    The verdict itself has to be read, not just the fact that it evaluated:
    a condition written the wrong way round -- ``exists`` where the author
    meant ``missing`` -- holds on the first page the provider serves, and
    the stream stops there reporting success. Nothing else in the suite
    sees that, because ``advance`` is driven directly and never consults
    the loop's stopping rule.

    Asserted only when the condition reads something that says, without
    interpretation, that this page is not the last one: the value the
    scheme continues from, the records themselves, or how many there were.
    A condition on other envelope fields -- a page number against a page
    total, say -- is about the traversal's position, and a scripted page
    has no position to be right about. Guessing one would replace a real
    verdict with the kit's arithmetic.
    """
    if not stops:
        return []
    evidence = _non_terminal_paths(probe)
    # An ancestor of an evidence path is evidence too: the container that
    # HOLDS the continuation is populated exactly when its leaf is, so a
    # condition on `response.body.pagination` deciding to stop is deciding
    # about the same full page a condition on `...pagination.next` is.
    matched = {
        lookup
        for lookup in scope_paths(declared)
        if lookup in evidence or any(path.startswith(lookup + ".") for path in evidence)
    }
    if not matched:
        return []
    return [
        Violation(
            STOP_CHECK,
            f"endpoint {probe.label!r}: stop_when holds on a full page that "
            f"carries {_PROBE_RECORDS} records and the value the traversal "
            f"continues from. It reads "
            f"{', '.join(sorted(repr(item) for item in matched))}, "
            f"so it is deciding about this page rather than about the "
            f"provider running out -- and it decides to stop. In production "
            f"the read ends after its first page and reports success. Check "
            f"the condition is not written the wrong way round.",
        )
    ]


def _non_terminal_paths(probe: _ReadProbe) -> set[str]:
    """Return the lookups that say a scripted page is not the last one."""
    # The body itself counts: a full page's body is present and non-empty,
    # so a condition reading the whole payload and stopping is deciding
    # about this page.
    evidence = {f"{_RESPONSE_PREFIX}record_count", _BODY_PREFIX.rstrip(".")}
    for path in _continuation_paths(probe):
        evidence.add(_BODY_PREFIX + ".".join(path))
    records_ref = ((probe.read.get("response") or {}).get("records") or {}).get("ref")
    if isinstance(records_ref, str):
        evidence.add(records_ref)
    return evidence


def _operands_are_declared(probe: _ReadProbe, declared: Any) -> bool:
    """Whether every body operand the condition reads has a declared type.

    A scripted page can only carry the types the response schema names. For
    a path it names no type for the kit would have to invent one, and an
    invented type is exactly what decides whether an ordering comparison
    raises -- so a condition reading such a path is not evaluated here at
    all. Where the schema does not reach the path, the contract
    (RULE-ENDP-023) refuses the document, which is the actionable finding:
    declare the field, and the evaluation follows.
    """
    schema = _response_schema(probe)
    return all(
        declared_type(_declared_schema(schema, path)) is not None
        for path in _body_paths(declared)
    )


def check_api_record_schema(target: ConformanceTarget) -> list[Violation]:
    """Certify that each read's declared records become an Arrow schema.

    ``records.ref`` addresses the per-record schema inside the declared
    response schema, every field's JSON type resolves to an Arrow type
    through the connector's read type-map, and the result builds a
    :class:`~cdk.schema_contract.SchemaContract`. A ref naming a field the
    schema does not declare, or a JSON type the type-map has no rule for,
    fails the read on its first page.
    """
    violations: list[Violation] = []
    mapper = target.type_mapper
    for label, read in read_operations(target):
        response = read.get("response")
        if not isinstance(response, Mapping):
            violations.append(
                Violation(
                    RECORDS_CHECK,
                    f"endpoint {label!r}: operations.read declares no response "
                    f"block, so a read has nowhere to find its records.",
                )
            )
            continue
        try:
            items = deepcopy(records_items_schema(label, response))
            _resolve_arrow_types(items, mapper)
            SchemaContract(items)
        except _RECORD_FAILURES as err:
            violations.append(
                Violation(
                    RECORDS_CHECK,
                    f"endpoint {label!r}: the declared records do not build a "
                    f"record schema: {err}",
                )
            )
    return violations


def _resolve_arrow_types(items: dict[str, Any], mapper: TypeMapper | None) -> None:
    """Fill each record field's ``arrow_type``, as the read's own walk does.

    The engine picks the mapper by the stream's endpoint scope; a
    definition-only run has one mapper and no stream, so the loaded one is
    handed straight to the same per-field resolution. A connector shipping
    none is reported by the read-type-map check, and every field here then
    has to carry its own ``arrow_type``.
    """

    def get_mapper() -> TypeMapper:
        if mapper is None:
            raise ReadError(
                "a field needs arrow_type resolution but the connector ships "
                "no type-map-read.json"
            )
        return mapper

    for name, prop in (items.get("properties") or {}).items():
        if isinstance(prop, dict):
            resolve_field_arrow_type(prop, name, get_mapper)
