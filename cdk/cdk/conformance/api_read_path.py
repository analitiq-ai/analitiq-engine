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

Two substitutions are the kit's own, and both are named in the message
when they bite:

* the page a scheme advances from is *scripted*, not fetched. Its body
  carries, at every path the pagination block reads, a value of the type
  the endpoint's own response schema declares there -- so a comparison in
  ``stop_when`` sees the operand the connector said it would, and a next
  link the author pointed at the object containing it is handed that
  object. A path the schema does not declare is a finding in its own right
  (``check_api_page_references``) rather than a type the kit invents: an
  invented type is what decides whether an ordering comparison raises, so
  inventing one would make the verdict the kit's rather than the
  connector's. Its records are shaped the same way, plus the keyset
  ordering field, which is planted because the engine walks the provider's
  raw record and not the declared schema.
* the origin the link guard is armed with is the default transport's
  literal ``base_url``, or a stand-in when the definition expresses it as
  a reference the connection document supplies. What the guard certifies
  -- that a link leaving the origin is refused before the records are
  yielded -- holds for either.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any

from cdk.api.page_loop import Page, PageRequest, PaginationStrategy
from cdk.api.read_setup import build_read_strategy, stop_condition
from cdk.api.records import split_records_ref
from cdk.api.request import ParamTable, RequestBuilder
from cdk.api.response_schema import records_items_schema, resolve_field_arrow_type
from cdk.api.urls import join_url
from cdk.connection_runtime import ConnectionRuntime
from cdk.exceptions import ReadError, TransportSpecError
from cdk.resolver import Resolver
from cdk.schema_contract import SchemaContract
from cdk.type_map import TypeMapper

from .api_surface import api_base_url, read_operations
from .fakes import NoSecretsResolver
from .target import ConformanceTarget
from .violations import Violation

__all__ = [
    "check_api_page_references",
    "check_api_read_advances",
    "check_api_read_compiles",
    "check_api_read_stop_condition",
    "check_api_record_schema",
]

COMPILE_CHECK = "api-read-compiles"
REFERENCES_CHECK = "api-page-references"
ADVANCE_CHECK = "api-read-advances"
STOP_CHECK = "api-read-stop-condition"
RECORDS_CHECK = "api-record-schema"

#: The failures a deterministic authoring defect surfaces as: exactly what
#: the read itself catches (``ReadError`` plus the engine's own resolution
#: failures). Anything wider would relabel a CDK bug as a connector finding
#: -- ``TypeError`` in particular, which the read does not catch and the
#: worker may classify as retryable, so a check swallowing one here would
#: hide it from the only person able to fix it.
_READ_FAILURES = (ReadError, TransportSpecError, ValueError, KeyError)

#: The page size the probe reads with. Any positive integer works; a value
#: unlike the contract's own bounds makes it obvious in a message that the
#: number came from the kit rather than from the connector.
_PROBE_BATCH_SIZE = 37

#: How many records the scripted page carries: a full one. A stop condition
#: comparing the record count against the page size is asking "was this page
#: short", and a short probe page would answer "the stream ended here" for
#: every connector that asks.
_PROBE_RECORDS = _PROBE_BATCH_SIZE

#: The value a scripted record carries for a declared field. Distinctive
#: on purpose: a keyset scheme advances to the last record's ordering
#: value, and a value that happened to equal the declared ``initial``
#: would read as a traversal that never moved.
_PROBE_KEY_VALUE = 9901

#: Stands in for a ``base_url`` the definition expresses as a reference.
_STAND_IN_ORIGIN = "https://conformance.invalid"

#: A next link on another host, for arming the origin guard.
_OFF_ORIGIN_URL = "https://elsewhere.invalid/page/2"

#: The page scope, and the part of it a value can be planted under.
_RESPONSE_PREFIX = "response."
_BODY_PREFIX = "response.body."

#: What ``cdk.api.records.page_scope`` puts in the response scope. Nothing
#: else is in it, so a declared expression reading anything else resolves to
#: nothing on every page the provider serves.
_PAGE_SCOPE_KEYS = ("body", "record_count")

#: "take the type the response schema declares" -- distinct from any value
#: a drive could legitimately want planted, ``None`` included.
_DECLARED = object()

#: Scopes a connection document fills in. A definition-only run has none,
#: so an expression reading one resolves to nothing here for a reason that
#: says nothing about the connector.
_CONNECTION_SCOPES = ("connection.", "secrets.", "auth.")

#: What marks a mapping as a value expression rather than a plain object
#: (``Resolver._EXPR_KEYS``).
_EXPRESSION_KEYS = frozenset({"ref", "template", "literal", "function"})

#: The schemes that keep no position of their own, and the field each one
#: continues from. Offset and page count for themselves; these two hold
#: only what the provider last handed back.
_POSITIONLESS_SCHEMES = {"cursor": "next_cursor", "link": "next_url"}

#: Markers from the CDK's own raise sites, so a drive can tell the refusal
#: it armed from some other failure that happened to raise first.
_KEYSET_REFUSAL = "keyset.order_by_field"
_ORIGIN_REFUSAL = "leaves the connection's origin"

#: A continuation value shaped for the scheme that reads it: a cursor token
#: is opaque text, a next link is a relative URL (so it resolves against
#: the page it came from whatever the origin is), and a numeric step is a
#: whole number. Not a statement about how a scheme paginates -- only about
#: what type its declared field holds.
_CONTINUATION_VALUES: dict[str, Any] = {
    "cursor": "conformance-next-page-token",  # nosec B105 - not a credential
    "link": "?conformance-page=2",
    "offset": _PROBE_BATCH_SIZE,
    "page": _PROBE_BATCH_SIZE,
    "keyset": _PROBE_BATCH_SIZE,
}


@dataclass(frozen=True)
class _ReadProbe:
    """One endpoint's read, compiled as far as the first request.

    Holds the pieces rather than a built strategy because every scheme is
    stateful and single-use -- a check that drives one page must start from
    a fresh traversal, so :meth:`strategy` builds a new adapter per drive
    through the read's own setup.
    """

    label: str
    read: dict[str, Any]
    pagination: dict[str, Any] | None
    url: str
    origin: str
    table: ParamTable
    resolver: Resolver
    first: PageRequest

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
    engine's page size, the param table built from it, then
    :func:`~cdk.api.read_setup.build_read_strategy` -- the same function
    the read calls, so the page size lands where the read puts it and the
    origin guard is armed the way the read arms it. A second resolver built
    here would leave ``runtime.batch_size`` unresolvable and probe at the
    wrong page size.
    """
    runtime = ConnectionRuntime(
        raw_config={},
        connection_id="conformance-definition",
        connector_id=target.connector_id,
        connector_type=target.kind,
        resolver=NoSecretsResolver(),
        connector_definition=target.definition,
    )
    resolver = runtime.request_resolver(
        runtime_values={"batch_size": _PROBE_BATCH_SIZE}
    )
    request_block = read.get("request")
    if not isinstance(request_block, Mapping):
        raise ReadError("operations.read declares no request block")
    path = request_block.get("path")
    if not isinstance(path, str) or not path:
        raise ReadError("operations.read.request declares no path")

    origin = api_base_url(target) or _STAND_IN_ORIGIN
    pagination = read.get("pagination")
    probe = _ReadProbe(
        label=label,
        read=read,
        pagination=pagination if isinstance(pagination, dict) else None,
        url=join_url(origin, path),
        origin=origin,
        table=ParamTable.for_read(read.get("params") or {}, resolver),
        resolver=resolver,
        first=PageRequest(""),
    )
    first = probe.strategy().first()
    _materialize_first_request(probe, request_block, first)
    return replace(probe, first=first)


def _materialize_first_request(
    probe: _ReadProbe, request_block: Mapping[str, Any], first: PageRequest
) -> None:
    """Build the first request's query and body, as the fetch does.

    ``strategy.first()`` answers where the request goes; the read then runs
    ``RequestBuilder.for_page`` to turn the param table into what is
    actually sent. A body-paginated read whose declared body is malformed
    -- a bad ``from_param`` node, a page value too wide for a JSON number
    -- gets that far and no further, so stopping at the ``PageRequest``
    would certify a read that cannot issue its first request.

    Skipped only when the body is *itself* one expression reading a scope a
    connection supplies. That one resolves to nothing here and is refused
    for the single reason that says nothing about the connector. A
    connection-scoped expression nested inside the body is not skipped:
    request-time resolution omits an unresolved field rather than failing,
    so the rest of the body still binds -- and a malformed branch beside it
    still has to be caught.
    """
    body = request_block.get("body")
    if body is None or _is_connection_expression(body):
        return
    RequestBuilder(
        probe.table,
        raw_body=body,
        resolver=probe.resolver,
        endpoint=str(request_block.get("path")),
    ).for_page(first.params)


def _is_connection_expression(node: Any) -> bool:
    """Whether *node* is one expression reading a scope only a connection has.

    Deliberately not "reads one anywhere": a nested unresolved expression
    omits its own field and the body still builds, so skipping the whole
    materialization over one would hide every other defect beside it.
    """
    if not isinstance(node, Mapping) or not _EXPRESSION_KEYS & set(node):
        return False
    return any(
        lookup.startswith(_CONNECTION_SCOPES) for lookup in _declared_lookups(node)
    )


def _probes(target: ConformanceTarget) -> tuple[list[_ReadProbe], list[Violation]]:
    """Compile every read, splitting what compiled from what did not.

    A read that does not compile has no ``advance`` and no ``stop_when`` to
    drive. Only :func:`check_api_read_compiles` reports why -- repeating
    the same defect from four checks would bury the one message that says
    what to change -- but every other check still says it did not drive
    that endpoint (:func:`_undriven`). Each check is exported on its own
    and a repo may wire one into a harness of its own, so "returned
    nothing" must never be how a check reports "ran against nothing".
    """
    probes: list[_ReadProbe] = []
    violations: list[Violation] = []
    for label, read in read_operations(target):
        try:
            probes.append(_compile_read(target, label, read))
        except _READ_FAILURES as err:
            violations.append(
                Violation(
                    COMPILE_CHECK,
                    f"endpoint {label!r}: the read does not compile into a "
                    f"first request: {err}. Every page this endpoint would "
                    f"serve fails here, before anything is sent.",
                )
            )
    return probes, violations


def _undriven(check: str, violations: list[Violation]) -> list[Violation]:
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


def _declared_lookups(node: Any) -> list[str]:
    """Every scope path *node* reads, in either spelling that reads one.

    The contract's value expressions are ``literal``, ``ref``, ``template``
    and ``function``. Two of them address a scope: ``ref`` names one path,
    and ``template`` names any number inside ``${...}`` placeholders. A
    ``function`` node's inputs are themselves expressions, so the walk
    reaches those by recursion.

    Reading only ``ref`` would leave a template-spelled next link invisible
    -- and an invisible read is worse than an unread one here, because the
    kit would then script a page missing the value and blame the connector
    for not advancing past it.
    """
    found: list[str] = []
    if isinstance(node, Mapping):
        if "literal" in node:
            # Opaque data, whatever it looks like. The resolver hands a
            # literal back untouched, so a ref or a placeholder inside one
            # reads nothing -- and counting it as a read would certify a
            # stop condition that sees the same constant on every page.
            return found
        ref = node.get("ref")
        if isinstance(ref, str):
            found.append(ref)
        template = node.get("template")
        if isinstance(template, str):
            found.extend(_template_lookups(template))
        for value in node.values():
            found.extend(_declared_lookups(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_declared_lookups(item))
    return found


def _template_lookups(template: str) -> list[str]:
    """Return the scope paths a ``${...}`` template substitutes.

    Deliberately forgiving where ``Resolver._resolve_template`` is strict:
    an unterminated placeholder is an authoring defect the resolver raises
    on, and re-raising it from a scripting helper would replace that
    message with a worse one.
    """
    found: list[str] = []
    rest = template
    while True:
        start = rest.find("${")
        if start < 0:
            return found
        end = rest.find("}", start + 2)
        if end < 0:
            return found
        found.append(rest[start + 2 : end])
        rest = rest[end + 1 :]


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
        for lookup in _declared_lookups(node)
        if lookup.startswith(_BODY_PREFIX)
    ]


def _response_schema(probe: _ReadProbe) -> Any:
    """Return the endpoint's declared response schema, or ``None``."""
    response = probe.read.get("response")
    return response.get("schema") if isinstance(response, Mapping) else None


def _schema_at(schema: Any, path: list[str]) -> Any | None:
    """Return the declared sub-schema at *path*, or ``None`` for none."""
    node = schema
    for key in path:
        properties = node.get("properties") if isinstance(node, Mapping) else None
        if not isinstance(properties, Mapping) or key not in properties:
            return None
        node = properties[key]
    return node


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
) -> Page:
    """Build the page a scheme advances from, with nothing fetched.

    Every body path the pagination block reads gets a value of the type the
    endpoint's own response schema declares for it -- the continuation paths
    included. Letting the declaration win there is what catches a next link
    the author pointed at the object *containing* it: the scheme is handed
    the dict the provider would send and refuses, rather than being handed a
    URL string the kit invented and succeeding.

    A path the schema does not declare gets a scheme-shaped value instead,
    so the traversal still runs; the reference check reports that path,
    which is the finding worth acting on. ``continuation`` overrides the
    continuation paths outright, which is how a drive arms the origin guard
    with a link the connector would never have declared.

    The records land at the declared ``records.ref``, so a stop condition
    written against the records array sees the page the loop would hand it.
    """
    scheme = str((probe.pagination or {}).get("type", ""))
    schema = _response_schema(probe)
    payload: dict[str, Any] = {}
    for path in _body_paths(probe.pagination):
        declared = _schema_at(schema, path)
        if declared is not None:
            _plant(payload, path, _sample_value(declared))
    for path in _continuation_paths(probe):
        if continuation is not _DECLARED:
            _plant(payload, path, continuation)
        elif _schema_at(schema, path) is None:
            _plant(payload, path, _CONTINUATION_VALUES.get(scheme, 1))
    records_ref = ((probe.read.get("response") or {}).get("records") or {}).get("ref")
    try:
        records_path = split_records_ref(records_ref)
    except ReadError:
        # Reported by the record-schema check; the page is still drivable.
        records_path = []
    if records_path:
        _plant(payload, records_path, records)
    return Page(records=records, payload=payload or records)


def _sample_value(schema: Any) -> Any:
    """One value of the JSON type *schema* declares, never ``None``.

    A record field the provider serves is a value; ``None`` is the answer a
    field walk gives for a field that is not there, so the two must not be
    confused when a scheme asks a record for its ordering value.
    """
    if not isinstance(schema, Mapping):
        return _PROBE_KEY_VALUE
    declared = schema.get("type")
    if isinstance(declared, list):
        declared = next((t for t in declared if t != "null"), None)
    if declared == "object":
        return _sample_object(schema)
    if declared == "array":
        return [_sample_value(schema.get("items"))]
    if declared in ("integer", "number"):
        return _PROBE_KEY_VALUE
    if declared == "boolean":
        return True
    return f"conformance-{_PROBE_KEY_VALUE}"


def _sample_object(schema: Mapping[str, Any]) -> dict[str, Any]:
    """Build an object carrying exactly the properties *schema* declares."""
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return {}
    return {name: _sample_value(prop) for name, prop in properties.items()}


def _declared_record(probe: _ReadProbe) -> dict[str, Any] | None:
    """Build a record shaped like the endpoint's own declared record schema.

    ``None`` when that schema does not resolve, which the record-schema
    check reports on its own.
    """
    response = probe.read.get("response")
    if not isinstance(response, Mapping):
        return None
    try:
        return _sample_object(records_items_schema(probe.label, response))
    except _READ_FAILURES:
        return None


def _probe_records(probe: _ReadProbe, *, declared: bool = True) -> list[dict[str, Any]]:
    """Build the records the scripted page carries.

    Shaped like the endpoint's own record schema, plus the keyset ordering
    field. The field is planted rather than taken from the schema because
    the engine walks the *provider's* record: ``extract_records`` hands the
    strategy the raw response objects, so ordering by a field the provider
    sends and the schema does not declare reads perfectly well. Asserting
    otherwise would fail a working connector.

    ``declared=False`` builds records carrying nothing, which is how the
    keyset refusal is armed.
    """
    if not declared:
        return [{} for _ in range(_PROBE_RECORDS)]
    template = _declared_record(probe) or {}
    ordering = _keyset_field(probe)
    if ordering:
        _plant(template, ordering.split("."), _PROBE_KEY_VALUE)
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
    """
    probes, violations = _probes(target)
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


def check_api_page_references(target: ConformanceTarget) -> list[Violation]:
    """Certify that every page value a read declares is one a page carries.

    The other drives script a page from these declarations, so on their own
    they can never find a reference that addresses nothing -- the page is
    built to satisfy whatever the author wrote. This is the check that reads
    the declarations against something independent: the scope a page
    actually has, and the response schema the connector itself published.

    Two ways a reference reaches for what is not there:

    * a scope the page has no notion of. ``page_scope`` builds ``body`` and
      ``record_count``; a condition on ``response.headers`` resolves to
      nothing on every page ever served;
    * a body field the endpoint's own response schema does not declare. The
      schema is what ``records.ref`` is already resolved against, so a
      pagination value pointing outside it is either a typo or a field the
      connector forgot to declare -- and both read as absent, forever.

    Absent is not neutral. A ``missing`` or ``empty`` condition on it holds
    on page one and the stream stops there reporting success; an ``exists``
    condition never holds and the read runs to exhaustion; a next cursor or
    link resolves to nothing and the traversal ends after one page.
    """
    probes, compile_violations = _probes(target)
    violations: list[Violation] = _undriven(REFERENCES_CHECK, compile_violations)
    for probe in probes:
        if probe.pagination is None:
            continue
        violations.extend(_reference_violations(probe))
        violations.extend(_positionless_scheme_violations(probe))
    return violations


def _positionless_scheme_violations(probe: _ReadProbe) -> list[Violation]:
    """Report a scheme that continues from nothing the provider supplies.

    Two of the five carry no position of their own. ``_Offset`` counts rows
    and ``_Page`` counts pages, so a constant step is exactly right for
    them; ``_Cursor`` and ``_Link`` hold only what the last page handed
    back, so a continuation that reads nothing under ``response`` sends the
    same token or the same URL on every request. The provider then answers
    the same page forever and the loop's only escape is the author's stop
    condition -- which, reading a page that never changes, also answers the
    same thing forever.
    """
    block = probe.pagination or {}
    scheme = str(block.get("type", ""))
    if scheme not in _POSITIONLESS_SCHEMES:
        return []
    field = _POSITIONLESS_SCHEMES[scheme]
    declared = (block.get(scheme) or {}).get(field)
    if any(
        lookup.startswith(_RESPONSE_PREFIX) for lookup in _declared_lookups(declared)
    ):
        return []
    return [
        Violation(
            REFERENCES_CHECK,
            f"endpoint {probe.label!r}: {scheme}.{field} is {declared!r}, "
            f"which reads nothing under 'response'. This scheme keeps no "
            f"position of its own -- it continues from what the last page "
            f"handed back -- so every request after the first is identical "
            f"to the one before it, and the read never moves past page two.",
        )
    ]


def _reference_violations(probe: _ReadProbe) -> list[Violation]:
    """Report every page reference this endpoint's pagination cannot resolve."""
    violations: list[Violation] = []
    schema = _response_schema(probe)
    for lookup in dict.fromkeys(_declared_lookups(probe.pagination)):
        if not lookup.startswith(_RESPONSE_PREFIX):
            continue
        scope = lookup[len(_RESPONSE_PREFIX) :].split(".")[0]
        if scope not in _PAGE_SCOPE_KEYS:
            violations.append(
                Violation(
                    REFERENCES_CHECK,
                    f"endpoint {probe.label!r}: pagination reads {lookup!r}, "
                    f"but a page carries only "
                    f"{', '.join(repr(k) for k in _PAGE_SCOPE_KEYS)} under "
                    f"'response'. This resolves to nothing on every page.",
                )
            )
            continue
        if not lookup.startswith(_BODY_PREFIX):
            continue
        path = lookup[len(_BODY_PREFIX) :].split(".")
        if schema is None or _schema_at(schema, path) is None:
            violations.append(
                Violation(
                    REFERENCES_CHECK,
                    f"endpoint {probe.label!r}: pagination reads {lookup!r}, "
                    f"which the declared response schema does not reach. "
                    f"Either the path is a typo or the field is one the "
                    f"schema does not declare; either way it resolves to "
                    f"nothing on every page.",
                )
            )
    return violations


def check_api_read_advances(target: ConformanceTarget) -> list[Violation]:
    """Certify that each read can work out the request after a page.

    ``advance`` runs before the loop yields, so what it answers decides
    whether a stream reads past its first page at all. Three things are
    driven, each on a fresh traversal:

    * a page carrying a continuation value must produce a request that
      differs from the first one. ``None`` there is a stream that reads one
      page and reports success;
    * a read declaring no pagination must answer ``None`` -- the single
      page is the whole stream, and a scheme that kept going would re-read
      it forever;
    * a keyset read must refuse a page whose last record carries no
      ordering value, and a link read must refuse a next URL on another
      host. Both refusals fire before the yield, which is what keeps them
      from landing records the read cannot continue past.
    """
    probes, compile_violations = _probes(target)
    violations: list[Violation] = _undriven(ADVANCE_CHECK, compile_violations)
    for probe in probes:
        violations.extend(_advance_violations(probe))
        violations.extend(_refusal_violations(probe))
    return violations


def _advance_violations(probe: _ReadProbe) -> list[Violation]:
    """Drive one page through ``advance`` and report what it answered."""
    scheme = str((probe.pagination or {}).get("type", ""))
    records = _probe_records(probe)
    page = _scripted_page(probe, records=records)
    try:
        following = probe.strategy().advance(page)
    except _READ_FAILURES as err:
        return [
            Violation(
                ADVANCE_CHECK,
                f"endpoint {probe.label!r}: advancing past a page raised "
                f"{err}. The read fails there having already been handed the "
                f"page's records.",
            )
        ]
    if probe.pagination is None:
        if following is None:
            return []
        return [
            Violation(
                ADVANCE_CHECK,
                f"endpoint {probe.label!r}: the read declares no pagination, "
                f"so its one page is the whole stream, but the traversal "
                f"asked for {following.url!r} next.",
            )
        ]
    if following is None:
        return [
            Violation(
                ADVANCE_CHECK,
                f"endpoint {probe.label!r}: pagination.type {scheme!r} has "
                f"nowhere to go after a page carrying a value at every "
                f"response path it declares. This stream reads its first "
                f"page and reports success.",
            )
        ]
    if (following.url, following.params) == (probe.first.url, probe.first.params):
        return [
            Violation(
                ADVANCE_CHECK,
                f"endpoint {probe.label!r}: the request after a page is the "
                f"first request again ({following.url!r}, "
                f"{sorted(following.params)}). The read would fetch one page "
                f"forever.",
            )
        ]
    return []


def _refusal_violations(probe: _ReadProbe) -> list[Violation]:
    """Arm the two refusals that have to fire before a page is yielded."""
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
    if scheme == "link" and _response_controls_the_url(probe):
        return _refuses(
            probe,
            _scripted_page(
                probe, records=_probe_records(probe), continuation=_OFF_ORIGIN_URL
            ),
            marker=_ORIGIN_REFUSAL,
            expected=(
                f"a next link on another host must be refused: the session "
                f"sends the connection's headers, credentials included, on "
                f"every request, and {probe.origin!r} is the only origin they "
                f"belong to"
            ),
        )
    return []


def _response_controls_the_url(probe: _ReadProbe) -> bool:
    """Whether the provider's value becomes the whole of the next URL.

    Only then is there an origin to leave. Where the author writes the URL
    around the value -- ``{"template": "/v1/events?after=${...}"}`` -- what
    the provider supplies is a query fragment on a path the connector chose,
    and the result is relative whatever the provider puts in it. Arming the
    guard there would report a violation for a connector that never could
    send its credentials elsewhere.
    """
    declared = ((probe.pagination or {}).get("link") or {}).get("next_url")
    if not isinstance(declared, Mapping):
        return False
    if isinstance(declared.get("ref"), str):
        return True
    template = declared.get("template")
    if not isinstance(template, str):
        # A function, or something this build does not recognise. Its result
        # is unconstrained, so treat it as controlling the URL.
        return "literal" not in declared
    # The scheme and host come from whatever sits at the front. A template
    # opening with a placeholder hands the provider the origin however much
    # the author appends after it -- ``${...}&limit=50`` is as off-origin as
    # a bare ref. One opening with the author's own text keeps the origin
    # whatever is substituted later on.
    return template.startswith("${")


def _refuses(
    probe: _ReadProbe, page: Page, *, marker: str, expected: str
) -> list[Violation]:
    """Report when ``advance`` did not refuse a page for the stated reason.

    The refusal has to be *the* refusal. Reading "it raised something" as
    "it refused correctly" is how a connector whose next-page value has the
    wrong shape passes: the strategy raises about the shape, the kit counts
    that as the origin guard firing, and the guard is never exercised at
    all. So the message is checked for the marker the CDK's own raise site
    carries.
    """
    try:
        following = probe.strategy().advance(page)
    except _READ_FAILURES as err:
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
        except _READ_FAILURES as err:
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
    if not any(
        lookup.startswith(_RESPONSE_PREFIX) for lookup in _declared_lookups(declared)
    ):
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
    lookups = set(_declared_lookups(declared))
    if not lookups & evidence:
        return []
    return [
        Violation(
            STOP_CHECK,
            f"endpoint {probe.label!r}: stop_when holds on a full page that "
            f"carries {_PROBE_RECORDS} records and the value the traversal "
            f"continues from. It reads "
            f"{', '.join(sorted(repr(item) for item in lookups & evidence))}, "
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
    a path it does not name the kit has to invent one, and an invented type
    is exactly what decides whether an ordering comparison raises -- so a
    condition reading an undeclared path is not evaluated here at all. The
    reference check reports that path instead, which is the actionable
    finding: declare the field, and the evaluation follows.
    """
    schema = _response_schema(probe)
    return all(_schema_at(schema, path) is not None for path in _body_paths(declared))


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
        except _READ_FAILURES as err:
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
