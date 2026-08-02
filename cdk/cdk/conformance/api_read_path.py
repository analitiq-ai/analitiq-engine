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

* the page a scheme advances from is *scripted*, not fetched. Its records
  are built from the endpoint's own declared record schema -- which the
  engine does execute, since ``SchemaContract`` builds every batch from it
  -- so a scheme continuing from a record field finds its value exactly
  when the connector declared that field. Its envelope is built by
  planting a continuation value at each ``response.body.<path>`` the
  scheme references, and nothing more: outside the records path the
  response schema is documentation the read never consults, so treating it
  as the whole of what a provider sends would fail connectors that are
  correct.
* the origin the link guard is armed with is the default transport's
  literal ``base_url``, or a stand-in when the definition expresses it as
  a reference the connection document supplies. What the guard certifies
  -- that a link leaving the origin is refused before the records are
  yielded -- holds for either.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any

from cdk.api.page_loop import Page, PageRequest, PaginationStrategy
from cdk.api.predicates import evaluate_predicate
from cdk.api.records import page_resolver, split_records_ref
from cdk.api.request import ParamTable
from cdk.api.response_schema import records_items_schema, resolve_field_arrow_type
from cdk.api.strategies import Resolve, build_strategy, resolve_page_size
from cdk.api.urls import follow_url, join_url
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
    "check_api_read_advances",
    "check_api_read_compiles",
    "check_api_read_stop_condition",
    "check_api_record_schema",
]

COMPILE_CHECK = "api-read-compiles"
ADVANCE_CHECK = "api-read-advances"
STOP_CHECK = "api-read-stop-condition"
RECORDS_CHECK = "api-record-schema"

#: The failures a deterministic authoring defect surfaces as. The engine's
#: own read path catches the same set (``_RESOLUTION_FAILURES`` plus the
#: read error the schema walk raises); anything outside it is a defect in
#: the CDK rather than in the connector, and propagates.
_READ_FAILURES = (ReadError, TransportSpecError, ValueError, KeyError, TypeError)

#: The page size the probe reads with. Any positive integer works; a value
#: unlike the contract's own bounds makes it obvious in a message that the
#: number came from the kit rather than from the connector.
_PROBE_BATCH_SIZE = 37

#: How many records the scripted page carries.
_PROBE_RECORDS = 2

#: The value a scripted record carries for a declared field. Distinctive
#: on purpose: a keyset scheme advances to the last record's ordering
#: value, and a value that happened to equal the declared ``initial``
#: would read as a traversal that never moved.
_PROBE_KEY_VALUE = 9901

#: Stands in for a ``base_url`` the definition expresses as a reference.
_STAND_IN_ORIGIN = "https://conformance.invalid"

#: A next link on another host, for arming the origin guard.
_OFF_ORIGIN_URL = "https://elsewhere.invalid/page/2"

#: The scope prefix a scripted page can plant a value under.
_BODY_PREFIX = "response.body."

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
    a fresh traversal, so :meth:`strategy` builds a new adapter per drive.
    """

    label: str
    read: dict[str, Any]
    pagination: dict[str, Any] | None
    url: str
    origin: str
    base_params: dict[str, Any]
    resolve: Resolve
    first: PageRequest

    def strategy(self) -> PaginationStrategy:
        """Build a fresh adapter for this read, exactly as the engine does."""
        return _strategy_for(
            self.pagination,
            url=self.url,
            origin=self.origin,
            base_params=self.base_params,
            resolve=self.resolve,
        )


def _strategy_for(
    pagination: dict[str, Any] | None,
    *,
    url: str,
    origin: str,
    base_params: dict[str, Any],
    resolve: Resolve,
) -> PaginationStrategy:
    """Build a paging adapter the way ``_build_strategy`` does in the read.

    The one construction both the compile step and every per-page drive go
    through, so a drive can never probe a differently-built traversal from
    the one the compile step certified.
    """
    return build_strategy(
        pagination,
        url=url,
        base_params=base_params,
        resolve=resolve,
        follow_url=partial(follow_url, origin=origin),
    )


def _page_expression_resolver(resolver: Resolver) -> Resolve:
    """Adapt *resolver* to what a strategy asks of it, as the read does."""

    def resolve(expr: Any, page: Page | None) -> Any:
        return page_resolver(resolver, page).resolve_for_request(expr)

    return resolve


def _compile_read(target: ConformanceTarget, label: str, read: dict[str, Any]) -> _ReadProbe:
    """Compile one read to its first request, raising on any authoring defect.

    Mirrors ``GenericAPIConnector._plan_read`` in the order that matters:
    one resolver per read carrying the engine's page size, the param table
    built from it, the page size resolved and placed under the declared
    limit param, then the strategy. A second resolver built anywhere else
    would leave ``runtime.batch_size`` unresolvable and probe at the wrong
    page size, which is the defect this ordering exists to prevent.
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
    table = ParamTable.for_read(read.get("params") or {}, resolver)

    page_size = resolve_page_size(
        pagination, batch_size=_PROBE_BATCH_SIZE, resolve=resolver.resolve_for_request
    )
    limit = (pagination or {}).get("limit") or {}
    if limit.get("param"):
        table.values[limit["param"]] = page_size

    block = pagination if isinstance(pagination, dict) else None
    url = join_url(origin, path)
    resolve = _page_expression_resolver(resolver)
    strategy = _strategy_for(
        block, url=url, origin=origin, base_params=table.values, resolve=resolve
    )
    return _ReadProbe(
        label=label,
        read=read,
        pagination=block,
        url=url,
        origin=origin,
        base_params=table.values,
        resolve=resolve,
        first=strategy.first(),
    )


def _probes(target: ConformanceTarget) -> tuple[list[_ReadProbe], list[Violation]]:
    """Compile every read, splitting what compiled from what did not.

    The violations are returned by :func:`check_api_read_compiles` alone.
    Every other check drops them and skips the endpoint: a read that does
    not compile has no ``advance`` and no ``stop_when`` to drive, and
    reporting the same defect from four checks would bury the one message
    that says what to change.
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


def _declared_refs(node: Any) -> list[str]:
    """Every ``{"ref": "..."}`` string declared below *node*."""
    found: list[str] = []
    if isinstance(node, Mapping):
        ref = node.get("ref")
        if isinstance(ref, str):
            found.append(ref)
        for value in node.values():
            found.extend(_declared_refs(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_declared_refs(item))
    return found


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


def _scripted_page(probe: _ReadProbe, *, continuation: Any, records: list[dict[str, Any]]) -> Page:
    """Build the page a scheme advances from, with nothing fetched.

    A value is planted at every ``response.body`` path the pagination block
    references, so each declared expression has something to resolve. The
    records land at the declared ``records.ref`` too, so a ``stop_when``
    written against the records array sees the same page the loop would.
    """
    payload: dict[str, Any] = {}
    for ref in _declared_refs(probe.pagination):
        if ref.startswith(_BODY_PREFIX):
            _plant(payload, ref[len(_BODY_PREFIX) :].split("."), continuation)
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
    """An object carrying exactly the properties *schema* declares."""
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return {}
    return {name: _sample_value(prop) for name, prop in properties.items()}


def _declared_record(probe: _ReadProbe) -> dict[str, Any] | None:
    """A record shaped like the endpoint's own declared record schema.

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
    """The records the scripted page carries.

    Built from the endpoint's own record schema, so a scheme that continues
    from a record field -- keyset -- finds its ordering value exactly when
    the connector declared the field it orders by, and finds nothing when
    it did not. ``declared=False`` builds records carrying nothing, which
    is how the keyset refusal is armed.
    """
    if not declared:
        return [{} for _ in range(_PROBE_RECORDS)]
    template = _declared_record(probe)
    if template is None:
        # The record schema is already a reported violation. Plant the
        # ordering field so this drive reports what it is about rather than
        # the same defect a second time.
        template = {}
        ordering = _keyset_field(probe)
        if ordering:
            _plant(template, ordering.split("."), _PROBE_KEY_VALUE)
    return [dict(template) for _ in range(_PROBE_RECORDS)]


def _keyset_field(probe: _ReadProbe) -> str | None:
    """The keyset scheme's ordering field, or ``None`` for the other four."""
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

    What the compiled request itself has to show is read back off it: the
    declared path is in the URL, and a cursor scheme's continuation token
    is *absent*. A param the loop owns that also carries a declared default
    is resolved into the table before the loop touches it, and for the four
    schemes that set their param on the first request the default is simply
    overwritten -- but a cursor sends no token on the first request, so
    there the stale default survives onto the wire.
    """
    probes, violations = _probes(target)
    for probe in probes:
        declared_path = probe.read["request"]["path"]
        if declared_path.lstrip("/") not in probe.first.url:
            violations.append(
                Violation(
                    COMPILE_CHECK,
                    f"endpoint {probe.label!r}: the first request goes to "
                    f"{probe.first.url!r}, which does not carry the declared "
                    f"path {declared_path!r}.",
                )
            )
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
    probes, _compile_violations = _probes(target)
    violations: list[Violation] = []
    for probe in probes:
        violations.extend(_advance_violations(probe))
        violations.extend(_refusal_violations(probe))
    return violations


def _advance_violations(probe: _ReadProbe) -> list[Violation]:
    """Drive one page through ``advance`` and report what it answered."""
    scheme = str((probe.pagination or {}).get("type", ""))
    records = _probe_records(probe)
    page = _scripted_page(
        probe,
        continuation=_CONTINUATION_VALUES.get(scheme, 1),
        records=records,
    )
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
        page = _scripted_page(
            probe,
            continuation=_CONTINUATION_VALUES["keyset"],
            records=_probe_records(probe, declared=False),
        )
        return _refuses(
            probe,
            page,
            expected=(
                f"keyset pagination continues from the last record's "
                f"{_keyset_field(probe)!r}, so a page without one has no next "
                f"request; accepting it would land records the read cannot "
                f"continue past"
            ),
        )
    if scheme == "link":
        page = _scripted_page(
            probe, continuation=_OFF_ORIGIN_URL, records=_probe_records(probe)
        )
        return _refuses(
            probe,
            page,
            expected=(
                f"a next link on another host must be refused: the session "
                f"sends the connection's headers, credentials included, on "
                f"every request, and {probe.origin!r} is the only origin they "
                f"belong to"
            ),
        )
    return []


def _refuses(probe: _ReadProbe, page: Page, *, expected: str) -> list[Violation]:
    """Report when ``advance`` accepted a page it had to refuse."""
    try:
        following = probe.strategy().advance(page)
    except _READ_FAILURES:
        return []
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
    probes, _compile_violations = _probes(target)
    violations: list[Violation] = []
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
    scheme = str(probe.pagination.get("type", "")) if probe.pagination else ""
    page = _scripted_page(
        probe,
        continuation=_CONTINUATION_VALUES.get(scheme, 1),
        records=_probe_records(probe),
    )
    try:
        evaluate_predicate(declared, _stop_resolver(probe, page))
    except _READ_FAILURES as err:
        violations.append(
            Violation(
                STOP_CHECK,
                f"endpoint {probe.label!r}: stop_when raised {err} against a "
                f"page. The loop evaluates it before every yield, so the read "
                f"fails there rather than ending.",
            )
        )
    if not any(ref.startswith("response.") for ref in _declared_refs(declared)):
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


def _stop_resolver(probe: _ReadProbe, page: Page) -> Callable[[Any], Any]:
    """The one-argument resolve a predicate is evaluated through."""

    def resolve(expr: Any) -> Any:
        return probe.resolve(expr, page)

    return resolve


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
            items = records_items_schema(label, response)
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
