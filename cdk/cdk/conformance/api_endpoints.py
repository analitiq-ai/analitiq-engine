"""The endpoint documents, against the read the CDK's API path executes.

A read is materialized in three passes, each with its own scope set, and
an endpoint document can address the wrong one in any of them:

* **the request** — declared param defaults resolve, then the request body
  binds ``{"from_param": ...}`` nodes against those values and resolves
  what is left. Per-request resolution runs connector-side, where the
  secret store never is, so an ``{"ref": "secrets.token"}`` param default
  is silently dropped and every request goes out without it.
* **each page** — the strategy's ``stop_when`` predicate and its
  ``next_cursor`` / ``next_url`` / ``increment_by`` expressions resolve
  against the page's response scope, which carries the parsed body and the
  record count and nothing else. A ``stop_when`` on ``response.headers``
  never holds, so the loop runs until the provider runs out of pages.
* **the records** — ``response.records.ref`` addresses the record schema
  inside the declared response schema. A ref naming a field the schema
  does not declare fails the read on its first page.

Each check resolves the real declarations through the CDK's real
resolver, against the connection the connector's own ``connection_contract``
promises, and reports every path that phase does not carry.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from cdk.api_paging import PageValueError, positive_page_value
from cdk.api_response import (
    ResponseSchemaError,
    record_field_exists,
    records_items_schema,
    resolve_record_arrow_types,
)
from cdk.request_binding import bind_param_refs, resolve_param_defaults
from cdk.resolver import Resolver
from cdk.schema_contract import SchemaContract
from cdk.type_map import TypeMapper

from .declared_connection import (
    RESOLVE_FAILURES,
    declared_connection,
    guaranteed_connection,
    is_stand_in,
    page_probe,
    request_probe,
    stand_in_for,
    unsatisfied,
)
from .violations import Violation

if TYPE_CHECKING:
    from .target import ConformanceTarget

REQUEST_CHECK = "api-request-expressions"
PAGINATION_CHECK = "api-pagination"
RECORDS_CHECK = "api-response-records"

#: Authored paging values the contract types as ``Any``, so a value that is
#: not a positive integer reaches the engine and fails the read there.
#: ``limit.default`` sets the effective page size and ``page.increment_by``
#: the page-number step; ``offset.increment_by`` steps the offset.
_PAGE_VALUES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("limit.default", ("limit", "default")),
    ("page.increment_by", ("page", "increment_by")),
    ("offset.increment_by", ("offset", "increment_by")),
)

#: The paging fields the loop resolves ONCE, before the first request, so
#: nothing under ``response`` exists for them yet. Everything else resolves
#: per page — which is what lets ``offset.increment_by`` step by
#: ``response.record_count``.
_PRE_PAGE_FIELDS = frozenset({("limit", "default"), ("page", "increment_by")})

#: The paging fields whose non-resolution is not survivable. The engine
#: parses a step through ``_positive_step``, which rejects ``None``, and it
#: ends the loop the moment a cursor or link resolves to nothing — so a
#: read whose continuation depends on a value the user may leave blank
#: either fails outright or silently returns one page. Everything else in
#: the block is survivable: an unresolved ``limit.default`` falls back to
#: the engine's batch size with a warning, and a ``stop_when`` operand that
#: does not resolve makes the predicate false.
#:
#: Certified against the narrowest connection the contract admits, for the
#: same reason a strict http field is.
_CONTINUATION_FIELDS = frozenset(
    {
        ("offset", "increment_by"),
        ("page", "increment_by"),
        ("cursor", "next_cursor"),
        ("link", "next_url"),
    }
)

#: Every field that leaves the survivable per-page slice, and the containers
#: holding them, derived so the two tables above stay the only statement.
_SPLIT_FIELDS = _PRE_PAGE_FIELDS | _CONTINUATION_FIELDS
_SPLIT_CONTAINERS = frozenset(key for key, _name in _SPLIT_FIELDS)


def read_operations(target: ConformanceTarget) -> list[tuple[str, Mapping[str, Any]]]:
    """Every endpoint document's read operation, labelled for messages.

    The label is the document's own ``endpoint_id`` when it declares one,
    so a violation names what a stream's ``endpoint_ref`` names. Documents
    with no read operation are write-only and carry nothing this module
    certifies.
    """
    reads: list[tuple[str, Mapping[str, Any]]] = []
    for stem, document in sorted(target.endpoints.items()):
        operations = document.get("operations")
        read = operations.get("read") if isinstance(operations, Mapping) else None
        if isinstance(read, Mapping):
            label = document.get("endpoint_id")
            reads.append((str(label) if label else stem, read))
    return reads


def _mapper_accessor(mapper: TypeMapper) -> Callable[[], TypeMapper]:
    """Wrap an already-loaded mapper as the accessor the walk calls.

    :func:`~cdk.api_response.resolve_record_arrow_types` takes a callable
    because the engine resolves its mapper lazily — an endpoint that
    annotates every field needs none. The kit's mapper is loaded with the
    target, so there is nothing to defer.
    """
    return lambda: mapper


def _dig(block: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    """Return the value at *path* in *block*, or ``None`` if absent."""
    node: Any = block
    for key in path:
        if not isinstance(node, Mapping):
            return None
        node = node.get(key)
    return node


def check_api_request_expressions(target: ConformanceTarget) -> list[Violation]:
    """Certify that each read's request resolves from request-time scopes."""
    violations: list[Violation] = []
    for label, read in read_operations(target):
        resolver, asked = request_probe(target.definition)

        # Params the pagination and replication loops fill are left out
        # exactly as the connector leaves them out: their values come from
        # those loops, never from a declared default.
        declared = read.get("params")
        declared = declared if isinstance(declared, Mapping) else {}
        uncontrolled = {
            name: decl
            for name, decl in declared.items()
            if isinstance(decl, Mapping) and not decl.get("controlled_by")
        }
        values: dict[str, Any] = {}
        try:
            values = resolve_param_defaults(uncontrolled, resolver)
        except RESOLVE_FAILURES as err:
            violations.append(
                Violation(
                    REQUEST_CHECK,
                    f"endpoint {label!r}: a declared param default is not a "
                    f"resolvable value expression: {err}. The engine resolves "
                    f"every uncontrolled default through this exact call "
                    f"before the first request.",
                )
            )

        # The body binds against the per-page param table build_request
        # receives, so the probe seeds what the runtime guarantees is in it
        # and nothing else: the resolved defaults above, plus the params the
        # pagination and replication loops fill. A param the user may omit
        # stays absent, exactly as _build_base_params leaves it -- seeding it
        # would certify a body that binds None at runtime. Each stand-in
        # carries the param's declared type, since a strict expression around
        # it is type-sensitive.
        values.update(
            {
                name: stand_in_for(decl)
                for name, decl in declared.items()
                if isinstance(decl, Mapping)
                and decl.get("controlled_by")
                and name not in values
            }
        )

        request = read.get("request")
        body = request.get("body") if isinstance(request, Mapping) else None
        if body is not None:
            try:
                resolver.resolve_for_request(bind_param_refs(body, values))
            except RESOLVE_FAILURES as err:
                violations.append(
                    Violation(
                        REQUEST_CHECK,
                        f"endpoint {label!r}: request.body does not bind and "
                        f"resolve: {err}. Every page request rebuilds the body "
                        f"this way, so no request on this endpoint can be sent.",
                    )
                )

        violations.extend(
            Violation(
                REQUEST_CHECK,
                f"endpoint {label!r}: the request reads {entry.path!r}, which "
                f"is not in scope when a request is built ({entry.detail}). "
                f"Request-time resolution runs connector-side and sees only "
                f"connection.parameters, connection.selections, "
                f"connection.discovered and runtime -- a secret or auth value "
                f"has to reach the wire through the transport instead. An "
                f"expression that does not resolve omits its param or field, "
                f"so the request goes out without it.",
            )
            for entry in unsatisfied(asked)
        )
    return violations


@dataclass(frozen=True)
class _PagingPass:
    """One (fields, phase, strictness) slice of a pagination block."""

    block: dict[str, Any]
    pre_page: bool
    continuation: bool

    @property
    def when(self) -> str:
        """When the paging loop evaluates this slice."""
        return "before the first request" if self.pre_page else "against every page"

    @property
    def scopes(self) -> str:
        """What the slice may address, and what happens when it cannot."""
        available = (
            "the request-time scopes alone, since no page exists yet"
            if self.pre_page
            else "response.body and response.record_count on top of the "
            "request-time scopes"
        )
        consequence = (
            "The engine rejects a step it cannot parse and ends the loop the "
            "moment a cursor or link resolves to nothing, so this either fails "
            "the read or silently returns one page."
            if self.continuation
            else "An unresolved value here is survivable -- the page size falls "
            "back to the engine's batch size, a stop_when operand makes the "
            "predicate false -- but it is not what the connector declared."
        )
        return f"It resolves against {available}. {consequence}"


def _pagination_passes(pagination: Mapping[str, Any]) -> list[_PagingPass]:
    """Split a pagination block by when the loop reads each field, and how hard.

    Four slices, because two independent facts decide what a field may
    address and what its non-resolution costs: the phase it is read in
    (:data:`_PRE_PAGE_FIELDS`) and whether the read survives it not
    resolving (:data:`_CONTINUATION_FIELDS`).
    """
    slices: dict[tuple[bool, bool], dict[str, Any]] = {}

    def slot(pre_page: bool, continuation: bool) -> dict[str, Any]:
        return slices.setdefault((pre_page, continuation), {})

    for key, value in pagination.items():
        if key not in _SPLIT_CONTAINERS or not isinstance(value, Mapping):
            slot(False, False)[key] = value
            continue
        for name, field in value.items():
            pair = (key, name)
            if pair in _SPLIT_FIELDS:
                slot(pair in _PRE_PAGE_FIELDS, pair in _CONTINUATION_FIELDS)[
                    f"{key}.{name}"
                ] = field
            else:
                slot(False, False).setdefault(key, {})[name] = field

    return [
        _PagingPass(block=block, pre_page=pre_page, continuation=continuation)
        for (pre_page, continuation), block in slices.items()
        if block
    ]


def check_api_pagination(target: ConformanceTarget) -> list[Violation]:
    """Certify that each read's paging strategy resolves when the loop reads it."""
    violations: list[Violation] = []
    declared = declared_connection(target.definition)
    guaranteed = guaranteed_connection(target.definition)
    for label, read in read_operations(target):
        pagination = read.get("pagination")
        if not isinstance(pagination, Mapping):
            continue

        for paging_pass in _pagination_passes(pagination):
            # A continuation resolves against the narrowest connection the
            # contract admits: a user may leave an optional input blank, and
            # the loop cannot survive the value going missing.
            connection = guaranteed if paging_pass.continuation else declared
            probe = request_probe if paging_pass.pre_page else page_probe
            resolver, asked = probe(target.definition, connection)

            # One deep resolve per slice reaches every expression it holds --
            # including stop_when's operands -- without this module restating
            # the predicate grammar. Non-expression structure passes through.
            try:
                resolver.resolve_for_request(paging_pass.block)
            except RESOLVE_FAILURES as err:
                violations.append(
                    Violation(
                        PAGINATION_CHECK,
                        f"endpoint {label!r}: the pagination block holds an "
                        f"expression the resolver refuses: {err}. The paging "
                        f"loop evaluates it {paging_pass.when}, so the read "
                        f"fails there.",
                    )
                )

            violations.extend(
                Violation(
                    PAGINATION_CHECK,
                    f"endpoint {label!r}: the paging strategy reads "
                    f"{entry.path!r} {paging_pass.when} ({entry.detail}). "
                    f"{paging_pass.scopes}",
                )
                for entry in unsatisfied(asked)
            )

        # Over the widest connection: this judges an AUTHORED constant, and a
        # value the connection supplies is whatever the user configured.
        violations.extend(
            _page_value_violations(
                label,
                pagination,
                request_probe(target.definition, declared)[0],
                page_probe(target.definition, declared)[0],
            )
        )
        violations.extend(
            _next_url_violations(
                label, pagination, page_probe(target.definition, guaranteed)[0]
            )
        )
        violations.extend(_keyset_violations(label, read, pagination))
    return violations


def _next_url_violations(
    label: str, pagination: Mapping[str, Any], resolver: Resolver
) -> list[Violation]:
    """Report a link continuation that cannot resolve to a URL string.

    The link loop replays the resolved ``next_url`` verbatim and refuses
    anything that is not a string, so an authored constant of another type
    fails the read the moment a second page is due.
    """
    authored = _dig(pagination, ("link", "next_url"))
    if authored is None:
        return []
    value = authored
    if Resolver.is_expression_node(value):
        try:
            value = resolver.resolve_for_request(value)
        except RESOLVE_FAILURES:
            return []  # the resolve pass reports this with its own message
        if value is None or is_stand_in(value):
            return []
    if isinstance(value, str):
        return []
    return [
        Violation(
            PAGINATION_CHECK,
            f"endpoint {label!r}: link.next_url resolves to "
            f"{type(value).__name__}, and the loop replays it as a URL. The "
            f"read fails the moment a second page is due.",
        )
    ]


def _keyset_violations(
    label: str, read: Mapping[str, Any], pagination: Mapping[str, Any]
) -> list[Violation]:
    """Report a keyset ordering field the declared record does not carry.

    The keyset loop takes the ordering value from each page's last record
    before yielding it, and fails when the field is absent — so a schema
    that does not declare it fails every full page.
    """
    field = _dig(pagination, ("keyset", "order_by_field"))
    if not isinstance(field, str):
        return []
    response = read.get("response")
    records = response.get("records") if isinstance(response, Mapping) else None
    ref = records.get("ref") if isinstance(records, Mapping) else None
    schema = response.get("schema") if isinstance(response, Mapping) else None
    if not isinstance(ref, str) or not isinstance(schema, Mapping):
        return []
    try:
        record_schema = records_items_schema(ref, schema)
    except ResponseSchemaError:
        return []  # check_api_response_records reports the ref itself
    if record_field_exists(record_schema, field):
        return []
    return [
        Violation(
            PAGINATION_CHECK,
            f"endpoint {label!r}: keyset.order_by_field {field!r} is not a "
            f"field of the record the response schema declares at {ref!r}. "
            f"The loop reads it from each page's last record before yielding "
            f"the page, so every full page fails.",
        )
    ]


def _page_value_violations(
    label: str,
    pagination: Mapping[str, Any],
    request_resolver: Resolver,
    page_resolver: Resolver,
) -> list[Violation]:
    """Report authored paging values the engine will reject as a step.

    An expression is resolved first, through the resolver of the phase that
    reads it: ``{"literal": 0}`` is as knowable as a bare ``0``, and the
    loop rejects both. Only a value that depends on the connection or a
    response is unknowable here, and resolving it yields the probe's
    stand-in, which no positivity claim can be made about.
    """
    violations: list[Violation] = []
    for location, path in _PAGE_VALUES:
        value = _dig(pagination, path)
        if value is None:
            continue
        if Resolver.is_expression_node(value):
            resolver = request_resolver if path in _PRE_PAGE_FIELDS else page_resolver
            try:
                value = resolver.resolve_for_request(value)
            except RESOLVE_FAILURES:
                # The resolve pass above reports this with its own message.
                continue
            if value is None or is_stand_in(value):
                continue
        try:
            positive_page_value(value, context=location)
        except PageValueError as err:
            violations.append(
                Violation(
                    PAGINATION_CHECK,
                    f"endpoint {label!r}: {err}. The paging loop parses this "
                    f"value before the first request and fails the read.",
                )
            )
    return violations


def check_api_response_records(target: ConformanceTarget) -> list[Violation]:
    """Certify that each read's records ref addresses its declared record schema."""
    violations: list[Violation] = []
    # One mapper for the whole connector, exactly as the engine composes it
    # for a connector-scoped endpoint. A connector shipping no read type map
    # fails test_connector_ships_a_read_type_map; saying it again per
    # endpoint here would only bury that one.
    mapper = target.type_mapper
    for label, read in read_operations(target):
        response = read.get("response")
        if not isinstance(response, Mapping):
            continue
        records = response.get("records")
        ref = records.get("ref") if isinstance(records, Mapping) else None
        schema = response.get("schema")
        if not isinstance(ref, str) or not isinstance(schema, Mapping):
            # Structure the published contract requires; analitiq-validate
            # reports its absence with the offending JSON pointer.
            continue
        try:
            record_schema = records_items_schema(ref, schema)
        except ResponseSchemaError as err:
            violations.append(
                Violation(
                    RECORDS_CHECK,
                    f"endpoint {label!r}: {err}. The engine walks the declared "
                    f"response schema by this ref to build the record schema it "
                    f"emits, so the read fails before its first batch.",
                )
            )
            continue

        if mapper is None:
            continue
        try:
            # On the walk's own copy: the engine annotates the schema it is
            # about to build a batch from, and a check must not leave the
            # target's documents mutated for whatever runs next.
            resolved = deepcopy(record_schema)
            resolve_record_arrow_types(resolved, _mapper_accessor(mapper))
            # The engine hands that same annotated schema straight to
            # SchemaContract, which parses every Arrow type -- including the
            # ones the walk left alone because the field annotated its own.
            # An unparseable arrow_type raises there, before any request.
            SchemaContract(resolved)
        # RecordTypeError is a ValueError, so the walk's own failure and
        # SchemaContract's arrive through the same arm.
        except (ValueError, TypeError, KeyError) as err:
            violations.append(
                Violation(
                    RECORDS_CHECK,
                    f"endpoint {label!r}: {err}. The engine resolves every "
                    f"record field's Arrow type through the read type map and "
                    f"builds the record schema from the result, both before it "
                    f"sends a request -- so this read fails on every run. Add "
                    f"the missing type-map rule, or fix the field's declared "
                    f"arrow_type.",
                )
            )
    return violations
