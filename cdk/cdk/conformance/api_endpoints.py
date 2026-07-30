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
from typing import TYPE_CHECKING, Any

from cdk.api_paging import PageValueError, positive_page_value
from cdk.api_response import (
    RecordTypeError,
    ResponseSchemaError,
    records_items_schema,
    resolve_record_arrow_types,
)
from cdk.request_binding import bind_param_refs, resolve_param_defaults
from cdk.resolver import Resolver
from cdk.type_map import TypeMapper

from .declared_connection import (
    RESOLVE_FAILURES,
    STAND_IN,
    page_probe,
    request_probe,
    unsatisfied,
)
from .violations import Violation

if TYPE_CHECKING:
    from .target import ConformanceTarget

REQUEST_CHECK = "api-request-expressions"
PAGINATION_CHECK = "api-pagination"
RECORDS_CHECK = "api-response-records"

#: Authored paging values the contract types as ``Any``, so a value that is
#: not a positive integer reaches the engine and fails the read there — and
#: the phase the loop reads each in, since that decides which scopes its
#: expression may address.
#:
#: ``limit.default`` sets the effective page size and ``page.increment_by``
#: the page-number step; both are resolved ONCE, before the first request,
#: when no response exists. ``offset.increment_by`` is resolved per page,
#: which is what lets it step by ``response.record_count``.
_PAGE_VALUES: tuple[tuple[str, tuple[str, ...], bool], ...] = (
    ("limit.default", ("limit", "default"), True),
    ("page.increment_by", ("page", "increment_by"), True),
    ("offset.increment_by", ("offset", "increment_by"), False),
)

#: The same pre-page fields, as the container/field pairs that split the
#: block into its two resolution phases.
_PRE_PAGE_FIELDS = frozenset(path for _loc, path, pre in _PAGE_VALUES if pre)
_PRE_PAGE_CONTAINERS = frozenset(key for key, _name in _PRE_PAGE_FIELDS)


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

        # The body binds against the FULL per-page param table, not just the
        # defaults: the pagination and replication loops have filled their
        # params by the time build_request runs. Standing them in keeps a
        # body that legitimately reads a controlled param from binding None
        # and being reported as a defect it is not.
        values.update({name: STAND_IN for name in declared if name not in values})

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


def _pagination_phases(
    pagination: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split a pagination block into what resolves before, and per, a page.

    The loop reads the block in two moments. The effective page size and
    the page-number step are resolved ONCE, before the first request, so
    nothing under ``response`` exists for them yet. Everything else --
    ``stop_when``, ``next_cursor``, ``next_url``, ``offset.increment_by``
    -- is resolved per page against that page's response.
    """
    pre_page: dict[str, Any] = {}
    for location, path, is_pre_page in _PAGE_VALUES:
        value = _dig(pagination, path) if is_pre_page else None
        if value is not None:
            pre_page[location] = value

    per_page: dict[str, Any] = {}
    for key, value in pagination.items():
        if key not in _PRE_PAGE_CONTAINERS:
            per_page[key] = value
        elif isinstance(value, Mapping):
            per_page[key] = {
                name: field
                for name, field in value.items()
                if (key, name) not in _PRE_PAGE_FIELDS
            }
    return pre_page, per_page


def check_api_pagination(target: ConformanceTarget) -> list[Violation]:
    """Certify that each read's paging strategy resolves when the loop reads it."""
    violations: list[Violation] = []
    for label, read in read_operations(target):
        pagination = read.get("pagination")
        if not isinstance(pagination, Mapping):
            continue

        pre_page, per_page = _pagination_phases(pagination)
        request_resolver, request_asked = request_probe(target.definition)
        page_resolver, page_asked = page_probe(target.definition)

        # One deep resolve per phase reaches every expression that phase
        # evaluates -- including stop_when's operands -- without this module
        # restating the predicate grammar. Non-expression structure passes
        # through.
        for block, resolver, when in (
            (pre_page, request_resolver, "before the first request"),
            (per_page, page_resolver, "against every page"),
        ):
            try:
                resolver.resolve_for_request(block)
            except RESOLVE_FAILURES as err:
                violations.append(
                    Violation(
                        PAGINATION_CHECK,
                        f"endpoint {label!r}: the pagination block holds an "
                        f"expression the resolver refuses: {err}. The paging "
                        f"loop evaluates it {when}, so the read fails there.",
                    )
                )

        violations.extend(
            _page_value_violations(label, pagination, request_resolver, page_resolver)
        )
        violations.extend(
            Violation(
                PAGINATION_CHECK,
                f"endpoint {label!r}: the paging strategy reads {entry.path!r} "
                f"before its first request, when no page exists ({entry.detail}). "
                f"The effective page size and the page-number step are resolved "
                f"once, from the request-time scopes alone -- only the per-page "
                f"expressions (stop_when, next_cursor, next_url, "
                f"offset.increment_by) see a response.",
            )
            for entry in unsatisfied(request_asked)
        )
        violations.extend(
            Violation(
                PAGINATION_CHECK,
                f"endpoint {label!r}: the paging strategy reads {entry.path!r}, "
                f"which a page does not carry ({entry.detail}). A page resolves "
                f"against response.body (the parsed payload) and "
                f"response.record_count, plus the request-time scopes. A "
                f"stop_when operand that never resolves makes the predicate "
                f"false on every page, and a next_cursor or next_url that never "
                f"resolves stops the read after one.",
            )
            for entry in unsatisfied(page_asked)
        )
    return violations


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
    for location, path, is_pre_page in _PAGE_VALUES:
        value = _dig(pagination, path)
        if value is None:
            continue
        if Resolver.is_expression_node(value):
            resolver = request_resolver if is_pre_page else page_resolver
            try:
                value = resolver.resolve_for_request(value)
            except RESOLVE_FAILURES:
                # The resolve pass above reports this with its own message.
                continue
            if value is None or value == STAND_IN:
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
            resolve_record_arrow_types(
                deepcopy(record_schema), _mapper_accessor(mapper)
            )
        except RecordTypeError as err:
            violations.append(
                Violation(
                    RECORDS_CHECK,
                    f"endpoint {label!r}: {err}. The engine resolves every "
                    f"record field's Arrow type through the read type map "
                    f"before it sends a request, so this read fails on every "
                    f"run -- add the rule, or annotate the field with an "
                    f"explicit arrow_type.",
                )
            )
    return violations
