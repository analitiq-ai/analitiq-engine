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

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from cdk.api_paging import PageValueError, positive_page_value
from cdk.api_response import ResponseSchemaError, records_items_schema
from cdk.exceptions import TransportSpecError
from cdk.request_binding import bind_param_refs, resolve_param_defaults
from cdk.resolver import Resolver

from .declared_connection import page_probe, request_probe, unsatisfied
from .violations import Violation

if TYPE_CHECKING:
    from .target import ConformanceTarget

REQUEST_CHECK = "api-request-expressions"
PAGINATION_CHECK = "api-pagination"
RECORDS_CHECK = "api-response-records"

#: Failures a resolve pass raises for an authoring defect. Missing data is
#: absorbed by the per-request policy and recorded by the probe instead.
RESOLVE_FAILURES = (TransportSpecError, KeyError, TypeError, ValueError)

#: Authored paging values the contract types as ``Any``, so a literal that
#: is not a positive integer reaches the engine and fails the read there.
#: Each is ``(<dotted location>, <path into the pagination block>)``.
_LITERAL_PAGE_VALUES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("limit.default", ("limit", "default")),
    ("page.increment_by", ("page", "increment_by")),
    ("offset.increment_by", ("offset", "increment_by")),
)


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


def check_api_pagination(target: ConformanceTarget) -> list[Violation]:
    """Certify that each read's paging strategy resolves from a page's response."""
    violations: list[Violation] = []
    for label, read in read_operations(target):
        pagination = read.get("pagination")
        if not isinstance(pagination, Mapping):
            continue
        resolver, asked = page_probe(target.definition)

        # One deep resolve over the whole block reaches every expression the
        # loop evaluates -- stop_when's operands, next_cursor, next_url,
        # increment_by, limit.default -- without this module restating the
        # predicate grammar. Non-expression structure passes through.
        try:
            resolver.resolve_for_request(pagination)
        except RESOLVE_FAILURES as err:
            violations.append(
                Violation(
                    PAGINATION_CHECK,
                    f"endpoint {label!r}: the pagination block holds an "
                    f"expression the resolver refuses: {err}. The paging loop "
                    f"evaluates these against every page, so the read fails "
                    f"after the first request.",
                )
            )

        violations.extend(_literal_page_value_violations(label, pagination))
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
            for entry in unsatisfied(asked)
        )
    return violations


def _literal_page_value_violations(
    label: str, pagination: Mapping[str, Any]
) -> list[Violation]:
    """Report authored paging literals the engine will reject as a step."""
    violations: list[Violation] = []
    for location, path in _LITERAL_PAGE_VALUES:
        value = _dig(pagination, path)
        # An expression's value is only known against a live response, so
        # only an authored literal can be judged here.
        if value is None or Resolver.is_expression_node(value):
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
            records_items_schema(ref, schema)
        except ResponseSchemaError as err:
            violations.append(
                Violation(
                    RECORDS_CHECK,
                    f"endpoint {label!r}: {err}. The engine walks the declared "
                    f"response schema by this ref to build the record schema it "
                    f"emits, so the read fails before its first batch.",
                )
            )
    return violations
