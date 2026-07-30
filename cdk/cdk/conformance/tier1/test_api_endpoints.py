"""The endpoint documents, against the read the CDK's API path executes.

Each pass of a read resolves against its own scope set — the request from
the connection's stored blocks and runtime, each page additionally from
that page's response, the records from the declared response schema. A
declaration addressing the wrong one resolves to nothing and the engine
drops the param, never stops paging, or fails the read; each test here
names the offending path before the connector ships.
"""

from __future__ import annotations

import pytest

from cdk.conformance.api_endpoints import (
    check_api_pagination,
    check_api_query_bindings,
    check_api_request_expressions,
    check_api_response_records,
)
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report

#: Every check reads api-endpoint documents (see
#: cdk.conformance.applicability).
APPLIES_TO_KINDS = ("api",)


def _require_endpoints(target: ConformanceTarget) -> None:
    """Fail when an API connector ships no endpoint document.

    An API connector is reached only through its endpoints; one shipping
    none exposes no stream at all. That is a definition defect, not an
    inapplicable check — skipping would let the emptiest connector pass
    the suite it is least able to earn.
    """
    if not target.endpoints:
        pytest.fail(
            "the connector ships no endpoint documents under "
            "definition/endpoints/; an API connector exposes every stream "
            "through one, so nothing on it can be read"
        )


def test_request_expressions_resolve_at_request_time(
    conformance_target: ConformanceTarget,
) -> None:
    """Declared param defaults and request bodies resolve from request scopes."""
    _require_endpoints(conformance_target)
    violations = check_api_request_expressions(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_pagination_resolves_against_a_page(
    conformance_target: ConformanceTarget,
) -> None:
    """The declared strategy's expressions resolve from a page's response."""
    _require_endpoints(conformance_target)
    violations = check_api_pagination(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_records_ref_addresses_the_declared_response_schema(
    conformance_target: ConformanceTarget,
) -> None:
    """The records ref walks to a record schema the response declares."""
    _require_endpoints(conformance_target)
    violations = check_api_response_records(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_query_bindings_are_ones_the_read_sends(
    conformance_target: ConformanceTarget,
) -> None:
    """No read renames a param through a query map the engine ignores."""
    _require_endpoints(conformance_target)
    violations = check_api_query_bindings(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))
