"""No read declares something the api path silently drops.

Two places where the published contract is wider than the CDK's api path:
the transport a request dispatches through, and the query-key map. Neither
is materialized, and neither failure is visible at runtime -- the request
goes out, just not the one the author wrote. Driving the read cannot find
them, because there is no execution to drive; the only way to report them
is to read the declaration.
"""

from __future__ import annotations

import pytest

from cdk.conformance.api_surface import (
    check_api_query_bindings,
    check_read_transport_selection,
)
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report

#: Both checks are about the api path's request dispatch (see
#: cdk.conformance.applicability).
APPLIES_TO_KINDS = ("api",)


def test_no_read_selects_a_transport_the_path_will_not_open(
    conformance_target: ConformanceTarget,
) -> None:
    """Every read goes out on default_transport, so nothing may ask for another."""
    violations = check_read_transport_selection(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_no_read_renames_a_param_through_the_query_map(
    conformance_target: ConformanceTarget,
) -> None:
    """A query key that is not its param's own name never reaches the provider."""
    violations = check_api_query_bindings(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))
