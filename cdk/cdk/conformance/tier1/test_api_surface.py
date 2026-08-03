"""The transport every read opens exists, is HTTP, and can be opened.

One place is left where the published contract is wider than the CDK's api
path: a request may name its own ``transport_ref``, and the path opens
``default_transport`` at connect time and dispatches every read through it.
That failure is silent -- the request goes out, just not on the connection
the author wrote -- and there is no execution to drive, so the only way to
report it is to read the declaration.
"""

from __future__ import annotations

import pytest

from cdk.conformance.api_surface import (
    check_api_has_reads,
    check_read_transport_selection,
)
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report

#: Every check is about the api path's request dispatch (see
#: cdk.conformance.applicability).
APPLIES_TO_KINDS = ("api",)


def test_the_connector_gives_the_api_checks_something_to_drive(
    conformance_target: ConformanceTarget,
) -> None:
    """Every api check drives a read, so there has to be one."""
    violations = check_api_has_reads(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_no_read_selects_a_transport_the_path_will_not_open(
    conformance_target: ConformanceTarget,
) -> None:
    """Every read goes out on default_transport, so nothing may ask for another."""
    violations = check_read_transport_selection(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))
