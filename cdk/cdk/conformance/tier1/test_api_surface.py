"""Every transport a read opens exists, is HTTP, and can be opened.

A read dispatches through the transport its ``request.transport_ref``
names, or through ``default_transport`` when it names none, so each of
them has to materialize. Whether a named ref points at a transport the
connector declares is settled before the kit runs, by the package
validator's ``endpoint-transport-ref``; what is left here is the half no
pair of documents can answer -- that the block behind the name builds a
session -- and it is answered by running the engine's own build.
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


def test_every_transport_a_read_opens_can_be_opened(
    conformance_target: ConformanceTarget,
) -> None:
    """A read reaches no rows if the transport it dispatches through cannot open."""
    violations = check_read_transport_selection(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))
