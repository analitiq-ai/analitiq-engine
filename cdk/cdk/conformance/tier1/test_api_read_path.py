"""The read path compiles, advances and stops, driven with no network.

The api half of what ``test_read_path.py`` does for databases: build the
read from the connector's own endpoint documents and run it as far as a
definition can be run. A request is compiled, a scripted page is advanced
past, the author's stop condition is evaluated, and the declared records
become an Arrow schema -- through the engine's own functions, so what
passes here is what the engine executes.

Thin on purpose: each test is one call into
:mod:`cdk.conformance.api_read_path`, whose checks return every violation
rather than raising on the first, so a run reports the whole list at once
and a repo can wire the same functions into a harness of its own.
"""

from __future__ import annotations

import pytest

from cdk.conformance.api_read_path import (
    check_api_page_references,
    check_api_read_advances,
    check_api_read_compiles,
    check_api_read_stop_condition,
    check_api_record_schema,
)
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report

#: Every check here drives the CDK's api read path (see
#: cdk.conformance.applicability).
APPLIES_TO_KINDS = ("api",)


def test_every_read_compiles_into_a_first_request(
    conformance_target: ConformanceTarget,
) -> None:
    """Params bind, the page size is placed, the paging adapter builds."""
    violations = check_api_read_compiles(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_every_page_value_a_read_declares_is_one_a_page_carries(
    conformance_target: ConformanceTarget,
) -> None:
    """Nothing pagination reads addresses a scope or field that is absent."""
    violations = check_api_page_references(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_every_read_advances_past_a_page(
    conformance_target: ConformanceTarget,
) -> None:
    """A page produces the next request, and the two refusals fire."""
    violations = check_api_read_advances(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_every_paginated_read_can_stop(
    conformance_target: ConformanceTarget,
) -> None:
    """The declared stop condition evaluates, and reads the page."""
    violations = check_api_read_stop_condition(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_every_read_yields_an_arrow_schema(
    conformance_target: ConformanceTarget,
) -> None:
    """The declared records address a record schema every field maps."""
    violations = check_api_record_schema(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))
