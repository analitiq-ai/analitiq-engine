"""The run assessed something: a check applies to this connector's kind.

Without this check, a connector whose kind the contract tier carries no
checks for collects nothing but skips and a green exit code — reporting
"not assessed" as "passed".
"""

from __future__ import annotations

import pytest

from cdk.conformance.applicability import check_kind_applicability
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report


def test_a_check_applies_to_this_connector_kind(
    conformance_target: ConformanceTarget, request: pytest.FixtureRequest
) -> None:
    """A run that can assess nothing fails, naming the kind it cannot."""
    violations = check_kind_applicability(conformance_target, request.session.items)
    if violations:
        pytest.fail(violation_report(violations))
