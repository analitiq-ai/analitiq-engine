"""The run assessed something: a check applies to this connector's kind.

Without this check, a connector whose kind the live tier carries no
checks for collects nothing but skips and a green exit code — reporting
"not assessed" as "passed".

A kind the live tier is *inapplicable* to is the one exception, and it is
taken before the gate rather than inside it: "no live provider is
reachable from a public CI" is a different statement from "the check was
never written", and only the first belongs to a tier rather than to the
kit (:data:`cdk.conformance.skips.NO_LIVE_TIER`).
"""

from __future__ import annotations

import pytest

from cdk.conformance.applicability import check_kind_applicability
from cdk.conformance.skips import require_live_tier
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report


def test_a_check_applies_to_this_connector_kind(
    conformance_target: ConformanceTarget, request: pytest.FixtureRequest
) -> None:
    """A run that can assess nothing fails, naming the kind it cannot."""
    require_live_tier(conformance_target)
    violations = check_kind_applicability(conformance_target, request.session.items)
    if violations:
        pytest.fail(violation_report(violations))
