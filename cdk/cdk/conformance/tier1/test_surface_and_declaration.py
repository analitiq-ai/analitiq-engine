"""Sanctioned override surface and declared-vs-implemented consistency.

Thin wrappers over the importable checks; each fails with the full
violation list so one CI run reports every defect at once.
"""

from __future__ import annotations

import pytest

from cdk.conformance.declaration import check_declaration_consistency
from cdk.conformance.surface import check_override_surface
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report


def test_override_surface_is_sanctioned(
    conformance_target: ConformanceTarget,
) -> None:
    """Only the sanctioned dialect hooks are overridden (ADR s.10)."""
    violations = check_override_surface(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))


def test_declaration_matches_implementation(
    conformance_target: ConformanceTarget,
) -> None:
    """Declared capabilities and dialect hooks agree, both directions."""
    violations = check_declaration_consistency(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))
