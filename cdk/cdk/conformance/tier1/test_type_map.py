"""Type-map stability under a write/read round trip (read closure and
convergence; see :mod:`cdk.conformance.roundtrip` for why the literal
``read(write(x)) == x`` is not the asserted form)."""

from __future__ import annotations

import pytest

from cdk.conformance.roundtrip import check_type_map_round_trip
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report


def test_type_maps_round_trip_stably(
    conformance_target: ConformanceTarget,
) -> None:
    """Rendered natives read back, and one round reaches a fixed point."""
    mapper = conformance_target.type_mapper
    if mapper is None or not mapper.has_write_map:
        pytest.skip("connector ships no type-map-write.json; nothing to check")
    violations = check_type_map_round_trip(mapper)
    if violations:
        pytest.fail(violation_report(violations))
