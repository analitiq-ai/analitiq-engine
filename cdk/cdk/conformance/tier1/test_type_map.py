"""Type-map stability under a write/read round trip (read closure and
convergence; see :mod:`cdk.conformance.roundtrip` for why the literal
``read(write(x)) == x`` is not the asserted form)."""

from __future__ import annotations

import pytest

from cdk.conformance.roundtrip import check_type_map_grammar, check_type_map_round_trip
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.violations import violation_report


def test_type_map_canonicals_are_in_the_published_grammar(
    conformance_target: ConformanceTarget,
) -> None:
    """Every literal canonical names a family the engine can parse.

    Runs for source-only connectors too: discovery emits canonicals from
    the read map alone, so this is the one type-map fact a connector
    without a write map still has to earn.
    """
    mapper = conformance_target.type_mapper
    if mapper is None:
        pytest.skip("connector ships no type map; nothing to check")
    violations = check_type_map_grammar(mapper)
    if violations:
        pytest.fail(violation_report(violations))


def test_type_maps_round_trip_stably(
    conformance_target: ConformanceTarget,
) -> None:
    """Rendered natives read back, and one round reaches a fixed point.

    Probes render through the connector's own dialect when one resolved
    (its ``render_column_type`` override participates), exactly as
    first-run DDL renders.
    """
    mapper = conformance_target.type_mapper
    if mapper is None or not mapper.has_write_map:
        pytest.skip("connector ships no type-map-write.json; nothing to check")
    violations = check_type_map_round_trip(mapper, conformance_target.dialect)
    if violations:
        pytest.fail(violation_report(violations))
