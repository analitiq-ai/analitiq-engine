"""Type-map stability under a write/read round trip.

First-run DDL renders a stream's canonical types through the write map;
discovery reads the created table's native types back through the read
map. Widening on that round is physically unavoidable — most systems
have no unsigned or 8-bit integers, so ``Int8 -> SMALLINT`` correctly
reads back as ``Int16`` — which is why the literal invariant
``read_map(write_map(x)) == x`` cannot hold for a well-authored map.
What must hold is:

* **read closure** — every native type the write map renders is one the
  read map can map back; a table the connector creates must be
  discoverable by the same connector.
* **convergence** — one write/read round reaches a fixed point:
  ``write_map(read_map(write_map(x))) == write_map(x)``. Without it,
  re-creating a logically identical table changes its column types
  (e.g. a write rule rendering ``INTERVAL`` whose read-back is ``Utf8``
  re-renders as ``TEXT``), and schema comparisons drift forever.

Exemplars are generated from the published canonical grammar
(:data:`~cdk.type_map.grammar.ARROW_TYPE_GRAMMAR`) plus the concrete
canonicals named by the connector's own rules, so the probe set covers
exactly the vocabulary the contract defines and can never drift from it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cdk.type_map.exceptions import InvalidTypeMapError, UnmappedTypeError
from cdk.type_map.grammar import ARROW_TYPE_GRAMMAR, IntParam, TimezoneParam, UnitParam
from cdk.type_map.rules import normalize_canonical_type, normalize_native_type

from .violations import Violation

if TYPE_CHECKING:
    from cdk.type_map.mapper import TypeMapper

CHECK_CLOSURE = "type-map-read-closure"
CHECK_CONVERGENCE = "type-map-convergence"

#: Render hints supplied to every write-map probe, standing in for the
#: per-column metadata the engine passes at DDL time. A template token
#: neither these nor a capture group provides is an authoring defect the
#: probe reports.
RENDER_HINTS: dict[str, object] = {
    "length": 255,
    "precision": 38,
    "scale": 9,
    "byte_width": 16,
}

#: Representative (precision, scale)-style argument tuples per
#: integer-parameterized family: one mid-range shape and the family's
#: widest shape, so both a regex rule's typical match and its boundary
#: are probed.
_INT_PARAM_EXEMPLARS: dict[str, tuple[tuple[int, ...], ...]] = {
    "Decimal128": ((10, 2), (38, 9)),
    "Decimal256": ((40, 2), (76, 10)),
    "FixedSizeBinary": ((16,),),
}


def _grammar_exemplars() -> list[str]:
    """Concrete canonical spellings covering every grammar family."""
    exemplars: list[str] = []
    for family, params in ARROW_TYPE_GRAMMAR.items():
        if family == "Null":
            # No system stores a null-typed column; write maps do not
            # cover it and probing it would be noise.
            continue
        if not params:
            exemplars.append(family)
            continue
        if isinstance(params[0], UnitParam):
            has_timezone = any(isinstance(p, TimezoneParam) for p in params[1:])
            for unit in params[0].allowed:
                exemplars.append(f"{family}({unit})")
                if has_timezone:
                    exemplars.append(f"{family}({unit}, UTC)")
            continue
        if isinstance(params[0], IntParam):
            for values in _INT_PARAM_EXEMPLARS.get(family, ()):
                rendered = ", ".join(str(v) for v in values)
                exemplars.append(f"{family}({rendered})")
    return exemplars


def _rule_exemplars(mapper: TypeMapper) -> list[str]:
    """Concrete canonicals named by the connector's own rules.

    Exact rules carry a literal canonical on both directions; regex
    rules carry templates and are probed through the grammar exemplars
    instead.
    """
    exemplars: list[str] = []
    for read_rule in mapper.rules:
        if read_rule.match == "exact" and "${" not in read_rule.canonical:
            exemplars.append(read_rule.canonical)
    for write_rule in mapper.write_rules:
        if write_rule.match == "exact":
            exemplars.append(write_rule.canonical)
    return exemplars


def probe_canonicals(mapper: TypeMapper) -> list[str]:
    """Build the deduplicated canonical probe set for *mapper*, in order."""
    seen: set[str] = set()
    probes: list[str] = []
    for canonical in _grammar_exemplars() + _rule_exemplars(mapper):
        normalized = normalize_canonical_type(canonical)
        if normalized in seen:
            continue
        seen.add(normalized)
        probes.append(canonical)
    return probes


def check_type_map_round_trip(mapper: TypeMapper) -> list[Violation]:
    """Certify read closure and convergence for every covered probe.

    A probe the write map does not cover is skipped — a connector is not
    required to render the whole canonical vocabulary, only to keep the
    part it does render stable.
    """
    if not mapper.has_write_map:
        return []
    violations: list[Violation] = []
    for canonical in probe_canonicals(mapper):
        try:
            native = mapper.to_native_type(canonical, params=RENDER_HINTS)
        except UnmappedTypeError:
            continue
        except InvalidTypeMapError as err:
            violations.append(
                Violation(
                    CHECK_CLOSURE,
                    f"write map fails to render {canonical}: {err}",
                )
            )
            continue
        try:
            recovered = mapper.to_arrow_type(native)
        except UnmappedTypeError:
            violations.append(
                Violation(
                    CHECK_CLOSURE,
                    f"write map renders {canonical} as {native!r}, but the "
                    f"read map cannot map {native!r} to any canonical type; "
                    f"a table this connector creates could not be read back "
                    f"by the same connector. Add a read rule for "
                    f"{native!r}.",
                )
            )
            continue
        try:
            second = mapper.to_native_type(recovered, params=RENDER_HINTS)
        except (UnmappedTypeError, InvalidTypeMapError) as err:
            violations.append(
                Violation(
                    CHECK_CONVERGENCE,
                    f"{canonical} renders as {native!r} and reads back as "
                    f"{recovered}, but the write map cannot render "
                    f"{recovered} ({err}); re-creating the same table would "
                    f"fail at DDL time.",
                )
            )
            continue
        if normalize_native_type(second) != normalize_native_type(native):
            violations.append(
                Violation(
                    CHECK_CONVERGENCE,
                    f"{canonical} -> {native!r} -> {recovered} -> "
                    f"{second!r}: one write/read round does not reach a "
                    f"fixed point, so a re-created destination table changes "
                    f"its column types. Align the read rule for {native!r} "
                    f"and the write rule for {recovered} on one stored "
                    f"type.",
                )
            )
    return violations
