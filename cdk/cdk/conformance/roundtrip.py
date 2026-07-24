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

Uncovered probes are skipped — a connector is not required to render
the whole canonical vocabulary — but the skipping is guarded two ways,
so it can never absorb a defect: a write map that covers *zero* probes
is a violation (the check must not go inert), and a regex rule that
matches a pre-normalization spelling of a probe while matching no
normalized probe is flagged as dead — a provable authoring defect. A
regex that simply matches no probe is left alone: a finite probe set
cannot prove a partial-family rule unreachable.

Exemplars are generated from the published canonical grammar
(:data:`~cdk.type_map.grammar.ARROW_TYPE_GRAMMAR`) plus the concrete
canonicals named by the connector's own rules, so the probe set covers
exactly the vocabulary the contract defines and can never drift from it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cdk.sql.dialects import SqlDialect
from cdk.type_map.exceptions import InvalidTypeMapError, UnmappedTypeError
from cdk.type_map.grammar import (
    ARROW_TYPE_GRAMMAR,
    UNIT_LONG_TO_SHORT,
    IntParam,
    TimezoneParam,
    UnitParam,
)
from cdk.type_map.rules import normalize_canonical_type, normalize_native_type

from .violations import Violation

if TYPE_CHECKING:
    from cdk.type_map.mapper import TypeMapper

CHECK_CLOSURE = "type-map-read-closure"
CHECK_CONVERGENCE = "type-map-convergence"
CHECK_COVERAGE = "type-map-coverage"

#: Representative (precision, scale)-style argument tuples per
#: integer-parameterized family: one mid-range shape and the family's
#: widest shape, so both a regex rule's typical match and its boundary
#: are probed. Every ``IntParam`` family in the grammar must have an
#: entry — :func:`_grammar_exemplars` refuses to run otherwise, so a new
#: family can never silently drop out of the probe set.
_INT_PARAM_EXEMPLARS: dict[str, tuple[tuple[int, ...], ...]] = {
    "Decimal128": ((10, 2), (38, 9)),
    "Decimal256": ((40, 2), (76, 10)),
    "FixedSizeBinary": ((16,),),
}

#: Structural canonical spellings (nested types) used ONLY for the
#: write-rule reachability check, never as round-trip probes: nested
#: types legitimately store as a document column (``List<...> -> JSONB``)
#: whose read-back is ``Json``, so probing them through the convergence
#: rule would flag correct authoring.
_STRUCTURAL_MATCH_EXEMPLARS: tuple[str, ...] = (
    "List<Int64>",
    "LargeList<Utf8>",
    "Struct<x: Int64>",
    "Map<Utf8, Int64>",
    "Object<x: Int64>",
)


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
            if family not in _INT_PARAM_EXEMPLARS:
                raise RuntimeError(
                    f"conformance kit defect: grammar family {family!r} has "
                    f"no probe exemplars; add it to _INT_PARAM_EXEMPLARS so "
                    f"the family cannot silently drop out of the probe set"
                )
            for values in _INT_PARAM_EXEMPLARS[family]:
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


def _misnormalized_write_rules(
    mapper: TypeMapper, probes: list[str]
) -> list[Violation]:
    """Flag regex write rules dead for a *provable* normalization reason.

    A finite probe set cannot prove a regex unreachable — a rule
    legitimately covering only part of a parameterized family (a
    low-precision decimal range, one timezone) matches valid grammar
    while matching none of the probes — so matching nothing is never,
    by itself, a violation. What is provable is a normalization defect:
    lookups always arrive normalized (comma-space separated, long-form
    temporal units), so a pattern that matches a *pre-normalization*
    spelling of a probe while matching no normalized candidate was
    authored against a spelling the matcher can never receive. The
    witness names the exact defect.
    """
    universe = [
        normalize_canonical_type(c) for c in probes + list(_STRUCTURAL_MATCH_EXEMPLARS)
    ]
    violations: list[Violation] = []
    regex_rules = [rule for rule in mapper.write_rules if rule.match != "exact"]
    for rule in regex_rules:
        pattern = rule.compile_pattern()
        if any(pattern.fullmatch(candidate) for candidate in universe):
            continue
        witness = next(
            (
                (candidate, variant)
                for candidate in universe
                for variant in _pre_normalization_variants(candidate)
                if pattern.fullmatch(variant)
            ),
            None,
        )
        if witness is None:
            continue
        candidate, variant = witness
        violations.append(
            Violation(
                CHECK_COVERAGE,
                f"write rule for canonical {rule.canonical!r} matches the "
                f"pre-normalization spelling {variant!r} but not the "
                f"normalized {candidate!r} the matcher actually receives; "
                f"the rule is dead. Author the pattern against normalized "
                f"spellings (comma followed by one space, long-form "
                f"temporal units).",
            )
        )
    return violations


def _pre_normalization_variants(candidate: str) -> list[str]:
    """Spellings an author plausibly wrote that normalization never emits."""
    variants = [candidate.replace(", ", ",")]
    for long_form, short_form in UNIT_LONG_TO_SHORT.items():
        if long_form in candidate:
            variants.append(candidate.replace(long_form, short_form))
    lowered = candidate.lower()
    if lowered != candidate:
        variants.append(lowered)
    return [v for v in variants if v != candidate]


def render_probe(
    mapper: TypeMapper, canonical: str, dialect: SqlDialect | None = None
) -> str:
    """Render one canonical exactly as first-run DDL renders it.

    The engine's only production render path is
    ``dialect.render_column_type(canonical, type_mapper)`` with no
    per-column hints (``cdk.sql.ddl.build_create_table_sql``), so the
    probe renders the same way — through the connector's own dialect
    when one is available (its ``render_column_type`` override
    participates), and hint-free either way. A rule whose template needs
    a hint no capture provides therefore fails here exactly as it fails
    on the first customer table, instead of passing under fabricated
    hints the engine never supplies.
    """
    if dialect is not None:
        return dialect.render_column_type(canonical, mapper)
    return mapper.to_native_type(canonical)


def check_type_map_round_trip(
    mapper: TypeMapper, dialect: SqlDialect | None = None
) -> list[Violation]:
    """Certify read closure and convergence for every covered probe."""
    if not mapper.has_write_map:
        return []
    probes = probe_canonicals(mapper)
    violations: list[Violation] = _misnormalized_write_rules(mapper, probes)
    rendered = 0
    for canonical in probes:
        try:
            native = render_probe(mapper, canonical, dialect)
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
        rendered += 1
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
            second = render_probe(mapper, recovered, dialect)
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
    if rendered == 0:
        violations.append(
            Violation(
                CHECK_COVERAGE,
                "the write map rendered none of the canonical probes; the "
                "round-trip check certified nothing. The map cannot render "
                "even the basic scalar vocabulary (Int64, Utf8, ...) the "
                "engine's DDL needs.",
            )
        )
    return violations
