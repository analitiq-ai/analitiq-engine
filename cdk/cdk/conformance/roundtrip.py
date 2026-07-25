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

import re
from typing import TYPE_CHECKING, Any

from cdk.sql.dialects import SqlDialect
from cdk.type_map.exceptions import InvalidTypeMapError, UnmappedTypeError
from cdk.type_map.grammar import (
    ARROW_TYPE_GRAMMAR,
    STRUCTURAL_FAMILIES,
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


def _canonical_family(spelling: str) -> str:
    """Return the family name of a canonical spelling (before arguments)."""
    for separator in ("(", "<"):
        index = spelling.find(separator)
        if index != -1:
            spelling = spelling[:index]
    return spelling.strip()


def _in_published_grammar(spelling: str) -> bool:
    """Whether a canonical literal's family exists in the published grammar."""
    family = _canonical_family(spelling)
    return family in ARROW_TYPE_GRAMMAR or family in STRUCTURAL_FAMILIES


def _foreign_literal_violations(mapper: TypeMapper) -> list[Violation]:
    """Flag rule canonicals whose family the grammar does not define.

    The rule loaders are string-level and accept any spelling, but a
    canonical outside the published grammar is a defect in either
    direction: a read rule emitting one hands discovery a family
    ``parse_arrow_type`` rejects, and a write rule matching one is
    unreachable, since no endpoint document can ever carry it. Left
    uncaught, such a literal would also count toward probe coverage (it
    trivially round-trips with itself), letting a write map that cannot
    render any real canonical read as covered.

    Which rules carry a literal canonical differs by direction, because
    the field means different things: a read rule's ``canonical`` is its
    *output*, literal whenever it interpolates no capture, so regex read
    rules are checked too; a write rule's ``canonical`` is its *match*,
    a pattern rather than a family name unless the rule is exact.
    """
    violations: list[Violation] = []
    seen: set[str] = set()
    directions: list[tuple[str, Any]] = [("read", rule) for rule in mapper.rules]
    directions += [
        ("write", rule) for rule in mapper.write_rules if rule.match == "exact"
    ]
    for direction, rule in directions:
        if "${" in rule.canonical or _in_published_grammar(rule.canonical):
            continue
        normalized = normalize_canonical_type(rule.canonical)
        if normalized in seen:
            continue
        seen.add(normalized)
        violations.append(
            Violation(
                CHECK_COVERAGE,
                f"{direction} rule declares canonical {rule.canonical!r}, "
                f"whose family {_canonical_family(rule.canonical)!r} is not "
                f"in the published grammar; no endpoint document can ever "
                f"produce it, so the rule is unreachable and does not count "
                f"as coverage.",
            )
        )
    return violations


def _rule_exemplars(mapper: TypeMapper) -> list[str]:
    """Concrete canonicals named by the connector's own rules.

    Exact rules carry a literal canonical on both directions; regex
    rules carry templates and are probed through the grammar exemplars
    instead. Literals outside the published grammar are excluded — they
    are reported as violations, and letting them probe would count
    self-round-tripping garbage toward coverage.
    """
    exemplars: list[str] = []
    for read_rule in mapper.rules:
        if (
            read_rule.match == "exact"
            and "${" not in read_rule.canonical
            and _in_published_grammar(read_rule.canonical)
        ):
            exemplars.append(read_rule.canonical)
    for write_rule in mapper.write_rules:
        if write_rule.match == "exact" and _in_published_grammar(write_rule.canonical):
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
    """Flag regex write rules dead for a *provable* spelling reason.

    A finite probe set cannot prove a regex unreachable — a rule
    legitimately covering only part of a parameterized family (a
    low-precision decimal range, one timezone) matches valid grammar
    while matching none of the probes — so matching nothing is never,
    by itself, a violation. What is provable is a spelling defect: the
    matcher only ever receives the normalized canonical, so a pattern
    that matches some *other* spelling of a probe while matching no
    candidate was authored against a string the matcher can never
    receive. The witness names both the spelling and why it never
    arrives.
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
                found
                for candidate in universe
                if (found := _unreachable_spelling(pattern, candidate)) is not None
            ),
            None,
        )
        if witness is None:
            continue
        candidate, spelling, reason = witness
        violations.append(
            Violation(
                CHECK_COVERAGE,
                f"write rule for canonical {rule.canonical!r} matches "
                f"{spelling!r} but not the {candidate!r} the matcher "
                f"actually receives, so the rule is dead: {reason}.",
            )
        )
    return violations


def _unreachable_spelling(
    pattern: re.Pattern[str], candidate: str
) -> tuple[str, str, str] | None:
    """Find a spelling of *candidate* that *pattern* matches but never sees.

    Two classes, each with its own true explanation. A pre-normalization
    spelling is rewritten before matching; a case variant is not
    rewritten at all, because canonical matching preserves case — the
    Arrow vocabulary is mixed-case and folding it would collapse
    distinct types. Reporting the second under the first's explanation
    would send the author to fix the wrong thing.
    """
    for spelling in _pre_normalization_variants(candidate):
        if pattern.fullmatch(spelling):
            return (
                candidate,
                spelling,
                "normalization rewrites that spelling before matching "
                "(commas are re-spaced and short temporal units expand to "
                "their long form)",
            )
    lowered = candidate.lower()
    if lowered != candidate and pattern.fullmatch(lowered):
        return (
            candidate,
            lowered,
            "canonical matching preserves case, so the mixed-case Arrow "
            "spelling is the only one ever offered",
        )
    return None


def _pre_normalization_variants(candidate: str) -> list[str]:
    """Spellings an author plausibly wrote that normalization never emits.

    The generated spellings are proposals; the normalizer itself decides
    which are real. A proposal counts only when it normalizes back to
    *candidate*, so this list can never claim a witness the matcher would
    in fact have accepted, and a change to normalization narrows the lint
    instead of turning it into a source of false accusations.
    """
    proposals = [candidate.replace(", ", ",")]
    for long_form, short_form in UNIT_LONG_TO_SHORT.items():
        if long_form in candidate:
            proposals.append(candidate.replace(long_form, short_form))
    lowered = candidate.lower()
    if lowered != candidate:
        proposals.append(lowered)
    return [
        proposal
        for proposal in proposals
        if proposal != candidate and normalize_canonical_type(proposal) == candidate
    ]


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


def check_type_map_grammar(mapper: TypeMapper) -> list[Violation]:
    """Certify every literal canonical against the published grammar.

    Applies to any connector that ships a type map, write map or not:
    a source-only connector still emits canonicals from discovery, and
    one outside the grammar fails at runtime in
    :func:`~cdk.type_map.arrow.parse_arrow_type`. Gating this on a write
    map would let exactly that connector — the one with nothing else to
    certify — pass with nothing certified.
    """
    return _foreign_literal_violations(mapper)


def check_type_map_round_trip(
    mapper: TypeMapper, dialect: SqlDialect | None = None
) -> list[Violation]:
    """Certify read closure and convergence for every covered probe.

    The write-direction half of the type-map contract; a connector
    shipping no write map has no round trip to certify. Its read-side
    literals are certified by :func:`check_type_map_grammar`, which runs
    either way.
    """
    if not mapper.has_write_map:
        return []
    probes = probe_canonicals(mapper)
    violations: list[Violation] = _foreign_literal_violations(mapper)
    violations += _misnormalized_write_rules(mapper, probes)
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
