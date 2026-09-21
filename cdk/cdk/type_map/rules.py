r"""Type-map rule parsing: the contract model, plus the engine's own normalizers.

A rule is one entry in a ``type-map.json`` section:

    {"match": "exact", "native_type": "JSONB", "arrow_type": "Utf8"}
    {"match": "regex", "native_type": "^VARCHAR\((?<n>\d+)\)$", "arrow_type": "Utf8"}

The rule models are **not defined here**. They are ``analitiq.contracts.type_map``,
the published contract, and this module imports them. Whether a document is
valid is that package's question, answered once, offline, at a pinned version.

Regex matchers are compiled and matched with the contract's own
:func:`~analitiq.contracts.type_map.compile_matcher`: RE2, the dialect the
contract fixes for them. The contract refuses at parse time any matcher RE2
cannot compile, so every rule :func:`parse_type_map` returns is one this
process can run. RE2 is automaton-based and cannot backtrack, so match time is
linear in input length for any pattern it accepts -- which is what bounds a
connector-authored, untrusted pattern against an adversarial input (#504).
"""

from __future__ import annotations

import re
from re import Pattern
from typing import Final, NamedTuple, cast

from analitiq.contracts.type_map import (
    TYPE_MAP_SCHEMA_URL,
    CompiledMatcher,
    TypeMapDoc,
    TypeMapReadExactRule,
    TypeMapReadRegexRule,
    TypeMapReadRule,
    TypeMapWriteRule,
    compile_matcher,
)
from analitiq.contracts.type_map import (
    normalize_native_type as _contract_normalize_native_type,
)
from pydantic import ValidationError

from .exceptions import InvalidTypeMapError
from .grammar import NULL_TZ_SENTINEL, UNIT_SHORT_TO_LONG, unit_families

# ``TypeMapReadRule``/``TypeMapWriteRule`` are discriminated *unions*, usable as
# annotations but not with ``isinstance``. Direction is decided against the two
# concrete read variants, which both directions' rules would otherwise satisfy
# structurally -- every rule carries both ``native_type`` and ``arrow_type``.
_READ_RULE_CLASSES: Final[tuple[type, ...]] = (
    TypeMapReadExactRule,
    TypeMapReadRegexRule,
)

__all__ = [
    "CompiledMatcher",
    "ParsedTypeMap",
    "TypeMapReadRule",
    "TypeMapWriteRule",
    "compile_pattern",
    "normalize_arrow_type",
    "normalize_native_type",
    "normalized_native",
    "parse_rules",
    "parse_type_map",
    "parse_write_rules",
]


# The unit vocabulary comes from the shared grammar table
# (cdk.type_map.grammar) -- the same source parse_arrow_type binds against and
# the published arrow_type_grammar.json renders from -- so the unit checks this
# surface performs can never contradict the parser's. That is this surface's
# whole validation scope: integer ranges, timezone, and arity are enforced
# only by parse_arrow_type. normalize_arrow_type expands short codes in every
# LOOKUP input, so either spelling resolves to the same key at lookup time. An
# authored rule is not expanded: the contract's arrow_type pattern admits
# long-form units only, so a write rule keyed ``Timestamp(us)`` does not validate.
_UNIT_LONG_FORMS: Final[frozenset[str]] = frozenset(UNIT_SHORT_TO_LONG.values())

# Allowed long-form units per temporal family, derived from the grammar. The
# check runs after unit expansion so the error fires on both short (us) and
# long (MICROSECOND) inputs.
_VALID_UNITS_BY_TYPE: Final[dict[str, frozenset[str]]] = unit_families()

# Matches the opening of a temporal-type parameter list, capturing the type
# name and its first argument (the unit). Both alternations are built from the
# grammar vocabulary, longest spelling first, so short codes (e.g. "s") do not
# partially match inside long-form names (e.g. "SECOND"); the trailing \b
# rejects a matched unit followed by another word character.
_TEMPORAL_UNIT_RE: Final[Pattern[str]] = re.compile(
    r"\b("
    + "|".join(sorted(_VALID_UNITS_BY_TYPE, key=len, reverse=True))
    + r")\(("
    + "|".join(
        sorted(_UNIT_LONG_FORMS, key=len, reverse=True)
        + sorted(UNIT_SHORT_TO_LONG, key=len, reverse=True)
    )
    + r")\b"
)
_NULL_TZ_RE: Final[Pattern[str]] = re.compile(
    r"\bTimestamp\(([^,)]+),\s*" + re.escape(NULL_TZ_SENTINEL) + r"\)"
)


def _expand_temporal_unit(m: re.Match[str]) -> str:
    """Expand a short temporal unit code to its long form, validating the pairing.

    Runs inside :func:`normalize_arrow_type` so that a write rule keyed
    ``Timestamp(us, UTC)`` and a lookup of ``Timestamp(MICROSECOND, UTC)`` resolve
    to the same string rather than missing each other and surfacing later
    with a misleading ``UnmappedTypeError``.

    After expansion, validates that the unit is legal for the given temporal
    type and raises ``InvalidTypeMapError`` for cross-type mismatches (e.g.
    ``Time32(MICROSECOND)`` or ``Time64(SECOND)``).
    """
    type_name, unit = m.group(1), m.group(2)
    if unit in UNIT_SHORT_TO_LONG:
        long_unit = UNIT_SHORT_TO_LONG[unit]
    elif unit in _UNIT_LONG_FORMS:
        long_unit = unit
    else:
        raise AssertionError(
            f"temporal-unit regex matched unexpected unit {unit!r}; "
            f"the regex and the grammar vocabulary have drifted out of sync"
        )
    allowed = _VALID_UNITS_BY_TYPE[type_name]
    if long_unit not in allowed:
        valid_str = "/".join(sorted(allowed))
        raise InvalidTypeMapError(
            f"{type_name} accepts {valid_str} only; got {long_unit}"
        )
    return f"{type_name}({long_unit}"


def normalize_arrow_type(value: str) -> str:
    """Normalize an Arrow type string for write-direction matching.

    Unlike :func:`normalize_native_type` this is **case-preserving**: the Arrow
    vocabulary is mixed-case (``Int64``, ``Decimal128(38, 9)``,
    ``Timestamp(MICROSECOND, UTC)``) and matching it case-insensitively would
    collapse distinct types.

    Three normalizations are applied so that every spelling accepted by
    :func:`~cdk.type_map.arrow.parse_arrow_type` maps to one string:

    1. Whitespace around ``(`` ``)`` ``,`` is removed and commas are re-spaced
       to ``", "``.
    2. Short temporal unit codes (``s``, ``ms``, ``us``, ``ns``) are expanded to
       their long-form equivalents (``SECOND``, ``MILLISECOND``, ``MICROSECOND``,
       ``NANOSECOND``).  Because every lookup input passes through this function,
       either spelling resolves to the same key.
    3. ``Timestamp(unit, null)`` is folded into ``Timestamp(unit)`` -- both are
       timezone-naive; ``parse_arrow_type`` already treats them identically.
    """
    if not isinstance(value, str):
        raise TypeError(f"arrow type must be a string, got {type(value).__name__}")
    # Step 1: whitespace normalization.
    compact = re.sub(r"\s*([(),])\s*", r"\1", value.strip())
    compact = compact.replace(",", ", ")
    # Step 2: fold short unit codes into long-form vocabulary.
    compact = _TEMPORAL_UNIT_RE.sub(_expand_temporal_unit, compact)
    # Step 3: Timestamp(unit, null) -> Timestamp(unit).
    compact = _NULL_TZ_RE.sub(r"Timestamp(\1)", compact)
    return compact


def normalize_native_type(value: str) -> str:
    """Normalize a native type string for matching, rejecting a non-string.

    Delegates to the published contract's normalization -- the matching rule is
    the contract's to define -- and adds only the type guard, which the
    contract's function does not carry because its own inputs are already
    schema-validated strings. The engine's are not: a lookup input is whatever
    a driver returned for a column's declared type, so an unguarded ``.strip()``
    would escape :meth:`~cdk.type_map.mapper.TypeMapper.to_arrow_type` as a bare
    ``AttributeError``, past the ``UnmappedTypeError`` handling that names the
    schema, table and column. Symmetric with :func:`normalize_arrow_type`.
    """
    if not isinstance(value, str):
        raise TypeError(f"native type must be a string, got {type(value).__name__}")
    return _contract_normalize_native_type(value)


def normalized_native(rule: TypeMapReadRule) -> str:
    """Normalize an exact read rule's ``native_type`` to its matching form."""
    if rule.match != "exact":
        raise RuntimeError("normalized_native is only defined for exact rules")
    return normalize_native_type(rule.native_type)


def compile_pattern(rule: TypeMapReadRule | TypeMapWriteRule) -> CompiledMatcher:
    r"""Compile a regex rule's matcher for forward matching.

    The matcher is the ``native_type`` on a read rule and the ``arrow_type`` on a
    write rule -- each direction matches on what the other renders.

    Read inputs are normalized to uppercase before matching, so literal
    characters in a read pattern must be authored in uppercase too; the pattern
    itself is never uppercased, because that would turn character classes like
    ``\d`` into ``\D``. Write inputs are matched case-sensitively, the Arrow
    vocabulary being mixed-case.
    """
    if rule.match != "regex":
        raise RuntimeError("compile_pattern is only defined for regex rules")
    matcher = (
        rule.native_type if isinstance(rule, _READ_RULE_CLASSES) else rule.arrow_type
    )
    return compile_matcher(matcher)


class ParsedTypeMap(NamedTuple):
    """A type-map document's rule lists; ``None`` for a direction it omits."""

    read: list[TypeMapReadRule] | None
    write: list[TypeMapWriteRule] | None


def parse_type_map(document: object, *, source: str) -> ParsedTypeMap:
    """Build a type-map document's contract models.

    Contract validity is analitiq-validator's gate; ``model_validate`` runs
    here because it is what builds the typed rules, and a document that
    skipped the gate still fails at load rather than mid-run.
    """
    try:
        doc = TypeMapDoc.model_validate(document)
    except ValidationError as err:
        raise InvalidTypeMapError(f"{source}: {err}") from err
    return ParsedTypeMap(read=doc.read, write=doc.write)


def parse_rules(payload: object, *, source: str) -> list[TypeMapReadRule]:
    """Parse a read-direction (native_type -> arrow_type) rule array."""
    document = {"$schema": TYPE_MAP_SCHEMA_URL, "read": payload}
    return cast(list[TypeMapReadRule], parse_type_map(document, source=source).read)


def parse_write_rules(payload: object, *, source: str) -> list[TypeMapWriteRule]:
    """Parse a write-direction (arrow_type -> native_type) rule array."""
    document = {"$schema": TYPE_MAP_SCHEMA_URL, "write": payload}
    return cast(list[TypeMapWriteRule], parse_type_map(document, source=source).write)
