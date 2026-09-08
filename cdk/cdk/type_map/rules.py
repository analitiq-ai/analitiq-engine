r"""Type-map rule parsing: contract validation, then execution safety.

A rule is one entry in ``type-map-read.json``:

    {"match": "exact", "native_type": "JSONB", "arrow_type": "Utf8"}
    {"match": "regex", "native_type": "^VARCHAR\((?<n>\d+)\)$", "arrow_type": "Utf8"}

The rule models themselves are **not defined here**. They are
``analitiq.contracts.type_map``, the published contract, and this module imports
them. Whether a document is valid is that package's question and it is answered
once, offline, at a pinned version -- a second model of the same shape in this
repo is how the document acquired two spellings for one concept in the first
place.

What is left here is the part the contract cannot answer: what *this process*
will agree to compile and run. A connector document is untrusted, AI-authored
input, and a rule that is perfectly valid can still be one this engine must
refuse to execute:

- The contract permits any ECMA-262 matcher. This engine compiles matchers with
  Python's ``re`` and runs them against every column name a source reports, so it
  additionally requires the RE2 subset -- no lookahead ``(?=…)`` / ``(?!…)``, no
  lookbehind ``(?<=…)`` / ``(?<!…)``, no atomic groups ``(?>…)``, no numeric
  ``\1``..``\9`` or named ``\k<name>`` / ``(?P=name)`` backreferences. Those are
  the constructs RE2 excludes because they admit catastrophic backtracking.
- ``(?<name>…)`` is rewritten to Python's ``(?P<name>…)`` so the compiled pattern
  works with ``re.fullmatch``.

Two further checks sit here because this process is the one that *renders*:

- A write rule whose *match* side carries a ``${…}`` sequence can never fire --
  the token is compared as literal text.
- A render template containing a malformed token (``${length-p}``, ``${length }``)
  is not matched by the substitution token, so it would survive rendering and
  land in the emitted DDL verbatim. The contract rejects the empty ``${}`` and
  unclosed ``${`` forms; the rest is caught here.

Both are document validity rather than execution safety, so both belong in the
contract, not here: analitiq-ai/claude-code-plugins#241 moves them. They are
kept until that lands -- without them a malformed token reaches emitted DDL as
literal text -- and this module drops them when it does.

Because these checks are no longer ``model_validator``s on a model this repo
owns, they are not invariants of the type -- they run in :func:`parse_rules`
and :func:`parse_write_rules`. Those two are the only sanctioned way to obtain
a rule the engine will execute; a rule validated straight off the contract
model is contract-valid but has not been cleared to run here.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from re import Pattern
from typing import Any, Final

from analitiq.contracts.type_map import (
    TypeMapReadDoc,
    TypeMapReadExactRule,
    TypeMapReadRegexRule,
    TypeMapReadRule,
    TypeMapWriteDoc,
    TypeMapWriteRule,
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
    "TypeMapReadRule",
    "TypeMapWriteRule",
    "compile_pattern",
    "normalize_arrow_type",
    "normalize_native_type",
    "normalized_native",
    "parse_rules",
    "parse_write_rules",
]

_NAMED_GROUP_RE2: Final[Pattern[str]] = re.compile(r"\(\?<([A-Za-z_][A-Za-z0-9_]*)>")
# The one substitution token the renderer recognises. Shared with the mapper so
# what validates and what renders can never drift apart.
_SUBSTITUTION_TOKEN: Final[Pattern[str]] = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
# Every substitution-token opener. Deliberately laxer than the token itself --
# it matches ``$ {`` too, because the renderer does NOT, so a spaced opener
# would otherwise survive into the emitted DDL exactly like a malformed name.
_PLACEHOLDER_OPENER: Final[Pattern[str]] = re.compile(r"\$\s*\{")

# RE2 excludes these Perl/Python extensions: each one admits input on which
# Python's backtracking engine runs in super-linear time.
_FORBIDDEN_CONSTRUCTS: Final[tuple[tuple[str, str], ...]] = (
    ("(?=", "lookahead"),
    ("(?!", "negative lookahead"),
    ("(?<=", "lookbehind"),
    ("(?<!", "negative lookbehind"),
    ("(?>", "atomic group"),
    ("(?P=", "Python-style named backreference"),
    (r"\k<", "named backreference"),
)
_BACKREFERENCE_DIGIT: Final[Pattern[str]] = re.compile(r"\\[1-9]")

# The unit vocabulary comes from the shared grammar table
# (cdk.type_map.grammar) -- the same source parse_arrow_type binds against and
# the published arrow_type_grammar.json renders from -- so the unit checks this
# surface performs can never contradict the parser's. That is this surface's
# whole validation scope: integer ranges, timezone, and arity are enforced
# only by parse_arrow_type. normalize_arrow_type expands short codes in every
# LOOKUP input, so either spelling resolves to the same key at lookup time. It
# no longer does so for the authored rule: the contract's arrow_type pattern
# admits long-form units only, so a write rule keyed ``Timestamp(us)`` -- legal
# before the move onto the published models -- no longer validates.
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


def _assert_re2_subset(pattern: str) -> None:
    """Reject Perl/Python regex extensions that RE2 does not support."""
    for token, label in _FORBIDDEN_CONSTRUCTS:
        if token in pattern:
            raise InvalidTypeMapError(
                f"regex pattern uses unsupported construct ({label}): {pattern!r}"
            )
    if _BACKREFERENCE_DIGIT.search(pattern):
        raise InvalidTypeMapError(
            f"regex pattern uses numeric backreference: {pattern!r}"
        )


def _to_python_named_groups(pattern: str) -> str:
    """Translate RE2-style ``(?<name>...)`` groups to Python ``(?P<name>...)``."""
    return _NAMED_GROUP_RE2.sub(lambda m: f"(?P<{m.group(1)}>", pattern)


def normalized_native(rule: TypeMapReadRule) -> str:
    """Normalize an exact read rule's ``native_type`` to its matching form."""
    if rule.match != "exact":
        raise RuntimeError("normalized_native is only defined for exact rules")
    return normalize_native_type(rule.native_type)


def compile_pattern(rule: TypeMapReadRule | TypeMapWriteRule) -> Pattern[str]:
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
    return re.compile(_to_python_named_groups(matcher))


def _assert_well_formed_placeholders(template: str, *, where: str, field: str) -> None:
    """Reject every ``${`` that is not the start of a valid ``${identifier}``.

    A malformed placeholder -- bad characters (``${length-p}``), trailing space
    (``${length }``), or an unterminated opener (``${length``) -- is not matched
    by the strict substitution token, so without this check it survives
    rendering as literal text and lands in the emitted DDL. The contract rejects
    the empty and unclosed forms; the rest is caught here because this is the
    process that does the rendering.
    """
    for opener in _PLACEHOLDER_OPENER.finditer(template):
        if _SUBSTITUTION_TOKEN.match(template, opener.start()) is None:
            raise InvalidTypeMapError(
                f"{where}: {field} {template!r} contains a malformed "
                f"substitution token at offset {opener.start()}; expected "
                f"${{name}} with an identifier name"
            )


def _assert_executable(rule: TypeMapReadRule | TypeMapWriteRule, *, where: str) -> None:
    """Refuse a contract-valid rule this process must not compile or run.

    The contract has already decided the rule is well-formed. This decides
    whether the engine will execute it -- see the module docstring for why the
    two are different questions.
    """
    is_read = isinstance(rule, _READ_RULE_CLASSES)
    if not is_read:
        if _PLACEHOLDER_OPENER.search(rule.arrow_type):
            raise InvalidTypeMapError(
                f"{where}: write rule arrow_type {rule.arrow_type!r} contains a "
                f"${{...}} sequence; substitution tokens belong only in the "
                f"rendered native type, so this rule can never match"
            )
        # The write rule renders DDL, and this process is what renders it.
        _assert_well_formed_placeholders(
            rule.native_type, where=where, field="write rule native_type"
        )
    if rule.match != "regex":
        return
    matcher = rule.native_type if is_read else rule.arrow_type
    # Compilability is already settled: the contract compiled this same matcher
    # in _compile_ecma_matcher before we got here, and the RE2 subset admits no
    # construct that survives that and then fails Python's compiler. Only the
    # subset itself is still ours to decide.
    try:
        _assert_re2_subset(matcher)
    except InvalidTypeMapError as err:
        raise InvalidTypeMapError(f"{where}: {err}") from err


def _render_validation_error(err: ValidationError, *, source: str) -> str:
    """Render a document-level pydantic failure as one per-rule message list.

    Always names the offending value. The Arrow-type constraint is published as
    a ``pattern``, so pydantic's own message for the commonest authoring mistake
    is the 800-character regex and nothing else -- an author told only that
    would have to read the grammar to find out which token of theirs was wrong.
    """
    lines = []
    for detail in err.errors():
        loc = detail["loc"]
        # A whole-document failure (an empty rule list) has no index and no
        # field; its own message already says everything, and inventing a
        # "rule #?" would send the reader looking for a rule that isn't there.
        if not loc:
            lines.append(detail["msg"])
            continue
        index = loc[0] if isinstance(loc[0], int) else "?"
        field = ".".join(str(part) for part in loc[1:]) or "rule"
        lines.append(
            f"rule #{index}: {field}: {detail['msg']} (got {detail['input']!r})"
        )
    return f"{source}: {'; '.join(lines)}"


def _parse(
    payload: Iterable[object],
    doc_model: type[TypeMapReadDoc] | type[TypeMapWriteDoc],
    *,
    source: str,
) -> list[Any]:
    """Validate a rule array against the contract, then against what we will run.

    Returns ``list[Any]`` because the two document models resolve to different
    rule unions; each public wrapper below re-narrows to its own direction.
    """
    try:
        doc = doc_model.model_validate(list(payload))
    except ValidationError as err:
        raise InvalidTypeMapError(_render_validation_error(err, source=source)) from err
    rules: list[Any] = list(doc.root)
    for index, rule in enumerate(rules):
        _assert_executable(rule, where=f"{source}: rule #{index}")
    return rules


def parse_rules(payload: Iterable[object], *, source: str) -> list[TypeMapReadRule]:
    """Validate and parse a read-direction (native_type -> arrow_type) rule array."""
    return _parse(payload, TypeMapReadDoc, source=source)


def parse_write_rules(
    payload: Iterable[object], *, source: str
) -> list[TypeMapWriteRule]:
    """Validate and parse a write-direction (arrow_type -> native_type) rule array."""
    return _parse(payload, TypeMapWriteDoc, source=source)
