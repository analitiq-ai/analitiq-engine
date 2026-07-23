r"""Type-map rule model and normalization primitives.

A rule is one entry in ``type-map-read.json``:

    {"match": "exact", "native": "JSONB", "canonical": "Utf8"}
    {"match": "regex", "native": "^VARCHAR\((?<n>\d+)\)$", "canonical": "Utf8"}

Regex rules must be written in an RE2-compatible subset so the same pattern
behaves identically across engine languages. The following Perl/Python
extensions are rejected at load time:

- lookahead ``(?=…)`` / negative lookahead ``(?!…)``
- lookbehind ``(?<=…)`` / negative lookbehind ``(?<!…)``
- atomic groups ``(?>…)``
- numeric backreferences ``\1``..``\9``
- named backreferences ``\k<name>`` and Python-style ``(?P=name)``

``(?<name>…)`` is rewritten to Python's ``(?P<name>…)`` so the compiled
pattern works with ``re.fullmatch``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from re import Pattern
from typing import Final, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .exceptions import InvalidTypeMapError
from .grammar import NULL_TZ_SENTINEL, UNIT_SHORT_TO_LONG, unit_families

_NAMED_GROUP_RE2: Final[Pattern[str]] = re.compile(r"\(\?<([A-Za-z_][A-Za-z0-9_]*)>")
_SUBSTITUTION_TOKEN: Final[Pattern[str]] = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
# Every ``${`` opener, used to catch typos (``${length-p}``, ``${length }``,
# the unterminated ``${length``) that the strict token above skips over and
# would otherwise leave as literal text in the rendered output.
_PLACEHOLDER_OPENER: Final[Pattern[str]] = re.compile(r"\$\{")

# RE2 excludes these Perl/Python extensions. We reject any rule that uses them
# at load time so the committed rule set stays portable.
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

# Unit vocabulary and per-family constraints come from the shared grammar
# table (cdk.type_map.grammar) — the same source parse_arrow_type binds
# against and the published arrow_type_grammar.json renders from — so this
# string-only surface can never disagree with the parser.
# normalize_canonical_type expands short codes in both the stored write-rule
# key and every lookup input, so either spelling in a write rule's canonical
# field matches either spelling at lookup time.
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

# Timestamp(unit, null) is semantically identical to Timestamp(unit) — both
# produce a timezone-naïve type.  Fold the explicit null into the no-tz form.
_NULL_TZ_RE: Final[Pattern[str]] = re.compile(
    r"\bTimestamp\(([^,)]+),\s*" + NULL_TZ_SENTINEL + r"\)"
)


def _expand_temporal_unit(m: re.Match[str]) -> str:
    """Substitution callback for :data:`_TEMPORAL_UNIT_RE`.

    Expands a short unit code to its long-form canonical spelling.  Long-form
    names matched by the regex pass through unchanged.  An unrecognized token
    (neither a known short code nor a known long form) means the regex and the
    dict have drifted out of sync — raised as ``AssertionError`` immediately
    rather than silently producing an un-expanded string that would later fail
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


def normalize_native_type(value: str) -> str:
    """Normalize a native type string for matching.

    - Trim leading/trailing whitespace.
    - Collapse internal whitespace runs to a single space.
    - Uppercase the whole string.

    The same normalization is applied to rule ``native`` values (for exact
    rules) and to inputs at lookup time.
    """
    if not isinstance(value, str):
        raise TypeError(f"native type must be a string, got {type(value).__name__}")
    return re.sub(r"\s+", " ", value.strip()).upper()


def normalize_canonical_type(value: str) -> str:
    """Normalize an Arrow canonical type string for write-direction matching.

    Unlike :func:`normalize_native_type` this is **case-preserving**: the Arrow
    vocabulary is mixed-case (``Int64``, ``Decimal128(38, 9)``,
    ``Timestamp(MICROSECOND, UTC)``) and matching it case-insensitively would
    collapse distinct types.

    Three normalizations are applied so that every spelling accepted by
    :func:`~cdk.type_map.arrow.parse_arrow_type` maps to one canonical string:

    1. Whitespace around ``(`` ``)`` ``,`` is removed and commas are re-spaced
       to ``", "``.
    2. Short temporal unit codes (``s``, ``ms``, ``us``, ``ns``) are expanded to
       their long-form equivalents (``SECOND``, ``MILLISECOND``, ``MICROSECOND``,
       ``NANOSECOND``).  Because both the write-rule's ``canonical`` field and
       every lookup input pass through this function, either spelling resolves to
       the same key regardless of which form the rule author used.
    3. ``Timestamp(unit, null)`` is folded into ``Timestamp(unit)`` — both are
       timezone-naïve; ``parse_arrow_type`` already treats them identically.
    """
    if not isinstance(value, str):
        raise TypeError(f"canonical type must be a string, got {type(value).__name__}")
    # Step 1: whitespace normalization.
    compact = re.sub(r"\s*([(),])\s*", r"\1", value.strip())
    compact = compact.replace(",", ", ")
    # Step 2: fold short unit codes into long-form canonical vocabulary.
    compact = _TEMPORAL_UNIT_RE.sub(_expand_temporal_unit, compact)
    # Step 3: Timestamp(unit, null) → Timestamp(unit).
    compact = _NULL_TZ_RE.sub(r"Timestamp(\1)", compact)
    return compact


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


def _assert_well_formed_placeholders(template: str, *, field: str) -> None:
    """Reject every ``${`` that is not the start of a valid ``${identifier}``.

    A malformed placeholder — bad characters (``${length-p}``), trailing space
    (``${length }``), or an unterminated opener (``${length``) — is not matched
    by the strict substitution token, so without this check it would survive
    rendering as literal text and corrupt the emitted DDL silently.
    """
    for opener in _PLACEHOLDER_OPENER.finditer(template):
        if _SUBSTITUTION_TOKEN.match(template, opener.start()) is None:
            raise InvalidTypeMapError(
                f"{field} {template!r} contains a malformed substitution token "
                f"at offset {opener.start()}; expected ${{name}} with an "
                f"identifier name"
            )


class TypeMapRule(BaseModel):
    """A single entry in ``type-map-read.json``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    match: Literal["exact", "regex"]
    native: str = Field(min_length=1)
    canonical: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate(self) -> TypeMapRule:
        tokens = set(_SUBSTITUTION_TOKEN.findall(self.canonical))

        if self.match == "exact":
            if tokens:
                raise InvalidTypeMapError(
                    f"exact rule for {self.native!r} has ${{...}} tokens in "
                    f"canonical {self.canonical!r}; only regex rules may use "
                    f"named-capture substitution"
                )
            _ = normalize_canonical_type(self.canonical)  # validate; raises on bad unit
            return self

        _assert_re2_subset(self.native)

        translated = _to_python_named_groups(self.native)
        try:
            compiled = re.compile(translated)
        except re.error as err:
            raise InvalidTypeMapError(
                f"regex pattern {self.native!r} failed to compile: {err}"
            ) from err

        groups = set(compiled.groupindex)
        missing = tokens - groups
        if missing:
            raise InvalidTypeMapError(
                f"canonical {self.canonical!r} references unknown named "
                f"groups: {sorted(missing)}"
            )

        return self

    def normalized_native(self) -> str:
        """Normalize ``native`` to the form used for exact matching."""
        if self.match != "exact":
            raise RuntimeError("normalized_native is only defined for exact rules")
        return normalize_native_type(self.native)

    def compile_pattern(self) -> Pattern[str]:
        r"""Compile the regex pattern (RE2-subset) for forward matching.

        Inputs are normalized to uppercase before matching, so literal
        characters in the pattern must be authored in uppercase too — we do
        NOT uppercase the pattern itself because that would turn character
        classes like ``\d`` into ``\D``.
        """
        if self.match != "regex":
            raise RuntimeError("compile_pattern is only defined for regex rules")
        translated = _to_python_named_groups(self.native)
        return re.compile(translated)


_RuleT = TypeVar("_RuleT", bound=BaseModel)


def _parse_rule_list(
    payload: Iterable[object], model: type[_RuleT], *, source: str
) -> list[_RuleT]:
    """Validate and parse a JSON array into *model* instances.

    Shared by the read (:class:`TypeMapRule`) and write
    (:class:`WriteTypeMapRule`) directions so the two never diverge.

    Args:
        payload: Iterable of rule dicts, typically the top-level JSON array.
        model: The rule model to instantiate each item as.
        source: Human-readable origin (e.g. file path) used in error messages.
    """
    rules: list[_RuleT] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise InvalidTypeMapError(f"{source}: rule #{index} is not a JSON object")
        try:
            rules.append(model(**item))
        except InvalidTypeMapError:
            raise
        except Exception as err:
            raise InvalidTypeMapError(
                f"{source}: rule #{index} is invalid: {err}"
            ) from err
    if not rules:
        raise InvalidTypeMapError(f"{source}: rule list is empty")
    return rules


def parse_rules(payload: Iterable[object], *, source: str) -> list[TypeMapRule]:
    """Validate and parse a read-direction (native -> canonical) rule array."""
    return _parse_rule_list(payload, TypeMapRule, source=source)


class WriteTypeMapRule(BaseModel):
    """A single entry in ``type-map-write.json`` (canonical -> native).

    The inverse of :class:`TypeMapRule`: it matches on the **canonical** Arrow
    type and renders the **native** DDL type. Two grammar differences follow
    from the inversion:

    - Matching is case-sensitive (the Arrow vocabulary is mixed-case), so the
      ``canonical`` pattern must be authored in Arrow case.
    - ``native`` may carry ``${name}`` tokens fed by **either** named captures
      in the ``canonical`` regex **or** per-column hints passed at render time
      (e.g. ``length``). Because a hint cannot be known at load time, write
      rules defer **all** ``native``-token validation to render time, where
      :func:`~cdk.type_map.mapper._substitute_tokens` raises on any
      unresolved token. This is the one place the write rule is looser than the
      read rule, which can cross-check its output tokens against captures at
      load time.

    The **match** side (``canonical``) is still validated eagerly: it must not
    contain ``${...}`` tokens (those belong only in the rendered ``native``),
    and a regex ``canonical`` must compile within the RE2 subset.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    match: Literal["exact", "regex"]
    canonical: str = Field(min_length=1)
    native: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate(self) -> WriteTypeMapRule:
        # Substitution tokens render into ``native``; any ``${`` on the match
        # side (well-formed or a typo like ``${p)``) would be matched as literal
        # text and never fire, so reject the whole class rather than just the
        # well-formed form.
        if _PLACEHOLDER_OPENER.search(self.canonical):
            raise InvalidTypeMapError(
                f"write rule canonical {self.canonical!r} contains a ${{...}} "
                f"sequence; substitution tokens belong only in the rendered "
                f"native type"
            )

        # A typo'd placeholder in the render template would otherwise leak into
        # the emitted DDL as literal text instead of failing at load time.
        _assert_well_formed_placeholders(self.native, field="write rule native")

        if self.match == "exact":
            _ = normalize_canonical_type(self.canonical)  # validate; raises on bad unit
            return self

        _assert_re2_subset(self.canonical)
        translated = _to_python_named_groups(self.canonical)
        try:
            re.compile(translated)
        except re.error as err:
            raise InvalidTypeMapError(
                f"regex pattern {self.canonical!r} failed to compile: {err}"
            ) from err
        return self

    def compile_pattern(self) -> Pattern[str]:
        """Compile the canonical-matching regex (RE2-subset).

        Canonical inputs are matched case-sensitively, so the pattern is used
        verbatim (no case folding).
        """
        if self.match != "regex":
            raise RuntimeError("compile_pattern is only defined for regex rules")
        translated = _to_python_named_groups(self.canonical)
        return re.compile(translated)


def parse_write_rules(
    payload: Iterable[object], *, source: str
) -> list[WriteTypeMapRule]:
    """Validate and parse a write-direction (canonical -> native) rule array."""
    return _parse_rule_list(payload, WriteTypeMapRule, source=source)
