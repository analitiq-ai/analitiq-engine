"""Declarative parameter grammar for canonical ``arrow_type`` strings.

One table (:data:`ARROW_TYPE_GRAMMAR`) declares, per family, what may appear
inside the parentheses: which temporal units, which integer ranges, whether a
timezone is accepted. Every engine surface consumes it — the pyarrow-backed
parser (:func:`cdk.type_map.arrow.parse_arrow_type`) binds parameters through
:func:`bind_parameters`, the string-only rule normalizer
(:mod:`cdk.type_map.rules`) derives its unit vocabulary from it — and the
published ``arrow_type_grammar.json`` renders from it, so the grammar cannot
drift between the parser, rule validation, and what consumers are told.

This module must stay importable without ``pyarrow``: the string-only
control-plane surface validates canonical strings through it (see the
thin-import tests).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from re import Pattern
from typing import Any, Final, Literal
from zoneinfo import available_timezones

from .exceptions import InvalidTypeMapError

# Long canonical unit spelling -> the short code pyarrow factories take. Both
# spellings are accepted on input; the long form is the canonical one.
UNIT_LONG_TO_SHORT: Final[dict[str, str]] = {
    "SECOND": "s",
    "MILLISECOND": "ms",
    "MICROSECOND": "us",
    "NANOSECOND": "ns",
}

UNIT_SHORT_TO_LONG: Final[dict[str, str]] = {
    short: long for long, short in UNIT_LONG_TO_SHORT.items()
}

_ALL_UNITS: Final[tuple[str, ...]] = tuple(UNIT_LONG_TO_SHORT)


@dataclass(frozen=True, slots=True)
class UnitParam:
    """A temporal-unit parameter accepting long and short spellings.

    ``style`` picks the wrong-unit error wording: ``exactly_one`` for the
    single-parameter families (``Time32(SECOND)``), ``enum`` for Timestamp,
    whose unit is followed by the optional timezone.
    """

    name: str
    allowed: tuple[str, ...]
    style: Literal["exactly_one", "enum"] = "exactly_one"


@dataclass(frozen=True, slots=True)
class IntParam:
    """An integer parameter with an inclusive range.

    ``maximum`` of ``None`` means unbounded above unless ``bounded_by`` names
    an earlier parameter whose resolved value caps this one (scale <=
    precision).
    """

    name: str
    minimum: int
    maximum: int | None = None
    bounded_by: str | None = None


@dataclass(frozen=True, slots=True)
class TimezoneParam:
    """An optional timezone: the ``null`` sentinel, an IANA name, or ±HH:MM."""

    name: str = "tz"


GrammarParam = UnitParam | IntParam | TimezoneParam

# The parameter grammar per scalar family. A family missing here is not part
# of the published arrow_type vocabulary; a family mapping to () takes no
# parameters (and a parenthesised argument on it is rejected, not ignored).
ARROW_TYPE_GRAMMAR: Final[dict[str, tuple[GrammarParam, ...]]] = {
    "Null": (),
    "Boolean": (),
    "Int8": (),
    "Int16": (),
    "Int32": (),
    "Int64": (),
    "UInt8": (),
    "UInt16": (),
    "UInt32": (),
    "UInt64": (),
    "Float16": (),
    "Float32": (),
    "Float64": (),
    "Utf8": (),
    "LargeUtf8": (),
    "Json": (),
    "Binary": (),
    "LargeBinary": (),
    "Date32": (),
    "Date64": (),
    "Time32": (UnitParam("unit", ("SECOND", "MILLISECOND")),),
    "Time64": (UnitParam("unit", ("MICROSECOND", "NANOSECOND")),),
    "Duration": (UnitParam("unit", _ALL_UNITS),),
    "Timestamp": (UnitParam("unit", _ALL_UNITS, style="enum"), TimezoneParam()),
    "Decimal128": (
        IntParam("precision", 1, 38),
        IntParam("scale", 0, bounded_by="precision"),
    ),
    "Decimal256": (
        IntParam("precision", 1, 76),
        IntParam("scale", 0, bounded_by="precision"),
    ),
    "FixedSizeBinary": (IntParam("byte_width", 1),),
}

# Nested-type markers: recognized vocabulary, but their shape comes from the
# named sub-schema key, never from parentheses.
STRUCTURAL_FAMILIES: Final[dict[str, str]] = {
    "Object": "properties",
    "List": "items",
}

# Timestamp(unit, null) is the explicit timezone-naive spelling.
NULL_TZ_SENTINEL: Final[str] = "null"

# Published verbatim in the artifact so consumers can enforce the exact same
# offset bounds instead of hand-writing an approximation.
FIXED_OFFSET_PATTERN: Final[str] = r"[+-](?:[01][0-9]|2[0-3]):[0-5][0-9]"

_FIXED_OFFSET_RE: Final[Pattern[str]] = re.compile(FIXED_OFFSET_PATTERN)

_PLAIN_INT_RE: Final[Pattern[str]] = re.compile(r"-?(?:0|[1-9][0-9]*)")

# In the tz database's key set but not castable by Arrow's tzdb; accepting it
# would only defer the failure to cast time.
_NON_CASTABLE_ZONES: Final[frozenset[str]] = frozenset({"Factory"})


@lru_cache(maxsize=1)
def _iana_zones() -> frozenset[str]:
    """Case-sensitive IANA key set, computed once.

    A membership check rather than a ``ZoneInfo(raw)`` construction probe: the
    probe opens ``TZPATH/<key>`` directly, so a continent-only typo
    (``America``) raises ``IsADirectoryError`` and a case-insensitive
    filesystem (macOS) accepts ``utc`` that the Linux runtime rejects. The key
    set has neither problem and behaves identically on every platform.
    """
    return frozenset(available_timezones()) - _NON_CASTABLE_ZONES


def resolve_timezone(raw: str) -> str | None:
    """Validate a Timestamp timezone argument at author time.

    Returns ``None`` for the ``null`` sentinel and the value itself when it is
    a valid IANA zone name or a ``+HH:MM``/``-HH:MM`` fixed offset. Anything
    else raises :class:`InvalidTypeMapError` here, instead of surviving to a
    cast failure deep inside a running pipeline.
    """
    if raw == NULL_TZ_SENTINEL:
        return None
    if _FIXED_OFFSET_RE.fullmatch(raw):
        return raw
    if raw not in _iana_zones():
        raise InvalidTypeMapError(
            f"Timestamp timezone {raw!r} is not a known IANA zone name, a "
            f"+HH:MM/-HH:MM fixed offset, or the null sentinel"
        )
    return raw


def bind_parameters(
    family: str, args: tuple[str, ...], *, has_parens: bool = False
) -> dict[str, Any]:
    """Validate *args* against *family*'s grammar and resolve each value.

    Units resolve to their long canonical spelling, integers to ``int``, and
    an absent or ``null`` timezone to ``None``. Every violation — wrong arity,
    unknown unit, out-of-range integer, invalid timezone — raises
    :class:`InvalidTypeMapError`, as does an empty parenthesis pair on a
    parameterless family when the caller signals ``has_parens`` (``Int64()``
    is an author-time mistake, and every other surface treats it as a distinct
    string from ``Int64``). The family must exist in
    :data:`ARROW_TYPE_GRAMMAR`; callers gate unknown families first, where the
    original input string is available for the error message.
    """
    params = ARROW_TYPE_GRAMMAR[family]
    if has_parens and not args and not params:
        raise InvalidTypeMapError(f"{family} takes no parameters; got ()")
    required = sum(1 for p in params if not isinstance(p, TimezoneParam))
    if not required <= len(args) <= len(params):
        raise _arity_error(family, params, args)
    values: dict[str, Any] = {}
    for spec, raw in zip(params, args):
        if not raw:
            raise InvalidTypeMapError(
                f"{family} has an empty parameter where {spec.name} was expected"
            )
        values[spec.name] = _resolve(family, spec, raw, values)
    for spec in params[len(args) :]:
        values[spec.name] = None
    return values


def _arity_error(
    family: str, params: tuple[GrammarParam, ...], args: tuple[str, ...]
) -> InvalidTypeMapError:
    if not params:
        return InvalidTypeMapError(f"{family} takes no parameters; got {args}")
    if len(params) == 1 and isinstance(params[0], UnitParam):
        return InvalidTypeMapError(
            f"{family}{args} requires exactly one unit from "
            f"{_unit_spellings(params[0])}"
        )
    if not args and isinstance(params[0], UnitParam):
        return InvalidTypeMapError(
            f"{family} requires at least a unit (e.g. {family}(MICROSECOND))"
        )
    names = ", ".join(p.name for p in params)
    return InvalidTypeMapError(f"{family} requires ({names}); got {args}")


def _unit_spellings(spec: UnitParam) -> tuple[str, ...]:
    """Every accepted spelling for *spec*: long forms plus their short codes."""
    return spec.allowed + tuple(UNIT_LONG_TO_SHORT[long] for long in spec.allowed)


def _resolve(
    family: str, spec: GrammarParam, raw: str, resolved: dict[str, Any]
) -> Any:
    if isinstance(spec, UnitParam):
        long = UNIT_SHORT_TO_LONG.get(raw, raw)
        if long not in spec.allowed:
            if spec.style == "enum":
                raise InvalidTypeMapError(
                    f"{family} unit must be one of {'/'.join(_ALL_UNITS)}, "
                    f"got {raw!r}"
                )
            raise InvalidTypeMapError(
                f"{family}({raw}) requires exactly one unit from "
                f"{_unit_spellings(spec)}"
            )
        return long
    if isinstance(spec, IntParam):
        # Strictly plain ASCII decimal, no leading zeros: Python's int() also
        # accepts "+38", "3_8", unicode digits, and "038" — spellings the
        # published grammar does not license and other surfaces (write-rule
        # regexes, non-Python consumers) treat differently. A minus sign is
        # admitted so a negative value fails as out-of-range, not as a typo.
        if not _PLAIN_INT_RE.fullmatch(raw):
            raise InvalidTypeMapError(
                f"{family}({raw}) {spec.name} is not an integer in plain "
                f"decimal form"
            )
        value = int(raw)
        maximum = spec.maximum
        cap_label = str(maximum) if maximum is not None else None
        if spec.bounded_by is not None:
            maximum = resolved[spec.bounded_by]
            cap_label = f"{spec.bounded_by} ({maximum})"
        if value < spec.minimum or (maximum is not None and value > maximum):
            bound = (
                f"between {spec.minimum} and {cap_label}"
                if cap_label is not None
                else f">= {spec.minimum}"
            )
            raise InvalidTypeMapError(
                f"{family} {spec.name} must be {bound}, got {value}"
            )
        return value
    return resolve_timezone(raw)


def unit_families() -> dict[str, frozenset[str]]:
    """Return the allowed long-form units per unit-parameterized family.

    The string-only rule normalizer derives its per-family unit checks from
    this, so it can never disagree with the parser's grammar.
    """
    return {
        family: frozenset(spec.allowed)
        for family, params in ARROW_TYPE_GRAMMAR.items()
        for spec in params
        if isinstance(spec, UnitParam)
    }


def _param_to_json(spec: GrammarParam) -> dict[str, Any]:
    if isinstance(spec, UnitParam):
        return {
            "name": spec.name,
            "kind": "unit",
            "allowed": list(spec.allowed),
            "accepted_short_forms": {
                UNIT_LONG_TO_SHORT[long]: long for long in spec.allowed
            },
        }
    if isinstance(spec, IntParam):
        return {
            "name": spec.name,
            "kind": "int",
            "min": spec.minimum,
            "max": spec.bounded_by if spec.bounded_by is not None else spec.maximum,
        }
    return {
        "name": spec.name,
        "kind": "timezone",
        "optional": True,
        "null_sentinel": NULL_TZ_SENTINEL,
        "accepted_forms": [
            "IANA zone name",
            "+HH:MM or -HH:MM fixed offset",
        ],
        # The exact bounds the engine enforces, so a consumer validates
        # offsets identically instead of hand-approximating the range.
        "fixed_offset_pattern": FIXED_OFFSET_PATTERN,
    }


def build_arrow_type_grammar() -> dict[str, Any]:
    """Materialise the grammar for publication.

    :data:`ARROW_TYPE_GRAMMAR` is the single source of truth; this flattens it
    into the serialisable document consumers read instead of hand-writing
    their own patterns. Scalar families carry ``params``; the structural
    markers carry the sub-schema key their shape comes from.
    """
    families: dict[str, Any] = {
        family: {"params": [_param_to_json(spec) for spec in params]}
        for family, params in ARROW_TYPE_GRAMMAR.items()
    }
    for family, sub_schema in STRUCTURAL_FAMILIES.items():
        families[family] = {"structural": sub_schema}
    return {"families": families}


# The published artifact, committed beside this module. Generated from
# build_arrow_type_grammar(); the conformance test fails if the two drift.
# Regenerate with:  ARROW_TYPE_GRAMMAR_PATH.write_text(render_arrow_type_grammar())
ARROW_TYPE_GRAMMAR_PATH: Final[Path] = Path(__file__).with_name(
    "arrow_type_grammar.json"
)


def render_arrow_type_grammar() -> str:
    """Canonical serialisation of the grammar, matching the committed artifact."""
    return json.dumps(build_arrow_type_grammar(), indent=2, sort_keys=True) + "\n"


def load_published_grammar() -> dict[str, Any]:
    """Return the committed, published grammar document."""
    document: dict[str, Any] = json.loads(ARROW_TYPE_GRAMMAR_PATH.read_text())
    return document
