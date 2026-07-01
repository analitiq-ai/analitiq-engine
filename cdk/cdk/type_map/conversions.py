"""Permitted type conversions: one declarative policy, enforced everywhere.

"Which ``source arrow_type -> target arrow_type`` conversions are permitted" was
historically derived independently at each build boundary. The transform build
(``pa.array``) rejected ``Int64 -> Utf8`` while the destination cast
(``pyarrow.compute.cast(safe=True)``) silently accepted it: identical author
intent, opposite outcomes. This module is the single source of truth those
boundaries consult instead, so the answer cannot diverge.

A conversion has one of four *modes*:

- ``identity``  -- source and target are the same arrow family; pass through.
- ``auto``      -- lossless and applied implicitly (e.g. ``Int32 -> Int64``).
- ``explicit``  -- permitted, but the author must declare a conversion function
  (``fn``) in the mapping. Stringifying a number, parsing a string back to a
  number, formatting a timestamp -- each is a policy choice, not a free
  widening, so it must be written down. A boundary that still sees the raw
  source type here means the function was not applied: it fails loud.
- ``forbidden`` -- never permitted (e.g. ``Object -> Int64``).

``runtime_checked`` marks a conversion that is permitted but may still reject
individual rows (an integer narrowing that overflows, a string that will not
parse). The two Arrow->Arrow boundaries -- the destination cast and the
transform retype -- run it through the same ``pc.cast(safe=True)``, which fails
loud on an out-of-range integer narrowing or a lossy ``Float -> Int`` rather
than truncating. (``safe=True`` is not total: a finite ``Float64`` magnitude
beyond ``Float32`` range still casts to +/-inf; that residue is tracked for a
follow-up, not relied on here.)

The grid is keyed on the **published arrow_type family vocabulary** -- the same
head names :func:`~cdk.type_map.arrow.parse_arrow_type` consumes (``"Int64"``,
``"Utf8"``, ``"Timestamp"``) -- so the control plane can publish it and the
frontend can consume it verbatim. This module imports no ``pyarrow``; the
runtime helper that maps a live ``DataType`` to its family lives in
:mod:`cdk.type_map.arrow`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

from .exceptions import InvalidTypeMapError

ConversionMode = Literal["identity", "auto", "explicit", "forbidden"]


@dataclass(frozen=True, slots=True)
class Conversion:
    """How one ``source -> target`` arrow-type conversion is permitted.

    ``fn`` is the mapping function an ``explicit`` conversion must declare (and
    is ``None`` for every other mode). ``runtime_checked`` flags a conversion
    that a per-row guard may still reject.
    """

    mode: ConversionMode
    fn: str | None = None
    runtime_checked: bool = False

    def to_dict(self) -> dict[str, object]:
        """Serialise for the published grid (stable key order)."""
        return {
            "mode": self.mode,
            "fn": self.fn,
            "runtime_checked": self.runtime_checked,
        }


# Each arrow_type family (the head of a canonical type string, e.g. "Int64",
# "Utf8", "Timestamp") belongs to one conversion *kind* -- a group of families
# that share a conversion policy. Width/precision/unit differences inside a kind
# (Int32 vs Int64, Decimal128(10, 2) vs Decimal128(38, 9), Timestamp units) are
# settled by the runtime safe-cast, not by separate matrix entries.
_FAMILY_KIND: Final[dict[str, str]] = {
    "Null": "null",
    "Boolean": "bool",
    "Int8": "int",
    "Int16": "int",
    "Int32": "int",
    "Int64": "int",
    "UInt8": "int",
    "UInt16": "int",
    "UInt32": "int",
    "UInt64": "int",
    "Float16": "float",
    "Float32": "float",
    "Float64": "float",
    "Utf8": "string",
    "LargeUtf8": "string",
    "Json": "json",
    "Binary": "binary",
    "LargeBinary": "binary",
    "FixedSizeBinary": "binary",
    "Date32": "date",
    "Date64": "date",
    "Time32": "time",
    "Time64": "time",
    "Timestamp": "timestamp",
    "Duration": "duration",
    "Decimal128": "decimal",
    "Decimal256": "decimal",
    "Object": "nested",
    "List": "nested",
}

# Stable publication order: the grid is materialised over these.
ARROW_FAMILIES: Final[tuple[str, ...]] = tuple(_FAMILY_KIND)

# The mapping function a "scalar -> string" formatting resolves to. Every fn
# named in this module must exist in the engine's mapping FUNCTION_CATALOG; the
# conformance test asserts the two never drift.
_TO_STRING: Final[str] = "to_string"

# Kinds whose values become a string only through an explicit choice -- the
# notation of the rendered text is a policy, never a free widening.
_STRINGABLE_KINDS: Final[frozenset[str]] = frozenset(
    {"int", "float", "bool", "decimal", "date", "time", "timestamp", "duration"}
)

# The numeric kinds. A conversion between any two of them -- widen, narrow, or an
# int/float/decimal representation change -- is attempted implicitly; the runtime
# safe-cast rejects a row it cannot convert exactly.
_NUMERIC_KINDS: Final[frozenset[str]] = frozenset({"int", "float", "decimal"})

# The cross-kind pairs the runtime safe-cast performs on the *stable* pyarrow
# kernels -- the ones that behave identically on pyarrow 12 and 24. A cross-kind
# pair absent here is forbidden: either pc.cast cannot perform it at all (Binary
# -> Int64, Duration -> Date, Time -> Boolean), or it is version-dependent (Utf8
# -> Date32 is unimplemented on 12), so the published grid must not promise it.
# Every listed pair resolves to auto + runtime_checked -- attempted, and the
# safe-cast fails loud on a row or width it cannot convert exactly. Same-kind
# width/unit changes (Int32 -> Int64, Date32 -> Date64) are settled a step
# earlier and never reach this table.
_CROSS_KIND_AUTO: Final[frozenset[tuple[str, str]]] = frozenset(
    # numeric <-> numeric: widen, narrow, or change representation.
    {(src, tgt) for src in _NUMERIC_KINDS for tgt in _NUMERIC_KINDS if src != tgt}
    # bool <-> int: the canonical 0/1 <-> false/true mapping.
    | {("bool", "int"), ("int", "bool")}
    # string -> numeric parse: an API source ships every value as a JSON string
    # and the engine has always parsed "1" -> Int, "1.5" -> Float.
    | {("string", "int"), ("string", "float"), ("string", "decimal")}
    # date <-> timestamp: the same instant at a coarser or finer resolution.
    | {("date", "timestamp"), ("timestamp", "date")}
)


def classify_conversion(source_family: str, target_family: str) -> Conversion:
    """Return the permitted :class:`Conversion` for one family pair.

    Raises :class:`InvalidTypeMapError` for an unknown family on either side --
    an unknown family is an author-time mistake that must surface loudly rather
    than resolve to a silent default.
    """
    src_kind = _kind_of(source_family, "source")
    tgt_kind = _kind_of(target_family, "target")

    # Same family: a no-op at the type level. (Width/unit differences inside the
    # family -- Timestamp(SECOND) -> Timestamp(MICROSECOND) -- arrive here too;
    # the runtime boundary handles those with a safe-cast, treating identity the
    # same as auto, while a truly identical type short-circuits before any cast.)
    if source_family == target_family:
        return Conversion("identity")

    # Null is the universal filler: nothing converts *to* Null, but a Null source
    # fills *any* typed column -- scalar, nested, or Json -- with typed nulls.
    # Decided before the nested/json guards so an all-null source column can still
    # feed a nested or Json target (a present, wholly-null batch column).
    if tgt_kind == "null":
        return Conversion("forbidden")
    if src_kind == "null":
        return Conversion("auto")

    # scalar -> string: a formatting choice (notation, precision), declared via
    # to_string. This is the one direction the destination cast must not perform
    # silently -- the Int64 -> Utf8 incident that motivated the matrix.
    if tgt_kind == "string" and src_kind in _STRINGABLE_KINDS:
        return Conversion("explicit", fn=_TO_STRING)

    # string -> a numeric scalar is *not* explicit: an API source ships every
    # value as a JSON string, and both the source read (_build_numeric_column)
    # and this cast already parse "1" -> Int, "1.5" -> Float. That parse is the
    # engine's standing behavior, so string -> int/float/decimal resolves to auto
    # via the cross-kind allowlist below. Parsing a string into a *temporal* is
    # not offered: Utf8 -> Date32 is unimplemented on pyarrow 12, so the grid
    # would promise a version-dependent cast; an author renders those with an
    # explicit function (iso_to_date) instead.

    # Nested structures never convert to or from a scalar, and one nested shape
    # never becomes another (Object <-> List).
    if src_kind == "nested" or tgt_kind == "nested":
        return Conversion("forbidden")

    # Json is an opaque blob built at the source boundary from a dict/list; it
    # has no defined scalar conversion in either direction. These rows are
    # publication-only: a live Json column is a pa.large_string, so the runtime
    # boundaries (via arrow_family) classify it as LargeUtf8, never "Json". The
    # rows exist so the published contract tells the authoring UI that a column
    # declared arrow_type="Json" cannot be retyped.
    if src_kind == "json" or tgt_kind == "json":
        return Conversion("forbidden")

    # Same kind, different family: a width/precision change inside one group
    # (Int32 -> Int64, Decimal128 -> Decimal256). The lossless direction passes
    # implicitly; the lossy one is caught per row by the safe-cast.
    if src_kind == tgt_kind:
        return Conversion("auto", runtime_checked=True)

    # Cross-kind: permitted only for the stable, well-defined casts on the
    # allowlist (numeric <-> numeric, bool <-> int, string -> numeric parse, date
    # <-> timestamp). The safe-cast performs the lossless cases and rejects a row
    # it cannot convert exactly. Every other cross-kind pair -- Binary -> Int64,
    # Duration -> Date, Time -> Boolean, and the like -- is forbidden: the runtime
    # cast already fails loud on it, so the published grid says so rather than
    # promising an "auto" conversion that can never run.
    if (src_kind, tgt_kind) in _CROSS_KIND_AUTO:
        return Conversion("auto", runtime_checked=True)
    return Conversion("forbidden")


def build_conversion_matrix() -> dict[str, dict[str, dict[str, object]]]:
    """Materialise the full ``source -> target`` grid for publication.

    :func:`classify_conversion` is the single source of truth; this flattens it
    into the serialisable grid the control plane publishes and the frontend
    consumes verbatim. Regenerating and diffing this against the committed
    artifact (the conformance test) turns drift into a test failure.
    """
    return {
        source: {
            target: classify_conversion(source, target).to_dict()
            for target in ARROW_FAMILIES
        }
        for source in ARROW_FAMILIES
    }


# The published artifact, committed beside this module. Generated from
# build_conversion_matrix(); the conformance test fails if the two drift.
# Regenerate with:  CONVERSION_MATRIX_PATH.write_text(render_conversion_matrix())
CONVERSION_MATRIX_PATH: Final[Path] = Path(__file__).with_name("conversion_matrix.json")


def render_conversion_matrix() -> str:
    """Canonical serialisation of the grid, matching the committed artifact."""
    return json.dumps(build_conversion_matrix(), indent=2, sort_keys=True) + "\n"


def load_published_matrix() -> dict[str, dict[str, dict[str, object]]]:
    """Return the committed, published grid.

    Consumers outside the engine (the control plane, the frontend build) read
    this artifact; the engine itself classifies live through
    :func:`classify_conversion`. The conformance test asserts the file equals
    :func:`render_conversion_matrix` so the export cannot drift from the policy.
    """
    matrix: dict[str, dict[str, dict[str, object]]] = json.loads(
        CONVERSION_MATRIX_PATH.read_text()
    )
    return matrix


def _kind_of(family: str, side: str) -> str:
    kind = _FAMILY_KIND.get(family)
    if kind is None:
        raise InvalidTypeMapError(
            f"unknown {side} arrow_type family {family!r}; expected one of "
            f"{', '.join(ARROW_FAMILIES)}"
        )
    return kind
