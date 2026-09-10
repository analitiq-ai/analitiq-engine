"""Arrow type string ↔ PyArrow ``DataType``.

:func:`parse_arrow_type` handles scalar types only — nested ``Object`` /
``List`` markers need the field's sub-schema (``properties`` / ``items``)
which only :func:`resolve_arrow_type` has access to.
"""

from __future__ import annotations

import base64
import numbers
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from re import Pattern
from typing import Any, Final

import pyarrow as pa
import pyarrow.compute as pc
import re2

from .._extras import reraise_for_missing_extra
from ._param_validation import require_enum_param, require_list_param, require_str_param
from .conversions import Conversion, classify_conversion
from .decoders import CODE_ENCODING_NAME, DECODER_PARAMS, EPOCH_UNITS
from .exceptions import InvalidTypeMapError
from .grammar import (
    ARROW_FAMILIES,
    UNIT_LONG_TO_SHORT,
    ArrowFamily,
    UnitParam,
    bind_parameters,
)

#: One decoded column: the raw wire values in, a typed ``pa.Array`` out.
DecodeFn = Callable[[pa.Field, "list[Any]"], pa.Array]

_PARAM_SPLIT: Final[Pattern[str]] = re.compile(r"\s*,\s*")


def _parse_head(canonical: str) -> tuple[str, tuple[str, ...], bool]:
    """Split ``Name(arg1, arg2)`` into (``Name``, args, had-parentheses).

    The third element distinguishes ``Name()`` from bare ``Name`` — both
    yield empty args, but the binder rejects empty parentheses on a
    parameterless family instead of silently equating the two spellings.
    """
    trimmed = canonical.strip()
    if "(" not in trimmed:
        return trimmed, (), False
    if not trimmed.endswith(")"):
        raise InvalidTypeMapError(
            f"canonical type {canonical!r} has unbalanced parentheses"
        )
    head, _, rest = trimmed.partition("(")
    body = rest[:-1]
    if not body.strip():
        return head.strip(), (), True
    args = tuple(part.strip() for part in _PARAM_SPLIT.split(body))
    return head.strip(), args, True


def _pyarrow_attribute(module: Any, name: str, family: str) -> Callable[..., Any]:
    """Resolve a pyarrow callable the family table names, or fail loud.

    The vocabulary table stays importable without pyarrow by naming its
    factories and predicates as strings; a name this pyarrow build does not
    provide is a defect in the table, and surfacing it when this module is
    imported beats a family that silently builds nothing or matches no live
    column.
    """
    attribute = getattr(module, name, None)
    if not callable(attribute):
        raise InvalidTypeMapError(
            f"arrow_type family {family!r} names {module.__name__}.{name}, "
            f"which this pyarrow build does not provide"
        )
    resolved: Callable[..., Any] = attribute
    return resolved


# The table's pyarrow bindings, resolved once here rather than on every call:
# this module is the only one that imports pyarrow, so the names can be looked
# up at import time, which both keeps the per-batch classification path free of
# attribute lookups and turns a name pyarrow does not provide into an import
# failure instead of a first-batch one.
#
# Family -> the factory the bound parameters are passed to. A structural family
# has none: its shape comes from its sub-schema, never from parentheses.
_FACTORIES: Final[dict[str, Callable[..., Any]]] = {
    family: _pyarrow_attribute(pa, spec.builder, family)
    for family, spec in ARROW_FAMILIES.items()
    if spec.builder is not None
}

# (predicate, family) pairs mapping a live DataType back to its family. The
# predicates test the type id, so they are mutually exclusive and this order
# does not decide the answer.
_FAMILY_PROBES: Final[tuple[tuple[Callable[[pa.DataType], bool], str], ...]] = tuple(
    (_pyarrow_attribute(pa.types, probe, family), family)
    for family, spec in ARROW_FAMILIES.items()
    for probe in spec.probes
)


def _build_arrow_type(
    family: str, spec: ArrowFamily, values: Mapping[str, Any]
) -> pa.DataType:
    """Build the pyarrow type for *family* from its bound parameter values.

    The declared parameters are passed to the declared factory positionally, in
    declaration order — the order every pyarrow factory takes them in
    (``time32(unit)``, ``timestamp(unit, tz)``, ``decimal128(precision,
    scale)``) — with units translated to the short codes pyarrow expects.
    """
    factory = _FACTORIES.get(family)
    if factory is None:
        raise InvalidTypeMapError(
            f"arrow_type family {family!r} declares no pyarrow factory"
        )
    args = [
        (
            UNIT_LONG_TO_SHORT[values[param.name]]
            if isinstance(param, UnitParam)
            else values[param.name]
        )
        for param in spec.params
    ]
    built = factory(*args)
    if not isinstance(built, pa.DataType):
        raise InvalidTypeMapError(
            f"arrow_type family {family!r} names pyarrow.{spec.builder}, "
            f"which did not build a DataType"
        )
    return built


def parse_arrow_type(canonical: str) -> pa.DataType:
    """Parse an Arrow type string into a PyArrow ``DataType``.

    The family vocabulary and its parameter grammar — allowed units, integer
    ranges, the timezone forms — come from
    :data:`cdk.type_map.grammar.ARROW_FAMILIES`, the same table the published
    ``arrow_type_grammar.json`` renders from. Raises
    :class:`InvalidTypeMapError` for malformed input, unsupported families, or
    any parameter the grammar rejects (including an invalid timezone, which
    fails here at author time rather than at cast time inside a running
    pipeline). The matcher is deliberately strict — an unknown family or a
    surplus parameter indicates an author-time mistake that should surface
    loudly.

    Nested-type markers (``Object``, ``List``) are intentionally rejected
    here: they need the property's sub-schema, which only the
    :class:`SchemaContract` walker has access to.
    """
    head, args, has_parens = _parse_head(canonical)
    spec = ARROW_FAMILIES.get(head)
    if spec is None:
        raise InvalidTypeMapError(
            f"arrow_type family {head!r} (from {canonical!r}) is not supported"
        )
    if spec.sub_schema is not None:
        raise InvalidTypeMapError(
            f"arrow_type {head!r} describes a nested type and cannot be "
            f"parsed in isolation; SchemaContract reads the property's "
            f"'properties' (Object) or 'items' (List) sub-schema to build it"
        )
    values = bind_parameters(head, args, has_parens=has_parens)
    return _build_arrow_type(head, spec, values)


def resolve_arrow_type(spec: Mapping[str, Any], where: str = "field") -> pa.DataType:
    """Walk a JSON-Schema-shaped field spec into a ``pa.DataType``.

    ``where`` is a caller-supplied breadcrumb (e.g. ``"field 'checkAccount'"``)
    threaded into error messages so authors can locate the offending
    declaration without reading the recursion stack.
    """
    arrow_type = spec.get("arrow_type")
    if not arrow_type:
        raise InvalidTypeMapError(f"{where}: missing 'arrow_type' declaration")
    if arrow_type == "Object":
        sub = spec.get("properties")
        if not isinstance(sub, dict) or not sub:
            raise InvalidTypeMapError(
                f"{where}: arrow_type='Object' requires a non-empty "
                f"'properties' map declaring each sub-field"
            )
        fields = [
            pa.field(
                name,
                resolve_arrow_type(child, where=f"{where}.{name}"),
                nullable=name not in set(spec.get("required") or ()),
            )
            for name, child in sub.items()
        ]
        return pa.struct(fields)
    if arrow_type == "List":
        items = spec.get("items")
        if not isinstance(items, dict):
            raise InvalidTypeMapError(
                f"{where}: arrow_type='List' requires an 'items' object "
                f"declaring the element type"
            )
        return pa.list_(resolve_arrow_type(items, where=f"{where}[]"))
    return parse_arrow_type(arrow_type)


def arrow_family(dtype: pa.DataType) -> str:
    """Return the conversion-matrix family name for a PyArrow ``DataType``.

    The inverse of the family head :func:`parse_arrow_type` consumes: the
    probes come from the ones each family declares in
    :data:`cdk.type_map.grammar.ARROW_FAMILIES`, which are ``pyarrow.types``
    predicates on the type id, so they are mutually exclusive and the table
    order does not decide the answer. Width and
    parameter detail is intentionally dropped -- a DataType collapses to the
    family head :func:`~cdk.type_map.conversions.classify_conversion` keys its
    policy on (``int32`` -> ``"Int32"``, ``timestamp[us, tz=UTC]`` ->
    ``"Timestamp"``). An unrecognised type raises
    :class:`InvalidTypeMapError` rather than resolve to a silent default --
    conversions classified against an unknown family would be meaningless.
    """
    if pa.types.is_dictionary(dtype):
        # A dictionary-encoded column (some ADBC drivers return these for
        # low-cardinality columns) is, for conversion purposes, its value type;
        # pc.cast transparently decodes it. Classify by the decoded value type
        # so dict<_, Utf8> is treated exactly like Utf8 rather than rejected.
        return arrow_family(dtype.value_type)
    for probe, family in _FAMILY_PROBES:
        if probe(dtype):
            return family
    raise InvalidTypeMapError(
        f"arrow type {dtype!r} has no conversion-matrix family; it is outside "
        f"the published arrow_type vocabulary"
    )


def classify_arrow_conversion(source: pa.DataType, target: pa.DataType) -> Conversion:
    """Classify a live ``source -> target`` DataType conversion via the matrix.

    Bridges the runtime build boundaries (``SchemaContract.cast_arrow_batch``,
    the Arrow-native transform retype) to the pure-string policy in
    :mod:`cdk.type_map.conversions` so both consult one source of truth.
    """
    return classify_conversion(arrow_family(source), arrow_family(target))


@dataclass(frozen=True, slots=True)
class BlockedLeaf:
    """A scalar leaf inside a nested conversion the matrix does not permit.

    ``path`` locates the leaf within the nested target (``"addr.zip"``,
    ``"tags[]"``); ``conversion`` is the offending :class:`Conversion` (its mode
    is ``explicit`` or ``forbidden``, and ``fn`` names the function an
    ``explicit`` leaf would require).
    """

    path: str
    source: pa.DataType
    target: pa.DataType
    conversion: Conversion


def _is_list_type(dtype: pa.DataType) -> bool:
    return bool(pa.types.is_list(dtype) or pa.types.is_large_list(dtype))


def first_blocked_nested_leaf(
    source: pa.DataType, target: pa.DataType, path: str = ""
) -> BlockedLeaf | None:
    """Classify every scalar leaf of a nested conversion through the matrix.

    A nested target is materialised structurally, but each scalar leaf inside it
    is a real ``source -> target`` conversion that must clear the same policy a
    top-level scalar retype does: an ``Int64 -> Utf8`` leaf is ``explicit``, and
    an ``Object -> Int64`` leaf is ``forbidden``, whether the leaf sits at the top
    level or three fields deep. This walks matching struct fields and list
    elements in lockstep and returns the first leaf whose mode is ``explicit`` or
    ``forbidden``, or ``None`` when every leaf is ``identity`` or ``auto`` (which
    the caller's ``pc.cast`` then materialises). A structural mismatch -- a struct
    facing a list, a scalar facing a struct -- classifies ``forbidden`` at that
    node and surfaces here too. A field only the target declares has no source
    leaf to gate and is left to the caller's cast.
    """
    if pa.types.is_struct(source) and pa.types.is_struct(target):
        source_fields = {field.name: field.type for field in source}
        for field in target:
            child = source_fields.get(field.name)
            if child is None:
                continue
            leaf_path = f"{path}.{field.name}" if path else field.name
            blocked = first_blocked_nested_leaf(child, field.type, leaf_path)
            if blocked is not None:
                return blocked
        return None
    if _is_list_type(source) and _is_list_type(target):
        elem_path = f"{path}[]" if path else "[]"
        return first_blocked_nested_leaf(
            source.value_type, target.value_type, elem_path
        )
    conversion = classify_arrow_conversion(source, target)
    if conversion.mode in ("explicit", "forbidden"):
        return BlockedLeaf(path, source, target, conversion)
    return None


# ---- decoders: the pyarrow-backed functions the decoders_catalog names ----
#
# The vocabulary (names, declared params, which conversion kinds require one)
# lives in :mod:`cdk.type_map.decoders`, which stays importable without
# pyarrow. Each function here takes the raw wire values for one column and
# returns a ``pa.Array`` already cast to ``field.type`` -- the same contract
# ``cdk.schema_contract._parse_with_source_format``/``_build_temporal_from_strings``
# held before this catalog replaced them.


def _coerce_ticks(field_name: str, values: list[Any]) -> list[int | None]:
    """Read each wire value as an integer tick count, or raise naming the row.

    ``numbers.Integral`` rather than a bare ``isinstance(v, int)``: a
    connector's own driver code can hand back a numpy integer scalar
    (``int32``/``int64``), which registers against the ABC but is not a
    Python ``int`` subclass -- the same accommodation
    ``_reject_floating_point_offset`` makes for numpy floats. A ``Decimal``
    is accepted too, when integral: a JSON Schema ``"number"``-typed field
    (as opposed to ``"integer"``) commonly carries a whole-valued epoch
    formatted with a decimal point (``1700000000.0``), and
    ``loads_preserving_decimals`` (``cdk.api.http``) parses any
    fractional-looking JSON token as ``Decimal`` regardless of the
    field's declared type. No range check here: whether a tick fits
    pyarrow's storage is a function of *field.type*'s own unit, decided
    once the tick is actually scaled to it, in :func:`_scale_tick`.
    """
    ticks: list[int | None] = []
    for row, v in enumerate(values):
        if v is None:
            ticks.append(None)
            continue
        if isinstance(v, bool):
            raise ValueError(
                f"column {field_name!r} at row {row}: got bool {v!r}, expected an "
                f"integer epoch tick count"
            )
        if isinstance(v, numbers.Integral):
            ticks.append(int(v))
            continue
        if isinstance(v, str):
            try:
                ticks.append(int(v))
            except ValueError as exc:
                raise ValueError(
                    f"column {field_name!r} at row {row}: {v!r} is not an integer "
                    f"epoch tick count"
                ) from exc
            continue
        if isinstance(v, Decimal):
            # loads_preserving_decimals (cdk.api.http) parses every
            # fractional-looking JSON token as Decimal regardless of the
            # field's declared type, so a JSON Schema "number" field's
            # whole-valued epoch (`1700000000.0`) arrives here as Decimal,
            # not int -- accepted when it truly is integral, refused by
            # name otherwise rather than silently truncated by int().
            if v == v.to_integral_value():
                ticks.append(int(v))
                continue
            raise ValueError(
                f"column {field_name!r} at row {row}: {v!r} is not an "
                f"integer epoch tick count (has a fractional part)"
            )
        raise ValueError(
            f"column {field_name!r} at row {row}: {v!r} "
            f"({type(v).__name__}) is not an integer epoch tick count"
        )
    return ticks


#: Nanoseconds per wire epoch unit -- the finest unit either vocabulary
#: names, so converting *to* it from any wire tick is always an exact
#: multiply, never a division that could leave a remainder. DAY has no
#: entry: a date has no sub-day resolution to convert through, and is
#: built straight from the day count in :func:`_ticks_to_array`'s own
#: branch for it instead.
_NANOSECONDS_PER_WIRE_UNIT: Final[dict[str, int]] = {
    "SECOND": 1_000_000_000,
    "MILLISECOND": 1_000_000,
    "MICROSECOND": 1_000,
    "NANOSECOND": 1,
}

#: Nanoseconds per pyarrow storage unit, keyed by its own short spelling --
#: the destination-side mirror of the table above.
_NANOSECONDS_PER_PYARROW_UNIT: Final[dict[str, int]] = {
    "s": 1_000_000_000,
    "ms": 1_000_000,
    "us": 1_000,
    "ns": 1,
}

#: pyarrow's own storage widths. Time32 is the one narrow (int32) case
#: this catalog's decoders ever build; every other Timestamp/Duration/
#: Time64 unit is int64 regardless of its declared resolution.
_INT32_MIN: Final[int] = -(2**31)
_INT32_MAX: Final[int] = 2**31 - 1
_INT64_MIN: Final[int] = -(2**63)
_INT64_MAX: Final[int] = 2**63 - 1


def _require_in_pyarrow_range(
    field: pa.Field, row: int, value: int, *, narrow: bool
) -> int:
    """Return *value* if it fits pyarrow's storage width for ``field.type``, or raise.

    One range-check-and-raise for every caller that scales a tick to a
    destination unit (:func:`_scale_tick`, :func:`_day_tick`,
    :func:`_iso_duration_ticks`'s caller) -- so a wording or bound change
    applies everywhere at once, not to whichever call site a review round
    happened to touch. *narrow* selects int32 (Time32, Date32) over int64
    (every other Timestamp/Duration/Time64 unit, regardless of its
    declared resolution).
    """
    lo, hi = (_INT32_MIN, _INT32_MAX) if narrow else (_INT64_MIN, _INT64_MAX)
    if not lo <= value <= hi:
        raise ValueError(
            f"column {field.name!r} at row {row}: {value} is outside the "
            f"range pyarrow can hold as a tick count for {field.type!s}"
        )
    return value


def _scale_tick(
    field: pa.Field,
    row: int,
    tick: int,
    wire_unit: str,
    storage_unit: str,
    *,
    narrow: bool,
    day_bound: bool = False,
) -> int:
    """Convert one tick from *wire_unit* to *storage_unit*, or raise naming the row.

    Scales through nanoseconds -- arbitrary-precision Python integer
    arithmetic, never a pyarrow array built at *wire_unit*'s own width --
    so a wire tick that would not fit that intermediate's storage (a
    NANOSECOND epoch for a year-9999 instant overflows int64, even though
    the same instant is a small, ordinary number of seconds) still
    converts exactly to a value the destination's own, possibly coarser,
    unit easily holds. Range-checked (:func:`_require_in_pyarrow_range`)
    against the destination's actual storage width only after scaling --
    the wire unit's own range was never the destination's to enforce.

    *day_bound* additionally requires the scaled value to be a genuine
    elapsed offset from midnight (``0 <= scaled < one day``), for a Time
    destination: pyarrow's own array construction only enforces the
    physical int32/int64 width, not the time-of-day domain, so a wire
    tick of ``86400`` (a whole day) or ``-1`` at unit ``SECOND`` would
    otherwise build silently -- storage-valid but not a representable
    time of day.
    """
    total_ns = tick * _NANOSECONDS_PER_WIRE_UNIT[wire_unit]
    scaled, remainder = divmod(total_ns, _NANOSECONDS_PER_PYARROW_UNIT[storage_unit])
    if remainder:
        raise ValueError(
            f"column {field.name!r} at row {row}: tick {tick} in unit "
            f"{wire_unit!r} is not exactly representable in {field.type!s}; "
            f"it has a nonzero remainder that would otherwise be silently "
            f"discarded"
        )
    scaled = _require_in_pyarrow_range(field, row, scaled, narrow=narrow)
    if day_bound:
        ticks_per_day = (
            _NANOSECONDS_PER_DAY // _NANOSECONDS_PER_PYARROW_UNIT[storage_unit]
        )
        if not 0 <= scaled < ticks_per_day:
            raise ValueError(
                f"column {field.name!r} at row {row}: tick {tick} in unit "
                f"{wire_unit!r} scales to {scaled}, outside the single "
                f"calendar day (0 to {ticks_per_day - 1}) a {field.type!s} "
                f"value can hold"
            )
    return scaled


_NANOSECONDS_PER_DAY: Final[int] = 24 * 3600 * 1_000_000_000


#: Milliseconds per day. Date64's own storage unit -- a day count is not
#: enough for it: Arrow stores Date64 as milliseconds since the epoch
#: (always day-aligned), an int64 range wider than Date32's int32 day
#: count.
_MILLISECONDS_PER_DAY: Final[int] = 24 * 3600 * 1000


def _day_tick(
    field: pa.Field, row: int, tick: int, *, wire_unit: str | None = None
) -> int:
    """Convert one tick to ``field.type``'s own day/millisecond count.

    *wire_unit* ``None`` means *tick* already is a day count (the ``DAY``
    epoch unit). Otherwise floors to the whole day through nanoseconds --
    arbitrary-precision integer arithmetic, never a Timestamp intermediate
    at any fixed sub-day precision, which a whole-day epoch value large
    enough to still fit Date64's own range can overflow (200,000,000 days
    is an ordinary day count but ~17.28e18 microseconds, past int64).
    Floors rather than rejecting sub-day precision, matching pyarrow's
    own Timestamp -> Date safe-cast (verified). Returned in ``field.type``'s
    own unit -- milliseconds for Date64, days for Date32 -- rather than
    always the narrower Date32 day count: a day count past Date32's int32
    range (3,000,000,000 days) still fits Date64's wider int64
    millisecond range, and forcing every Date target through the Date32
    intermediate this used to build would reject it. Range-checked
    (:func:`_require_in_pyarrow_range`) against the actual storage width
    being returned.
    """
    days = (
        tick
        if wire_unit is None
        else tick * _NANOSECONDS_PER_WIRE_UNIT[wire_unit] // _NANOSECONDS_PER_DAY
    )
    if pa.types.is_date64(field.type):
        return _require_in_pyarrow_range(
            field, row, days * _MILLISECONDS_PER_DAY, narrow=False
        )
    return _require_in_pyarrow_range(field, row, days, narrow=True)


def _ticks_to_array(
    field: pa.Field, ticks: list[int | None], wire_unit: str
) -> pa.Array:
    """Build ``field.type`` from epoch ticks in *wire_unit*.

    Each tick is scaled to ``field.type``'s own storage unit directly
    (see :func:`_scale_tick`) rather than built through the wire unit's
    own pyarrow type and safe-cast down to ``field.type`` afterward: a
    wire tick that would overflow that intermediate's width must not be
    rejected for a range limit that was never the destination's.
    """
    if wire_unit == "DAY":
        if not pa.types.is_date(field.type):
            raise InvalidTypeMapError(
                f"column {field.name!r}: epoch unit 'DAY' only applies to a "
                f"Date32/Date64 arrow_type, got {field.type}"
            )
        days = [
            None if t is None else _day_tick(field, row, t)
            for row, t in enumerate(ticks)
        ]
        return pa.array(days, type=field.type)
    if pa.types.is_timestamp(field.type) or pa.types.is_duration(field.type):
        scaled = [
            None
            if t is None
            else _scale_tick(field, row, t, wire_unit, field.type.unit, narrow=False)
            for row, t in enumerate(ticks)
        ]
        return pa.array(scaled, type=field.type)
    if pa.types.is_date(field.type):
        # Date32/Date64 have no sub-day unit to scale into directly, and
        # a Timestamp(us) intermediate (an earlier fix here) reintroduces
        # exactly the overflow this whole function exists to avoid: a
        # whole-day epoch value that fits Date32's/Date64's own range
        # easily can still overflow int64 microseconds (200,000,000 days
        # is ~17.28e18 us). Floors straight to a whole-day tick instead --
        # matching pyarrow's own Timestamp -> Date cast, which floors
        # sub-day precision rather than rejecting it (verified) -- built
        # directly in field.type's own unit (days for Date32,
        # milliseconds for Date64, see _day_tick) rather than always
        # through a Date32 intermediate, which would cap every target at
        # Date32's narrower int32 range even when field.type is Date64.
        days = [
            None if t is None else _day_tick(field, row, t, wire_unit=wire_unit)
            for row, t in enumerate(ticks)
        ]
        return pa.array(days, type=field.type)
    if pa.types.is_time(field.type):
        narrow = pa.types.is_time32(field.type)
        scaled = [
            None
            if t is None
            else _scale_tick(
                field,
                row,
                t,
                wire_unit,
                field.type.unit,
                narrow=narrow,
                day_bound=True,
            )
            for row, t in enumerate(ticks)
        ]
        return pa.array(scaled, type=field.type)
    raise InvalidTypeMapError(
        f"column {field.name!r}: epoch ticks cannot build {field.type}; expected "
        f"a Timestamp, Date, Time, or Duration arrow_type"
    )


def parse_iso8601_scalar(
    field: pa.Field, row: int, v: str, *, is_ts: bool, is_date_type: bool, tz: Any
) -> Any:
    """Parse one ISO-8601 wire value into the datetime/date/time ``pa.array`` expects.

    Shared by the declared-encoding decoder (:func:`_decode_iso8601`) and
    the tolerant database ("columns") path
    (:func:`~cdk.schema_contract._parse_db_temporal_strings`), which has no
    ``encoding`` vocabulary to declare this parse with -- one parse, two
    callers, so they cannot drift on what "ISO-8601" means here.
    """
    try:
        if is_ts:
            dt = datetime.fromisoformat(v)
            if tz and dt.tzinfo is None:
                raise ValueError(f"value {v!r} is naive but column declares tz={tz!r}")
            if not tz and dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        if is_date_type:
            return date.fromisoformat(v[:10])
        return time.fromisoformat(v)
    except ValueError as exc:
        raise ValueError(
            f"column {field.name!r} at row {row}: cannot parse {v!r} as "
            f"{field.type}: {exc}"
        ) from exc


def _decode_iso8601(_config: Mapping[str, Any]) -> DecodeFn:
    """ISO-8601 text -> Timestamp/Date/Time.

    The retired implicit default, now only applied when a field names it.
    Capped at microsecond precision, matching ``datetime.fromisoformat``
    (and orjson's own retired native rendering this replaces): a
    fractional-second digit past the sixth is dropped the same way it
    always was, including for a column declared at nanosecond resolution.

    Built at microsecond resolution and safe-cast to ``field.type`` --
    same as every other decoder in this module (:func:`_ticks_to_array`,
    :func:`_decode_iso_duration`) -- rather than handed straight to
    ``pa.array(type=field.type)``: a Timestamp/Time column declared at a
    coarser unit than microseconds (``Timestamp(SECOND)``) must reject a
    value that does not fit evenly, not silently floor it, the same
    author intent ``epoch``/``iso_duration`` already refuse identically.
    Date has no finer unit to lose (pyarrow floors a Timestamp built from
    a ``date`` uniformly regardless of the source decoder), so it is
    built directly at ``field.type``.
    """

    def decode(field: pa.Field, values: list[Any]) -> pa.Array:
        is_ts = pa.types.is_timestamp(field.type)
        is_date_type = pa.types.is_date(field.type)
        is_time_type = pa.types.is_time(field.type)
        if not (is_ts or is_date_type or is_time_type):
            raise InvalidTypeMapError(
                f"column {field.name!r}: encoding 'iso8601' requires a "
                f"Timestamp, Date, or Time arrow_type, got {field.type}"
            )
        tz = field.type.tz if is_ts else None
        parsed: list[Any] = []
        for row, v in enumerate(values):
            if v is None:
                parsed.append(None)
                continue
            if not isinstance(v, str):
                raise ValueError(
                    f"column {field.name!r} at row {row}: encoding 'iso8601' "
                    f"expects a string, got {type(v).__name__}"
                )
            parsed.append(
                parse_iso8601_scalar(
                    field, row, v, is_ts=is_ts, is_date_type=is_date_type, tz=tz
                )
            )
        if is_ts:
            wire = pa.array(parsed, type=pa.timestamp("us", tz=tz))
            return pc.cast(wire, field.type, safe=True)
        if is_time_type:
            wire = pa.array(parsed, type=pa.time64("us"))
            return pc.cast(wire, field.type, safe=True)
        return pa.array(parsed, type=field.type)

    return decode


def _decode_epoch(config: Mapping[str, Any]) -> DecodeFn:
    unit = require_enum_param(config, "unit", EPOCH_UNITS, "encoding 'epoch'")

    def decode(field: pa.Field, values: list[Any]) -> pa.Array:
        return _ticks_to_array(field, _coerce_ticks(field.name, values), unit)

    return decode


def _decode_strptime(config: Mapping[str, Any]) -> DecodeFn:
    """``pc.strptime`` against a declared pattern.

    The retired ``source_format`` hatch, now a named, declared catalog
    entry rather than an unvalidated one.
    """
    pattern = require_str_param(config, "pattern", "encoding 'strptime'")

    def decode(field: pa.Field, values: list[Any]) -> pa.Array:
        for row, v in enumerate(values):
            if v is not None and not isinstance(v, str):
                raise TypeError(
                    f"column {field.name!r} at row {row}: encoding 'strptime' "
                    f"expects a string, got {type(v).__name__}"
                )
        string_col = pa.array(values, type=pa.string())
        unit = getattr(field.type, "unit", None) or (
            "us" if pa.types.is_timestamp(field.type) else "s"
        )
        parsed = pc.strptime(string_col, format=pattern, unit=unit)
        if parsed.type == field.type:
            return parsed
        tz = getattr(field.type, "tz", None)
        if tz and not getattr(parsed.type, "tz", None):
            parsed = pc.assume_timezone(parsed, tz)
        return pc.cast(parsed, field.type, safe=False)

    return decode


#: RE2 logs every compile failure to raw process stderr by default; suppressed
#: for the same reason ``cdk.type_map.rules``/``cdk.api.param_rules`` suppress
#: it, so a bad endpoint-authored pattern surfaces as the clean
#: ``InvalidTypeMapError`` below rather than an uncontrolled C++ log line.
_RE2_OPTIONS: Final = re2.Options()
_RE2_OPTIONS.log_errors = False


def _decode_regex_epoch(config: Mapping[str, Any]) -> DecodeFn:
    """Extract epoch ticks from a wrapper string via a capturing regex.

    Xero's ``/Date(1541176290160+0000)/``: ``pattern`` names the single
    capture group holding the ticks, ``unit`` the tick unit. Anything else in
    the wrapper -- Xero's decorative offset suffix -- is matched but not
    captured, so it is read and discarded rather than shifting the instant
    (ticks are always UTC per the MS/ASP.NET AJAX date convention).

    Compiled and matched with ``re2``, not stdlib ``re``: ``pattern`` is
    endpoint-authored, untrusted input, matched against every row of every
    batch, and RE2's linear-time guarantee is what bounds match time against
    an adversarial pattern (issue #504) -- the same policy already applied to
    every other author-declared regex in this engine
    (``cdk.type_map.rules.compile_pattern``, ``cdk.api.param_rules``).
    """
    pattern = require_str_param(config, "pattern", "encoding 'regex_epoch'")
    unit = require_enum_param(config, "unit", EPOCH_UNITS, "encoding 'regex_epoch'")
    try:
        compiled = re2.compile(pattern, options=_RE2_OPTIONS)
    except (re2.error, UnicodeEncodeError) as exc:
        raise InvalidTypeMapError(
            f"encoding 'regex_epoch' pattern {pattern!r} is not a valid "
            f"regular expression: {exc}"
        ) from exc
    if compiled.groups != 1:
        raise InvalidTypeMapError(
            f"encoding 'regex_epoch' pattern {pattern!r} must declare exactly "
            f"one capture group for the ticks, found {compiled.groups}"
        )

    def decode(field: pa.Field, values: list[Any]) -> pa.Array:
        ticks: list[int | None] = []
        for row, v in enumerate(values):
            if v is None:
                ticks.append(None)
                continue
            if not isinstance(v, str):
                raise ValueError(
                    f"column {field.name!r} at row {row}: encoding 'regex_epoch' "
                    f"expects a string, got {type(v).__name__}"
                )
            try:
                match = compiled.fullmatch(v)
            except UnicodeEncodeError:
                # A lone surrogate RE2's internal UTF-8 encode step cannot
                # interpret -- treated as "does not match", the same verdict
                # every other value this pattern refuses gets.
                match = None
            if match is None:
                raise ValueError(
                    f"column {field.name!r} at row {row}: {v!r} does not match "
                    f"encoding 'regex_epoch' pattern {pattern!r}"
                )
            ticks.append(int(match.group(1)))
        return _ticks_to_array(field, ticks, unit)

    return decode


def _decode_decimal(_config: Mapping[str, Any]) -> DecodeFn:
    def decode(field: pa.Field, values: list[Any]) -> pa.Array:
        if not pa.types.is_decimal(field.type):
            raise InvalidTypeMapError(
                f"column {field.name!r}: encoding 'decimal' requires a "
                f"Decimal128/Decimal256 arrow_type, got {field.type}"
            )
        converted: list[Decimal | None] = []
        for row, v in enumerate(values):
            if v is None:
                converted.append(None)
                continue
            try:
                converted.append(Decimal(str(v)))
            except InvalidOperation as exc:
                # decimal.InvalidOperation is neither a ValueError nor a
                # TypeMapError -- left uncaught, this is an authoring/wire
                # defect the worker's deterministic-error classifier
                # (src.worker.source_service) would never recognize,
                # misclassifying a repeatable bad-data failure as
                # retryable and re-reading the same record forever.
                raise ValueError(
                    f"column {field.name!r} at row {row}: {v!r} is not a "
                    f"valid decimal"
                ) from exc
        return pa.array(converted, type=field.type)

    return decode


def _decode_bool_map(config: Mapping[str, Any]) -> DecodeFn:
    true_values = require_list_param(config, "true_values", "encoding 'bool_map'")
    false_values = require_list_param(config, "false_values", "encoding 'bool_map'")
    if not true_values or not false_values:
        raise InvalidTypeMapError(
            "encoding 'bool_map' requires a non-empty 'true_values' and "
            "'false_values'"
        )
    overlap = set(true_values) & set(false_values)
    if overlap:
        raise InvalidTypeMapError(
            f"encoding 'bool_map': {list(overlap)!r} appear in both "
            f"'true_values' and 'false_values' -- a token cannot map to both"
        )

    def decode(field: pa.Field, values: list[Any]) -> pa.Array:
        mapped: list[bool | None] = []
        for row, v in enumerate(values):
            if v is None:
                mapped.append(None)
            elif v in true_values:
                mapped.append(True)
            elif v in false_values:
                mapped.append(False)
            else:
                raise ValueError(
                    f"column {field.name!r} at row {row}: {v!r} is not in "
                    f"encoding 'bool_map''s true_values {true_values} or "
                    f"false_values {false_values}"
                )
        return pa.array(mapped, type=field.type)

    return decode


def _decode_base64(_config: Mapping[str, Any]) -> DecodeFn:
    def decode(field: pa.Field, values: list[Any]) -> pa.Array:
        decoded: list[bytes | None] = []
        for row, v in enumerate(values):
            if v is None:
                decoded.append(None)
                continue
            if not isinstance(v, str):
                raise ValueError(
                    f"column {field.name!r} at row {row}: encoding 'base64' "
                    f"expects a string, got {type(v).__name__}"
                )
            try:
                decoded.append(base64.b64decode(v, validate=True))
            except ValueError as exc:
                raise ValueError(
                    f"column {field.name!r} at row {row}: {v!r} is not valid "
                    f"base64: {exc}"
                ) from exc
        return pa.array(decoded, type=field.type)

    return decode


#: Microseconds per ISO-8601 duration component. Applied directly to
#: ``isoduration``'s own ``Decimal`` fields, not through ``timedelta``:
#: ISO-8601 permits a decimal fraction on *any* one component (``PT1.5H``
#: is 90 minutes, not "1 hour" with a dropped 0.5), and ``isoduration``
#: parses every field as ``Decimal`` for exactly that reason, so
#: ``timedelta``'s integer-only weeks/days/hours/minutes parameters would
#: have to truncate first. ``timedelta`` is also bounded to
#: +/-999,999,999 days -- a wire value like ``P2000000000D`` is a
#: perfectly ordinary tick count in a coarse enough Duration unit, but
#: raises ``OverflowError`` constructing a ``timedelta`` at all. Unlike
#: the epoch encoder's chained, nested arithmetic that shipped a real
#: "1000x too small" bug (see ``TestEpochEncoderUnitArithmetic``), each
#: factor here multiplies exactly one already-isolated component by one
#: fixed, unambiguous conversion constant -- nothing chains or nests.
_MICROSECONDS_PER_DURATION_COMPONENT: Final[dict[str, int]] = {
    "weeks": 7 * 24 * 3600 * 1_000_000,
    "days": 24 * 3600 * 1_000_000,
    "hours": 3600 * 1_000_000,
    "minutes": 60 * 1_000_000,
    "seconds": 1_000_000,
}

#: Decimal context precision for parsing and scaling one duration string.
#: The default (28 significant digits) is ambient, not a cap this
#: function alone controls: ``isoduration``'s own internal arithmetic on
#: a duration's Decimal components respects it too, so an unusually
#: precise fractional-seconds string can already be silently rounded
#: inside ``parse_duration`` itself, before ``parsed.time.seconds`` is
#: ever multiplied here. 50 digits is comfortably past any realistic
#: duration text while still bounding the arithmetic cost of a
#: pathological one.
_DURATION_DECIMAL_PRECISION: Final[int] = 50


def _iso_duration_ticks(
    field: pa.Field,
    row: int,
    v: str,
    parse_duration: Callable[[str], Any],
    duration_parsing_exception: type[Exception],
) -> int:
    """Parse one ISO-8601 duration string into a signed microsecond tick count.

    Parsed by ``isoduration`` (an actual conformant implementation of the
    standard) rather than a hand-rolled grammar. Capped at microsecond
    precision, matching every other temporal value this catalog decodes
    through a Python stdlib type (``datetime``/``timedelta`` hold no
    finer): a fractional microsecond beyond the sixth digit is rounded
    (half-to-even), not chased. Scaling to ``field.type``'s own declared
    unit -- and rejecting a value that does not fit evenly into a coarser
    one -- is :func:`_scale_tick`'s job (called with wire_unit
    ``"MICROSECOND"``), not this function's.
    """
    if not isinstance(v, str):
        raise ValueError(
            f"column {field.name!r} at row {row}: encoding "
            f"'iso_duration' expects a string, got {type(v).__name__}"
        )
    # Widened, not the default 28-digit ambient context: isoduration's own
    # internal Decimal arithmetic respects whatever context is active when
    # it runs, so parse_duration itself -- not only the multiply below --
    # can silently round an unusually precise fractional-seconds string.
    with localcontext() as ctx:
        ctx.prec = _DURATION_DECIMAL_PRECISION
        try:
            parsed = parse_duration(v)
        except duration_parsing_exception as exc:
            raise ValueError(
                f"column {field.name!r} at row {row}: {v!r} is not a valid "
                f"ISO-8601 duration this decoder supports (weeks, days, "
                f"and clock components only -- no calendar Y/M): {exc}"
            ) from exc
        if parsed.date.years or parsed.date.months:
            # A Duration is a fixed physical length; a calendar year or
            # month is not (a month is 28-31 days depending which one), so
            # neither has a tick count to convert to.
            raise ValueError(
                f"column {field.name!r} at row {row}: {v!r} names a "
                f"calendar year/month component, which this decoder does "
                f"not support -- a Duration is a fixed physical length and "
                f"a month has none"
            )
        total_micros = (
            parsed.date.weeks * _MICROSECONDS_PER_DURATION_COMPONENT["weeks"]
            + parsed.date.days * _MICROSECONDS_PER_DURATION_COMPONENT["days"]
            + parsed.time.hours * _MICROSECONDS_PER_DURATION_COMPONENT["hours"]
            + parsed.time.minutes * _MICROSECONDS_PER_DURATION_COMPONENT["minutes"]
            + parsed.time.seconds * _MICROSECONDS_PER_DURATION_COMPONENT["seconds"]
        )
        return int(total_micros.to_integral_value(rounding=ROUND_HALF_EVEN))


def _decode_iso_duration(_config: Mapping[str, Any]) -> DecodeFn:
    try:
        from isoduration import parse_duration
        from isoduration.parser.exceptions import DurationParsingException
    except ImportError as exc:
        reraise_for_missing_extra(
            exc,
            feature="encoding 'iso_duration'",
            extra="api",
            modules=("isoduration",),
        )

    def decode(field: pa.Field, values: list[Any]) -> pa.Array:
        if not pa.types.is_duration(field.type):
            raise InvalidTypeMapError(
                f"column {field.name!r}: encoding 'iso_duration' requires a "
                f"Duration arrow_type, got {field.type}"
            )
        ticks = [
            None
            if v is None
            else _scale_tick(
                field,
                row,
                _iso_duration_ticks(
                    field, row, v, parse_duration, DurationParsingException
                ),
                "MICROSECOND",
                field.type.unit,
                narrow=False,
            )
            for row, v in enumerate(values)
        ]
        return pa.array(ticks, type=field.type)

    return decode


_DECODER_FACTORIES: Final[dict[str, Callable[[Mapping[str, Any]], DecodeFn]]] = {
    "iso8601": _decode_iso8601,
    "epoch": _decode_epoch,
    "strptime": _decode_strptime,
    "regex_epoch": _decode_regex_epoch,
    "decimal": _decode_decimal,
    "bool_map": _decode_bool_map,
    "base64": _decode_base64,
    "iso_duration": _decode_iso_duration,
}


def resolve_decoder(field_def: Mapping[str, Any], field: pa.Field) -> DecodeFn | None:
    """Build the decode function a field's declared ``encoding`` names.

    Returns ``None`` for an undeclared field (the caller decides whether
    that is an error via :data:`cdk.type_map.decoders.REQUIRES_ENCODING_KINDS`)
    and for :data:`~cdk.type_map.decoders.CODE_ENCODING_NAME`, which the
    caller must route to ``ApiDialect.decode_field`` before calling this --
    there is no catalog function backing it.
    """
    encoding = field_def.get("encoding")
    if encoding is None:
        return None
    if not isinstance(encoding, Mapping):
        raise InvalidTypeMapError(
            f"field {field.name!r}: 'encoding' must be an object, got "
            f"{type(encoding).__name__}"
        )
    name = encoding.get("name")
    if name == CODE_ENCODING_NAME:
        return None
    if not isinstance(name, str) or name not in _DECODER_FACTORIES:
        raise InvalidTypeMapError(
            f"unknown encoding name {name!r} on field {field.name!r}; expected "
            f"one of {', '.join([*_DECODER_FACTORIES, CODE_ENCODING_NAME])}"
        )
    factory = _DECODER_FACTORIES[name]
    config = {k: v for k, v in encoding.items() if k != "name"}
    allowed = {p.name for p in DECODER_PARAMS[name]}
    unknown = set(config) - allowed
    if unknown:
        raise InvalidTypeMapError(
            f"encoding {name!r} on field {field.name!r}: unknown parameter(s) "
            f"{sorted(unknown)!r}; a factory silently ignores a key it does "
            f"not read, so a misspelled or unpublished parameter would "
            f"otherwise apply a different wire format than the one declared"
        )
    return factory(config)
