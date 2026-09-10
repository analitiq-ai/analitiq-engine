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
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
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
    field's declared type.
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


def _ticks_to_array(
    field: pa.Field, ticks: list[int | None], wire_unit: str
) -> pa.Array:
    """Build ``field.type`` from epoch ticks in *wire_unit*.

    Built through the wire unit's own pyarrow type and cast to ``field.type``
    (``pc.cast(safe=True)``) rather than hand-computed, so a unit mismatch
    between the wire and the declared type (ms ticks into a Timestamp(us)
    column, or seconds-since-midnight into a Time64(MICROSECOND) column) is
    the same safe-cast every other boundary in this package uses, not a
    second arithmetic implementation that could disagree with it.
    """
    if wire_unit == "DAY":
        if not pa.types.is_date(field.type):
            raise InvalidTypeMapError(
                f"column {field.name!r}: epoch unit 'DAY' only applies to a "
                f"Date32/Date64 arrow_type, got {field.type}"
            )
        return pc.cast(pa.array(ticks, type=pa.date32()), field.type, safe=True)
    short = UNIT_LONG_TO_SHORT[wire_unit]
    if pa.types.is_timestamp(field.type):
        naive = pa.array(ticks, type=pa.timestamp(short))
        if field.type.tz is not None:
            # Epoch ticks are an absolute UTC instant, not local wall-clock
            # time in the target zone -- assume_timezone(naive, tz) would
            # instead reinterpret the tick count as already being local time
            # in `tz`, shifting the instant by the zone's offset.
            naive = pc.assume_timezone(naive, "UTC")
        return pc.cast(naive, field.type, safe=True)
    if pa.types.is_date(field.type):
        naive = pa.array(ticks, type=pa.timestamp(short))
        return pc.cast(naive, field.type, safe=True)
    if pa.types.is_duration(field.type):
        return pc.cast(pa.array(ticks, type=pa.duration(short)), field.type, safe=True)
    if pa.types.is_time(field.type):
        wire_time_type = pa.time32(short) if short in ("s", "ms") else pa.time64(short)
        return pc.cast(pa.array(ticks, type=wire_time_type), field.type, safe=True)
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
    finer): a fractional second beyond the sixth digit is dropped, not
    chased. Scaling to ``field.type``'s own declared unit is left to
    pyarrow's own safe cast in :func:`_decode_iso_duration` -- a value
    that does not fit evenly into a coarser unit is rejected there, the
    same mechanism :func:`_ticks_to_array` already relies on for epoch.
    """
    if not isinstance(v, str):
        raise ValueError(
            f"column {field.name!r} at row {row}: encoding "
            f"'iso_duration' expects a string, got {type(v).__name__}"
        )
    try:
        parsed = parse_duration(v)
    except duration_parsing_exception as exc:
        raise ValueError(
            f"column {field.name!r} at row {row}: {v!r} is not a valid "
            f"ISO-8601 duration this decoder supports (weeks, days, and "
            f"clock components only -- no calendar Y/M): {exc}"
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
    # timedelta's own constructor converts weeks/days/hours/minutes/seconds
    # to a single duration internally -- not hand-multiplied conversion
    # factors (604800/86400/3600/60), the same class of arithmetic that
    # shipped a real bug once already in this catalog's epoch encoder
    # (see TestEpochEncoderUnitArithmetic). weeks/days/hours/minutes are
    # always whole in the ISO-8601 grammar (only seconds may carry a
    # fraction); ``float`` on the seconds Decimal is exact enough for the
    # microsecond resolution timedelta itself caps at, and timedelta //
    # timedelta(microseconds=1) is Python's own exact integer division.
    delta = timedelta(
        weeks=int(parsed.date.weeks),
        days=int(parsed.date.days),
        hours=int(parsed.time.hours),
        minutes=int(parsed.time.minutes),
        seconds=float(parsed.time.seconds),
    )
    return delta // timedelta(microseconds=1)


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
            else _iso_duration_ticks(
                field, row, v, parse_duration, DurationParsingException
            )
            for row, v in enumerate(values)
        ]
        return pc.cast(pa.array(ticks, type=pa.duration("us")), field.type, safe=True)

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
