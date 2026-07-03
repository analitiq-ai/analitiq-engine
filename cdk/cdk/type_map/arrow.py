"""Arrow type string ↔ PyArrow ``DataType``.

:func:`parse_arrow_type` handles scalar types only — nested ``Object`` /
``List`` markers need the field's sub-schema (``properties`` / ``items``)
which only :func:`resolve_arrow_type` has access to.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from re import Pattern
from typing import Any, Final

import pyarrow as pa

from .conversions import Conversion, classify_conversion
from .exceptions import InvalidTypeMapError

_PARAM_SPLIT: Final[Pattern[str]] = re.compile(r"\s*,\s*")


_UNIT_ALIASES: Final[dict[str, str]] = {
    "SECOND": "s",
    "MILLISECOND": "ms",
    "MICROSECOND": "us",
    "NANOSECOND": "ns",
}


def _normalize_unit(unit: str) -> str:
    """Map the schema's long unit names to PyArrow's short codes."""
    return _UNIT_ALIASES.get(unit, unit)


def _parse_head(canonical: str) -> tuple[str, tuple[str, ...]]:
    """Split ``Name(arg1, arg2)`` into (``Name``, (``arg1``, ``arg2``))."""
    trimmed = canonical.strip()
    if "(" not in trimmed:
        return trimmed, ()
    if not trimmed.endswith(")"):
        raise InvalidTypeMapError(
            f"canonical type {canonical!r} has unbalanced parentheses"
        )
    head, _, rest = trimmed.partition("(")
    body = rest[:-1]
    if not body.strip():
        return head.strip(), ()
    args = tuple(part.strip() for part in _PARAM_SPLIT.split(body))
    return head.strip(), args


def parse_arrow_type(canonical: str) -> pa.DataType:
    """Parse an Arrow type string into a PyArrow ``DataType``.

    Raises :class:`InvalidTypeMapError` for malformed input or unsupported
    families. The matcher is deliberately strict — an unknown family
    indicates an author-time mistake that should surface loudly.

    Nested-type markers (``Object``, ``List``) are intentionally rejected
    here: they need the property's sub-schema, which only the
    :class:`SchemaContract` walker has access to.
    """
    head, args = _parse_head(canonical)
    match head:
        case "Null":
            return pa.null()
        case "Boolean":
            return pa.bool_()
        case "Int8":
            return pa.int8()
        case "Int16":
            return pa.int16()
        case "Int32":
            return pa.int32()
        case "Int64":
            return pa.int64()
        case "UInt8":
            return pa.uint8()
        case "UInt16":
            return pa.uint16()
        case "UInt32":
            return pa.uint32()
        case "UInt64":
            return pa.uint64()
        case "Float16":
            return pa.float16()
        case "Float32":
            return pa.float32()
        case "Float64":
            return pa.float64()
        case "Utf8":
            return pa.string()
        case "LargeUtf8":
            return pa.large_string()
        case "Binary":
            return pa.binary()
        case "LargeBinary":
            return pa.large_binary()
        case "Date32":
            return pa.date32()
        case "Date64":
            return pa.date64()
        case "Time32":
            return pa.time32(_require_unit(args, head, ("s", "ms")))
        case "Time64":
            return pa.time64(_require_unit(args, head, ("us", "ns")))
        case "Duration":
            return pa.duration(_require_unit(args, head, ("s", "ms", "us", "ns")))
        case "Timestamp":
            return _parse_timestamp(args)
        case "Decimal128":
            return _parse_decimal(args, pa.decimal128, head)
        case "Decimal256":
            return _parse_decimal(args, pa.decimal256, head)
        case "FixedSizeBinary":
            return _parse_fixed_binary(args, head)
        case "Json":
            # Opaque JSON blob — shape not declared. Carried over the wire as
            # a JSON-encoded string; destinations json.loads it back to a
            # dict/list at the write boundary.
            return pa.large_string()
        case "Object" | "List":
            raise InvalidTypeMapError(
                f"arrow_type {head!r} describes a nested type and cannot be "
                f"parsed in isolation; SchemaContract reads the property's "
                f"'properties' (Object) or 'items' (List) sub-schema to build it"
            )
    raise InvalidTypeMapError(
        f"arrow_type family {head!r} (from {canonical!r}) is not supported"
    )


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


def _require_unit(args: tuple[str, ...], head: str, allowed: tuple[str, ...]) -> str:
    if len(args) != 1:
        raise InvalidTypeMapError(
            f"{head}{args} requires exactly one unit from {allowed}"
        )
    unit = _normalize_unit(args[0])
    if unit not in allowed:
        raise InvalidTypeMapError(
            f"{head}{args} requires exactly one unit from {allowed}"
        )
    return unit


def _parse_timestamp(args: tuple[str, ...]) -> pa.DataType:
    if not args:
        raise InvalidTypeMapError(
            "Timestamp requires at least a unit (e.g. Timestamp(MICROSECOND))"
        )
    unit = _normalize_unit(args[0])
    if unit not in ("s", "ms", "us", "ns"):
        raise InvalidTypeMapError(
            f"Timestamp unit must be one of "
            f"SECOND/MILLISECOND/MICROSECOND/NANOSECOND, got {args[0]!r}"
        )
    tz = args[1] if len(args) > 1 and args[1] and args[1] != "null" else None
    return pa.timestamp(unit, tz=tz)


def _parse_decimal(
    args: tuple[str, ...],
    factory: Callable[[int, int], pa.DataType],
    head: str,
) -> pa.DataType:
    if len(args) != 2:
        raise InvalidTypeMapError(f"{head} requires (precision, scale); got {args}")
    try:
        precision = int(args[0])
        scale = int(args[1])
    except ValueError as err:
        raise InvalidTypeMapError(
            f"{head}({', '.join(args)}) has non-integer parameters"
        ) from err
    return factory(precision, scale)


def _parse_fixed_binary(args: tuple[str, ...], head: str) -> pa.DataType:
    if len(args) != 1:
        raise InvalidTypeMapError(f"{head} requires (byte_width); got {args}")
    try:
        width = int(args[0])
    except ValueError as err:
        raise InvalidTypeMapError(
            f"{head}({args[0]}) byte_width is not an integer"
        ) from err
    return pa.binary(width)


# Ordered (predicate, family) probes mapping a live DataType back to its
# conversion-matrix family. Probes are mutually exclusive and first-match-wins.
# Width/parameter detail is intentionally dropped: a DataType collapses to the
# family head conversions.classify_conversion keys its policy on (int32 ->
# "Int32", timestamp[us, tz=UTC] -> "Timestamp"). Kept in step with the family
# heads parse_arrow_type produces; list and large_list both fold to "List", the
# single nested-list family the contract emits.
_FAMILY_PROBES: Final[tuple[tuple[Callable[[pa.DataType], bool], str], ...]] = (
    (pa.types.is_null, "Null"),
    (pa.types.is_boolean, "Boolean"),
    (pa.types.is_int8, "Int8"),
    (pa.types.is_int16, "Int16"),
    (pa.types.is_int32, "Int32"),
    (pa.types.is_int64, "Int64"),
    (pa.types.is_uint8, "UInt8"),
    (pa.types.is_uint16, "UInt16"),
    (pa.types.is_uint32, "UInt32"),
    (pa.types.is_uint64, "UInt64"),
    (pa.types.is_float16, "Float16"),
    (pa.types.is_float32, "Float32"),
    (pa.types.is_float64, "Float64"),
    (pa.types.is_string, "Utf8"),
    (pa.types.is_large_string, "LargeUtf8"),
    (pa.types.is_fixed_size_binary, "FixedSizeBinary"),
    (pa.types.is_large_binary, "LargeBinary"),
    (pa.types.is_binary, "Binary"),
    (pa.types.is_date32, "Date32"),
    (pa.types.is_date64, "Date64"),
    (pa.types.is_time32, "Time32"),
    (pa.types.is_time64, "Time64"),
    (pa.types.is_timestamp, "Timestamp"),
    (pa.types.is_duration, "Duration"),
    (pa.types.is_decimal128, "Decimal128"),
    (pa.types.is_decimal256, "Decimal256"),
    (pa.types.is_struct, "Object"),
    (pa.types.is_list, "List"),
    (pa.types.is_large_list, "List"),
)


def arrow_family(dtype: pa.DataType) -> str:
    """Return the conversion-matrix family name for a PyArrow ``DataType``.

    The inverse of the family head :func:`parse_arrow_type` consumes. An
    unrecognised type raises :class:`InvalidTypeMapError` rather than resolve to
    a silent default -- conversions classified against an unknown family would
    be meaningless.
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
