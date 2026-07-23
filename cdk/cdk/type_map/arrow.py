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
from .grammar import STRUCTURAL_FAMILIES, UNIT_LONG_TO_SHORT, bind_parameters

_PARAM_SPLIT: Final[Pattern[str]] = re.compile(r"\s*,\s*")


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


# Family -> pyarrow factory over the values bind_parameters resolved. Keyed on
# the same family set as the grammar table; the conformance test pins the two
# key sets equal so a family cannot be declared without being buildable.
_FACTORIES: Final[dict[str, Callable[[Mapping[str, Any]], pa.DataType]]] = {
    "Null": lambda v: pa.null(),
    "Boolean": lambda v: pa.bool_(),
    "Int8": lambda v: pa.int8(),
    "Int16": lambda v: pa.int16(),
    "Int32": lambda v: pa.int32(),
    "Int64": lambda v: pa.int64(),
    "UInt8": lambda v: pa.uint8(),
    "UInt16": lambda v: pa.uint16(),
    "UInt32": lambda v: pa.uint32(),
    "UInt64": lambda v: pa.uint64(),
    "Float16": lambda v: pa.float16(),
    "Float32": lambda v: pa.float32(),
    "Float64": lambda v: pa.float64(),
    "Utf8": lambda v: pa.string(),
    "LargeUtf8": lambda v: pa.large_string(),
    "Binary": lambda v: pa.binary(),
    "LargeBinary": lambda v: pa.large_binary(),
    "Date32": lambda v: pa.date32(),
    "Date64": lambda v: pa.date64(),
    "Time32": lambda v: pa.time32(UNIT_LONG_TO_SHORT[v["unit"]]),
    "Time64": lambda v: pa.time64(UNIT_LONG_TO_SHORT[v["unit"]]),
    "Duration": lambda v: pa.duration(UNIT_LONG_TO_SHORT[v["unit"]]),
    "Timestamp": lambda v: pa.timestamp(UNIT_LONG_TO_SHORT[v["unit"]], tz=v["tz"]),
    "Decimal128": lambda v: pa.decimal128(v["precision"], v["scale"]),
    "Decimal256": lambda v: pa.decimal256(v["precision"], v["scale"]),
    "FixedSizeBinary": lambda v: pa.binary(v["byte_width"]),
    # Opaque JSON blob — shape not declared. Carried over the wire as a
    # JSON-encoded string; destinations json.loads it back to a dict/list at
    # the write boundary.
    "Json": lambda v: pa.large_string(),
}


def parse_arrow_type(canonical: str) -> pa.DataType:
    """Parse an Arrow type string into a PyArrow ``DataType``.

    The parameter grammar — allowed units, integer ranges, the timezone forms
    — comes from :data:`cdk.type_map.grammar.ARROW_TYPE_GRAMMAR`, the same
    table the published ``arrow_type_grammar.json`` renders from. Raises
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
    head, args = _parse_head(canonical)
    if head in STRUCTURAL_FAMILIES:
        raise InvalidTypeMapError(
            f"arrow_type {head!r} describes a nested type and cannot be "
            f"parsed in isolation; SchemaContract reads the property's "
            f"'properties' (Object) or 'items' (List) sub-schema to build it"
        )
    factory = _FACTORIES.get(head)
    if factory is None:
        raise InvalidTypeMapError(
            f"arrow_type family {head!r} (from {canonical!r}) is not supported"
        )
    return factory(bind_parameters(head, args))


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
