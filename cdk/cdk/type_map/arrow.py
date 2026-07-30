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
from .grammar import (
    ARROW_FAMILIES,
    UNIT_LONG_TO_SHORT,
    ArrowFamily,
    UnitParam,
    bind_parameters,
)

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
    provide is a defect in the table, and surfacing it here beats a family
    that silently builds nothing or matches no live column.
    """
    attribute = getattr(module, name, None)
    if not callable(attribute):
        raise InvalidTypeMapError(
            f"arrow_type family {family!r} names {module.__name__}.{name}, "
            f"which this pyarrow build does not provide"
        )
    resolved: Callable[..., Any] = attribute
    return resolved


def _build_arrow_type(
    family: str, spec: ArrowFamily, values: Mapping[str, Any]
) -> pa.DataType:
    """Build the pyarrow type for *family* from its bound parameter values.

    The declared parameters are passed to the declared factory positionally, in
    declaration order — the order every pyarrow factory takes them in
    (``time32(unit)``, ``timestamp(unit, tz)``, ``decimal128(precision,
    scale)``) — with units translated to the short codes pyarrow expects.
    """
    if spec.builder is None:
        raise InvalidTypeMapError(
            f"arrow_type family {family!r} declares no pyarrow factory"
        )
    factory = _pyarrow_attribute(pa, spec.builder, family)
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
    probes each family declares in :data:`cdk.type_map.grammar.ARROW_FAMILIES`
    are ``pyarrow.types`` predicates on the type id, so they are mutually
    exclusive and the table order does not decide the answer. Width and
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
    for family, spec in ARROW_FAMILIES.items():
        for probe in spec.probes:
            if _pyarrow_attribute(pa.types, probe, family)(dtype):
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
