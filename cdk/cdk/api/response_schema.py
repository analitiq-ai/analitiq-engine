"""The per-record schema a read yields, and the Arrow type of every field.

The API analogue of ``cdk.sql.discovery``: walk the declared response JSON
Schema to the per-record items schema, then fill each field's
``arrow_type`` from the scope-correct read type-map. An API endpoint
declares per-field JSON ``type``/``format`` and ships a
``type-map-read.json`` -- the same read type-map the database source path
consumes -- so one vocabulary covers both families.

Imports the type-map surface, not ``pyarrow``: this module produces the
annotated schema, and ``SchemaContract`` turns it into Arrow.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from analitiq.contracts.endpoints import ResponseExtraction
from analitiq.contracts.stream import EndpointRef

from ..exceptions import ReadError
from ..type_map import TypeMapper, UnmappedTypeError
from ..types import EndpointScope
from .records import split_records_ref

__all__ = [
    "apply_read_type_map",
    "FieldDeclaration",
    "declared_json_types",
    "record_field_declaration",
    "records_items_schema",
    "resolve_field_arrow_type",
]


def records_items_schema(
    endpoint_id: str, response_block: ResponseExtraction
) -> dict[str, Any]:
    """Walk the declared response schema to the per-record items schema.

    The path comes from :func:`cdk.api.records.split_records_ref` -- the
    same parser the live payload walk uses, so the schema and the data can
    never be read from two different places in the body. The response
    schema itself is free-form JSON Schema in the contract, so the walk
    stays dict-shaped even though the block around it is a model.

    The answer is a deep copy because :func:`apply_read_type_map`
    annotates each field in place, and this subtree is reached from an
    endpoint document the connector holds for its whole life: annotating
    the document itself would let the first read's type-map scope decide
    the Arrow types every later read sees. The model is frozen, but a
    ``dict[str, Any]`` field's contents are not, so freezing is no
    protection here.
    """
    node: Any = response_block.schema_
    records_ref = response_block.records.ref
    for field in split_records_ref(records_ref):
        properties = node.get("properties") if isinstance(node, dict) else None
        if not isinstance(properties, dict) or field not in properties:
            available = sorted(properties) if isinstance(properties, dict) else []
            raise ReadError(
                f"endpoint {endpoint_id!r}: records.ref {records_ref!r} "
                f"references field {field!r} that is not declared under "
                f"properties; available: {available}"
            )
        node = properties[field]

    items = (
        node.get("items")
        if isinstance(node, dict) and node.get("type") == "array"
        else node
    )
    if not isinstance(items, dict) or not items.get("properties"):
        raise ReadError(
            f"endpoint {endpoint_id!r}: cannot resolve the record schema at "
            f"{records_ref!r} (no 'properties' under the addressed items)"
        )
    return deepcopy(items)


def declared_json_types(field: dict[str, Any]) -> list[str]:
    """Read the non-null JSON types a field's ``type`` declares, in declared order.

    One reading of JSON Schema's ``type`` for every consumer here: a plain
    string is one type, a list is a union whose ``null`` member only says
    the field is nullable -- ``["string", "null"]`` is a string field. A
    ``type`` that is neither yields nothing.
    """
    declared = field.get("type")
    if isinstance(declared, str):
        return [declared]
    if isinstance(declared, list):
        return [t for t in declared if isinstance(t, str) and t != "null"]
    return []


@dataclass(frozen=True)
class FieldDeclaration:
    """What the record schema says a field holds: its JSON type and format.

    Both are read in one walk of the field so no consumer pairs a type
    from one reading with a format from another. ``format`` is whatever
    the schema declares, or ``None``; which formats mean anything is the
    consumer's vocabulary to apply.
    """

    json_type: str
    format: str | None


def record_field_declaration(
    endpoint_id: str, items_schema: dict[str, Any], cursor_field: str
) -> FieldDeclaration:
    """Read the JSON type and format the record schema declares for the cursor field.

    The stored cursor is the last record's value for this field, so its
    declared type is what says how a checkpoint reads back, and its
    declared format names the unit an integer moment is stored in. A
    nullable declaration (``["integer", "null"]``) is the one type it
    names: the checkpoint never stores ``None``, so null says nothing
    about how a stored value reads. A cursor field the schema does not
    declare, or declares with no or several real types, is an authoring
    defect named here rather than a value guessed at later.
    """
    field = (items_schema.get("properties") or {}).get(cursor_field)
    if not isinstance(field, dict):
        raise ReadError(
            f"endpoint {endpoint_id!r}: cursor field {cursor_field!r} is not "
            f"declared under the record schema's properties"
        )
    types = declared_json_types(field)
    if len(types) != 1:
        raise ReadError(
            f"endpoint {endpoint_id!r}: cursor field {cursor_field!r} declares "
            f"type {field.get('type')!r}; a cursor field needs one plain JSON "
            f"type, nullable or not"
        )
    fmt = field.get("format")
    return FieldDeclaration(types[0], fmt if isinstance(fmt, str) and fmt else None)


def apply_read_type_map(
    items_schema: dict[str, Any],
    endpoint_ref: EndpointRef,
    runtime: Any,
) -> None:
    """Resolve each record field's ``arrow_type`` from the read type-map.

    ``SchemaContract`` requires an explicit ``arrow_type`` per field and
    recurses into ``Object``/``List`` children, so resolution walks nested
    ``properties``/``items`` too. A field that already declares an
    ``arrow_type`` keeps it, so a hand-annotated connector stays valid and
    the mapper is only consulted when a field needs one; an unmapped JSON
    type fails loud naming the field.

    The mapper is chosen by the endpoint's scope so a connection-scoped
    endpoint's ``type-map-read.json`` composes over the connector defaults,
    matching the database path. A missing or invalid type-map is a
    deterministic config defect, so it surfaces as a :class:`ReadError`
    rather than the raw ``RuntimeError`` the worker would classify as
    retryable.

    ``endpoint_ref`` is the stream document's ``scope``-discriminated ref,
    parsed by the read's own funnel. A ref with no scope no longer reaches
    here: the union has no such member, so the parse refuses it before the
    read addresses anything. ``EndpointScope(scope)`` still stands between
    the contract's vocabulary and this CDK's, and raises on a scope the CDK
    has no mapper family for.
    """
    scope = endpoint_ref.scope

    mapper: TypeMapper | None = None

    def get_mapper() -> TypeMapper:
        # Resolved lazily: an endpoint that hand-annotates every field never
        # needs a type-map at all.
        nonlocal mapper
        if mapper is None:
            try:
                mapper = runtime.type_mapper_for(scope=EndpointScope(scope))
            except (RuntimeError, ValueError) as err:
                raise ReadError(
                    f"no usable read type-map for {scope!r}-scoped endpoint; a "
                    f"field needs arrow_type resolution but the type-map is "
                    f"absent or invalid"
                ) from err
        return mapper

    for name, prop in (items_schema.get("properties") or {}).items():
        if isinstance(prop, dict):
            resolve_field_arrow_type(prop, name, get_mapper)


def resolve_field_arrow_type(
    field: dict[str, Any],
    name: str,
    get_mapper: Callable[[], TypeMapper],
) -> None:
    """Fill ``field['arrow_type']`` from the type-map if absent, then recurse.

    Recursion is gated to the resolved ``arrow_type`` exactly as
    ``SchemaContract.resolve_arrow_type`` does: it descends into
    ``properties`` only for ``Object`` and into ``items`` only for ``List``,
    and treats everything else -- including a ``Json`` blob that keeps
    ``properties``/``items`` for documentation -- as a leaf. A nested child
    authored with only JSON ``type``/``format`` under a real
    ``Object``/``List`` must be resolved here too, or the schema build
    fails; descending into a ``Json`` blob's documentary children would
    instead fail a read on a child type the schema build never consults.
    Recursion runs even when a container already carries an ``arrow_type``,
    because a hand-annotated container can still hold children that do not.
    """
    if not field.get("arrow_type"):
        json_type = next(iter(declared_json_types(field)), None)
        if json_type is not None:
            fmt = field.get("format")
            native = f"{json_type}:{fmt}" if isinstance(fmt, str) and fmt else json_type
            try:
                field["arrow_type"] = get_mapper().to_arrow_type(native)
            except UnmappedTypeError as err:
                raise ReadError(
                    f"field {name!r}: JSON type {native!r} has no rule in the "
                    f"endpoint's read type-map"
                ) from err
    arrow_type = field.get("arrow_type")
    if arrow_type == "Object":
        nested = field.get("properties")
        if isinstance(nested, dict):
            for child_name, child in nested.items():
                if isinstance(child, dict):
                    resolve_field_arrow_type(child, f"{name}.{child_name}", get_mapper)
    elif arrow_type == "List":
        items = field.get("items")
        if isinstance(items, dict):
            resolve_field_arrow_type(items, f"{name}[]", get_mapper)
