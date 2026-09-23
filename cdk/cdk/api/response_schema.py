"""The per-record schema a read yields, and the Arrow type of every field.

The API analogue of ``cdk.sql.discovery``: walk the declared response JSON
Schema to the per-record items schema, then fill each field's
``arrow_type`` from the scope-correct read type-map. An API endpoint
declares per-field JSON ``type``/``format`` and ships a read type map
-- the same read type-map the database source path
consumes -- so one vocabulary covers both families.

Imports the type-map surface, not ``pyarrow``: this module produces the
annotated schema, and ``SchemaContract`` turns it into Arrow.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from analitiq.contracts.endpoints import (
    ResponseExtraction,
    find_record_field_properties,
    materialize_node,
    resolve_read_record_schema,
)
from analitiq.contracts.stream import EndpointRef

from ..exceptions import ReadError
from ..type_map import TypeMapper, UnmappedTypeError
from ..types import EndpointScope

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
    """Resolve the declared ``records`` ref to the per-record items schema.

    The walk is the contract's own :func:`resolve_read_record_schema`,
    the record-locator every consumer of the read contract shares, so a
    path composed through ``$ref``/``$defs``/``allOf`` resolves here exactly
    as it did at document load. Each field declaration comes back folded
    over its ``$ref`` target and ``allOf`` branches by the contract's
    :func:`find_record_field_properties`, so a field declared by reference
    carries the type an inline one would. Children below a field are
    folded as :func:`resolve_field_arrow_type` reaches them.

    ``None`` from the locator means the ref addressed nothing resolvable.
    That is a refusal, never the response schema itself: the schema is the
    ENVELOPE, and handing it back would enumerate ``data``/``next_cursor``
    as the record's fields.

    The answer is a deep copy because :func:`apply_read_type_map`
    annotates each field in place, and this subtree is reached from an
    endpoint document the connector holds for its whole life: annotating
    the document itself would let the first read's type-map scope decide
    the Arrow types every later read sees. The model is frozen, but a
    ``dict[str, Any]`` field's contents are not, so freezing is no
    protection here.
    """
    records_ref = response_block.records.ref
    response_schema = response_block.schema_
    record: Any = resolve_read_record_schema(records_ref, response_schema)
    if not isinstance(record, dict) or not record.get("properties"):
        raise ReadError(
            f"endpoint {endpoint_id!r}: records.ref {records_ref!r} does not "
            f"resolve to a record schema in the declared response schema "
            f"(no object with 'properties' under the addressed path)"
        )
    fields = find_record_field_properties(record, response_schema)
    return deepcopy({**record, "properties": fields})


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
    response_schema: Any,
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
    endpoint's read type map composes over the connector defaults,
    matching the database path. A missing or invalid type-map is a
    deterministic config defect, so it surfaces as a :class:`ReadError`
    rather than the raw ``RuntimeError`` the worker would classify as
    retryable.

    ``endpoint_ref`` is the stream document's ``scope``-discriminated ref,
    parsed by the read's own funnel. A ref with no scope never reaches
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
                raise ReadError(_no_read_type_map(scope)) from err
            if not mapper.has_read_map:
                raise ReadError(_no_read_type_map(scope))
        return mapper

    for name, prop in (items_schema.get("properties") or {}).items():
        if isinstance(prop, dict):
            resolve_field_arrow_type(prop, name, get_mapper, response_schema)


def _no_read_type_map(scope: str) -> str:
    return (
        f"no usable read type-map for {scope!r}-scoped endpoint; a field needs "
        f"arrow_type resolution but the read type-map is absent or invalid"
    )


def resolve_field_arrow_type(
    field: dict[str, Any],
    name: str,
    get_mapper: Callable[[], TypeMapper],
    response_schema: Any,
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

    Each child is folded over its ``$ref``/``allOf`` contributors against
    ``response_schema`` only when the walk descends into it, because the
    contract lets a ``$defs`` entry refer to itself: folding every child up
    front would never end on such a document, while a walk gated like the
    schema build reaches one only under a declared ``Object``/``List``.
    """
    _resolve_field(field, name, get_mapper, response_schema, ())


def _resolve_field(
    field: dict[str, Any],
    name: str,
    get_mapper: Callable[[], TypeMapper],
    response_schema: Any,
    enclosing: tuple[dict[str, Any], ...],
) -> None:
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
                nested[child_name] = _resolve_child(
                    child,
                    f"{name}.{child_name}",
                    get_mapper,
                    response_schema,
                    enclosing,
                )
    elif arrow_type == "List":
        field["items"] = _resolve_child(
            field.get("items"), f"{name}[]", get_mapper, response_schema, enclosing
        )


def _resolve_child(
    declaration: Any,
    name: str,
    get_mapper: Callable[[], TypeMapper],
    response_schema: Any,
    enclosing: tuple[dict[str, Any], ...],
) -> Any:
    """Fold one child declaration, resolve it, and return it in its place.

    The fold is a copy: a ``$defs`` entry several fields share must not
    carry the first one's resolution to the rest, or into the document.

    A child declared exactly as one of its enclosing declarations folds to
    the same node, so the walk below it would repeat forever. That is a
    record with no Arrow type, refused by name.
    """
    if not isinstance(declaration, dict):
        return declaration
    if declaration in enclosing:
        raise ReadError(
            f"field {name!r} contains itself through a $ref; an Arrow "
            f"Object/List has no recursive form"
        )
    child = deepcopy(materialize_node(declaration, response_schema))
    _resolve_field(child, name, get_mapper, response_schema, (*enclosing, declaration))
    return child
