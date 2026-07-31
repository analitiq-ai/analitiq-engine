"""Walk a read endpoint's response schema to the record it declares.

And resolve that record's Arrow types from the connector's read type map,
which is the second half of the same question: the ref has to reach a
record schema, and every field of that schema has to name an Arrow type
the engine can build a batch from.

``operations.read.response.records.ref`` names where the records live in
the provider's payload, and ``operations.read.response.schema`` declares
that payload's shape. The two are authored separately, so they can
disagree: a ``ref`` of ``response.body.data`` against a schema whose only
property is ``results`` addresses nothing, and the read fails on its first
page with the connector already published.

The walk is here rather than in the API connector because two callers
need the same answer: the connector, which turns the addressed items into
the Arrow schema it emits, and the conformance kit, which certifies the
pair offline before the connector ships.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from cdk.type_map import TypeMapper, UnmappedTypeError

#: The only prefix a records ref may carry. The response scope name is the
#: resolver's; ``body`` is the provider payload the connector parsed.
RECORDS_REF_ROOT = "response.body"


class ResponseSchemaError(ValueError):
    """A records ref does not address a record schema in the declared response."""


class RecordTypeError(ValueError):
    """A record field's JSON type has no rule in the read type map."""


def records_items_schema(
    records_ref: str, response_schema: Mapping[str, Any]
) -> dict[str, Any]:
    """Return the per-record schema *records_ref* addresses in *response_schema*.

    The response schema is free-form JSON Schema in the contract, so the
    walk stays dict-shaped: each ref segment is a key under ``properties``,
    and the addressed node is either the record itself or an array whose
    ``items`` is.
    """
    if records_ref == RECORDS_REF_ROOT:
        node: Any = response_schema
    elif records_ref.startswith(f"{RECORDS_REF_ROOT}."):
        node = response_schema
        for field in records_ref[len(RECORDS_REF_ROOT) + 1 :].split("."):
            properties = node.get("properties") if isinstance(node, Mapping) else None
            if not isinstance(properties, Mapping) or field not in properties:
                available = (
                    sorted(properties.keys()) if isinstance(properties, Mapping) else []
                )
                raise ResponseSchemaError(
                    f"records.ref {records_ref!r} references field {field!r} "
                    f"that is not declared under properties; "
                    f"available: {available}"
                )
            node = properties[field]
    else:
        raise ResponseSchemaError(
            f"unsupported records.ref {records_ref!r}; expected "
            f"{RECORDS_REF_ROOT!r} or '{RECORDS_REF_ROOT}.<field>[.<field>...]'"
        )

    items = (
        node.get("items")
        if isinstance(node, Mapping) and node.get("type") == "array"
        else node
    )
    if not isinstance(items, Mapping) or not items.get("properties"):
        raise ResponseSchemaError(
            f"cannot resolve record schema at {records_ref!r} "
            f"(no 'properties' under the addressed items)"
        )
    return dict(items)


def resolve_record_arrow_types(
    record_schema: dict[str, Any], get_mapper: Callable[[], TypeMapper]
) -> None:
    """Fill each record field's ``arrow_type`` from the read type map, in place.

    API endpoints declare per-field JSON ``type``/``format`` and ship a
    ``type-map-read.json`` mapping those to Arrow types — the same read type
    map the database source path consumes. A field that already declares
    ``arrow_type`` keeps it, so hand-annotated connectors stay valid and the
    mapper is consulted only when a field needs it; *get_mapper* is called
    lazily for that reason, since an endpoint that annotates every field
    needs no map at all.

    An unmapped JSON type raises :class:`RecordTypeError` naming the field:
    the engine cannot build a batch from a field it has no Arrow type for,
    so the read fails before its first request.
    """
    for name, field in (record_schema.get("properties") or {}).items():
        if isinstance(field, dict):
            _resolve_field_arrow_type(field, name, get_mapper)


def _resolve_field_arrow_type(
    field: dict[str, Any], name: str, get_mapper: Callable[[], TypeMapper]
) -> None:
    """Fill ``field['arrow_type']`` from the type map if absent, then recurse.

    Recursion is gated to the resolved ``arrow_type`` exactly as
    ``SchemaContract.resolve_arrow_type`` does: it descends into
    ``properties`` only for ``Object`` and into ``items`` only for ``List``,
    and treats everything else (including a ``Json`` blob that keeps
    ``properties``/``items`` for documentation, and every scalar) as a leaf.
    A nested child authored with only JSON ``type``/``format`` under a real
    ``Object``/``List`` must be resolved here too, or the schema build
    fails; but descending into a ``Json`` blob's documentary children would
    wrongly fail a read on a child type the schema build never consults.
    Recursion runs even when a container already carries an ``arrow_type``,
    because a hand-annotated ``Object``/``List`` can still hold children
    that do not.
    """
    if not field.get("arrow_type"):
        json_type = field.get("type")
        if isinstance(json_type, list):
            json_type = next((t for t in json_type if t != "null"), None)
        if isinstance(json_type, str):
            fmt = field.get("format")
            native = f"{json_type}:{fmt}" if isinstance(fmt, str) and fmt else json_type
            try:
                field["arrow_type"] = get_mapper().to_arrow_type(native)
            except UnmappedTypeError as err:
                raise RecordTypeError(
                    f"field {name!r}: JSON type {native!r} has no rule in "
                    f"the endpoint's read type-map"
                ) from err
    arrow_type = field.get("arrow_type")
    if arrow_type == "Object":
        nested = field.get("properties")
        if isinstance(nested, dict):
            for child_name, child in nested.items():
                if isinstance(child, dict):
                    _resolve_field_arrow_type(child, f"{name}.{child_name}", get_mapper)
    elif arrow_type == "List":
        items = field.get("items")
        if isinstance(items, dict):
            _resolve_field_arrow_type(items, f"{name}[]", get_mapper)


def record_field_exists(record_schema: Mapping[str, Any], dotted_path: str) -> bool:
    """Whether *dotted_path* names a field the record schema declares.

    Each segment is a key under ``properties``, descending through nested
    objects. What the engine walks on the record *data*; asking the same
    question of the declared shape is how a keyset ordering field can be
    checked before a page comes back.
    """
    node: Any = record_schema
    for segment in dotted_path.split("."):
        properties = node.get("properties") if isinstance(node, Mapping) else None
        if not isinstance(properties, Mapping) or segment not in properties:
            return False
        node = properties[segment]
    return True
