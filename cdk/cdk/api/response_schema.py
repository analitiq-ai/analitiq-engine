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

from collections.abc import Callable, Mapping
from copy import deepcopy
from typing import Any

from analitiq.contracts.endpoints import ResponseExtraction

from ..exceptions import ReadError
from ..type_map import TypeMapper, UnmappedTypeError
from ..types import EndpointScope
from .records import split_records_ref

__all__ = ["apply_read_type_map", "records_items_schema", "resolve_field_arrow_type"]


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


def apply_read_type_map(
    items_schema: dict[str, Any],
    endpoint_ref: Mapping[str, Any],
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
    """
    scope = endpoint_ref.get("scope")
    if not scope:
        raise ReadError(
            f"stream_source endpoint_ref has no 'scope'; expected one of "
            f"{[s.value for s in EndpointScope]}"
        )

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
        json_type = field.get("type")
        if isinstance(json_type, list):
            json_type = next((t for t in json_type if t != "null"), None)
        if isinstance(json_type, str):
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
