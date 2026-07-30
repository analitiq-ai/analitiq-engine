"""Walk a read endpoint's response schema to the record it declares.

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

from collections.abc import Mapping
from typing import Any

#: The only prefix a records ref may carry. The response scope name is the
#: resolver's; ``body`` is the provider payload the connector parsed.
RECORDS_REF_ROOT = "response.body"


class ResponseSchemaError(ValueError):
    """A records ref does not address a record schema in the declared response."""


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
