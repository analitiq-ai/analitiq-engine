"""JSON helpers shared by the CDK (Json-typed column decoding, authored form)."""

import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

__all__ = [
    "authored_json",
    "decimals_to_float",
    "declared_json_types",
    "decode_json_fields",
]


def decimals_to_float(value: Any) -> Any:
    """Replace every ``Decimal`` with ``float`` inside a value bound for JSON.

    The API reader parses bodies with ``parse_float=Decimal`` so a row value
    reaches its typed column exact. Anything that leaves the CDK as a JSON
    document instead -- a ``Json`` column's blob, a batch's response
    metadata -- carries no per-field type, so a number in it is a JSON
    number: ``json.dumps`` cannot spell a ``Decimal`` and a string would
    turn the provider's number into text. This is the one rule for that
    narrowing; ``float(Decimal(token))`` equals ``float(token)``. The dump
    itself refuses a non-finite result (``allow_nan=False``): ``1e400``
    narrows to infinity and a permissive parser hands ``NaN`` through as a
    float, and neither is a token JSON can spell.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: decimals_to_float(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decimals_to_float(v) for v in value]
    return value


def authored_json(value: Any) -> Any:
    """Return a contract model's authored JSON form, or *value* unchanged.

    The value-expression grammar belongs to the :class:`~cdk.resolver.Resolver`
    and is shared by every transport, so a contract model reaching it arrives
    as the JSON its author wrote rather than as a shape the resolver would
    have to learn. ``by_alias`` restores the contract's own field names
    (``in``, ``schema``, ``and``) and ``exclude_unset`` keeps an author's
    omissions omitted, so what gets walked is the authored node and not the
    model's defaults.

    This needs pydantic and nothing else. That is what lets the resolver stay
    contract-version-agnostic, and lets the predicate walker evaluate a stop
    condition without importing the contract's seventeen predicate models.
    """
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", by_alias=True, exclude_unset=True)
    return value


def declared_json_types(field: Mapping[str, Any]) -> list[str]:
    """Read the non-null JSON types a field's ``type`` declares, in declared order.

    One reading of JSON Schema's ``type`` for every consumer -- read-side
    decoder compatibility, write-side encoder compatibility, and the API
    response-schema resolver alike: a plain string is one type, a list is a
    union whose ``null`` member only says the field is nullable
    (``["string", "null"]`` is a string field). A ``type`` that is neither
    yields nothing.
    """
    declared = field.get("type")
    if isinstance(declared, str):
        return [declared]
    if isinstance(declared, list):
        return [t for t in declared if isinstance(t, str) and t != "null"]
    return []


def decode_json_fields(
    records: list[dict[str, Any]], json_fields: set[str]
) -> list[dict[str, Any]]:
    """Parse JSON-encoded string values for the named fields in place.

    Skips non-string values (already-parsed dicts/lists, None). Raises
    ``ValueError`` with column name and row index on malformed JSON.
    """
    if not json_fields or not records:
        return records
    for row, record in enumerate(records):
        for col in json_fields:
            value = record.get(col)
            if not isinstance(value, str):
                continue
            try:
                record[col] = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Json column {col!r} at row {row}: value is not valid JSON ({exc})"
                ) from exc
    return records
