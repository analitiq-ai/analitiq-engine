"""JSON helpers shared by the CDK (Json-typed column decoding, authored form)."""

import json
from typing import Any

from pydantic import BaseModel

__all__ = ["authored_json", "decode_json_fields"]


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
