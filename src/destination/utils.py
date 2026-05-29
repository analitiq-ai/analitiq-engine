"""Shared destination utilities."""

import json
from typing import Any, Dict, List, Set


def decode_json_fields(
    records: List[Dict[str, Any]], json_fields: Set[str]
) -> List[Dict[str, Any]]:
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
