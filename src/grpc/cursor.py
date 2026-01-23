"""Cursor utilities for opaque cursor encoding/decoding.

The cursor is an opaque bytes token produced by the engine and stored/returned
by the destination. The destination never interprets the cursor - it only
stores and returns it in ACK responses.

Cursor format (internal to engine):
{
    "field": "cursor_field_name",
    "value": "cursor_value",
    "tie_breakers": [{"field": "id", "value": "123"}],
    "timestamp": "2025-01-08T10:00:00Z"
}

This is JSON-encoded and stored as bytes for simplicity and debuggability.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .generated.analitiq.v1 import Cursor

logger = logging.getLogger(__name__)


def encode_cursor(
    cursor_field: str,
    cursor_value: Any,
    tie_breaker_fields: Optional[List[str]] = None,
    tie_breaker_values: Optional[Dict[str, Any]] = None,
) -> Cursor:
    """
    Encode cursor information into an opaque Cursor message.

    Args:
        cursor_field: Name of the cursor field (e.g., "created_at")
        cursor_value: Value of the cursor (timestamp, ID, etc.)
        tie_breaker_fields: Optional list of tie-breaker field names
        tie_breaker_values: Optional dict of tie-breaker field values

    Returns:
        Cursor protobuf message with opaque token
    """
    cursor_data = {
        "field": cursor_field,
        "value": _serialize_value(cursor_value),
        "encoded_at": datetime.now(timezone.utc).isoformat(),
    }

    if tie_breaker_fields and tie_breaker_values:
        cursor_data["tie_breakers"] = [
            {"field": field, "value": _serialize_value(tie_breaker_values.get(field))}
            for field in tie_breaker_fields
            if field in tie_breaker_values
        ]

    token = json.dumps(cursor_data, separators=(",", ":")).encode("utf-8")
    return Cursor(token=token)


def decode_cursor(cursor: Cursor) -> Dict[str, Any]:
    """
    Decode an opaque Cursor message back to its components.

    Args:
        cursor: Cursor protobuf message

    Returns:
        Dictionary with cursor components:
        - field: cursor field name
        - value: cursor value
        - tie_breakers: list of tie-breaker dicts (optional)
        - encoded_at: timestamp when cursor was encoded
    """
    if not cursor.token:
        return {}

    try:
        return json.loads(cursor.token.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(f"Failed to decode cursor: {e}")
        return {}


def compute_max_cursor(
    batch: List[Dict[str, Any]],
    cursor_field: str,
    tie_breaker_fields: Optional[List[str]] = None,
) -> Cursor:
    """
    Compute the maximum cursor value from a batch of records.

    The batch may not be ordered, so we find the MAX watermark across all records.

    Args:
        batch: List of records (dicts)
        cursor_field: Name of the cursor field
        tie_breaker_fields: Optional list of tie-breaker field names for ordering

    Returns:
        Cursor representing the maximum watermark in the batch
    """
    if not batch:
        return Cursor(token=b"")

    max_record = None
    max_cursor_value = None

    for record in batch:
        cursor_value = record.get(cursor_field)
        if cursor_value is None:
            continue

        if max_cursor_value is None:
            max_cursor_value = cursor_value
            max_record = record
        elif _compare_values(cursor_value, max_cursor_value) > 0:
            max_cursor_value = cursor_value
            max_record = record
        elif _compare_values(cursor_value, max_cursor_value) == 0 and tie_breaker_fields:
            # Same cursor value, compare tie-breakers
            if _compare_tie_breakers(record, max_record, tie_breaker_fields) > 0:
                max_record = record

    if max_record is None:
        return Cursor(token=b"")

    tie_breaker_values = None
    if tie_breaker_fields:
        tie_breaker_values = {
            field: max_record.get(field) for field in tie_breaker_fields
        }

    return encode_cursor(
        cursor_field=cursor_field,
        cursor_value=max_cursor_value,
        tie_breaker_fields=tie_breaker_fields,
        tie_breaker_values=tie_breaker_values,
    )


def cursor_to_state_dict(cursor: Cursor) -> Dict[str, Any]:
    """
    Convert a Cursor to a state dictionary for persistence.

    This is used when saving checkpoint state after receiving ACK.

    Args:
        cursor: Cursor protobuf message

    Returns:
        Dictionary suitable for state persistence
    """
    decoded = decode_cursor(cursor)
    if not decoded:
        return {}

    state = {
        "cursor": {
            "primary": {
                "field": decoded.get("field"),
                "value": decoded.get("value"),
                "inclusive": True,
            }
        }
    }

    tie_breakers = decoded.get("tie_breakers", [])
    if tie_breakers:
        state["cursor"]["tiebreakers"] = [
            {"field": tb["field"], "value": tb["value"], "inclusive": True}
            for tb in tie_breakers
        ]

    return state


def _serialize_value(value: Any) -> Any:
    """Serialize a value for JSON encoding."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _compare_values(a: Any, b: Any) -> int:
    """
    Compare two cursor values.

    Returns:
        -1 if a < b, 0 if a == b, 1 if a > b
    """
    # Handle datetime strings
    if isinstance(a, str) and isinstance(b, str):
        # Try to parse as ISO datetime for proper comparison
        try:
            from dateutil import parser as date_parser

            a_dt = date_parser.isoparse(a)
            b_dt = date_parser.isoparse(b)
            if a_dt < b_dt:
                return -1
            elif a_dt > b_dt:
                return 1
            return 0
        except (ValueError, TypeError):
            pass

    # Default comparison
    if a < b:
        return -1
    elif a > b:
        return 1
    return 0


def _compare_tie_breakers(
    record_a: Dict[str, Any],
    record_b: Dict[str, Any],
    tie_breaker_fields: List[str],
) -> int:
    """
    Compare two records by tie-breaker fields.

    Returns:
        -1 if a < b, 0 if a == b, 1 if a > b
    """
    for field in tie_breaker_fields:
        a_val = record_a.get(field)
        b_val = record_b.get(field)

        if a_val is None and b_val is None:
            continue
        if a_val is None:
            return -1
        if b_val is None:
            return 1

        cmp = _compare_values(a_val, b_val)
        if cmp != 0:
            return cmp

    return 0
