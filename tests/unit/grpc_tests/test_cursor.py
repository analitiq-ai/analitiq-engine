"""Unit tests for cursor utilities."""

import json
import pytest
from datetime import datetime, timezone

from src.grpc.cursor import (
    encode_cursor,
    decode_cursor,
    compute_max_cursor,
    cursor_to_state_dict,
)
from src.grpc.generated.analitiq.v1 import Cursor


class TestEncodeDecode:
    """Tests for cursor encoding and decoding."""

    def test_encode_basic_cursor(self):
        """Test encoding a basic cursor with field and value."""
        cursor = encode_cursor(
            cursor_field="created_at",
            cursor_value="2025-01-08T10:00:00Z",
        )

        assert cursor.token is not None
        assert len(cursor.token) > 0

        # Verify we can decode it
        decoded = json.loads(cursor.token.decode("utf-8"))
        assert decoded["field"] == "created_at"
        assert decoded["value"] == "2025-01-08T10:00:00Z"

    def test_encode_cursor_with_tie_breakers(self):
        """Test encoding cursor with tie-breaker fields."""
        cursor = encode_cursor(
            cursor_field="updated_at",
            cursor_value="2025-01-08T12:00:00Z",
            tie_breaker_fields=["id", "seq"],
            tie_breaker_values={"id": "123", "seq": 456},
        )

        decoded = json.loads(cursor.token.decode("utf-8"))
        assert decoded["field"] == "updated_at"
        assert decoded["value"] == "2025-01-08T12:00:00Z"
        assert len(decoded["tie_breakers"]) == 2
        assert decoded["tie_breakers"][0]["field"] == "id"
        assert decoded["tie_breakers"][0]["value"] == "123"
        assert decoded["tie_breakers"][1]["field"] == "seq"
        assert decoded["tie_breakers"][1]["value"] == 456

    def test_encode_cursor_with_datetime(self):
        """Test encoding cursor with datetime object."""
        dt = datetime(2025, 1, 8, 10, 30, 0, tzinfo=timezone.utc)
        cursor = encode_cursor(
            cursor_field="timestamp",
            cursor_value=dt,
        )

        decoded = json.loads(cursor.token.decode("utf-8"))
        assert decoded["value"] == dt.isoformat()

    def test_decode_cursor(self):
        """Test decoding a cursor back to components."""
        cursor = encode_cursor(
            cursor_field="created_at",
            cursor_value="2025-01-08T10:00:00Z",
        )

        decoded = decode_cursor(cursor)
        assert decoded["field"] == "created_at"
        assert decoded["value"] == "2025-01-08T10:00:00Z"
        assert "encoded_at" in decoded

    def test_decode_empty_cursor(self):
        """Test decoding an empty cursor returns empty dict."""
        cursor = Cursor(token=b"")
        decoded = decode_cursor(cursor)
        assert decoded == {}

    def test_decode_invalid_cursor(self):
        """Test decoding invalid cursor returns empty dict."""
        cursor = Cursor(token=b"not valid json")
        decoded = decode_cursor(cursor)
        assert decoded == {}


class TestComputeMaxCursor:
    """Tests for compute_max_cursor function."""

    def test_compute_max_from_batch(self):
        """Test computing max cursor from batch of records."""
        batch = [
            {"id": 1, "created_at": "2025-01-08T09:00:00Z"},
            {"id": 2, "created_at": "2025-01-08T11:00:00Z"},  # MAX
            {"id": 3, "created_at": "2025-01-08T10:00:00Z"},
        ]

        cursor = compute_max_cursor(batch, "created_at")
        decoded = decode_cursor(cursor)

        assert decoded["field"] == "created_at"
        assert decoded["value"] == "2025-01-08T11:00:00Z"

    def test_compute_max_with_tie_breakers(self):
        """Test computing max cursor with tie-breaker fields."""
        batch = [
            {"id": 1, "created_at": "2025-01-08T10:00:00Z"},
            {"id": 3, "created_at": "2025-01-08T10:00:00Z"},  # Same time, higher ID
            {"id": 2, "created_at": "2025-01-08T10:00:00Z"},
        ]

        cursor = compute_max_cursor(batch, "created_at", tie_breaker_fields=["id"])
        decoded = decode_cursor(cursor)

        # Should pick record with id=3 (highest tie-breaker)
        assert decoded["field"] == "created_at"
        assert len(decoded["tie_breakers"]) == 1
        assert decoded["tie_breakers"][0]["value"] == 3

    def test_compute_max_numeric_cursor(self):
        """Test computing max cursor with numeric values."""
        batch = [
            {"id": 100, "offset": 50},
            {"id": 101, "offset": 75},
            {"id": 102, "offset": 25},
        ]

        cursor = compute_max_cursor(batch, "offset")
        decoded = decode_cursor(cursor)

        assert decoded["value"] == 75

    def test_compute_max_empty_batch(self):
        """Test computing max cursor from empty batch."""
        cursor = compute_max_cursor([], "created_at")
        assert cursor.token == b""

    def test_compute_max_missing_cursor_field(self):
        """Test computing max cursor when field is missing in some records."""
        batch = [
            {"id": 1, "created_at": "2025-01-08T09:00:00Z"},
            {"id": 2},  # Missing cursor field
            {"id": 3, "created_at": "2025-01-08T11:00:00Z"},
        ]

        cursor = compute_max_cursor(batch, "created_at")
        decoded = decode_cursor(cursor)

        assert decoded["value"] == "2025-01-08T11:00:00Z"


class TestCursorToStateDict:
    """Tests for cursor_to_state_dict function."""

    def test_convert_to_state_dict(self):
        """Test converting cursor to state dictionary."""
        cursor = encode_cursor(
            cursor_field="updated_at",
            cursor_value="2025-01-08T15:00:00Z",
        )

        state = cursor_to_state_dict(cursor)

        assert "cursor" in state
        assert "primary" in state["cursor"]
        assert state["cursor"]["primary"]["field"] == "updated_at"
        assert state["cursor"]["primary"]["value"] == "2025-01-08T15:00:00Z"
        assert state["cursor"]["primary"]["inclusive"] is True

    def test_convert_with_tie_breakers(self):
        """Test converting cursor with tie-breakers to state dict."""
        cursor = encode_cursor(
            cursor_field="created_at",
            cursor_value="2025-01-08T10:00:00Z",
            tie_breaker_fields=["id"],
            tie_breaker_values={"id": "abc123"},
        )

        state = cursor_to_state_dict(cursor)

        assert "tiebreakers" in state["cursor"]
        assert len(state["cursor"]["tiebreakers"]) == 1
        assert state["cursor"]["tiebreakers"][0]["field"] == "id"
        assert state["cursor"]["tiebreakers"][0]["value"] == "abc123"

    def test_convert_empty_cursor(self):
        """Test converting empty cursor returns empty dict."""
        cursor = Cursor(token=b"")
        state = cursor_to_state_dict(cursor)
        assert state == {}
