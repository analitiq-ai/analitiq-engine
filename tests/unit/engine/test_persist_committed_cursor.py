"""Unit tests for StreamingEngine._persist_committed_cursor.

The watermark-persist helper is the one place a destination-acked cursor
becomes a durable bookmark. It must:
- checkpoint a real watermark value,
- skip (not fabricate) when a batch advanced no watermark, and
- fail loud rather than checkpoint a wall-clock now() when a cursor carries
  components but no value (which would make the next run skip rows).
"""

import json
from unittest.mock import MagicMock

import pytest

from src.engine.engine import StreamingEngine
from src.engine.exceptions import StreamProcessingError
from src.grpc.cursor import encode_cursor
from src.grpc.generated.analitiq.v1 import Cursor


def _engine() -> StreamingEngine:
    engine = StreamingEngine.__new__(StreamingEngine)
    engine.state_manager = MagicMock()
    return engine


@pytest.mark.unit
class TestPersistCommittedCursor:
    def test_real_value_is_checkpointed(self):
        engine = _engine()
        cursor = encode_cursor("created_at", "2025-01-08T10:00:00Z")

        cursor_data, hwm = engine._persist_committed_cursor(cursor, "s1", 3)

        assert hwm == "2025-01-08T10:00:00Z"
        assert cursor_data["primary"]["value"] == "2025-01-08T10:00:00Z"
        engine.state_manager.save_stream_checkpoint.assert_called_once()
        kwargs = engine.state_manager.save_stream_checkpoint.call_args.kwargs
        assert kwargs["hwm"] == "2025-01-08T10:00:00Z"
        assert kwargs["stream_name"] == "s1"
        assert kwargs["stream_version"] == 3

    def test_empty_cursor_skips_checkpoint(self):
        # A batch that advanced no watermark (empty token) must not write a
        # checkpoint and must not raise — any prior bookmark stays untouched.
        engine = _engine()

        cursor_data, hwm = engine._persist_committed_cursor(Cursor(token=b""), "s1", 1)

        assert cursor_data == {}
        assert hwm == ""
        engine.state_manager.save_stream_checkpoint.assert_not_called()

    def test_none_cursor_skips_checkpoint(self):
        engine = _engine()

        cursor_data, hwm = engine._persist_committed_cursor(None, "s1", 1)

        assert (cursor_data, hwm) == ({}, "")
        engine.state_manager.save_stream_checkpoint.assert_not_called()

    def test_cursor_without_value_fails_loud(self):
        # A token that carries a field but no value must raise rather than
        # fabricate datetime.now() as the high-water mark.
        engine = _engine()
        token = json.dumps({"field": "created_at", "encoded_at": "x"}).encode()

        with pytest.raises(StreamProcessingError, match="no watermark value"):
            engine._persist_committed_cursor(Cursor(token=token), "s1", 1)

        engine.state_manager.save_stream_checkpoint.assert_not_called()

    def test_zero_value_is_a_valid_watermark(self):
        # A literal 0 (int cursor) is a real value, not "missing"; it must be
        # checkpointed, not treated as absent.
        engine = _engine()
        cursor = encode_cursor("offset", 0)

        cursor_data, hwm = engine._persist_committed_cursor(cursor, "s1", 1)

        assert hwm == 0
        engine.state_manager.save_stream_checkpoint.assert_called_once()
