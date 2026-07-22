"""Unit tests for StreamProcessor._persist_committed_cursor.

The watermark-persist helper is the one place a destination-acked cursor
becomes a durable bookmark. It must:
- checkpoint a real watermark value,
- skip (not fabricate) when a batch advanced no watermark, and
- fail loud rather than checkpoint a wall-clock now() when a cursor carries
  components but no value (which would make the next run skip rows).
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.engine.exceptions import StreamProcessingError
from src.engine.stream_processor import StreamProcessor
from src.grpc.cursor import encode_cursor
from src.grpc.generated.analitiq.v1 import Cursor
from src.state.store import decode_value


def _processor(stream_version: int = 1) -> StreamProcessor:
    return StreamProcessor(
        stream_id="s1",
        stream_config={"name": "s1", "stream_version": stream_version},
        pipeline_config={},
        pipeline_id="p1",
        state_manager=MagicMock(),
        pipeline_metrics=MagicMock(),
        worker_readable=MagicMock(),
        dlq_root=".",
        batch_size=1,
        buffer_size=1,
        max_retries=0,
        retry_delay=0,
        error_strategy="fail",
    )


@pytest.mark.unit
class TestPersistCommittedCursor:
    def test_real_value_is_checkpointed(self):
        processor = _processor(stream_version=3)
        cursor = encode_cursor("created_at", "2025-01-08T10:00:00Z")

        cursor_data, hwm = processor._persist_committed_cursor(cursor)

        assert hwm == "2025-01-08T10:00:00Z"
        assert cursor_data["primary"]["value"] == "2025-01-08T10:00:00Z"
        processor.state_manager.save_stream_checkpoint.assert_called_once()
        kwargs = processor.state_manager.save_stream_checkpoint.call_args.kwargs
        assert kwargs["hwm"] == "2025-01-08T10:00:00Z"
        assert kwargs["stream_name"] == "s1"
        assert kwargs["stream_version"] == 3

    def test_empty_cursor_skips_checkpoint(self):
        # A batch that advanced no watermark (empty token) must not write a
        # checkpoint and must not raise — any prior bookmark stays untouched.
        processor = _processor()

        cursor_data, hwm = processor._persist_committed_cursor(Cursor(token=b""))

        assert cursor_data == {}
        assert hwm == ""
        processor.state_manager.save_stream_checkpoint.assert_not_called()

    def test_none_cursor_skips_checkpoint(self):
        processor = _processor()

        cursor_data, hwm = processor._persist_committed_cursor(None)

        assert (cursor_data, hwm) == ({}, "")
        processor.state_manager.save_stream_checkpoint.assert_not_called()

    def test_cursor_without_value_fails_loud(self):
        # A token that carries a field but no value must raise rather than
        # fabricate datetime.now() as the high-water mark.
        processor = _processor()
        token = json.dumps({"field": "created_at", "encoded_at": "x"}).encode()

        with pytest.raises(StreamProcessingError, match="no watermark value"):
            processor._persist_committed_cursor(Cursor(token=token))

        processor.state_manager.save_stream_checkpoint.assert_not_called()

    def test_zero_value_is_a_valid_watermark(self):
        # A literal 0 (int cursor) is a real value, not "missing"; it must be
        # checkpointed, not treated as absent.
        processor = _processor()
        cursor = encode_cursor("offset", 0)

        cursor_data, hwm = processor._persist_committed_cursor(cursor)

        assert hwm == 0
        processor.state_manager.save_stream_checkpoint.assert_called_once()

    def test_datetime_watermark_is_checkpointed_as_tagged(self):
        # The realistic incremental case: a timestamp cursor arrives tagged
        # (`{"__type__": "datetime", ...}`), not a bare scalar. The tagged dict
        # must not trip the value-less fail-loud guard, and must round-trip
        # back to the original datetime via decode_value.
        processor = _processor()
        dt = datetime(2025, 1, 8, 10, 0, 0, tzinfo=timezone.utc)
        cursor = encode_cursor("created_at", dt)

        cursor_data, hwm = processor._persist_committed_cursor(cursor)

        assert hwm == {"__type__": "datetime", "value": dt.isoformat()}
        processor.state_manager.save_stream_checkpoint.assert_called_once()
        saved = processor.state_manager.save_stream_checkpoint.call_args.kwargs[
            "cursor"
        ]
        assert decode_value(saved["primary"]["value"]) == dt

    def test_decimal_watermark_is_checkpointed_as_tagged(self):
        # Ties the Decimal-tagging change to its consumer: a NUMERIC cursor
        # round-trips losslessly through the checkpoint.
        processor = _processor()
        value = Decimal("123.4500")
        cursor = encode_cursor("amount", value)

        cursor_data, hwm = processor._persist_committed_cursor(cursor)

        assert hwm == {"__type__": "decimal", "value": "123.4500"}
        saved = processor.state_manager.save_stream_checkpoint.call_args.kwargs[
            "cursor"
        ]
        assert decode_value(saved["primary"]["value"]) == value
