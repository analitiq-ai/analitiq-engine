"""Guard: serialize_batch() returning empty bytes for non-empty records → FATAL.

A formatter contract violation (non-empty input → empty output) must not
silently produce a zero-byte file that reports success. The guard introduced
in issue #322 catches this at write time and returns FATAL so the batch is
routed to the dead-letter queue (DLQ) rather than committed as written.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from src.destination.connectors.file import FileDestinationHandler
from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

_EMITTED_AT = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)
"""A fixed, timezone-aware emit instant for write_batch/send_batch calls;
the engine stamps this per batch (issue #353). Value is arbitrary for sinks
that ignore it."""


def _handler_with_formatter_returning(data) -> FileDestinationHandler:
    handler = FileDestinationHandler()
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=data)
    handler._formatter.file_extension = "jsonl"
    handler._formatter.content_type = "application/jsonl"
    handler._storage = MagicMock()
    handler._storage.build_path = MagicMock(return_value="/tmp/output.jsonl")
    handler._storage.write_file = AsyncMock(return_value="/tmp/output.jsonl")
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": ""}
    return handler


def _record_batch(rows: int = 1) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([{"id": i, "v": "row"} for i in range(rows)])


def _cursor() -> Cursor:
    return Cursor(token=b"tok")


@pytest.mark.asyncio
async def test_serialize_batch_empty_bytes_returns_fatal():
    """serialize_batch() → b'' for non-empty records must yield FATAL, not RETRYABLE.

    A formatter that returns empty bytes for a non-empty batch is a deterministic
    contract violation — retrying the same formatter with the same records will
    always produce the same empty result. FATAL routes directly to DLQ, avoiding
    pointless retry burn.
    """
    handler = _handler_with_formatter_returning(b"")
    result = await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert result.records_written == 0
    assert "serialize_batch()" in result.failure_summary
    assert "1 records" in result.failure_summary


@pytest.mark.asyncio
async def test_serialize_batch_empty_bytes_does_not_write_file():
    """No file write must occur when the guard fires."""
    handler = _handler_with_formatter_returning(b"")
    await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )
    handler._storage.write_file.assert_not_called()


@pytest.mark.asyncio
async def test_serialize_batch_none_returns_fatal():
    """serialize_batch() → None (missing return) must yield FATAL, not TypeError.

    `not None` is truthy so the guard catches this case. A test pins the
    behavior so a refactor to `data == b""` does not accidentally let None
    slip through to write_file and raise TypeError.
    """
    handler = _handler_with_formatter_returning(None)
    result = await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert result.records_written == 0
    handler._storage.write_file.assert_not_called()


@pytest.mark.asyncio
async def test_serialize_batch_non_empty_succeeds():
    """Normal path (non-empty bytes) must return SUCCESS with correct record count."""
    handler = _handler_with_formatter_returning(b'{"id":0}\n')
    result = await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["0"],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )
    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 1
    handler._storage.write_file.assert_called_once()


@pytest.mark.asyncio
async def test_serialize_batch_failure_summary_includes_formatter_name():
    """Failure summary must name the formatter class for operator diagnostics."""

    class MyBrokenFormatter:
        file_extension = "jsonl"
        content_type = "application/jsonl"

        def serialize_batch(self, records):
            return b""

        def configure(self, config):
            pass

    handler = FileDestinationHandler()
    handler._connected = True
    handler._formatter = MyBrokenFormatter()
    handler._storage = MagicMock()
    handler._storage.build_path = MagicMock(return_value="/tmp/output.jsonl")
    handler._storage.write_file = AsyncMock(return_value="/tmp/output.jsonl")
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": ""}
    result = await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["0"],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )
    assert "MyBrokenFormatter" in result.failure_summary


@pytest.mark.asyncio
async def test_serialize_batch_failure_summary_includes_record_count():
    """Failure summary record count must reflect the actual batch size."""
    handler = _handler_with_formatter_returning(b"")
    result = await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(rows=5),
        record_ids=[str(i) for i in range(5)],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )
    assert "5 records" in result.failure_summary


@pytest.mark.asyncio
async def test_serialize_batch_failure_summary_includes_run_context():
    """Failure summary must name run_id, stream_id, and batch_seq for DLQ."""
    handler = _handler_with_formatter_returning(b"")
    result = await handler.write_batch(
        run_id="run-abc",
        stream_id="orders",
        batch_seq=7,
        record_batch=_record_batch(),
        record_ids=["0"],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )
    assert "run-abc" in result.failure_summary
    assert "orders" in result.failure_summary
    assert "7" in result.failure_summary
