"""Guard: serialize_batch() returning empty bytes for non-empty records → FATAL.

A formatter contract violation (non-empty input → empty output) must not
silently produce a zero-byte file that reports success. The guard introduced
in issue #322 catches this at write time and returns FATAL so the batch is
routed to the DLQ rather than committed as written.
"""

from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from src.destination.connectors.file import FileDestinationHandler
from src.grpc.generated.analitiq.v1 import AckStatus, Cursor


def _handler_with_formatter_returning(data: bytes) -> FileDestinationHandler:
    handler = FileDestinationHandler()
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=data)
    handler._formatter.file_extension = "jsonl"
    handler._formatter.content_type = "application/jsonl"
    handler.__class__.__name__ = "FileDestinationHandler"
    handler._storage = MagicMock()
    handler._storage.build_path = MagicMock(return_value="/tmp/output.jsonl")
    handler._storage.write_file = AsyncMock(return_value="/tmp/output.jsonl")
    handler._manifest = MagicMock()
    handler._manifest.check_committed = AsyncMock(return_value=None)
    handler._manifest.record_commit = AsyncMock()
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": ""}
    return handler


def _record_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([{"id": 1, "v": "row"}])


def _cursor() -> Cursor:
    return Cursor(token=b"tok")


@pytest.mark.asyncio
async def test_serialize_batch_empty_bytes_returns_fatal():
    """serialize_batch() → b'' for non-empty records must yield FATAL."""
    handler = _handler_with_formatter_returning(b"")
    result = await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
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
    )
    handler._storage.write_file.assert_not_called()


@pytest.mark.asyncio
async def test_serialize_batch_empty_bytes_does_not_commit():
    """Manifest must not be updated when the guard fires."""
    handler = _handler_with_formatter_returning(b"")
    await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
    )
    handler._manifest.record_commit.assert_not_called()


@pytest.mark.asyncio
async def test_serialize_batch_non_empty_succeeds():
    """Normal path (non-empty bytes) must still succeed."""
    handler = _handler_with_formatter_returning(b'{"id":1}\n')
    result = await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
    )
    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 1


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
    handler._manifest = MagicMock()
    handler._manifest.check_committed = AsyncMock(return_value=None)
    handler._manifest.record_commit = AsyncMock()
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": ""}
    result = await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
    )
    assert "MyBrokenFormatter" in result.failure_summary
