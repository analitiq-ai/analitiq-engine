"""OSError errno classification tests for file/stream destinations.

Disk-full, permission, EROFS, and EPIPE are not recoverable by retry —
classifying them as RETRYABLE would burn the DLQ budget and mask the
root cause. These tests pin the errno → status mapping so a regression
that drops an errno from the fatal set fails CI loudly.
"""

import errno
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from src.destination.connectors.file import FileDestinationHandler
from src.destination.connectors.stream import StreamDestinationHandler
from src.grpc.generated.analitiq.v1 import AckStatus, Cursor


def _record_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([{"id": 1, "v": "row"}])


def _cursor() -> Cursor:
    return Cursor(token=b"")


async def _drive_file(handler: FileDestinationHandler, raise_exc: BaseException):
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=b"data\n")
    handler._formatter.file_extension = "jsonl"
    handler._formatter.content_type = "application/jsonl"
    handler._storage = MagicMock()
    handler._storage.build_path = MagicMock(return_value="/tmp/output.jsonl")
    handler._storage.write_file = AsyncMock(side_effect=raise_exc)
    handler._manifest = MagicMock()
    handler._manifest.check_committed = AsyncMock(return_value=None)
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": "out/"}
    return await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=_record_batch(), record_ids=["1"], cursor=_cursor(),
    )


async def _drive_stream(handler: StreamDestinationHandler, raise_exc: BaseException):
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=b"data\n")
    with patch("src.destination.connectors.stream.sys") as fake_sys:
        fake_sys.stdout = MagicMock()
        fake_sys.stdout.buffer = MagicMock()
        fake_sys.stdout.buffer.write = MagicMock(side_effect=raise_exc)
        fake_sys.stdout.buffer.flush = MagicMock()
        return await handler.write_batch(
            run_id="r", stream_id="s", batch_seq=1,
            record_batch=_record_batch(), record_ids=["1"], cursor=_cursor(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("errno_code", [
    errno.ENOSPC, errno.EACCES, errno.EROFS, errno.EDQUOT,
])
async def test_file_handler_fatal_errnos(errno_code):
    """Disk-full, permission, read-only fs, quota: never recoverable."""
    exc = OSError(errno_code, "fatal i/o")
    result = await _drive_file(FileDestinationHandler(), exc)
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert result.success is False
    assert errno.errorcode[errno_code] in result.failure_summary


@pytest.mark.asyncio
async def test_file_handler_eio_is_retryable():
    """Transient I/O errors (EIO) should stay RETRYABLE."""
    exc = OSError(errno.EIO, "transient")
    result = await _drive_file(FileDestinationHandler(), exc)
    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


@pytest.mark.asyncio
async def test_file_handler_non_oserror_is_fatal():
    """Programming errors (KeyError, TypeError, …) become FATAL."""
    result = await _drive_file(FileDestinationHandler(), KeyError("missing"))
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "KeyError" in result.failure_summary


@pytest.mark.asyncio
@pytest.mark.parametrize("errno_code", [
    errno.EPIPE, errno.ENOSPC, errno.EACCES, errno.EBADF,
])
async def test_stream_handler_fatal_errnos(errno_code):
    """Closed pipe, disk-full on redirect, permission, bad-fd: never
    recoverable by retry."""
    exc = OSError(errno_code, "fatal stdout")
    result = await _drive_stream(StreamDestinationHandler(), exc)
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert result.success is False


@pytest.mark.asyncio
async def test_stream_handler_eio_is_retryable():
    exc = OSError(errno.EIO, "transient stdout")
    result = await _drive_stream(StreamDestinationHandler(), exc)
    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


@pytest.mark.asyncio
async def test_stream_handler_non_oserror_is_fatal():
    result = await _drive_stream(StreamDestinationHandler(), TypeError("bug"))
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "TypeError" in result.failure_summary
