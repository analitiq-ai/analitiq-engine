"""OSError errno classification tests for file/stream destinations.

Disk-full, permission, EROFS, and EPIPE are not recoverable by retry —
classifying them as RETRYABLE would burn the DLQ budget and mask the
root cause. These tests pin the errno → status mapping so a regression
that drops an errno from the fatal set fails CI loudly.

Also covers operations that were previously outside the try/except —
to_pylist, check_committed, and the empty-batch record_commit — which
would propagate as bare exceptions instead of a BatchWriteResult.
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
        run_id="r",
        stream_id="s",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
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
            run_id="r",
            stream_id="s",
            batch_seq=1,
            record_batch=_record_batch(),
            record_ids=["1"],
            cursor=_cursor(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "errno_code",
    [
        errno.ENOSPC,
        errno.EACCES,
        errno.EROFS,
        errno.EDQUOT,
    ],
)
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
@pytest.mark.parametrize(
    "errno_code",
    [
        errno.EPIPE,
        errno.ENOSPC,
        errno.EACCES,
        errno.EBADF,
    ],
)
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


@pytest.mark.asyncio
async def test_stream_handler_to_pylist_arrow_invalid_is_fatal():
    """ArrowInvalid from to_pylist in stream handler must not propagate — FATAL."""
    handler = StreamDestinationHandler()
    handler._connected = True
    handler._formatter = MagicMock()
    mock_batch = MagicMock(spec=pa.RecordBatch)
    mock_batch.to_pylist = MagicMock(
        side_effect=pa.ArrowInvalid("corrupt buffer")
    )

    result = await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=mock_batch, record_ids=[], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "ArrowInvalid" in result.failure_summary


def _make_file_handler(
    *,
    check_committed_result=None,
    check_committed_exc=None,
    record_commit_exc=None,
):
    """Return a wired FileDestinationHandler for early write_batch path tests.

    Verifies that to_pylist, check_committed, and the empty-batch
    record_commit are caught by the try/except rather than propagating.
    """
    handler = FileDestinationHandler()
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=b"data\n")
    handler._formatter.file_extension = "jsonl"
    handler._formatter.content_type = "application/jsonl"
    handler._storage = MagicMock()
    handler._storage.build_path = MagicMock(return_value="/tmp/out.jsonl")
    handler._storage.write_file = AsyncMock(return_value="/tmp/out.jsonl")
    handler._manifest = MagicMock()
    handler._manifest.check_committed = AsyncMock(
        return_value=check_committed_result,
        side_effect=check_committed_exc,
    )
    handler._manifest.record_commit = AsyncMock(side_effect=record_commit_exc)
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": ""}
    return handler


@pytest.mark.asyncio
async def test_to_pylist_arrow_invalid_is_fatal():
    """ArrowInvalid from to_pylist must not propagate — returns FATAL."""
    handler = _make_file_handler()
    mock_batch = MagicMock(spec=pa.RecordBatch)
    mock_batch.to_pylist = MagicMock(
        side_effect=pa.ArrowInvalid("corrupt buffer")
    )

    result = await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=mock_batch, record_ids=[], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "ArrowInvalid" in result.failure_summary


@pytest.mark.asyncio
async def test_to_pylist_memory_error_is_fatal():
    """MemoryError from to_pylist must not propagate — returns FATAL."""
    handler = _make_file_handler()
    mock_batch = MagicMock(spec=pa.RecordBatch)
    mock_batch.to_pylist = MagicMock(side_effect=MemoryError("OOM"))

    result = await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=mock_batch, record_ids=[], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE


@pytest.mark.asyncio
async def test_check_committed_runtime_error_is_fatal():
    """RuntimeError from check_committed must not propagate — returns FATAL."""
    handler = _make_file_handler(
        check_committed_exc=RuntimeError("manifest corrupted")
    )

    result = await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=_record_batch(), record_ids=["1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "RuntimeError" in result.failure_summary


@pytest.mark.asyncio
async def test_check_committed_fatal_oserror_is_fatal():
    """Fatal OSError (ENOSPC) from check_committed must not propagate."""
    handler = _make_file_handler(
        check_committed_exc=OSError(errno.ENOSPC, "disk full")
    )

    result = await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=_record_batch(), record_ids=["1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "ENOSPC" in result.failure_summary


@pytest.mark.asyncio
async def test_check_committed_eio_is_retryable():
    """Transient OSError (EIO) from check_committed must not propagate — RETRYABLE."""
    handler = _make_file_handler(
        check_committed_exc=OSError(errno.EIO, "storage unreachable")
    )

    result = await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=_record_batch(), record_ids=["1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert "EIO" in result.failure_summary


@pytest.mark.asyncio
async def test_empty_batch_record_commit_io_error_is_retryable():
    """Transient OSError from empty-batch record_commit must not propagate."""
    handler = _make_file_handler(
        record_commit_exc=OSError(errno.EIO, "storage unreachable")
    )
    empty_batch = pa.RecordBatch.from_pylist([])

    result = await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=empty_batch, record_ids=[], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert "EIO" in result.failure_summary


@pytest.mark.asyncio
async def test_empty_batch_record_commit_runtime_error_is_fatal():
    """RuntimeError from empty-batch record_commit must not propagate — FATAL."""
    handler = _make_file_handler(
        record_commit_exc=RuntimeError("manifest write failed")
    )
    empty_batch = pa.RecordBatch.from_pylist([])

    result = await handler.write_batch(
        run_id="r", stream_id="s", batch_seq=1,
        record_batch=empty_batch, record_ids=[], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "RuntimeError" in result.failure_summary
