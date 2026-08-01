"""OSError errno classification tests for file/stream destinations.

Disk-full, permission, EROFS, and EPIPE are not recoverable by retry —
classifying them as RETRYABLE would burn the DLQ budget and mask the
root cause. These tests pin the errno → status mapping so a regression
that drops an errno from the fatal set fails CI loudly.

Also covers operations that were previously outside the try/except —
to_pylist — which would propagate as bare exceptions instead of a
BatchWriteResult.
"""

import errno
import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from cdk.file.generic import GenericFileConnector
from cdk.stdout.generic import GenericStdoutConnector
from cdk.types import FailureCategory
from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

# Derived from the subject, never hand-written: a stale literal here selects
# no records at all, and an assertion over an empty list is the quiet kind of
# pass that hides a broken test.
_FILE_LOGGER = GenericFileConnector.__module__

# A fixed, timezone-aware emit instant for write_batch/send_batch calls; the
# engine stamps this per batch (issue #353). Value is arbitrary for sinks
# that ignore it.
_EMITTED_AT = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)


def _record_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([{"id": 1, "v": "row"}])


def _cursor() -> Cursor:
    return Cursor(token=b"")


async def _drive_file(handler: GenericFileConnector, raise_exc: BaseException):
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=b"data\n")
    handler._formatter.file_extension = "jsonl"
    handler._formatter.content_type = "application/jsonl"
    handler._storage = MagicMock()
    handler._storage.build_path = MagicMock(return_value="/tmp/output.jsonl")
    handler._storage.write_file = AsyncMock(side_effect=raise_exc)
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": "out/"}
    return await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=1,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )


async def _drive_stream(handler: GenericStdoutConnector, raise_exc: BaseException):
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=b"data\n")
    with patch("cdk.stdout.generic.sys") as fake_sys:
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
            emitted_at=_EMITTED_AT,
        )


#: The one table both sinks judge by. It is the union of the two lists they
#: kept separately: EPIPE cannot occur writing to a file and EROFS cannot
#: occur writing to stdout, so each sink carrying the other's entries costs
#: nothing -- and "EROFS is retryable on stdout" was nobody's decision.
FATAL_ERRNOS = [
    errno.EPIPE,
    errno.ENOSPC,
    errno.EACCES,
    errno.EROFS,
    errno.EDQUOT,
    errno.EBADF,
]


@pytest.mark.asyncio
@pytest.mark.parametrize("errno_code", FATAL_ERRNOS)
async def test_file_handler_fatal_errnos(errno_code):
    """Closed pipe, disk-full, permission, read-only fs, quota, bad fd."""
    exc = OSError(errno_code, "fatal i/o")
    result = await _drive_file(GenericFileConnector(), exc)
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert result.success is False
    assert errno.errorcode[errno_code] in result.failure_summary


@pytest.mark.asyncio
@pytest.mark.parametrize("errno_code", FATAL_ERRNOS)
async def test_both_sinks_judge_an_errno_the_same_way(errno_code):
    """One table, so neither sink can call fatal what the other retries."""
    exc = OSError(errno_code, "fatal i/o")
    file_result = await _drive_file(GenericFileConnector(), exc)
    stream_result = await _drive_stream(GenericStdoutConnector(), exc)
    assert file_result.status == stream_result.status
    assert file_result.failure_category == stream_result.failure_category


@pytest.mark.asyncio
@pytest.mark.parametrize("errno_code", FATAL_ERRNOS)
async def test_a_failed_write_says_who_owns_it(errno_code):
    """A full disk is the destination's; a closed descriptor is ours.

    EBADF means this process wrote to a descriptor it already closed, which
    no operator can act on. Reporting it as a write rejection would send
    them looking for a full volume or a permissions problem that is not
    there.
    """
    result = await _drive_file(GenericFileConnector(), OSError(errno_code, "x"))
    expected = (
        FailureCategory.FAILURE_CATEGORY_INTERNAL
        if errno_code == errno.EBADF
        else FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
    )
    assert result.failure_category == expected


@pytest.mark.asyncio
async def test_file_handler_eio_is_retryable():
    """Transient I/O errors (EIO) should stay RETRYABLE."""
    exc = OSError(errno.EIO, "transient")
    result = await _drive_file(GenericFileConnector(), exc)
    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


@pytest.mark.asyncio
async def test_file_handler_non_oserror_is_fatal():
    """Programming errors (KeyError, TypeError, …) become FATAL."""
    result = await _drive_file(GenericFileConnector(), KeyError("missing"))
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "KeyError" in result.failure_summary


@pytest.mark.asyncio
@pytest.mark.parametrize("errno_code", FATAL_ERRNOS)
async def test_stream_handler_fatal_errnos(errno_code):
    """Closed pipe, disk-full on redirect, permission, bad-fd: never
    recoverable by retry."""
    exc = OSError(errno_code, "fatal stdout")
    result = await _drive_stream(GenericStdoutConnector(), exc)
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert result.success is False


@pytest.mark.asyncio
async def test_stream_handler_eio_is_retryable():
    exc = OSError(errno.EIO, "transient stdout")
    result = await _drive_stream(GenericStdoutConnector(), exc)
    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


@pytest.mark.asyncio
async def test_stream_handler_no_errno_oserror_is_retryable():
    """OSError with no errno renders 'unknown' not 'None' in failure_summary."""
    exc = OSError("connection reset with no errno")
    result = await _drive_stream(GenericStdoutConnector(), exc)
    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert "None" not in result.failure_summary
    assert "unknown" in result.failure_summary


@pytest.mark.asyncio
async def test_stream_handler_non_oserror_is_fatal():
    result = await _drive_stream(GenericStdoutConnector(), TypeError("bug"))
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "TypeError" in result.failure_summary


@pytest.mark.asyncio
async def test_stream_handler_to_pylist_arrow_invalid_is_fatal():
    """ArrowInvalid from to_pylist in stream handler must not propagate — FATAL."""
    handler = GenericStdoutConnector()
    handler._connected = True
    handler._formatter = MagicMock()
    mock_batch = MagicMock(spec=pa.RecordBatch)
    mock_batch.to_pylist = MagicMock(side_effect=pa.ArrowInvalid("corrupt buffer"))

    result = await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=1,
        record_batch=mock_batch,
        record_ids=[],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "ArrowInvalid" in result.failure_summary


def _make_file_handler():
    """Return a wired GenericFileConnector for early write_batch path tests.

    Verifies that to_pylist is caught by the try/except rather than
    propagating.
    """
    handler = GenericFileConnector()
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=b"data\n")
    handler._formatter.file_extension = "jsonl"
    handler._formatter.content_type = "application/jsonl"
    handler._storage = MagicMock()
    handler._storage.build_path = MagicMock(return_value="/tmp/out.jsonl")
    handler._storage.write_file = AsyncMock(return_value="/tmp/out.jsonl")
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": ""}
    return handler


@pytest.mark.asyncio
async def test_to_pylist_arrow_invalid_is_fatal():
    """ArrowInvalid from to_pylist must not propagate — returns FATAL."""
    handler = _make_file_handler()
    mock_batch = MagicMock(spec=pa.RecordBatch)
    mock_batch.to_pylist = MagicMock(side_effect=pa.ArrowInvalid("corrupt buffer"))

    result = await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=1,
        record_batch=mock_batch,
        record_ids=[],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
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
        run_id="r",
        stream_id="s",
        batch_seq=1,
        record_batch=mock_batch,
        record_ids=[],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE


async def _drive_file_with_context(
    handler: GenericFileConnector, raise_exc: BaseException
):
    """Like _drive_file but with distinct batch identity values for log assertions."""
    handler._connected = True
    handler._formatter = MagicMock()
    handler._formatter.serialize_batch = MagicMock(return_value=b"data\n")
    handler._formatter.file_extension = "jsonl"
    handler._formatter.content_type = "application/jsonl"
    handler._storage = MagicMock()
    handler._storage.build_path = MagicMock(return_value="/tmp/output.jsonl")
    handler._storage.write_file = AsyncMock(side_effect=raise_exc)
    handler._path_template = None
    handler._config = {"path": "/tmp", "prefix": "out/"}
    return await handler.write_batch(
        run_id="run-abc123",
        stream_id="stream-xyz789",
        batch_seq=42,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=_cursor(),
        emitted_at=_EMITTED_AT,
    )


def _assert_batch_context_in_log(caplog):
    """Assert one ERROR record from file.py carries the batch identity fields."""
    logger_name = _FILE_LOGGER
    msgs = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.ERROR and r.name == logger_name
    ]
    assert msgs, f"No ERROR records from {logger_name}; all records: {caplog.records}"
    assert any("run=run-abc123" in m for m in msgs), f"run_id missing: {msgs}"
    assert any("stream=stream-xyz789" in m for m in msgs), f"stream_id missing: {msgs}"
    assert any("seq=42" in m for m in msgs), f"batch_seq missing: {msgs}"


@pytest.mark.asyncio
async def test_file_handler_fatal_errno_log_includes_batch_context(caplog):
    """Fatal OSError log must include run_id, stream_id, batch_seq."""
    exc = OSError(errno.ENOSPC, "disk full")
    with caplog.at_level(logging.ERROR, logger=_FILE_LOGGER):
        await _drive_file_with_context(GenericFileConnector(), exc)

    _assert_batch_context_in_log(caplog)
    assert any(
        "ENOSPC" in r.getMessage()
        for r in caplog.records
        if r.levelno == logging.ERROR and r.name == _FILE_LOGGER
    )


@pytest.mark.asyncio
async def test_file_handler_retryable_oserror_log_includes_batch_context(caplog):
    """Retryable OSError log must include run_id, stream_id, batch_seq."""
    exc = OSError(errno.EIO, "transient")
    with caplog.at_level(logging.ERROR, logger=_FILE_LOGGER):
        await _drive_file_with_context(GenericFileConnector(), exc)

    _assert_batch_context_in_log(caplog)


@pytest.mark.asyncio
async def test_file_handler_non_oserror_log_includes_batch_context(caplog):
    """Catch-all Exception log must include run_id, stream_id, batch_seq."""
    with caplog.at_level(logging.ERROR, logger=_FILE_LOGGER):
        await _drive_file_with_context(GenericFileConnector(), KeyError("missing"))

    _assert_batch_context_in_log(caplog)
