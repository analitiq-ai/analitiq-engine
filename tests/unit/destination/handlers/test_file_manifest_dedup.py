"""Content-based manifest dedup tests (issue #306).

The manifest must dedup by record content, not batch position, to avoid
silently dropping rows on a same-RUN_ID restart.  The bug: when the source
resumes from the committed cursor, batch_seq restarts from 0, so a committed
position re-arrives carrying different rows — the old positional key matched
and those rows were skipped.  The fix: key on a hash of sorted record_ids
(content-derived, position-independent) so a restart with new rows misses
the manifest and a true in-run replay hits it.
"""

from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from src.destination.connectors.file import FileDestinationHandler
from src.grpc.generated.analitiq.v1 import AckStatus, Cursor


def _make_handler() -> FileDestinationHandler:
    handler = FileDestinationHandler()
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


def _batch(*rows) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(list(rows))


def _cursor(token: bytes = b"tok") -> Cursor:
    return Cursor(token=token)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_same_run_restart_with_new_rows_writes_not_skipped():
    """Core bug regression: batch_seq=0 on restart carries different rows.

    After the fix, the manifest misses (different record_ids → different content
    hash) and the new rows are written rather than skipped.
    """
    from src.destination.idempotency.manifest import BatchCommit

    handler = _make_handler()
    manifest = MagicMock()

    # First call (original run): no prior commit found → write succeeds
    manifest.check_committed = AsyncMock(return_value=None)
    manifest.record_commit = AsyncMock()
    handler._manifest = manifest

    result1 = await handler.write_batch(
        run_id="run-1",
        stream_id="s1",
        batch_seq=0,
        record_batch=_batch({"id": 1}),
        record_ids=["id-original"],
        cursor=_cursor(b"cur-0"),
    )
    assert result1.status == AckStatus.ACK_STATUS_SUCCESS
    assert result1.records_written == 1

    # Restart: same run_id + same batch_seq=0, but DIFFERENT record_ids (new rows).
    # The manifest returns None → these rows must be written, not skipped.
    manifest.check_committed = AsyncMock(return_value=None)

    result2 = await handler.write_batch(
        run_id="run-1",
        stream_id="s1",
        batch_seq=0,
        record_batch=_batch({"id": 2}),
        record_ids=["id-new-after-cursor"],
        cursor=_cursor(b"cur-1"),
    )
    assert result2.status == AckStatus.ACK_STATUS_SUCCESS
    assert result2.records_written == 1

    # Verify manifest was checked with the new record_ids (content-based)
    manifest.check_committed.assert_called_once_with("run-1", "s1", ["id-new-after-cursor"])


@pytest.mark.asyncio
@pytest.mark.unit
async def test_in_run_replay_same_content_returns_already_committed():
    """ACK-lost replay: same batch resent with same record_ids → skip."""
    from src.destination.idempotency.manifest import BatchCommit

    existing = BatchCommit(
        run_id="run-1",
        stream_id="s1",
        batch_seq=0,
        records_written=1,
        cursor_bytes=b"cur-0",
        file_path="/tmp/out.jsonl",
    )

    handler = _make_handler()
    manifest = MagicMock()
    manifest.check_committed = AsyncMock(return_value=existing)
    manifest.record_commit = AsyncMock()
    handler._manifest = manifest

    result = await handler.write_batch(
        run_id="run-1",
        stream_id="s1",
        batch_seq=0,
        record_batch=_batch({"id": 1}),
        record_ids=["id-original"],
        cursor=_cursor(b"cur-0"),
    )
    assert result.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED
    assert result.records_written == 1
    # No write should have occurred
    handler._storage.write_file.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_empty_batch_bypasses_manifest_entirely():
    """Empty batches have no record_ids; the manifest must not be consulted."""
    handler = _make_handler()
    manifest = MagicMock()
    manifest.check_committed = AsyncMock()
    manifest.record_commit = AsyncMock()
    handler._manifest = manifest

    result = await handler.write_batch(
        run_id="run-1",
        stream_id="s1",
        batch_seq=0,
        record_batch=_batch(),
        record_ids=[],
        cursor=_cursor(),
    )
    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 0
    manifest.check_committed.assert_not_called()
    manifest.record_commit.assert_not_called()
    handler._storage.write_file.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_manifest_check_uses_record_ids_not_batch_seq():
    """Verify the manifest is called with record_ids, not batch_seq."""
    handler = _make_handler()
    manifest = MagicMock()
    manifest.check_committed = AsyncMock(return_value=None)
    manifest.record_commit = AsyncMock()
    handler._manifest = manifest

    await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=42,
        record_batch=_batch({"x": 1}),
        record_ids=["hash-abc"],
        cursor=_cursor(),
    )

    check_call = manifest.check_committed.call_args
    # Should be called with (run_id, stream_id, record_ids)
    assert check_call.args == ("r", "s", ["hash-abc"])

    record_call = manifest.record_commit.call_args
    # batch_seq still passed for audit; record_ids also passed for key derivation
    assert record_call.kwargs["record_ids"] == ["hash-abc"]
    assert record_call.kwargs["batch_seq"] == 42
