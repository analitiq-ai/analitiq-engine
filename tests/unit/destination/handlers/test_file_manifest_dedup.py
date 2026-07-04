"""Content-based manifest dedup tests (issue #306).

The manifest must dedup by record content, not batch position, to avoid
silently dropping rows on a same-RUN_ID restart.  The bug: when the source
resumes from the committed cursor, batch_seq restarts from 0, so a committed
position re-arrives carrying different rows — the old positional key matched
and those rows were skipped.  The fix: key on a hash of sorted record_ids
(content-derived, position-independent) so a restart with new rows misses
the manifest and a true in-run replay hits it.
"""

import json
import logging
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from src.destination.connectors.file import FileDestinationHandler
from src.destination.idempotency.manifest import BatchCommit, ManifestTracker
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


# ── Handler-level tests (via mocked manifest) ─────────────────────────────


@pytest.mark.asyncio
@pytest.mark.unit
async def test_same_run_restart_with_new_rows_writes_not_skipped():
    """Core bug regression: batch_seq=0 on restart carries different rows.

    After the fix, the manifest misses (different record_ids → different content
    hash) and the new rows are written rather than skipped.
    """
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
    # Reset the mock between calls to test the second write in isolation.
    manifest.check_committed = AsyncMock(return_value=None)
    manifest.record_commit.reset_mock()

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

    manifest.check_committed.assert_called_once_with("run-1", "s1", ["id-new-after-cursor"])
    manifest.record_commit.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_in_run_replay_same_content_returns_already_committed():
    """ACK-lost replay: same batch resent with same record_ids → skip."""
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
    assert result.committed_cursor.token == b"cur-0"
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
    assert check_call.args == ("r", "s", ["hash-abc"])

    record_call = manifest.record_commit.call_args
    assert record_call.kwargs["record_ids"] == ["hash-abc"]
    assert record_call.kwargs["batch_seq"] == 42


@pytest.mark.asyncio
@pytest.mark.unit
async def test_non_empty_records_with_empty_record_ids_is_fatal():
    """Engine contract: record_ids must be populated for non-empty batches.

    An empty record_ids list for a non-empty batch would bypass dedup and
    record a constant-key commit that silently drops future batches.  The
    handler must refuse and return FATAL rather than silently losing data.
    """
    handler = _make_handler()
    handler._manifest = MagicMock()

    result = await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=0,
        record_batch=_batch({"id": 1}),
        record_ids=[],
        cursor=_cursor(),
    )
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "record_ids" in result.failure_summary
    handler._storage.write_file.assert_not_called()


# ── ManifestTracker unit tests ────────────────────────────────────────────


_BASE = "base"
_MANIFEST_PATH = f"{_BASE}/_manifest.json"


def _fake_storage(existing_json: bytes | None = None):
    """In-memory storage backend for ManifestTracker unit tests.

    Uses base_path=_BASE so the manifest lives at _MANIFEST_PATH.
    """
    stored: dict[str, bytes] = {}
    if existing_json is not None:
        stored[_MANIFEST_PATH] = existing_json

    async def _exists(path: str) -> bool:
        return path in stored

    async def _read(path: str) -> bytes:
        return stored.get(path, b"")

    async def _write(path: str, data: bytes) -> str:
        stored[path] = data
        return path

    storage = MagicMock()
    storage.file_exists = AsyncMock(side_effect=_exists)
    storage.read_file = AsyncMock(side_effect=_read)
    storage.write_file = AsyncMock(side_effect=_write)
    return storage, stored


@pytest.mark.asyncio
@pytest.mark.unit
async def test_manifest_round_trip_dedup_survives_restart():
    """Critical: content-hash key written in one process must be found after reload.

    This test exposed the load() bug: it called _make_key(commit.batch_seq) instead
    of using the persisted content_hash, so previously-committed batches were never
    found after a process restart.
    """
    storage, _ = _fake_storage()
    tracker1 = ManifestTracker(storage, _BASE)

    await tracker1.record_commit(
        run_id="r", stream_id="s", batch_seq=0,
        record_ids=["id-a", "id-b"],
        records_written=2, cursor_bytes=b"c", file_path="/f",
    )

    # Fresh instance simulates a process restart — forces load() from persisted JSON
    tracker2 = ManifestTracker(storage, _BASE)
    result = await tracker2.check_committed("r", "s", ["id-a", "id-b"])

    assert result is not None, "Same record_ids must be found after restart"
    assert result.records_written == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_content_hash_is_order_independent():
    """Sorting before hashing: same rows in different order → same key → dedup hit."""
    storage, _ = _fake_storage()
    tracker = ManifestTracker(storage, _BASE)

    await tracker.record_commit(
        run_id="r", stream_id="s", batch_seq=0,
        record_ids=["id-b", "id-a"],
        records_written=2, cursor_bytes=b"c", file_path="/f",
    )

    result = await tracker.check_committed("r", "s", ["id-a", "id-b"])
    assert result is not None, "Order-swapped record_ids must hit the same key"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_different_content_misses_manifest():
    """Different record_ids (restart with new rows) produce a cache miss."""
    storage, _ = _fake_storage()
    tracker = ManifestTracker(storage, _BASE)

    await tracker.record_commit(
        run_id="r", stream_id="s", batch_seq=0,
        record_ids=["id-original"],
        records_written=1, cursor_bytes=b"c", file_path="/f",
    )

    result = await tracker.check_committed("r", "s", ["id-new"])
    assert result is None, "Different record_ids must not match the stored commit"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_legacy_positional_entries_skipped_on_load(caplog):
    """Entries without content_hash (old manifest format) are skipped on load.

    They cannot be migrated because the original record_ids were never persisted.
    A warning must be logged so operators know re-writes may occur.
    """
    old_manifest = json.dumps({
        "version": 1,
        "commits": [{
            "run_id": "r",
            "stream_id": "s",
            "batch_seq": 0,
            # no content_hash field — this is the pre-fix format
            "records_written": 1,
            "cursor_bytes": b"c".hex(),
            "file_path": "/f",
            "committed_at": "2024-01-01T00:00:00+00:00",
        }],
    }).encode()

    storage, _ = _fake_storage(old_manifest)

    tracker = ManifestTracker(storage, _BASE)

    with caplog.at_level(logging.WARNING):
        await tracker.load()

    # Legacy entry must not be in _commits
    assert len(tracker._commits) == 0
    # But a warning about re-write risk must have been emitted
    assert any("legacy" in record.message.lower() for record in caplog.records)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_malformed_commit_entry_raises_runtime_error():
    """A structurally invalid commit entry must raise RuntimeError, not crash silently."""
    bad_manifest = json.dumps({
        "version": 1,
        "commits": [{"run_id": "r"}],  # missing required fields
    }).encode()

    storage, _ = _fake_storage(bad_manifest)

    tracker = ManifestTracker(storage, _BASE)
    with pytest.raises(RuntimeError, match="malformed"):
        await tracker.load()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_corrupt_json_manifest_raises_runtime_error():
    """Totally corrupted manifest JSON must raise RuntimeError (not silently start fresh)."""
    storage, _ = _fake_storage(b"not valid json {{")

    tracker = ManifestTracker(storage, _BASE)
    with pytest.raises(RuntimeError, match="corrupted"):
        await tracker.load()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_same_content_different_run_id_misses_manifest():
    """Different run_id always produces a different key, even for identical record_ids.

    run_id is part of the key so cross-run dedup never occurs — each run
    writes its own data independently.
    """
    storage, _ = _fake_storage()
    tracker = ManifestTracker(storage, _BASE)

    await tracker.record_commit(
        run_id="run-1", stream_id="s", batch_seq=0,
        record_ids=["id-a"],
        records_written=1, cursor_bytes=b"c", file_path="/f",
    )

    result = await tracker.check_committed("run-2", "s", ["id-a"])
    assert result is None, "Different run_id must not match a commit from run-1"
