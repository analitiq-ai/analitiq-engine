"""File-path uniqueness tests for same-RUN_ID restarts (issue #319).

On a same-RUN_ID restart batch_seq resets to 0 while the source resumes
from the committed cursor, so without a content-derived suffix two batches
with different content land on the same filename and the storage backend
(which opens in "wb" mode) silently overwrites the pre-restart data.

The fix: include the first 16 hex chars of SHA-256(serialized data) in the
filename.  Same content → same path (idempotent for in-run replays); different
content → different path (restart cannot overwrite).
"""

import hashlib
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from src.destination.connectors.file import FileDestinationHandler
from src.destination.storage.base import BaseStorageBackend
from src.destination.storage.local import LocalFileStorage
from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

# ---------------------------------------------------------------------------
# build_path: unit tests on the base class method
# ---------------------------------------------------------------------------


_EMITTED_AT = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)
"""A fixed, timezone-aware emit instant for write_batch/send_batch calls;
the engine stamps this per batch (issue #353). Value is arbitrary for sinks
that ignore it."""


class TestBuildPathContentHash:
    """build_path embeds the content hash to avoid same-seq overwrites."""

    def _storage(self) -> BaseStorageBackend:
        return LocalFileStorage()

    def test_no_hash_falls_back_to_batch_seq_only(self):
        path = self._storage().build_path(
            base_path="/out",
            stream_id="orders",
            batch_seq=3,
            extension=".jsonl",
            timestamp=_EMITTED_AT,
        )
        assert path == "/out/orders/3.jsonl"

    def test_hash_included_in_stem(self):
        path = self._storage().build_path(
            base_path="/out",
            stream_id="orders",
            batch_seq=3,
            extension=".jsonl",
            timestamp=_EMITTED_AT,
            content_hash="abcd1234ef567890",
        )
        assert path == "/out/orders/3_abcd1234ef567890.jsonl"

    def test_different_hashes_produce_different_paths(self):
        s = self._storage()
        p1 = s.build_path(
            "/out", "s", 0, ".jsonl", _EMITTED_AT, content_hash="aaaa0000aaaa0000"
        )
        p2 = s.build_path(
            "/out", "s", 0, ".jsonl", _EMITTED_AT, content_hash="bbbb1111bbbb1111"
        )
        assert p1 != p2

    def test_same_hash_produces_same_path(self):
        s = self._storage()
        h = "deadbeef12345678"
        p1 = s.build_path("/out", "s", 5, ".jsonl", _EMITTED_AT, content_hash=h)
        p2 = s.build_path("/out", "s", 5, ".jsonl", _EMITTED_AT, content_hash=h)
        assert p1 == p2

    def test_partition_template_includes_hash(self):
        path = self._storage().build_path(
            base_path="/out",
            stream_id="orders",
            batch_seq=1,
            extension=".jsonl",
            timestamp=_EMITTED_AT,
            content_hash="cafe0000cafe0000",
            partition_template="year={year}/month={month}",
        )
        # Path must contain the hash as part of the final filename stem
        assert "orders_1_cafe0000cafe0000.jsonl" in path
        assert path.startswith("/out/year=")

    def test_partition_placeholders_resolve_from_timestamp_not_wall_clock(self):
        """The #353 guarantee: the partition dir comes from the passed
        timestamp, so two calls with the same batch timestamp but different
        wall clocks resolve the same path -- a replayed batch overwrites in
        place instead of drifting across an hour/day boundary.
        """
        s = self._storage()
        tmpl = "year={year}/month={month}/day={day}/hour={hour}"
        ts = datetime(2026, 7, 21, 9, 30, 0, tzinfo=timezone.utc)
        common = dict(
            base_path="/out",
            stream_id="orders",
            batch_seq=3,
            extension=".jsonl",
            content_hash="ab12cd34ef56ab78",
            partition_template=tmpl,
        )
        # Same batch timestamp -> same partition path, regardless of "now".
        p1 = s.build_path(timestamp=ts, **common)
        p2 = s.build_path(timestamp=ts, **common)
        assert (
            p1
            == p2
            == "/out/year=2026/month=07/day=21/hour=09/orders_3_ab12cd34ef56ab78.jsonl"
        )
        # A later batch timestamp (next hour) lands in the next partition.
        p_next = s.build_path(timestamp=ts.replace(hour=10), **common)
        assert "/hour=10/" in p_next and p_next != p1


# ---------------------------------------------------------------------------
# write_batch: content hash is derived from serialized data and passed down
# ---------------------------------------------------------------------------


def _make_handler(serialize_return: bytes) -> FileDestinationHandler:
    """Return a wired-up handler whose formatter returns a fixed byte string."""
    handler = FileDestinationHandler()
    handler._connected = True

    mock_formatter = MagicMock()
    mock_formatter.serialize_batch.return_value = serialize_return
    mock_formatter.file_extension = ".jsonl"
    mock_formatter.content_type = "application/jsonl"
    handler._formatter = mock_formatter

    mock_storage = MagicMock()
    mock_storage.build_path.return_value = "/tmp/out/s/0_abc.jsonl"
    mock_storage.write_file = AsyncMock(return_value="/tmp/out/s/0_abc.jsonl")
    handler._storage = mock_storage

    handler._path_template = None
    handler._config = {"path": "/tmp/out", "prefix": ""}
    return handler


@pytest.mark.asyncio
async def test_write_batch_passes_content_hash_to_build_path():
    """write_batch derives SHA-256[:16] of serialized data and passes it."""
    data = b'{"id":1}\n'
    expected_hash = hashlib.sha256(data).hexdigest()[:16]

    handler = _make_handler(serialize_return=data)
    await handler.write_batch(
        run_id="r1",
        stream_id="s",
        batch_seq=0,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["r1"],
        cursor=Cursor(token=b"c"),
        emitted_at=_EMITTED_AT,
    )

    call_kwargs = handler._storage.build_path.call_args[1]
    assert call_kwargs["content_hash"] == expected_hash
    assert len(call_kwargs["content_hash"]) == 16


@pytest.mark.asyncio
async def test_same_run_id_restart_different_content_gets_different_path():
    """Simulates a same-RUN_ID restart: batch_seq=0 on first and second run.

    First run: rows A  → hash_A → path_A
    Second run: rows B → hash_B → path_B (different file, no overwrite)
    """
    data_a = b'{"id":1}\n'
    data_b = b'{"id":2}\n'
    hash_a = hashlib.sha256(data_a).hexdigest()[:16]
    hash_b = hashlib.sha256(data_b).hexdigest()[:16]
    assert hash_a != hash_b

    handler_a = _make_handler(serialize_return=data_a)
    handler_b = _make_handler(serialize_return=data_b)

    for handler in (handler_a, handler_b):
        await handler.write_batch(
            run_id="run-1",
            stream_id="s",
            batch_seq=0,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["r1"],
            cursor=Cursor(token=b"c"),
            emitted_at=_EMITTED_AT,
        )

    captured_hash_a = handler_a._storage.build_path.call_args[1]["content_hash"]
    captured_hash_b = handler_b._storage.build_path.call_args[1]["content_hash"]
    # Different hashes guarantee different paths given the deterministic format.
    assert captured_hash_a != captured_hash_b


@pytest.mark.asyncio
async def test_in_run_replay_same_content_overwrites_same_path():
    """A replayed batch reaches the write and lands on the same path.

    There is no batch-level commit ledger (issue #306): a replay is not
    skipped, it is re-written. Identical data always maps to the same
    hash and therefore the same file path, so the rewrite overwrites the
    same file with the same bytes — the write itself is the dedup.
    """
    data = b'{"id":1}\n'
    handler = _make_handler(serialize_return=data)

    for _ in range(2):
        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s",
            batch_seq=0,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["r1"],
            cursor=Cursor(token=b"c"),
            emitted_at=_EMITTED_AT,
        )
        assert result.status == AckStatus.ACK_STATUS_SUCCESS

    calls = handler._storage.build_path.call_args_list
    assert len(calls) == 2
    assert calls[0][1]["content_hash"] == calls[1][1]["content_hash"]
    assert handler._storage.write_file.call_count == 2


@pytest.mark.asyncio
async def test_restart_batch_at_committed_seq_is_written_not_skipped():
    """A same-RUN_ID restart batch is never skipped as a replay (issue #306).

    The removed manifest keyed commits on (run_id, stream_id, batch_seq)
    and answered ALREADY_COMMITTED for a committed position — dropping the
    different rows a restart re-batches onto that position (the #282 row-
    drop class). Both writes at the same position must reach storage and
    ack SUCCESS.
    """
    handler = _make_handler(serialize_return=b'{"id":1}\n')

    for data in (b'{"id":1}\n', b'{"id":2}\n'):
        handler._formatter.serialize_batch.return_value = data
        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s",
            batch_seq=0,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["r1"],
            cursor=Cursor(token=b"c"),
            emitted_at=_EMITTED_AT,
        )
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 1

    assert handler._storage.write_file.call_count == 2


@pytest.mark.asyncio
async def test_write_batch_result_is_success():
    """Sanity check: the happy path still returns ACK_STATUS_SUCCESS."""
    handler = _make_handler(serialize_return=b'{"id":1}\n')
    result = await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=0,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["r1"],
        cursor=Cursor(token=b"c"),
        emitted_at=_EMITTED_AT,
    )
    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.committed_cursor.token == b"c"


@pytest.mark.asyncio
async def test_empty_batch_skips_serialize_and_build_path():
    """An empty batch returns SUCCESS without serializing or building a path.

    The empty-batch guard fires before the serialize→hash→build_path
    sequence, so an empty payload never reaches the hashing step.
    """
    handler = _make_handler(serialize_return=b"")
    result = await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=0,
        record_batch=pa.RecordBatch.from_pylist([]),
        record_ids=[],
        cursor=Cursor(token=b"c"),
        emitted_at=_EMITTED_AT,
    )
    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 0
    # The branch exists to keep the checkpoint advancing on empty batches.
    assert result.committed_cursor.token == b"c"
    handler._formatter.serialize_batch.assert_not_called()
    handler._storage.build_path.assert_not_called()


@pytest.mark.asyncio
async def test_serialize_exception_is_fatal_and_build_path_not_called():
    """A formatter error during serialization becomes FATAL_FAILURE.

    Serialization runs before build_path. An exception there must be caught
    by the outer except block and must never allow build_path to be called
    with a partial or absent hash.
    """
    handler = _make_handler(serialize_return=b"unused")
    handler._formatter.serialize_batch.side_effect = RuntimeError("encoding failed")

    result = await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=0,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["r1"],
        cursor=Cursor(token=b"c"),
        emitted_at=_EMITTED_AT,
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "RuntimeError" in result.failure_summary
    handler._storage.build_path.assert_not_called()


# ---------------------------------------------------------------------------
# write_batch: emitted_at feeds the partition path (issue #353)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partitioned_write_passes_emitted_at_to_build_path():
    """With a path_template, write_batch forwards emitted_at as build_path's
    timestamp, so the partition dir is a function of the replay-stable batch
    instant rather than the write-time clock (issue #353).
    """
    handler = _make_handler(serialize_return=b'{"id":1}\n')
    handler._path_template = "year={year}/month={month}/day={day}/hour={hour}"

    await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=0,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["r1"],
        cursor=Cursor(token=b"c"),
        emitted_at=_EMITTED_AT,
    )

    call_kwargs = handler._storage.build_path.call_args[1]
    assert call_kwargs["timestamp"] is _EMITTED_AT
    assert call_kwargs["partition_template"] == handler._path_template


@pytest.mark.asyncio
async def test_partitioned_write_with_unstamped_emitted_at_is_fatal():
    """A partitioned file destination must fail loud on an unstamped
    emitted_at (epoch 0) rather than bucket every replay under year=1970
    (issue #353). build_path is never reached.
    """
    handler = _make_handler(serialize_return=b'{"id":1}\n')
    handler._path_template = "year={year}/month={month}"
    unstamped = datetime.fromtimestamp(0, tz=timezone.utc)  # wire default

    result = await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=0,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["r1"],
        cursor=Cursor(token=b"c"),
        emitted_at=unstamped,
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "emitted_at" in result.failure_summary
    handler._storage.build_path.assert_not_called()
    handler._storage.write_file.assert_not_called()


@pytest.mark.asyncio
async def test_unpartitioned_write_tolerates_unstamped_emitted_at():
    """Without a path_template there is no partition to render, so an
    unstamped emitted_at is harmless -- the guard only fires when the layout
    actually needs the instant.
    """
    handler = _make_handler(serialize_return=b'{"id":1}\n')
    handler._path_template = None
    unstamped = datetime.fromtimestamp(0, tz=timezone.utc)

    result = await handler.write_batch(
        run_id="r",
        stream_id="s",
        batch_seq=0,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["r1"],
        cursor=Cursor(token=b"c"),
        emitted_at=unstamped,
    )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    handler._storage.build_path.assert_called_once()
