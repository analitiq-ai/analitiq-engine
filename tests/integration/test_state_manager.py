"""Integration tests for state manager functionality."""

import json
import logging
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from src.state.state_manager import StateManager


def _make_manager(tmp_path, pipeline_id="test-pipeline"):
    """Create a StateManager rooted at tmp_path/state."""
    state_dir = tmp_path / "state"
    return StateManager(pipeline_id=pipeline_id, base_dir=str(state_dir))


class TestStateManager:
    """Test state manager core functionality."""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.temp_dir)
        self.pipeline_id = "test-pipeline"

    def teardown_method(self):
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def test_state_manager_initialization(self):
        """Test state manager proper initialization."""
        manager = _make_manager(self.tmp_path, self.pipeline_id)

        assert manager.pipeline_id == self.pipeline_id
        state_dir = self.tmp_path / "state"
        assert manager.base_dir == state_dir
        assert manager.pipeline_dir == state_dir / self.pipeline_id

    def test_start_run_new_pipeline(self):
        """Test starting a run for a new pipeline."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test-pipeline", "version": "1.0"}
        run_id = manager.start_run(config)

        assert run_id is not None
        assert len(run_id) > 0
        assert manager.get_run_info()["run_id"] == run_id

    def test_start_run_with_custom_run_id(self):
        """Test starting a run with custom run ID."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        custom_run_id = "custom-run-123"

        returned_run_id = manager.start_run(config, custom_run_id)

        assert returned_run_id == custom_run_id
        assert manager.get_run_info()["run_id"] == custom_run_id

    def test_start_run_multiple_runs(self):
        """Test starting multiple runs overwrites current run ID."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}

        run_id1 = manager.start_run(config, "run1")
        run_id2 = manager.start_run(config, "run2")

        assert run_id1 == "run1"
        assert run_id2 == "run2"
        assert manager.get_run_info()["run_id"] == "run2"

    def test_save_stream_checkpoint_emits_state_log(self):
        """Test that save_stream_checkpoint emits ANALITIQ_STATE:: log line."""
        manager = _make_manager(self.tmp_path)
        manager.start_run({"pipeline_id": "test"}, "run-abc")

        cursor = {
            "primary": {"field": "created", "value": "2025-08-18T12:00:00Z"},
        }
        hwm = "2025-08-18T12:00:00Z"

        with patch("src.state.state_manager.emit_state_log") as mock_emit:
            manager.save_stream_checkpoint(
                stream_name="stream1",
                partition={},
                cursor=cursor,
                hwm=hwm,
                stream_version=2,
            )

            mock_emit.assert_called_once()
            call_kwargs = mock_emit.call_args
            assert call_kwargs.kwargs["run_id"] == "run-abc"
            assert call_kwargs.kwargs["pipeline_id"] == "test-pipeline"
            assert call_kwargs.kwargs["stream_id"] == "stream1"
            assert call_kwargs.kwargs["cursor_value"] == hwm
            assert call_kwargs.kwargs["cursor_hex"] == json.dumps(cursor).encode().hex()
            assert call_kwargs.kwargs["stream_version"] == 2
            # emitted_at is stamped centrally by emit_log, not passed here.
            assert "emitted_at" not in call_kwargs.kwargs

    def test_save_stream_checkpoint_real_line_carries_both_fields(self, caplog):
        """End-to-end through the real emit chain: the ANALITIQ_STATE:: line a
        checkpoint produces carries every original field plus stream_version and
        emitted_at (the two fields issue 260 adds), with nothing else dropped."""
        manager = _make_manager(self.tmp_path)
        manager.start_run({"pipeline_id": "test"}, "run-xyz")

        cursor = {"primary": {"field": "created", "value": "2025-08-18T12:00:00Z"}}
        with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
            manager.save_stream_checkpoint(
                stream_name="orders",
                partition={},
                cursor=cursor,
                hwm="2025-08-18T12:00:00Z",
                stream_version=2,
            )

        lines = [
            r.getMessage()
            for r in caplog.records
            if r.getMessage().startswith("ANALITIQ_STATE::")
        ]
        assert len(lines) == 1
        payload = json.loads(lines[0].split("::", 1)[1])

        # Original fields preserved.
        assert payload["run_id"] == "run-xyz"
        assert payload["pipeline_id"] == "test-pipeline"
        assert payload["stream_id"] == "orders"
        assert payload["cursor_value"] == "2025-08-18T12:00:00Z"
        assert payload["cursor_hex"] == json.dumps(cursor).encode().hex()
        # The two added fields.
        assert payload["stream_version"] == 2
        assert datetime.fromisoformat(payload["emitted_at"]).tzinfo is not None

    def test_get_run_info_no_run(self):
        """Test get_run_info returns empty dict when no run started."""
        manager = _make_manager(self.tmp_path)
        manager.current_run_id = None

        assert manager.get_run_info() == {}

    def test_init_commit_tracker(self):
        """Test batch commit tracker initialization."""
        manager = _make_manager(self.tmp_path)

        assert manager.commit_tracker is None

        manager.init_commit_tracker("run-123")

        assert manager.commit_tracker is not None


def _checkpoint_path(tmp_path, stream_id, pipeline_id="test-pipeline"):
    return tmp_path / "state" / pipeline_id / f"{stream_id}.json"


def _write_checkpoint(tmp_path, stream_id, value, pipeline_id="test-pipeline"):
    """Stage a per-stream committed checkpoint the way a prior run -- or the
    config bundle on a fresh container -- leaves it:
    ``state/{pipeline}/{stream_id}.json`` = ``{"cursor": <value>}``.
    """
    path = _checkpoint_path(tmp_path, stream_id, pipeline_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"cursor": value}))
    return path


def _commit(manager, stream_id, value, stream_version=1):
    """Record a committed (destination-ACKed) high-water mark, as the engine
    does on ACK via ``save_stream_checkpoint`` (which writes the per-stream file).

    ``value`` is in the cursor-token form the engine carries: a JSON-native
    scalar, or the tagged ``{"__type__": ..., "value": ...}`` form for a
    datetime/date/decimal (which is what survives the ACK round trip, not a raw
    Python object -- the checkpoint emits it as JSON).
    """
    manager.save_stream_checkpoint(
        stream_name=stream_id,
        partition={},
        cursor={"primary": {"field": "cursor", "value": value, "inclusive": True}},
        hwm=str(value),
        stream_version=stream_version,
    )


class TestStateManagerCommittedCheckpoint:
    """The per-stream committed checkpoint a fresh run resumes from.

    Each stream's cursor lives in its own ``state/{pipeline}/{stream_id}.json``,
    written on each destination ACK (``save_stream_checkpoint``) and read at the
    next run's start (``get_cursor``). A fresh container restores from the same
    per-stream files the config bundle delivers. ``save_cursor`` is the source's
    pre-ACK position and is never persisted, so the on-disk bookmark can never
    run ahead of what actually landed.
    """

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.temp_dir)

    def teardown_method(self):
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    async def test_restores_numeric_cursor_across_fresh_container(self):
        _write_checkpoint(self.tmp_path, "orders", 100)
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("orders") == {"cursor": 100}

    async def test_restores_timestamp_cursor_as_datetime(self):
        # A timestamp cursor is stored tagged, so it comes back a datetime
        # (asyncpg rejects a plain string for a timestamp bind).
        ts = "2024-06-01T12:00:00+00:00"
        _write_checkpoint(
            self.tmp_path, "events", {"__type__": "datetime", "value": ts}
        )
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("events") == {
            "cursor": datetime.fromisoformat(ts)
        }

    async def test_unknown_stream_has_no_cursor(self):
        _write_checkpoint(self.tmp_path, "orders", 100)
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("not-a-stream") is None

    async def test_malformed_checkpoint_degrades_to_no_cursor(self):
        # A corrupt tagged cursor degrades to a full re-scan, never a crash.
        _write_checkpoint(
            self.tmp_path, "orders", {"__type__": "datetime", "value": "garbage"}
        )
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("orders") is None

    async def test_no_checkpoint_means_no_cursor(self):
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("orders") is None

    async def test_commit_round_trips_across_runs(self):
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        first = _make_manager(self.tmp_path)
        _commit(first, "orders", 100)
        # A timestamp crosses the ACK tagged, the same form the cursor token uses.
        _commit(first, "events", {"__type__": "datetime", "value": ts.isoformat()})

        # A fresh manager reads the per-stream files the first run wrote.
        second = _make_manager(self.tmp_path)
        assert await second.get_cursor("orders") == {"cursor": 100}
        # The timestamp survives tagged, so it comes back a datetime not a string.
        assert await second.get_cursor("events") == {"cursor": ts}

    async def test_committed_checkpoint_on_disk_shape(self):
        manager = _make_manager(self.tmp_path)
        _commit(manager, "orders", 100)

        written = json.loads(_checkpoint_path(self.tmp_path, "orders").read_text())
        assert written == {"cursor": 100}

    async def test_resumes_from_committed_not_preack_cursor(self):
        # The source advances its cursor as it yields batches, ahead of the
        # destination ACK. Only the committed (ACKed) value is persisted, never
        # that pre-ACK position -- otherwise a stream that failed after extraction
        # raced ahead would resume past rows that never landed and skip them.
        first = _make_manager(self.tmp_path)
        _commit(first, "orders", 5)  # ACKed up to id=5
        await first.save_cursor("orders", {}, {"cursor": 999})  # source raced ahead

        second = _make_manager(self.tmp_path)
        assert await second.get_cursor("orders") == {"cursor": 5}  # committed 5

    async def test_partial_progress_persists_per_batch(self):
        # save_stream_checkpoint writes on every ACK, so an interrupted run's
        # ACKed progress survives with no end-of-run snapshot needed.
        first = _make_manager(self.tmp_path)
        _commit(first, "orders", 50)
        # No clean shutdown -- a fresh process still sees the committed batch.

        second = _make_manager(self.tmp_path)
        assert await second.get_cursor("orders") == {"cursor": 50}

    async def test_save_cursor_is_not_persisted_across_runs(self):
        # save_cursor updates only the in-run cache; the source's pre-ACK position
        # is never written to disk, so a fresh manager does not see it.
        first = _make_manager(self.tmp_path)
        await first.save_cursor("orders", {}, {"cursor": 50})

        second = _make_manager(self.tmp_path)
        assert await second.get_cursor("orders") is None

    async def test_in_run_save_updates_cache(self):
        # save_cursor feeds the in-run cache so a same-run get_cursor sees it
        # (the engine reads the resume point once at stream start).
        manager = _make_manager(self.tmp_path)
        await manager.save_cursor("orders", {}, {"cursor": 250})

        assert await manager.get_cursor("orders") == {"cursor": 250}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
