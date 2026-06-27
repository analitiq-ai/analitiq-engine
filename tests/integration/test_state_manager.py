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


def _resume_path(tmp_path, pipeline_id="test-pipeline"):
    return tmp_path / "state" / pipeline_id / "resume" / "cursors.json"


def _write_resume_file(tmp_path, payload, pipeline_id="test-pipeline"):
    """Write the resume-state file the deployment delivers in the config bundle.

    A local run writes the same ``state/{pipeline_id}/resume/cursors.json`` itself
    at the end of a run; these tests stage it directly to exercise the restore
    path.
    """
    resume_path = _resume_path(tmp_path, pipeline_id)
    resume_path.parent.mkdir(parents=True, exist_ok=True)
    resume_path.write_text(json.dumps(payload))
    return resume_path


def _commit(manager, stream_id, value, stream_version=1):
    """Record a committed (destination-ACKed) high-water mark, as the engine
    does on ACK via ``save_stream_checkpoint``.

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


class TestStateManagerDurableRestore:
    """Restoring incremental cursors from the delivered resume-state file.

    A fresh container's local per-stream ``state/`` checkpoints are empty
    (Fargate wipes them every task), so the cursor a prior run emitted must come
    back through the ``state/{pipeline_id}/resume/cursors.json`` file the
    deployment delivers in the config bundle. The engine reads the source cursor
    keyed by ``stream_id`` with the empty partition (see
    ``engine._extract_stage``), so that is the shape these assert.
    """

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.temp_dir)

    def teardown_method(self):
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    async def test_restores_numeric_cursor_across_fresh_container(self):
        # No per-stream checkpoint on disk -> the resume file is the only bookmark.
        _write_resume_file(self.tmp_path, {"orders": 100})
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("orders") == {"cursor": 100}

    async def test_restores_timestamp_cursor_as_datetime(self):
        # A timestamp cursor crosses durable state tagged, so it comes back a
        # datetime (asyncpg rejects a plain string for a timestamp bind).
        ts = "2024-06-01T12:00:00+00:00"
        tagged = {"__type__": "datetime", "value": ts}
        _write_resume_file(self.tmp_path, {"events": tagged})
        manager = _make_manager(self.tmp_path)

        restored = await manager.get_cursor("events")
        from datetime import datetime

        assert restored == {"cursor": datetime.fromisoformat(ts)}

    async def test_unknown_stream_has_no_cursor(self):
        _write_resume_file(self.tmp_path, {"orders": 100})
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("not-in-payload") is None

    async def test_malformed_resume_state_does_not_abort_construction(self):
        # A corrupt tagged cursor must degrade to a full re-scan, not crash
        # StateManager.__init__ on a fresh container.
        _write_resume_file(
            self.tmp_path,
            {"orders": {"__type__": "datetime", "value": "garbage"}},
        )
        manager = _make_manager(self.tmp_path)  # must not raise

        assert await manager.get_cursor("orders") is None

    async def test_no_resume_file_means_no_cursor(self):
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("orders") is None

    async def test_in_run_save_overrides_restored_cursor(self):
        # A cursor saved during the run supersedes the restored bookmark.
        _write_resume_file(self.tmp_path, {"orders": 100})
        manager = _make_manager(self.tmp_path)

        await manager.save_cursor("orders", {}, {"cursor": 250})

        assert await manager.get_cursor("orders") == {"cursor": 250}

    async def test_null_valued_stream_is_skipped_not_seeded(self):
        # A null cursor in the payload (a stream harvested before it ever emitted
        # one) is skipped, not seeded as {"cursor": None}.
        _write_resume_file(self.tmp_path, {"orders": None})
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("orders") is None

    async def test_resume_file_omission_is_authoritative_over_stale_checkpoint(self):
        # The data-loss case: a stream that has a stale pre-ACK per-stream
        # checkpoint (the source raced to 999) but committed nothing, so the
        # resume file omits it. With a resume file present, get_cursor must NOT
        # resume from the stale 999 (which would skip un-landed rows); the omitted
        # stream resumes from nothing (a full re-scan).
        first = _make_manager(self.tmp_path)
        await first.save_cursor("orders", {}, {"cursor": 999})  # pre-ACK on disk

        _write_resume_file(self.tmp_path, {"other": 5})  # resume file omits "orders"
        second = _make_manager(self.tmp_path)

        assert await second.get_cursor("orders") is None
        assert await second.get_cursor("other") == {"cursor": 5}

    async def test_no_resume_file_consults_on_disk_checkpoint(self):
        # The one case the per-stream checkpoint is still read: a true first run
        # with no resume file at all (the engine has not written one yet).
        first = _make_manager(self.tmp_path)
        await first.save_cursor("orders", {}, {"cursor": 50})  # writes ./state
        # No resume file written (write_resume_snapshot not called).

        second = _make_manager(self.tmp_path)
        assert second._resume_file_present is False
        assert await second.get_cursor("orders") == {"cursor": 50}

    async def test_restored_cursor_wins_over_stale_on_disk_checkpoint(self):
        # A leftover per-stream checkpoint must not shadow the delivered resume
        # file: the delivered value is authoritative, the local file is stale.
        first = _make_manager(self.tmp_path)
        await first.save_cursor("orders", {}, {"cursor": 50})  # writes ./state

        _write_resume_file(self.tmp_path, {"orders": 100})
        second = _make_manager(self.tmp_path)  # same base_dir -> stale 50 on disk

        assert await second.get_cursor("orders") == {"cursor": 100}


class TestStateManagerResumeSnapshot:
    """Writing the consolidated resume file a local run hands to its successor.

    A local run has no deployment to harvest its emitted state, so the engine
    writes ``state/{pipeline_id}/resume/cursors.json`` itself when the pipeline
    finishes (``StateManager.write_resume_snapshot``). The snapshot is the
    committed (destination-ACKed) high-water mark per stream -- the same value
    the deployment harvests from the emitted ``ANALITIQ_STATE`` lines -- so the
    local output and the cloud-delivered input are the same contract, and a fresh
    manager restores from it the same way either was produced.
    """

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.temp_dir)

    def teardown_method(self):
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def _written(self, pipeline_id="test-pipeline"):
        return json.loads(_resume_path(self.tmp_path, pipeline_id).read_text())

    async def test_snapshot_round_trips_committed_cursors(self):
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        first = _make_manager(self.tmp_path)
        _commit(first, "orders", 100)
        # A timestamp crosses the ACK tagged, the same form the cursor token uses.
        _commit(first, "events", {"__type__": "datetime", "value": ts.isoformat()})

        first.write_resume_snapshot()

        # A fresh manager (empty cache) seeds only from the resume file.
        second = _make_manager(self.tmp_path)
        assert await second.get_cursor("orders") == {"cursor": 100}
        # The timestamp survives tagged, so it comes back a datetime not a string.
        assert await second.get_cursor("events") == {"cursor": ts}

    async def test_snapshot_uses_committed_not_preack_source_cursor(self):
        # The source advances the in-run cursor as it yields batches, ahead of
        # the destination ACK. The snapshot must use the committed (ACKed) value,
        # never that pre-ACK position -- otherwise a stream that fails after
        # extraction would persist a watermark for rows that never landed and
        # skip them on the next run.
        manager = _make_manager(self.tmp_path)
        _commit(manager, "orders", 5)  # ACKed up to id=5
        # Source raced ahead to id=999 but those batches were never ACKed.
        await manager.save_cursor("orders", {}, {"cursor": 999})

        manager.write_resume_snapshot()

        assert self._written() == {"orders": 5}  # committed 5, not pre-ACK 999

    async def test_snapshot_omits_stream_that_never_committed(self):
        manager = _make_manager(self.tmp_path)
        _commit(manager, "orders", 100)
        # Pre-ACK source cursor only, no ACK -> nothing safe to resume from.
        await manager.save_cursor("never", {}, {"cursor": 7})

        manager.write_resume_snapshot()

        assert self._written() == {"orders": 100}

    async def test_snapshot_retains_seeded_cursor_for_unadvanced_stream(self):
        # A stream delivered in resume.json that commits nothing this run must
        # keep its bookmark in the next snapshot, not vanish from it.
        _write_resume_file(self.tmp_path, {"orders": 100})
        manager = _make_manager(self.tmp_path)  # seeds the committed baseline

        manager.write_resume_snapshot()  # no commits this run

        assert self._written() == {"orders": 100}

    async def test_advanced_stream_overrides_seeded_cursor(self):
        # When a seeded stream does commit this run, the committed value wins.
        _write_resume_file(self.tmp_path, {"orders": 100})
        manager = _make_manager(self.tmp_path)
        _commit(manager, "orders", 250)

        manager.write_resume_snapshot()

        assert self._written() == {"orders": 250}

    async def test_fallback_checkpoint_is_carried_into_snapshot(self):
        # First run with no resume file but a leftover per-stream checkpoint:
        # when get_cursor uses that checkpoint as the bookmark, it must be carried
        # into the snapshot. Otherwise an end-of-run snapshot writes {} and the
        # next run -- now seeing a resume file -- ignores the checkpoint and
        # re-scans, duplicating rows for an insert/keyless stream.
        first = _make_manager(self.tmp_path)
        await first.save_cursor("orders", {}, {"cursor": 100})  # per-stream only

        second = _make_manager(self.tmp_path)  # no resume file written yet
        assert second._resume_file_present is False
        assert await second.get_cursor("orders") == {"cursor": 100}  # uses fallback
        second.write_resume_snapshot()  # no new commits this run

        assert self._written() == {"orders": 100}  # carried forward, not {}

    async def test_recorded_committed_value_advances_snapshot(self):
        # The in-run idempotency skip path advances the snapshot from the commit
        # tracker's recorded watermark (what landed) without re-sending a batch;
        # a None value (non-incremental batch) is ignored.
        manager = _make_manager(self.tmp_path)
        manager.record_committed_value("orders", 5)
        manager.record_committed_value("noncursor", None)

        manager.write_resume_snapshot()

        assert self._written() == {"orders": 5}

    async def test_resume_file_does_not_collide_with_resume_named_stream(self):
        # A stream literally named "resume" writes its per-stream checkpoint to
        # state/<pipeline>/resume.json; the consolidated file lives in a resume/
        # sub-directory (cursors.json), so the per-stream write can't clobber the
        # map and a fresh start restores the real "resume" stream, not a bogus
        # "cursor" stream parsed out of a {"cursor": ...} checkpoint.
        manager = _make_manager(self.tmp_path)
        await manager.save_cursor("resume", {}, {"cursor": 42})  # per-stream file
        _commit(manager, "resume", 42)

        manager.write_resume_snapshot()

        per_stream = self.tmp_path / "state" / "test-pipeline" / "resume.json"
        assert json.loads(per_stream.read_text()) == {"cursor": 42}
        assert self._written() == {"resume": 42}  # consolidated map intact

        second = _make_manager(self.tmp_path)
        assert await second.get_cursor("resume") == {"cursor": 42}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
