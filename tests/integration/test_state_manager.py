"""Integration tests for state manager functionality."""

import json
import shutil
import tempfile
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
            )

            mock_emit.assert_called_once()
            call_kwargs = mock_emit.call_args
            assert call_kwargs.kwargs["run_id"] == "run-abc"
            assert call_kwargs.kwargs["pipeline_id"] == "test-pipeline"
            assert call_kwargs.kwargs["stream_id"] == "stream1"
            assert call_kwargs.kwargs["cursor_value"] == hwm
            assert call_kwargs.kwargs["cursor_hex"] == json.dumps(cursor).encode().hex()

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


class TestStateManagerDurableRestore:
    """Restoring incremental cursors from the injected RESUME_STATE env var.

    A fresh container's local ``state/`` directory is empty (Fargate wipes it
    every task), so the cursor a prior run emitted must come back through the
    deployment-injected env var. The engine reads the source cursor keyed by
    ``stream_id`` with the empty partition (see ``engine._extract_stage``), so
    that is the shape these assert.
    """

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.temp_dir)

    def teardown_method(self):
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    async def test_restores_numeric_cursor_across_fresh_container(self, monkeypatch):
        # No local checkpoint on disk -> the env var is the only bookmark.
        monkeypatch.setenv("RESUME_STATE", json.dumps({"orders": 100}))
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("orders") == {"cursor": 100}

    async def test_restores_timestamp_cursor_as_datetime(self, monkeypatch):
        ts = "2024-06-01T12:00:00+00:00"
        monkeypatch.setenv("RESUME_STATE", json.dumps({"events": ts}))
        manager = _make_manager(self.tmp_path)

        restored = await manager.get_cursor("events")
        from datetime import datetime

        assert restored == {"cursor": datetime.fromisoformat(ts)}

    async def test_unknown_stream_has_no_cursor(self, monkeypatch):
        monkeypatch.setenv("RESUME_STATE", json.dumps({"orders": 100}))
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("not-in-payload") is None

    async def test_no_env_var_means_no_cursor(self, monkeypatch):
        monkeypatch.delenv("RESUME_STATE", raising=False)
        manager = _make_manager(self.tmp_path)

        assert await manager.get_cursor("orders") is None

    async def test_in_run_save_overrides_restored_cursor(self, monkeypatch):
        # A cursor saved during the run supersedes the restored bookmark.
        monkeypatch.setenv("RESUME_STATE", json.dumps({"orders": 100}))
        manager = _make_manager(self.tmp_path)

        await manager.save_cursor("orders", {}, {"cursor": 250})

        assert await manager.get_cursor("orders") == {"cursor": 250}

    async def test_null_valued_stream_is_skipped_not_seeded(self, monkeypatch):
        # A null cursor in the payload (stream harvested before it emitted one)
        # must be skipped, not seeded as {"cursor": None} -- otherwise it would
        # shadow a real on-disk checkpoint with a useless value.
        first = _make_manager(self.tmp_path)
        await first.save_cursor("orders", {}, {"cursor": 50})  # writes ./state

        monkeypatch.setenv("RESUME_STATE", json.dumps({"orders": None}))
        second = _make_manager(self.tmp_path)  # same base_dir -> 50 on disk

        # The null seed is skipped, so get_cursor falls through to disk.
        assert await second.get_cursor("orders") == {"cursor": 50}

    async def test_restored_cursor_wins_over_stale_on_disk_checkpoint(
        self, monkeypatch
    ):
        # A leftover on-disk checkpoint must not shadow the injected resume
        # state: the durable value is authoritative, the local file is stale.
        monkeypatch.delenv("RESUME_STATE", raising=False)
        first = _make_manager(self.tmp_path)
        await first.save_cursor("orders", {}, {"cursor": 50})  # writes ./state

        monkeypatch.setenv("RESUME_STATE", json.dumps({"orders": 100}))
        second = _make_manager(self.tmp_path)  # same base_dir -> stale 50 on disk

        assert await second.get_cursor("orders") == {"cursor": 100}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
