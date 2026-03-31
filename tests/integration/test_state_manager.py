"""Integration tests for state manager functionality."""

import json
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
