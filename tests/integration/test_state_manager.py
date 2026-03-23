"""Integration tests for state manager functionality."""

import json
import pytest
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.state.state_manager import StateManager
from src.state.state_storage import LocalStateStorage


def _make_manager(tmp_path, pipeline_id="test-pipeline"):
    """Create a StateManager with a LocalStateStorage rooted at tmp_path/state/pipeline_id.

    This ensures list_streams (which reads self.base_dir/pipeline_id/streams)
    and storage writes (which go to storage.base_dir/streams/...) both resolve
    to the same directory on disk.
    """
    state_dir = tmp_path / "state"
    pipeline_dir = state_dir / pipeline_id
    storage = LocalStateStorage(base_dir=str(pipeline_dir))
    return StateManager(
        pipeline_id=pipeline_id,
        base_dir=str(state_dir),
        storage_backend=storage,
    )


class TestStateManager:
    """Test state manager core functionality."""

    def setup_method(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.temp_dir)
        self.pipeline_id = "test-pipeline"

    def teardown_method(self):
        """Clean up test environment."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def test_state_manager_initialization(self):
        """Test state manager proper initialization."""
        manager = _make_manager(self.tmp_path, self.pipeline_id)

        assert manager.pipeline_id == self.pipeline_id
        state_dir = self.tmp_path / "state"
        assert manager.base_dir == state_dir
        assert manager.pipeline_dir == state_dir / self.pipeline_id
        # state_file and lock_file are always None in the current API
        assert manager.state_file is None
        assert manager.lock_file is None

    def test_get_stream_state_path(self):
        """Test stream state path generation."""
        manager = _make_manager(self.tmp_path)

        path = manager._get_stream_state_path("stream1")
        assert path == "streams/stream1/state.json"

        path2 = manager._get_stream_state_path("stream2")
        assert path2 == "streams/stream2/state.json"

    def test_load_save_stream_state(self):
        """Test stream state loading and saving."""
        manager = _make_manager(self.tmp_path)

        # Load non-existent stream state should return None
        state = manager._load_stream_state("stream1")
        assert state is None

        # Save and load stream state
        test_state = {
            "version": 1,
            "stream_id": "stream1",
            "cursor": {"primary": {"field": "id", "value": 100}},
            "hwm": "2025-08-18T12:00:00Z",
            "last_updated": "2025-08-18T12:00:00Z",
            "stats": {"records_synced": 50},
        }

        manager._save_stream_state("stream1", test_state)
        loaded_state = manager._load_stream_state("stream1")

        assert loaded_state == test_state

    def test_start_run_new_pipeline(self):
        """Test starting a run for a new pipeline."""
        manager = _make_manager(self.tmp_path)

        config = {
            "pipeline_id": "test-pipeline",
            "version": "1.0",
            "source": {"type": "api", "endpoint": "/data"},
            "destination": {"type": "api", "endpoint": "/output"},
        }

        run_id = manager.start_run(config)

        # Should generate a run ID
        assert run_id is not None
        assert len(run_id) > 0

        # get_run_info only returns run_id
        run_info = manager.get_run_info()
        assert run_info["run_id"] == run_id

    def test_start_run_with_custom_run_id(self):
        """Test starting a run with custom run ID."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        custom_run_id = "custom-run-123"

        returned_run_id = manager.start_run(config, custom_run_id)

        assert returned_run_id == custom_run_id
        assert manager.get_run_info()["run_id"] == custom_run_id

    def test_start_run_multiple_runs(self):
        """Test starting multiple runs with same or different configs."""
        manager = _make_manager(self.tmp_path)

        config1 = {
            "pipeline_id": "test",
            "version": "1.0",
            "source": {"type": "api", "endpoint": "/data"},
        }

        config2 = {
            "pipeline_id": "test",
            "version": "1.0",
            "source": {"type": "api", "endpoint": "/different"},
        }

        run_id1 = manager.start_run(config1, "run1")
        run_id2 = manager.start_run(config2, "run2")

        assert run_id1 == "run1"
        assert run_id2 == "run2"
        assert manager.get_run_info()["run_id"] == "run2"

    def test_save_stream_checkpoint_basic(self):
        """Test basic stream checkpoint saving."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        cursor = {
            "primary": {"field": "created", "value": "2025-08-18T12:00:00Z"},
            "tiebreaker": {"field": "id", "value": 12345},
        }
        hwm = "2025-08-18T12:00:00Z"
        partition = {}

        manager.save_stream_checkpoint(
            stream_name="stream1",
            partition=partition,
            cursor=cursor,
            hwm=hwm,
        )

        # Verify stream state was saved (partition key not in result)
        state = manager.get_partition_state("stream1", partition)
        assert state is not None
        assert state["cursor"] == cursor
        assert state["hwm"] == hwm
        assert state["stream_id"] == "stream1"
        assert "last_updated" in state
        assert "stats" in state

    def test_save_stream_checkpoint_with_optional_data(self):
        """Test checkpoint saving with optional data."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        cursor = {"primary": {"field": "id", "value": 100}}
        hwm = "2025-08-18T13:00:00Z"
        partition = {"region": "EU"}
        page_state = {"next_token": "abc123", "offset": 1000}
        http_conditionals = {"etag": "xyz789"}
        stats = {"records_synced": 500, "batches_written": 5}

        manager.save_stream_checkpoint(
            stream_name="stream1",
            partition=partition,
            cursor=cursor,
            hwm=hwm,
            page_state=page_state,
            http_conditionals=http_conditionals,
            stats=stats,
        )

        # Verify all data was saved
        state = manager.get_partition_state("stream1", partition)
        assert state["page_state"] == page_state
        assert state["http_conditionals"] == http_conditionals
        assert state["stats"] == stats

    def test_partitioning_ignored(self):
        """Test that partitions are ignored -- only one state per stream."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        # Save with different partitions (all go to same stream state)
        for i, partition in enumerate([{}, {"region": "US"}, {"region": "EU"}]):
            cursor = {"primary": {"field": "id", "value": i * 100}}
            manager.save_stream_checkpoint(
                stream_name="stream1",
                partition=partition,
                cursor=cursor,
                hwm=f"2025-08-18T{12+i:02d}:00:00Z",
            )

        # Only the last write survives (partitions are ignored)
        state = manager.get_partition_state("stream1", {})
        assert state is not None
        assert state["cursor"]["primary"]["value"] == 200

    def test_get_partition_state(self):
        """Test getting specific stream state (partition ignored)."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        partition = {"account_id": "12345"}
        cursor = {"primary": {"field": "created", "value": "2025-08-18T12:00:00Z"}}
        hwm = "2025-08-18T12:00:00Z"

        manager.save_stream_checkpoint("stream1", partition, cursor, hwm)

        # Get state -- partition param is ignored
        state = manager.get_partition_state("stream1", partition)
        assert state is not None
        assert state["cursor"] == cursor
        assert state["hwm"] == hwm

        # Non-existent stream returns None
        state = manager.get_partition_state("nonexistent", {})
        assert state is None

    def test_list_streams(self):
        """Test listing all streams with state."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        # No streams initially
        assert manager.list_streams() == []

        # Add checkpoints for multiple streams
        streams = ["stream1", "stream2", "stream3"]
        for stream in streams:
            manager.save_stream_checkpoint(
                stream_name=stream,
                partition={},
                cursor={"primary": {"field": "id", "value": 1}},
                hwm="2025-08-18T12:00:00Z",
            )

        # Should list all streams
        listed_streams = manager.list_streams()
        assert len(listed_streams) == 3
        for stream in streams:
            assert stream in listed_streams

    def test_clear_stream_state(self):
        """Test clearing state for a specific stream."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        manager.save_stream_checkpoint(
            "stream1", {}, {"primary": {"field": "id", "value": 1}}, "2025-08-18T12:00:00Z"
        )
        manager.save_stream_checkpoint(
            "stream2", {}, {"primary": {"field": "id", "value": 2}}, "2025-08-18T12:00:00Z"
        )

        assert "stream1" in manager.list_streams()
        assert "stream2" in manager.list_streams()

        # Clear stream1
        manager.clear_stream_state("stream1")

        # stream1 should be gone, stream2 should remain
        assert "stream1" not in manager.list_streams()
        assert "stream2" in manager.list_streams()

        # State should be gone for stream1
        assert manager.get_partition_state("stream1", {}) is None
        assert manager.get_partition_state("stream2", {}) is not None

    def test_clear_all_state(self):
        """Test clearing all pipeline state."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        run_id = manager.start_run(config)

        manager.save_stream_checkpoint(
            "stream1", {}, {"primary": {"field": "id", "value": 1}}, "2025-08-18T12:00:00Z"
        )
        manager.save_stream_checkpoint(
            "stream2", {}, {"primary": {"field": "id", "value": 2}}, "2025-08-18T12:00:00Z"
        )

        # Verify state exists
        assert len(manager.list_streams()) == 2

        # Clear all state
        manager.clear_all_state()

        # All stream state should be gone
        assert len(manager.list_streams()) == 0

    def test_get_resume_info(self):
        """Test getting resume information for a stream."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        run_id = manager.start_run(config)

        # No checkpoints yet
        resume_info = manager.get_resume_info("stream1")
        assert resume_info["can_resume"] is False
        assert resume_info["records_synced"] == 0

        # Save a checkpoint with stats
        stats = {"records_synced": 500, "batches_written": 5}
        manager.save_stream_checkpoint(
            stream_name="stream1",
            partition={},
            cursor={"primary": {"field": "id", "value": 100}},
            hwm="2025-08-18T12:00:00Z",
            stats=stats,
        )

        # Now should have resume info
        resume_info = manager.get_resume_info("stream1")
        assert resume_info["can_resume"] is True
        assert resume_info["records_synced"] == 500
        assert resume_info["run_id"] == run_id
        assert resume_info["cursor"] is not None
        assert resume_info["hwm"] == "2025-08-18T12:00:00Z"


class TestStateManagerEdgeCases:
    """Test state manager edge cases and error conditions."""

    def setup_method(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.temp_dir)
        self.pipeline_id = "test-pipeline"

    def teardown_method(self):
        """Clean up test environment."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def test_load_stream_state_corrupted_json(self):
        """Test handling of corrupted stream state file."""
        manager = _make_manager(self.tmp_path, self.pipeline_id)

        # Create corrupted state file in the storage directory
        stream_dir = manager.pipeline_dir / "streams" / "stream1"
        stream_dir.mkdir(parents=True, exist_ok=True)
        with open(stream_dir / "state.json", "w") as f:
            f.write("invalid json content")

        # Should return None for corrupted data
        state = manager._load_stream_state("stream1")
        assert state is None

    def test_save_stream_state_file_error(self):
        """Test error handling when saving stream state fails."""
        manager = _make_manager(self.tmp_path, self.pipeline_id)

        # Mock storage.write_json to raise
        with patch.object(manager.storage, "write_json", side_effect=PermissionError("Permission denied")):
            with pytest.raises(PermissionError):
                manager._save_stream_state("stream1", {"test": "data"})

    def test_concurrent_access_simulation(self):
        """Test simulation of concurrent access with threading locks."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}

        # Should be able to start multiple runs (simulating different workers)
        run_id1 = manager.start_run(config, "run1")
        run_id2 = manager.start_run(config, "run2")

        assert run_id1 == "run1"
        assert run_id2 == "run2"

        # Last run should be active
        assert manager.get_run_info()["run_id"] == "run2"

    def test_checkpoint_updates_overwrite(self):
        """Test that successive checkpoints overwrite the previous state."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        # Save multiple checkpoints for the same stream
        for i in range(3):
            manager.save_stream_checkpoint(
                stream_name="stream1",
                partition={},
                cursor={"primary": {"field": "id", "value": i}},
                hwm=f"2025-08-18T{12+i:02d}:00:00Z",
            )

        # Only the last checkpoint should be present
        state = manager.get_stream_state("stream1")
        assert state["cursor"]["primary"]["value"] == 2
        assert state["hwm"] == "2025-08-18T14:00:00Z"

    def test_empty_partition_handling(self):
        """Test handling of empty partitions."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        empty_partition = {}
        cursor = {"primary": {"field": "id", "value": 1}}
        hwm = "2025-08-18T12:00:00Z"

        manager.save_stream_checkpoint("stream1", empty_partition, cursor, hwm)

        state = manager.get_partition_state("stream1", empty_partition)
        assert state is not None
        assert state["cursor"] == cursor

    def test_non_empty_partition_ignored(self):
        """Test that non-empty partition keys are ignored (logged as warning)."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        complex_partition = {
            "region": "US",
            "customer": {"tier": "premium", "account_id": "12345"},
            "filters": ["active", "verified"],
            "priority": 1,
        }

        cursor = {"primary": {"field": "id", "value": 100}}
        hwm = "2025-08-18T12:00:00Z"

        manager.save_stream_checkpoint("stream1", complex_partition, cursor, hwm)

        # State is stored per stream, partition is ignored
        state = manager.get_partition_state("stream1", {})
        assert state is not None
        assert state["cursor"] == cursor

        # Retrieving with a different partition also works (partition is ignored)
        state2 = manager.get_partition_state("stream1", {"other": "key"})
        assert state2 is not None
        assert state2["cursor"] == cursor

    def test_get_stream_state(self):
        """Test get_stream_state returns the stored state."""
        manager = _make_manager(self.tmp_path)

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        # No state yet
        assert manager.get_stream_state("stream1") is None

        manager.save_stream_checkpoint(
            "stream1",
            {},
            {"primary": {"field": "id", "value": 1}},
            "2025-08-18T12:00:00Z",
        )

        state = manager.get_stream_state("stream1")
        assert state is not None
        assert state["stream_id"] == "stream1"
        assert state["hwm"] == "2025-08-18T12:00:00Z"

    def test_get_run_info_no_run(self):
        """Test get_run_info returns empty dict when no run started."""
        manager = _make_manager(self.tmp_path)
        # Clear any run_id from env
        manager.current_run_id = None

        assert manager.get_run_info() == {}

    @pytest.mark.asyncio
    async def test_get_cursor(self):
        """Test the async get_cursor convenience method."""
        manager = _make_manager(self.tmp_path)
        manager.start_run({"pipeline_id": "test"})

        # No cursor yet
        result = await manager.get_cursor("stream1", {})
        assert result is None

        # Save a checkpoint
        manager.save_stream_checkpoint(
            "stream1",
            {},
            {"primary": {"field": "replication_key", "value": "2025-08-18T12:00:00Z"}},
            "2025-08-18T12:00:00Z",
        )

        result = await manager.get_cursor("stream1", {})
        assert result is not None
        assert result["cursor"] == "2025-08-18T12:00:00Z"

    @pytest.mark.asyncio
    async def test_save_cursor(self):
        """Test the async save_cursor convenience method."""
        manager = _make_manager(self.tmp_path)
        manager.start_run({"pipeline_id": "test"})

        await manager.save_cursor("stream1", {}, {"cursor": "2025-08-18T12:00:00Z"})

        state = manager.get_stream_state("stream1")
        assert state is not None
        assert state["cursor"]["primary"]["value"] == "2025-08-18T12:00:00Z"
        assert state["hwm"] == "2025-08-18T12:00:00Z"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
