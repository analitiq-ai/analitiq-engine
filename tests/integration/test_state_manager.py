"""Unit tests for state manager functionality."""

import json
import pytest
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.state.state_manager import StateManager


class TestStateManager:
    """Test state manager core functionality."""
    
    def setup_method(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.state_dir = Path(self.temp_dir) / "state"
        self.pipeline_id = "test-pipeline"
        
    def teardown_method(self):
        """Clean up test environment."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def test_state_manager_initialization(self):
        """Test state manager proper initialization."""
        manager = StateManager(
            pipeline_id=self.pipeline_id,
            base_dir=str(self.state_dir)
        )

        assert manager.pipeline_id == self.pipeline_id
        assert manager.base_dir == self.state_dir
        assert manager.pipeline_dir == self.state_dir / self.pipeline_id
        assert manager.state_file == manager.pipeline_dir / "state.json"
        assert manager.lock_file == manager.pipeline_dir / "lock"
    
    def test_get_partition_file(self):
        """Test partition file path generation."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        # Default partition (empty)
        default_file = manager._get_partition_file("stream1", {})
        expected = manager.streams_dir / "stream1" / "partition-default.json"
        assert default_file == expected
        
        # Named partition
        partition = {"region": "EU", "account_type": "business"}
        partition_file = manager._get_partition_file("stream1", partition)
        
        # Should be deterministic hash-based filename
        assert partition_file.parent == manager.streams_dir / "stream1"
        assert partition_file.name.startswith("partition-")
        assert partition_file.name.endswith(".json")
        
        # Same partition should give same file
        partition_file2 = manager._get_partition_file("stream1", partition)
        assert partition_file == partition_file2
    
    def test_load_save_state(self):
        """Test state loading and saving."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))

        # Load non-existent state should return default
        state = manager._load_state()
        assert state["version"] == 1
        assert state["streams"] == {}
        assert state["run"] == {}

        # Save and load state
        test_state = {
            "version": 1,
            "streams": {"stream1": {"partitions": []}},
            "run": {"run_id": "test-run"}
        }

        manager._save_state(test_state)
        loaded_state = manager._load_state()

        assert loaded_state == test_state
    
    def test_load_save_partition_state(self):
        """Test partition state loading and saving."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        partition_file = manager.streams_dir / "stream1" / "partition-test.json"
        
        # Load non-existent partition state should return None
        state = manager._load_partition_state(partition_file)
        assert state is None
        
        # Save and load partition state
        test_state = {
            "partition": {"region": "US"},
            "cursor": {"primary": {"field": "id", "value": 12345}},
            "hwm": "2025-08-18T12:00:00Z",
            "stats": {"records_synced": 100}
        }
        
        manager._save_partition_state(partition_file, test_state)
        loaded_state = manager._load_partition_state(partition_file)
        
        assert loaded_state == test_state
        assert partition_file.exists()
    
    def test_start_run_new_pipeline(self):
        """Test starting a run for a new pipeline."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))

        config = {
            "pipeline_id": "test-pipeline",
            "version": "1.0",
            "source": {"type": "api", "endpoint": "/data"},
            "destination": {"type": "api", "endpoint": "/output"}
        }

        run_id = manager.start_run(config)

        # Should generate a run ID
        assert run_id is not None
        assert len(run_id) > 0

        # Should save run info to index
        run_info = manager.get_run_info()
        assert run_info["run_id"] == run_id
        assert run_info["pipeline_id"] == "test-pipeline"
        assert "started_at" in run_info
        assert "lease_owner" in run_info
    
    def test_start_run_with_custom_run_id(self):
        """Test starting a run with custom run ID."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        custom_run_id = "custom-run-123"
        
        returned_run_id = manager.start_run(config, custom_run_id)
        
        assert returned_run_id == custom_run_id
        assert manager.get_run_info()["run_id"] == custom_run_id
    
    def test_start_run_multiple_runs(self):
        """Test starting multiple runs with same or different configs."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))

        config1 = {
            "pipeline_id": "test",
            "version": "1.0",
            "source": {"type": "api", "endpoint": "/data"}
        }

        config2 = {
            "pipeline_id": "test",
            "version": "1.0",
            "source": {"type": "api", "endpoint": "/different"}
        }

        # Start first run
        run_id1 = manager.start_run(config1, "run1")

        # Start second run with different config should succeed (no validation)
        run_id2 = manager.start_run(config2, "run2")

        assert run_id1 == "run1"
        assert run_id2 == "run2"
        assert manager.get_run_info()["run_id"] == "run2"

    def test_save_stream_checkpoint_basic(self):
        """Test basic stream checkpoint saving."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        # Start a run first
        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)
        
        # Save checkpoint
        cursor = {
            "primary": {"field": "created", "value": "2025-08-18T12:00:00Z"},
            "tiebreaker": {"field": "id", "value": 12345}
        }
        hwm = "2025-08-18T12:00:00Z"
        partition = {}
        
        manager.save_stream_checkpoint(
            stream_name="stream1",
            partition=partition,
            cursor=cursor,
            hwm=hwm
        )
        
        # Verify partition state was saved
        partition_state = manager.get_partition_state("stream1", partition)
        assert partition_state is not None
        assert partition_state["cursor"] == cursor
        assert partition_state["hwm"] == hwm
        assert partition_state["partition"] == partition
        assert "last_updated" in partition_state
        assert "stats" in partition_state
    
    def test_save_stream_checkpoint_with_optional_data(self):
        """Test checkpoint saving with optional data."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))

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
            stats=stats
        )

        # Verify all data was saved
        partition_state = manager.get_partition_state("stream1", partition)
        assert partition_state["page_state"] == page_state
        assert partition_state["http_conditionals"] == http_conditionals
        assert partition_state["stats"] == stats
    
    def test_get_stream_partitions(self):
        """Test getting all partitions for a stream."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)
        
        # Save multiple partitions
        partitions = [
            {},
            {"region": "US"},
            {"region": "EU"}
        ]
        
        for i, partition in enumerate(partitions):
            cursor = {"primary": {"field": "id", "value": i * 100}}
            manager.save_stream_checkpoint(
                stream_name="stream1",
                partition=partition,
                cursor=cursor,
                hwm=f"2025-08-18T{12+i:02d}:00:00Z"
            )
        
        # Get all partitions
        all_partitions = manager.get_stream_partitions("stream1")
        
        assert len(all_partitions) == 3
        partition_keys = [p["partition"] for p in all_partitions]
        assert {} in partition_keys
        assert {"region": "US"} in partition_keys
        assert {"region": "EU"} in partition_keys
    
    def test_get_partition_state(self):
        """Test getting specific partition state."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)
        
        partition = {"account_id": "12345"}
        cursor = {"primary": {"field": "created", "value": "2025-08-18T12:00:00Z"}}
        hwm = "2025-08-18T12:00:00Z"
        
        # Save checkpoint
        manager.save_stream_checkpoint("stream1", partition, cursor, hwm)
        
        # Get specific partition state
        state = manager.get_partition_state("stream1", partition)
        assert state is not None
        assert state["partition"] == partition
        assert state["cursor"] == cursor
        assert state["hwm"] == hwm
        
        # Non-existent partition should return None
        state = manager.get_partition_state("stream1", {"account_id": "99999"})
        assert state is None
    
    def test_list_streams(self):
        """Test listing all streams with state."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
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
                hwm="2025-08-18T12:00:00Z"
            )
        
        # Should list all streams
        listed_streams = manager.list_streams()
        assert len(listed_streams) == 3
        for stream in streams:
            assert stream in listed_streams
    
    def test_clear_stream_state(self):
        """Test clearing state for a specific stream."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)
        
        # Save checkpoints for multiple streams
        manager.save_stream_checkpoint("stream1", {}, {"primary": {"field": "id", "value": 1}}, "2025-08-18T12:00:00Z")
        manager.save_stream_checkpoint("stream2", {}, {"primary": {"field": "id", "value": 2}}, "2025-08-18T12:00:00Z")
        
        assert "stream1" in manager.list_streams()
        assert "stream2" in manager.list_streams()
        
        # Clear stream1
        manager.clear_stream_state("stream1")
        
        # stream1 should be gone, stream2 should remain
        assert "stream1" not in manager.list_streams()
        assert "stream2" in manager.list_streams()
        
        # Partition state should be gone
        assert manager.get_partition_state("stream1", {}) is None
        assert manager.get_partition_state("stream2", {}) is not None
    
    def test_clear_all_state(self):
        """Test clearing all pipeline state."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        run_id = manager.start_run(config)
        
        # Save some checkpoints
        manager.save_stream_checkpoint("stream1", {}, {"primary": {"field": "id", "value": 1}}, "2025-08-18T12:00:00Z")
        manager.save_stream_checkpoint("stream2", {}, {"primary": {"field": "id", "value": 2}}, "2025-08-18T12:00:00Z")
        
        # Verify state exists
        assert len(manager.list_streams()) == 2
        assert manager.get_run_info()["run_id"] == run_id
        
        # Clear all state
        manager.clear_all_state()
        
        # All state should be gone
        assert len(manager.list_streams()) == 0
        assert manager.get_run_info() == {}
        assert not manager.state_file.exists()
    
    def test_get_resume_info(self):
        """Test getting resume information for a stream."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))

        config = {"pipeline_id": "test", "version": "1.0"}
        run_id = manager.start_run(config)

        # No checkpoints yet
        resume_info = manager.get_resume_info("stream1")
        assert resume_info["can_resume"] is False
        assert resume_info["partition_count"] == 0
        assert resume_info["total_records_synced"] == 0

        # Save checkpoints for multiple partitions
        partitions = [{}, {"region": "US"}, {"region": "EU"}]
        for i, partition in enumerate(partitions):
            stats = {"records_synced": (i + 1) * 100, "batches_written": i + 1}
            manager.save_stream_checkpoint(
                stream_name="stream1",
                partition=partition,
                cursor={"primary": {"field": "id", "value": i * 100}},
                hwm=f"2025-08-18T{12+i:02d}:00:00Z",
                stats=stats
            )

        # Now should have resume info
        resume_info = manager.get_resume_info("stream1")
        assert resume_info["can_resume"] is True
        assert resume_info["partition_count"] == 3
        assert resume_info["total_records_synced"] == 600  # 100 + 200 + 300
        assert resume_info["run_id"] == run_id
        assert resume_info["last_checkpoint_seq"] > 0
        assert len(resume_info["partitions"]) == 3


class TestStateManagerEdgeCases:
    """Test state manager edge cases and error conditions."""
    
    def setup_method(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.state_dir = Path(self.temp_dir) / "state"
        self.pipeline_id = "test-pipeline"
    
    def teardown_method(self):
        """Clean up test environment."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def test_load_state_corrupted_json(self):
        """Test handling of corrupted state file."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))

        # Create corrupted state file
        manager.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(manager.state_file, "w") as f:
            f.write("invalid json content")

        # Should return default state
        state = manager._load_state()
        assert state["version"] == 1
        assert state["streams"] == {}
        assert state["run"] == {}
    
    def test_load_partition_state_corrupted_json(self):
        """Test handling of corrupted partition file."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        partition_file = manager.streams_dir / "stream1" / "partition-test.json"
        partition_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Create corrupted partition file
        with open(partition_file, "w") as f:
            f.write("invalid json content")
        
        # Should return None
        state = manager._load_partition_state(partition_file)
        assert state is None
    
    def test_save_state_file_error(self):
        """Test error handling when saving state fails."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))

        # Mock open to raise an exception
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            with pytest.raises(PermissionError):
                manager._save_state({"test": "data"})
    
    def test_save_partition_state_file_error(self):
        """Test error handling when saving partition state fails."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        partition_file = manager.streams_dir / "stream1" / "partition-test.json"
        
        # Mock open to raise an exception
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            with pytest.raises(PermissionError):
                manager._save_partition_state(partition_file, {"test": "data"})
    
    def test_concurrent_access_simulation(self):
        """Test simulation of concurrent access with threading locks."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        
        # Should be able to start multiple runs (simulating different workers)
        run_id1 = manager.start_run(config, "run1")
        run_id2 = manager.start_run(config, "run2")
        
        assert run_id1 == "run1"
        assert run_id2 == "run2"
        
        # Last run should be active
        assert manager.get_run_info()["run_id"] == "run2"
    
    def test_partition_hash_consistency(self):
        """Test that partition hashes are consistent."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        partition = {"region": "EU", "account_type": "business", "priority": 1}
        
        # Multiple calls should return same file path
        file1 = manager._get_partition_file("stream1", partition)
        file2 = manager._get_partition_file("stream1", partition)
        file3 = manager._get_partition_file("stream2", partition)  # Different stream
        
        assert file1 == file2  # Same stream, same partition
        assert file1.name == file3.name  # Same partition hash, different stream path
        assert file1.parent != file3.parent  # Different parent directories
    
    def test_checkpoint_sequence_increment(self):
        """Test that checkpoint sequence increments correctly."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)
        
        # Initial checkpoint sequence should be 0
        run_info = manager.get_run_info()
        initial_seq = run_info.get("checkpoint_seq", 0)
        
        # Save multiple checkpoints
        for i in range(3):
            manager.save_stream_checkpoint(
                stream_name="stream1",
                partition={},
                cursor={"primary": {"field": "id", "value": i}},
                hwm=f"2025-08-18T{12+i:02d}:00:00Z"
            )
        
        # Checkpoint sequence should have incremented
        run_info = manager.get_run_info()
        final_seq = run_info.get("checkpoint_seq", 0)
        
        assert final_seq == initial_seq + 3
    
    def test_empty_partition_handling(self):
        """Test handling of empty partitions."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)
        
        # Empty partition should be handled as default
        empty_partition = {}
        cursor = {"primary": {"field": "id", "value": 1}}
        hwm = "2025-08-18T12:00:00Z"
        
        manager.save_stream_checkpoint("stream1", empty_partition, cursor, hwm)
        
        # Should be able to retrieve state
        state = manager.get_partition_state("stream1", empty_partition)
        assert state is not None
        assert state["partition"] == empty_partition
        assert state["cursor"] == cursor
    
    def test_complex_partition_keys(self):
        """Test handling of complex partition keys."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))
        
        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)
        
        # Complex partition with nested data
        complex_partition = {
            "region": "US",
            "customer": {
                "tier": "premium", 
                "account_id": "12345"
            },
            "filters": ["active", "verified"],
            "priority": 1
        }
        
        cursor = {"primary": {"field": "id", "value": 100}}
        hwm = "2025-08-18T12:00:00Z"
        
        manager.save_stream_checkpoint("stream1", complex_partition, cursor, hwm)
        
        # Should be able to retrieve with exact same partition key
        state = manager.get_partition_state("stream1", complex_partition)
        assert state is not None
        assert state["partition"] == complex_partition
        
        # Different order of same keys should work (JSON sorting)
        same_partition_different_order = {
            "priority": 1,
            "region": "US", 
            "customer": {
                "account_id": "12345",
                "tier": "premium"
            },
            "filters": ["active", "verified"]
        }
        
        state2 = manager.get_partition_state("stream1", same_partition_different_order)
        assert state2 is not None
        assert state2["partition"] == complex_partition
    
    def test_get_stream_partitions_missing_files(self):
        """Test getting partitions when some files are missing."""
        manager = StateManager(self.pipeline_id, str(self.state_dir))

        config = {"pipeline_id": "test", "version": "1.0"}
        manager.start_run(config)

        # Save a checkpoint
        manager.save_stream_checkpoint(
            "stream1", {}, {"primary": {"field": "id", "value": 1}}, "2025-08-18T12:00:00Z"
        )

        # Manually corrupt the state to reference a non-existent file
        state = manager._load_state()
        state["streams"]["stream1"]["partitions"].append({
            "partition": {"region": "missing"},
            "file": "nonexistent/partition-missing.json"
        })
        manager._save_state(state)

        # Should only return partitions with existing files
        partitions = manager.get_stream_partitions("stream1")
        assert len(partitions) == 1  # Only the valid one
        assert partitions[0]["partition"] == {}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])