"""Unit tests for config module."""

from pathlib import Path
from unittest.mock import patch
import pytest

from analitiq_stream import config


class TestConfig:
    """Test suite for configuration module."""

    @pytest.mark.unit
    def test_project_root_path(self):
        """Test that PROJECT_ROOT is correctly determined."""
        assert isinstance(config.PROJECT_ROOT, Path)
        assert config.PROJECT_ROOT.exists()
        # Verify it's the parent of the analitiq_stream package
        assert (config.PROJECT_ROOT / "analitiq_stream").exists()

    @pytest.mark.unit
    def test_directories_structure(self):
        """Test that DIRECTORIES contains expected keys and Path objects."""
        expected_keys = {"state", "deadletter", "logs"}
        
        assert set(config.DIRECTORIES.keys()) == expected_keys
        
        for key, value in config.DIRECTORIES.items():
            assert isinstance(value, Path)
            assert value.parent == config.PROJECT_ROOT

    @pytest.mark.unit
    def test_state_directory_path(self):
        """Test state directory path configuration."""
        state_dir = config.DIRECTORIES["state"]
        
        assert isinstance(state_dir, Path)
        assert state_dir.name == "state"
        assert state_dir.parent == config.PROJECT_ROOT

    @pytest.mark.unit
    def test_deadletter_directory_path(self):
        """Test dead letter queue directory path configuration."""
        dlq_dir = config.DIRECTORIES["deadletter"]
        
        assert isinstance(dlq_dir, Path)
        assert dlq_dir.name == "deadletter"
        assert dlq_dir.parent == config.PROJECT_ROOT

    @pytest.mark.unit
    def test_logs_directory_path(self):
        """Test logs directory path configuration."""
        logs_dir = config.DIRECTORIES["logs"]
        
        assert isinstance(logs_dir, Path)
        assert logs_dir.name == "logs"
        assert logs_dir.parent == config.PROJECT_ROOT

    @pytest.mark.unit
    def test_default_monitoring_interval(self):
        """Test default monitoring interval configuration."""
        assert config.DEFAULT_MONITORING_INTERVAL_SECONDS == 5
        assert isinstance(config.DEFAULT_MONITORING_INTERVAL_SECONDS, int)

    @pytest.mark.unit
    def test_directories_are_absolute_paths(self):
        """Test that all directory paths are absolute."""
        for key, path in config.DIRECTORIES.items():
            assert path.is_absolute(), f"Directory {key} should be an absolute path"

    @pytest.mark.unit
    def test_config_immutability_attempt(self):
        """Test that configuration values behave as expected when modified."""
        # Store original values
        original_state = config.DIRECTORIES["state"]
        original_interval = config.DEFAULT_MONITORING_INTERVAL_SECONDS
        
        # Attempt to modify (this will create new local variables in the test)
        config.DIRECTORIES["state"] = Path("/tmp/new_state")
        config.DEFAULT_MONITORING_INTERVAL_SECONDS = 10
        
        # Values are modified since Python doesn't enforce immutability
        assert config.DIRECTORIES["state"] == Path("/tmp/new_state")
        assert config.DEFAULT_MONITORING_INTERVAL_SECONDS == 10
        
        # Restore original values for other tests
        config.DIRECTORIES["state"] = original_state
        config.DEFAULT_MONITORING_INTERVAL_SECONDS = original_interval


    @pytest.mark.unit
    def test_directories_use_project_root(self):
        """Test that DIRECTORIES are properly based on PROJECT_ROOT."""
        # This ensures that if PROJECT_ROOT changes, directories follow
        project_root = config.PROJECT_ROOT
        
        assert config.DIRECTORIES["state"] == project_root / "state"
        assert config.DIRECTORIES["deadletter"] == project_root / "deadletter"
        assert config.DIRECTORIES["logs"] == project_root / "logs"

    @pytest.mark.unit
    def test_config_module_attributes(self):
        """Test that config module exports expected attributes."""
        expected_attributes = [
            "PROJECT_ROOT",
            "DIRECTORIES",
            "DEFAULT_MONITORING_INTERVAL_SECONDS"
        ]
        
        for attr in expected_attributes:
            assert hasattr(config, attr), f"config module should have attribute: {attr}"

    @pytest.mark.unit
    def test_directories_can_be_created(self):
        """Test that directory paths can be used to create actual directories."""
        import tempfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            test_root = Path(tmpdir)
            
            # Create directories based on config pattern
            for dir_name in ["state", "deadletter", "logs"]:
                dir_path = test_root / dir_name
                dir_path.mkdir(parents=True, exist_ok=True)
                assert dir_path.exists()
                assert dir_path.is_dir()

    @pytest.mark.unit
    def test_path_string_conversion(self):
        """Test that paths can be converted to strings for compatibility."""
        for key, path in config.DIRECTORIES.items():
            path_str = str(path)
            assert isinstance(path_str, str)
            assert key in path_str

    @pytest.mark.unit
    def test_path_joining(self):
        """Test that paths can be properly joined with additional components."""
        state_dir = config.DIRECTORIES["state"]
        pipeline_state = state_dir / "pipeline-1" / "checkpoint.json"
        
        assert "state" in str(pipeline_state)
        assert "pipeline-1" in str(pipeline_state)
        assert "checkpoint.json" in str(pipeline_state)