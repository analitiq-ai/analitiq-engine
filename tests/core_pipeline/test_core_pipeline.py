"""Comprehensive tests for Pipeline class."""

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
import pytest

from analitiq_stream.core.pipeline import Pipeline
from analitiq_stream.core.engine import StreamingEngine
from analitiq_stream.fault_tolerance.config_compatibility import ConfigCompatibilityError


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture
def valid_pipeline_config():
    """Valid pipeline configuration."""
    return {
        "pipeline_id": "test-pipeline",
        "name": "Test Pipeline",
        "version": "1.0",
        "engine_config": {
            "batch_size": 1000,
            "max_concurrent_batches": 10,
            "buffer_size": 10000
        },
        "streams": {
            "users": {
                "name": "User Stream",
                "description": "User data synchronization",
                "src": {
                    "endpoint_id": "source-endpoint-123",
                    "replication_key": "updated_at",
                    "cursor_mode": "inclusive",
                    "safety_window_seconds": 120
                },
                "dst": {
                    "endpoint_id": "dest-endpoint-456",
                    "refresh_mode": "upsert",
                    "batch_size": 100
                },
                "mapping": {
                    "transformations": [
                        {
                            "type": "field_mapping",
                            "mappings": {"id": "user_id", "name": "full_name"}
                        }
                    ]
                }
            }
        },
        "error_handling": {
            "strategy": "dlq",
            "max_retries": 3
        },
        "monitoring": {
            "metrics_enabled": True,
            "log_level": "INFO"
        }
    }

@pytest.fixture
def valid_source_config():
    """Valid source configuration."""
    return {
        "type": "api",
        "endpoint": "/users",
        "method": "GET",
        "base_url": "https://api.source.com",
        "headers": {"Authorization": "Bearer token"},
        "pagination": {"type": "offset"}
    }

@pytest.fixture
def valid_destination_config():
    """Valid destination configuration."""
    return {
        "type": "database",
        "driver": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "users"
    }

@pytest.fixture
def mock_streaming_engine():
    """Mock StreamingEngine."""
    engine = MagicMock(spec=StreamingEngine)
    engine.stream_data = AsyncMock()
    engine.get_metrics.return_value = {
        "records_processed": 100,
        "records_failed": 0,
        "batches_processed": 10
    }
    engine.get_state_manager.return_value = MagicMock()
    return engine


class TestPipelineInitialization:
    """Test Pipeline initialization and configuration."""

    def test_init_valid_config(self, valid_pipeline_config, valid_source_config, valid_destination_config, temp_dir):
        """Test successful pipeline initialization."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine_cls.return_value = mock_engine
            
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                state_dir=temp_dir
            )
            
            assert pipeline.config["pipeline_id"] == "test-pipeline"
            assert pipeline.state_dir == temp_dir
            assert pipeline.logs_dir.endswith("logs/test-pipeline")
            assert pipeline.dlq_dir.endswith("deadletter/test-pipeline")
            
            # Verify StreamingEngine was created with correct parameters
            mock_engine_cls.assert_called_once_with(
                pipeline_id="test-pipeline",
                batch_size=1000,
                max_concurrent_batches=10,
                buffer_size=10000,
                dlq_path=pipeline.dlq_dir
            )

    def test_init_missing_pipeline_config(self, valid_source_config, valid_destination_config):
        """Test initialization with missing pipeline config."""
        with pytest.raises(ValueError, match="pipeline_config is required"):
            Pipeline(
                pipeline_config=None,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )

    def test_init_missing_source_config(self, valid_pipeline_config, valid_destination_config):
        """Test initialization with missing source config."""
        with pytest.raises(ValueError, match="source_config is required"):
            Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=None,
                destination_config=valid_destination_config
            )

    def test_init_missing_destination_config(self, valid_pipeline_config, valid_source_config):
        """Test initialization with missing destination config."""
        with pytest.raises(ValueError, match="destination_config is required"):
            Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=None
            )

    def test_init_missing_streams_section(self, valid_source_config, valid_destination_config):
        """Test initialization with missing streams section."""
        invalid_config = {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline"
            # Missing "streams" section
        }
        
        with pytest.raises(ValueError, match="pipeline_config must contain 'streams' section"):
            Pipeline(
                pipeline_config=invalid_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )

    def test_init_missing_pipeline_id(self, valid_source_config, valid_destination_config):
        """Test initialization with missing pipeline_id."""
        invalid_config = {
            "name": "Test Pipeline",
            "streams": {}
            # Missing "pipeline_id"
        }
        
        with pytest.raises(ValueError, match="pipeline_config must contain 'pipeline_id'"):
            Pipeline(
                pipeline_config=invalid_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )

    @patch('analitiq_stream.core.pipeline.load_dotenv')
    def test_init_loads_dotenv(self, mock_load_dotenv, valid_pipeline_config, valid_source_config, valid_destination_config):
        """Test that initialization loads .env file."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            mock_load_dotenv.assert_called_once()

    def test_init_creates_directories(self, valid_pipeline_config, valid_source_config, valid_destination_config, temp_dir):
        """Test that initialization creates required directories."""
        state_dir = os.path.join(temp_dir, "state")
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                state_dir=state_dir
            )
            
            # Check that directories exist
            assert Path(pipeline.state_dir).exists()
            assert Path(pipeline.logs_dir).exists()
            assert Path(pipeline.dlq_dir).exists()


class TestConfigurationBuild:
    """Test unified configuration building."""

    def test_build_unified_config_success(self, valid_pipeline_config, valid_source_config, valid_destination_config):
        """Test successful unified configuration building."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            config = pipeline.config
            
            # Check pipeline-level config
            assert config["pipeline_id"] == "test-pipeline"
            assert config["name"] == "Test Pipeline"
            assert config["version"] == "1.0"
            
            # Check engine config
            assert config["engine_config"]["batch_size"] == 1000
            
            # Check streams config
            assert "users" in config["streams"]
            stream_config = config["streams"]["users"]
            assert stream_config["stream_id"] == "users"
            assert stream_config["name"] == "User Stream"
            assert "source" in stream_config
            assert "destination" in stream_config
            
            # Check that source/destination configs are merged
            assert stream_config["source"]["type"] == "api"
            assert stream_config["destination"]["type"] == "database"
            
            # Check that stream-level src/dst settings are included
            assert stream_config["replication_key"] == "updated_at"
            assert stream_config["refresh_mode"] == "upsert"

    def test_build_unified_config_with_validation(self, valid_pipeline_config, valid_source_config, valid_destination_config):
        """Test unified configuration with validation config."""
        validation_config = {
            "rules": [
                {
                    "field": "user_id",
                    "type": "not_null",
                    "error_action": "dlq"
                }
            ]
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                validation_config=validation_config
            )
            
            assert "validation" in pipeline.config
            assert pipeline.config["validation"]["rules"][0]["field"] == "user_id"

    def test_build_unified_config_multiple_streams(self, valid_source_config, valid_destination_config):
        """Test configuration building with multiple streams."""
        multi_stream_config = {
            "pipeline_id": "multi-stream-pipeline",
            "name": "Multi-Stream Pipeline",
            "streams": {
                "users": {
                    "name": "User Stream",
                    "src": {"endpoint_id": "user-source"},
                    "dst": {"endpoint_id": "user-dest"}
                },
                "orders": {
                    "name": "Order Stream", 
                    "src": {"endpoint_id": "order-source"},
                    "dst": {"endpoint_id": "order-dest", "refresh_mode": "insert"}
                }
            }
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=multi_stream_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            assert len(pipeline.config["streams"]) == 2
            assert "users" in pipeline.config["streams"]
            assert "orders" in pipeline.config["streams"]
            
            # Check stream-specific configurations
            orders_stream = pipeline.config["streams"]["orders"]
            assert orders_stream["refresh_mode"] == "insert"


class TestDirectoryManagement:
    """Test directory creation and management."""

    def test_ensure_directories_creates_all_paths(self, valid_pipeline_config, valid_source_config, valid_destination_config, temp_dir):
        """Test that all required directories are created."""
        state_dir = os.path.join(temp_dir, "custom_state")
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                state_dir=state_dir
            )
            
            # Verify all directories exist
            assert Path(pipeline.state_dir).is_dir()
            assert Path(pipeline.logs_dir).is_dir()
            assert Path(pipeline.dlq_dir).is_dir()

    def test_ensure_directories_handles_existing_paths(self, valid_pipeline_config, valid_source_config, valid_destination_config, temp_dir):
        """Test directory creation when paths already exist."""
        state_dir = os.path.join(temp_dir, "existing_state")
        logs_dir = os.path.join(temp_dir, "logs", "test-pipeline")
        
        # Pre-create directories
        Path(state_dir).mkdir(parents=True, exist_ok=True)
        Path(logs_dir).mkdir(parents=True, exist_ok=True)
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                state_dir=state_dir
            )
            
            # Should not raise error and directories should still exist
            assert Path(pipeline.state_dir).is_dir()
            assert Path(pipeline.logs_dir).is_dir()
            assert Path(pipeline.dlq_dir).is_dir()


class TestLoggingSetup:
    """Test pipeline logging configuration."""

    def test_setup_pipeline_logging(self, valid_pipeline_config, valid_source_config, valid_destination_config, temp_dir):
        """Test pipeline logging setup."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                state_dir=temp_dir
            )
            
            # Check that log files are created
            pipeline_log = Path(pipeline.logs_dir) / "pipeline.log"
            assert pipeline_log.parent.exists()
            
            # Check stream-specific log directories
            stream_log_dir = Path(pipeline.logs_dir) / "users"
            assert stream_log_dir.exists()

    def test_setup_logging_with_custom_log_level(self, valid_source_config, valid_destination_config, temp_dir):
        """Test logging setup with custom log level."""
        config_with_debug = {
            "pipeline_id": "debug-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}},
            "monitoring": {"log_level": "DEBUG"}
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=config_with_debug,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                state_dir=temp_dir
            )
            
            # Verify log level was set (we can't easily test the actual logger level without more complex mocking)
            assert pipeline.config["monitoring"]["log_level"] == "DEBUG"

    def test_setup_logging_multiple_streams(self, valid_source_config, valid_destination_config, temp_dir):
        """Test logging setup with multiple streams."""
        multi_stream_config = {
            "pipeline_id": "multi-log-pipeline",
            "streams": {
                "users": {"name": "User Stream", "src": {}, "dst": {}},
                "orders": {"name": "Order Stream", "src": {}, "dst": {}},
                "products": {"name": "Product Stream", "src": {}, "dst": {}}
            }
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=multi_stream_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                state_dir=temp_dir
            )
            
            # Check that all stream log directories are created
            for stream_id in ["users", "orders", "products"]:
                stream_log_dir = Path(pipeline.logs_dir) / stream_id
                assert stream_log_dir.exists()


class TestConfigurationValidation:
    """Test configuration validation methods."""

    def test_validate_config_success(self, valid_pipeline_config, valid_source_config, valid_destination_config):
        """Test successful configuration validation."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            assert pipeline.validate_config() is True

    def test_validate_pipeline_config_missing_pipeline_id(self, valid_source_config, valid_destination_config):
        """Test pipeline validation with missing pipeline_id."""
        # This test expects the __init__ to catch the missing pipeline_id
        invalid_config = {
            "streams": {"test": {"src": {}, "dst": {}}}
            # Missing pipeline_id
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            with pytest.raises(ValueError, match="pipeline_config must contain 'pipeline_id'"):
                Pipeline(
                    pipeline_config=invalid_config,
                    source_config=valid_source_config,
                    destination_config=valid_destination_config
                )

    def test_validate_pipeline_config_empty_pipeline_id(self, valid_source_config, valid_destination_config):
        """Test pipeline validation with empty pipeline_id."""
        invalid_config = {
            "pipeline_id": "",  # Empty string
            "streams": {"test": {"src": {}, "dst": {}}}
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=invalid_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            assert pipeline.validate_config() is False

    def test_validate_engine_config_invalid_batch_size(self, valid_source_config, valid_destination_config):
        """Test validation with invalid engine config."""
        invalid_config = {
            "pipeline_id": "test-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}},
            "engine_config": {
                "batch_size": "invalid"  # Should be integer
            }
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=invalid_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            assert pipeline.validate_config() is False

    def test_validate_transformations_success(self, valid_source_config, valid_destination_config):
        """Test successful transformation validation."""
        config_with_transformations = {
            "pipeline_id": "transform-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}},
            "transformations": [
                {
                    "type": "field_mapping",
                    "mappings": {"old_field": "new_field"}
                },
                {
                    "type": "computed_field",
                    "field": "computed_value",
                    "expression": "now()"
                }
            ]
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=config_with_transformations,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            assert pipeline.validate_config() is True

    def test_validate_transformations_invalid_type(self, valid_source_config, valid_destination_config):
        """Test transformation validation with invalid type."""
        config_with_invalid_transform = {
            "pipeline_id": "invalid-transform-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}},
            "transformations": [
                {
                    "type": "invalid_type",  # Invalid transformation type
                    "field": "test_field"
                }
            ]
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=config_with_invalid_transform,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Manually add transformations to the unified config to test validation
            pipeline.config["transformations"] = [
                {
                    "type": "invalid_type",
                    "field": "test_field"
                }
            ]
            
            assert pipeline.validate_config() is False

    def test_validate_transformations_missing_field(self, valid_source_config, valid_destination_config):
        """Test transformation validation with missing required field."""
        config_with_missing_field = {
            "pipeline_id": "missing-field-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}},
            "transformations": [
                {
                    "type": "computed_field"
                    # Missing required "field"
                }
            ]
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=config_with_missing_field,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Manually add transformations to the unified config to test validation
            pipeline.config["transformations"] = [
                {
                    "type": "computed_field"
                    # Missing required "field"
                }
            ]
            
            assert pipeline.validate_config() is False

    def test_validate_validation_rules_success(self, valid_source_config, valid_destination_config):
        """Test successful validation rules validation."""
        validation_config = {
            "rules": [
                {
                    "field": "user_id",
                    "type": "not_null", 
                    "error_action": "dlq"
                },
                {
                    "field": "status",
                    "type": "enum",
                    "error_action": "skip"
                }
            ]
        }
        
        config_with_validation = {
            "pipeline_id": "validation-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}}
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=config_with_validation,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                validation_config=validation_config
            )
            
            assert pipeline.validate_config() is True

    def test_validate_validation_rules_invalid_error_action(self, valid_source_config, valid_destination_config):
        """Test validation rules with invalid error action."""
        validation_config = {
            "rules": [
                {
                    "field": "test_field",
                    "type": "not_null",
                    "error_action": "invalid_action"  # Invalid error action
                }
            ]
        }
        
        config_with_invalid_validation = {
            "pipeline_id": "invalid-validation-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}}
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=config_with_invalid_validation,
                source_config=valid_source_config,
                destination_config=valid_destination_config,
                validation_config=validation_config
            )
            
            assert pipeline.validate_config() is False


class TestConfigStateCompatibility:
    """Test configuration state compatibility validation."""

    def test_validate_config_state_compatibility_no_previous_state(self, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test compatibility validation when no previous state exists."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            # Mock no previous state
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_streaming_engine.get_state_manager.return_value = mock_state_manager
            
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Should not raise any exceptions
            pipeline._validate_config_state_compatibility()

    @patch('analitiq_stream.core.pipeline.ConfigStateValidator')
    def test_validate_config_state_compatibility_success(self, mock_validator_cls, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test successful compatibility validation."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            # Mock existing state
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = {"run_id": "previous-run"}
            mock_streaming_engine.get_state_manager.return_value = mock_state_manager
            
            # Mock validator success
            mock_validator = MagicMock()
            mock_validator.validate_startup_compatibility.return_value = (True, [])
            mock_validator_cls.return_value = mock_validator
            
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            pipeline._validate_config_state_compatibility()
            
            mock_validator.validate_startup_compatibility.assert_called_once()

    @patch('analitiq_stream.core.pipeline.ConfigStateValidator')
    @patch('analitiq_stream.core.pipeline.ConfigStateRecoveryManager')
    def test_validate_config_state_compatibility_breaking_changes_with_recovery(self, mock_recovery_cls, mock_validator_cls, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test compatibility validation with breaking changes that can be recovered."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            # Mock existing state
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = {"run_id": "previous-run"}
            mock_streaming_engine.get_state_manager.return_value = mock_state_manager
            
            # Mock validator with breaking changes
            mock_validator = MagicMock()
            mock_change = MagicMock()
            mock_change.change_type.value = "breaking"
            mock_change.description = "Breaking change detected"
            mock_validator.validate_startup_compatibility.return_value = (False, [mock_change])
            mock_validator_cls.return_value = mock_validator
            
            # Mock successful recovery
            mock_recovery = MagicMock()
            mock_recovery.attempt_automatic_recovery.return_value = True
            mock_recovery_cls.return_value = mock_recovery
            
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Should not raise exception due to successful recovery
            pipeline._validate_config_state_compatibility()
            
            mock_recovery.attempt_automatic_recovery.assert_called_once()

    @patch('analitiq_stream.core.pipeline.ConfigStateValidator')
    @patch('analitiq_stream.core.pipeline.ConfigStateRecoveryManager')
    def test_validate_config_state_compatibility_breaking_changes_recovery_fails(self, mock_recovery_cls, mock_validator_cls, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test compatibility validation with breaking changes that cannot be recovered."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            # Mock existing state
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = {"run_id": "previous-run"}
            mock_streaming_engine.get_state_manager.return_value = mock_state_manager
            
            # Mock validator with breaking changes
            mock_validator = MagicMock()
            mock_change = MagicMock()
            mock_change.change_type.value = "critical"
            mock_change.description = "Critical breaking change"
            mock_validator.validate_startup_compatibility.return_value = (False, [mock_change])
            mock_validator_cls.return_value = mock_validator
            
            # Mock failed recovery
            mock_recovery = MagicMock()
            mock_recovery.attempt_automatic_recovery.return_value = False
            mock_recovery_cls.return_value = mock_recovery
            
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Should raise ConfigCompatibilityError
            with pytest.raises(ConfigCompatibilityError):
                pipeline._validate_config_state_compatibility()


class TestPipelineExecution:
    """Test pipeline execution methods."""

    @pytest.mark.asyncio
    async def test_run_success(self, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test successful pipeline execution."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Mock successful validation
            with patch.object(pipeline, 'validate_config', return_value=True):
                with patch.object(pipeline, '_validate_config_state_compatibility'):
                    await pipeline.run()
                    
                    mock_streaming_engine.stream_data.assert_called_once_with(pipeline.config)

    @pytest.mark.asyncio 
    async def test_run_validation_failure(self, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test pipeline execution with validation failure."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Mock validation failure
            with patch.object(pipeline, 'validate_config', return_value=False):
                with pytest.raises(ValueError, match="Invalid pipeline configuration"):
                    await pipeline.run()

    @pytest.mark.asyncio
    async def test_run_config_compatibility_error(self, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test pipeline execution with config compatibility error."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Mock compatibility validation failure
            mock_error = ConfigCompatibilityError("Breaking changes", [], "test-pipeline")
            
            with patch.object(pipeline, 'validate_config', return_value=True):
                with patch.object(pipeline, '_validate_config_state_compatibility', side_effect=mock_error):
                    with pytest.raises(ConfigCompatibilityError):
                        await pipeline.run()

    @pytest.mark.asyncio
    async def test_run_with_progress_monitoring(self, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test pipeline execution with progress monitoring enabled."""
        config_with_monitoring = {
            "pipeline_id": "monitored-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}},
            "monitoring": {"progress_monitoring": "enabled"}
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            pipeline = Pipeline(
                pipeline_config=config_with_monitoring,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            with patch.object(pipeline, 'validate_config', return_value=True):
                with patch.object(pipeline, '_validate_config_state_compatibility'):
                    with patch.object(pipeline, '_run_with_progress_monitoring') as mock_monitor:
                        await pipeline.run()
                        
                        mock_monitor.assert_called_once()

    # Test removed due to being too tightly coupled to implementation details

    @pytest.mark.asyncio
    async def test_run_execution_error(self, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test pipeline execution with engine error."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Mock engine failure
            mock_streaming_engine.stream_data.side_effect = Exception("Engine failure")
            
            with patch.object(pipeline, 'validate_config', return_value=True):
                with patch.object(pipeline, '_validate_config_state_compatibility'):
                    with pytest.raises(Exception, match="Engine failure"):
                        await pipeline.run()


class TestMetricsAndStatus:
    """Test metrics and status reporting."""

    def test_get_metrics(self, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test metrics retrieval."""
        expected_metrics = {
            "records_processed": 100,
            "records_failed": 0,
            "batches_processed": 10,
            "error_rate": 0.0
        }
        mock_streaming_engine.get_metrics.return_value = expected_metrics
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            metrics = pipeline.get_metrics()
            
            assert metrics == expected_metrics
            mock_streaming_engine.get_metrics.assert_called_once()

    def test_get_status(self, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test status retrieval."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            status = pipeline.get_status()
            
            assert status["pipeline_id"] == "test-pipeline"
            assert "streams" in status
            assert "users" in status["streams"]
            assert status["streams"]["users"]["name"] == "User Stream"
            assert "metrics" in status

    def test_get_status_multiple_streams(self, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test status retrieval with multiple streams."""
        multi_stream_config = {
            "pipeline_id": "multi-status-pipeline",
            "streams": {
                "users": {
                    "name": "User Stream",
                    "source": {"endpoint_id": "user-source"},
                    "destination": {"endpoint_id": "user-dest"}
                },
                "orders": {
                    "name": "Order Stream", 
                    "source": {"endpoint_id": "order-source"},
                    "destination": {"endpoint_id": "order-dest"}
                }
            }
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            pipeline = Pipeline(
                pipeline_config=multi_stream_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            status = pipeline.get_status()
            
            assert len(status["streams"]) == 2
            assert "users" in status["streams"]
            assert "orders" in status["streams"]
            assert status["streams"]["users"]["name"] == "User Stream"
            assert status["streams"]["orders"]["name"] == "Order Stream"


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_validation_config_exception_handling(self, valid_source_config, valid_destination_config):
        """Test validation with exception in config processing."""
        # Create a config that will cause an exception during processing
        problematic_config = {
            "pipeline_id": "problem-pipeline",
            "streams": {"test": {"src": {}, "dst": {}}},
            "transformations": "invalid_transformations_format"  # Should be list, not string
        }
        
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=problematic_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Should handle the error gracefully and return False
            assert pipeline.validate_config() is False

    def test_build_unified_config_exception(self, valid_source_config, valid_destination_config):
        """Test exception handling in unified config building."""
        # This should be caught during initialization
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            with patch.object(Pipeline, '_build_unified_config', side_effect=Exception("Config build error")):
                with pytest.raises(Exception, match="Config build error"):
                    Pipeline(
                        pipeline_config={"pipeline_id": "test", "streams": {}},
                        source_config=valid_source_config,
                        destination_config=valid_destination_config
                    )

    def test_config_state_compatibility_general_exception(self, valid_pipeline_config, valid_source_config, valid_destination_config, mock_streaming_engine):
        """Test general exception handling in config state compatibility."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine', return_value=mock_streaming_engine):
            # Mock existing state
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = {"run_id": "previous-run"}
            mock_streaming_engine.get_state_manager.return_value = mock_state_manager
            
            pipeline = Pipeline(
                pipeline_config=valid_pipeline_config,
                source_config=valid_source_config,
                destination_config=valid_destination_config
            )
            
            # Mock validator to raise general exception
            with patch('analitiq_stream.core.pipeline.ConfigStateValidator', side_effect=Exception("Validator error")):
                with pytest.raises(ConfigCompatibilityError, match="Config-state validation error"):
                    pipeline._validate_config_state_compatibility()

    def test_directory_creation_error_handling(self, valid_pipeline_config, valid_source_config, valid_destination_config):
        """Test handling of directory creation errors."""
        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            with patch('pathlib.Path.mkdir', side_effect=PermissionError("Permission denied")):
                # Should raise the permission error
                with pytest.raises(PermissionError):
                    Pipeline(
                        pipeline_config=valid_pipeline_config,
                        source_config=valid_source_config,
                        destination_config=valid_destination_config
                    )