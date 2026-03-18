"""Comprehensive unit tests for PipelineConfigPrep module - NO MOCKING, real functionality only."""

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from src.engine.pipeline_config_prep import (
    PipelineConfigPrep,
    PipelineConfigPrepSettings,
    validate_pipeline_config,
    validate_stream_config,
)


@pytest.fixture
def temp_config_dir():
    """Create temporary directory with config structure."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create subdirectories for consolidated structure
        Path(temp_dir, "pipelines").mkdir()
        Path(temp_dir, "secrets").mkdir()
        yield temp_dir


@pytest.fixture
def mock_analitiq_config(temp_config_dir):
    """Fixture that patches load_analitiq_config to use the temp directory paths."""
    mock_config = {
        "paths": {
            "pipelines": f"{temp_config_dir}/pipelines",
            "secrets": f"{temp_config_dir}/secrets",
        }
    }
    with patch("src.engine.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
        yield mock_config


@pytest.fixture
def valid_pipeline_config():
    """Valid pipeline configuration for testing."""
    return {
        "version": 1,
        "org_id": "org-123",
        "pipeline_id": "test-pipeline-123",
        "name": "Test Pipeline",
        "status": "active",
        "tags": ["test"],
        "connections": {
            "source": {
                "conn_src": "source-connection-id"
            },
            "destinations": [
                {
                    "conn_dst": "dest-connection-id"
                }
            ]
        },
        "streams": ["stream-456"],
        "engine_config": {
            "expression": {"lang": "jsonata"},
            "logging": {"log_level": "INFO"},
            "error_handling": {"max_retries": 3},
            "retry": {"max_attempts": 5},
            "batching": {"batch_size": 100},
            "engine": {"buffer_size": 5000},
            "schedule": {"type": "interval", "interval_minutes": 60}
        },
        "function_catalog": {"catalog_id": "core", "version": 1}
    }


@pytest.fixture
def valid_stream_config():
    """Valid stream configuration for testing (uses endpoint_id format)."""
    return {
        "version": 1,
        "stream_id": "stream-456",
        "pipeline_id": "test-pipeline-123",
        "org_id": "org-123",
        "status": "active",
        "is_enabled": True,
        "source": {
            "connection_ref": "conn_src",
            "endpoint_id": "source-endpoint-id",
            "primary_key": ["id"],
            "replication": {
                "method": "incremental",
                "cursor_field": ["created_at"]
            }
        },
        "destinations": [
            {
                "connection_ref": "conn_dst",
                "endpoint_id": "dest-endpoint-id",
                "write": {"mode": "upsert"},
                "batching": {"supported": True, "size": 100}
            }
        ],
        "mapping": {
            "assignments": [
                {
                    "target": {"path": ["id"], "type": "string", "nullable": False},
                    "value": {"kind": "expr", "expr": {"op": "get", "path": ["id"]}}
                }
            ]
        }
    }


@pytest.fixture
def incomplete_pipeline_config():
    """Pipeline config missing required fields."""
    return {
        "name": "Incomplete Pipeline",
        "version": "1.0"
        # Missing pipeline_id
    }


@pytest.fixture
def valid_connection_config():
    """Valid connection configuration."""
    return {
        "connection_id": "source-connection-id",
        "connector_id": "connector-1",
        "host": "https://api.example.com",
        "headers": {
            "Authorization": "Bearer ${API_TOKEN}",
            "Content-Type": "application/json"
        },
        "rate_limit": {
            "max_requests": 100,
            "time_window": 60
        }
    }


@pytest.fixture
def valid_endpoint_config():
    """Valid endpoint configuration."""
    return {
        "endpoint_id": "source-endpoint-id",
        "endpoint": "/v1/data",
        "method": "GET",
        "endpoint_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"}
                }
            }
        }
    }


class TestPipelineConfigPrepSettings:
    """Test PipelineConfigPrepSettings validation and behavior."""

    def test_default_settings_local(self):
        """Test default settings for local environment."""
        settings = PipelineConfigPrepSettings(pipeline_id="test-pipeline")

        assert settings.env == "local"
        assert settings.pipeline_id == "test-pipeline"
        assert settings.aws_region == "eu-central-1"

    def test_custom_local_settings(self):
        """Test custom local settings."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="custom-pipeline",
            org_id="client-xyz",
            aws_region="us-west-2"
        )

        assert settings.env == "local"
        assert settings.pipeline_id == "custom-pipeline"
        assert settings.org_id == "client-xyz"
        assert settings.aws_region == "us-west-2"

    def test_missing_pipeline_id_raises_validation_error(self):
        """Test that missing pipeline_id raises ValidationError."""
        with pytest.raises(ValidationError):
            PipelineConfigPrepSettings()

    def test_empty_pipeline_id_validation(self):
        """Test that empty pipeline_id raises error."""
        settings = PipelineConfigPrepSettings(pipeline_id="")
        with pytest.raises(RuntimeError) as exc_info:
            PipelineConfigPrep(settings)
        assert "PIPELINE_ID" in str(exc_info.value)


class TestValidatePipelineConfig:
    """Test validate_pipeline_config function."""

    def test_valid_config_passes(self, valid_pipeline_config):
        """Test that valid pipeline configuration passes validation."""
        assert validate_pipeline_config(valid_pipeline_config) is True

    def test_missing_pipeline_id_fails(self, incomplete_pipeline_config):
        """Test that missing pipeline_id fails validation."""
        assert validate_pipeline_config(incomplete_pipeline_config) is False

    def test_empty_pipeline_id_fails(self):
        """Test that empty pipeline_id fails validation."""
        config = {"pipeline_id": "", "name": "Test"}
        assert validate_pipeline_config(config) is False

    def test_non_string_pipeline_id_fails(self):
        """Test that non-string pipeline_id fails validation."""
        config = {"pipeline_id": 123, "name": "Test"}
        assert validate_pipeline_config(config) is False


class TestValidateStreamConfig:
    """Test validate_stream_config function."""

    def test_valid_config_passes(self, valid_stream_config):
        """Test that valid stream configuration passes validation."""
        assert validate_stream_config(valid_stream_config) is True

    def test_missing_stream_id_fails(self, valid_stream_config):
        """Test that missing stream_id fails validation."""
        config = valid_stream_config.copy()
        del config["stream_id"]
        assert validate_stream_config(config) is False

    def test_missing_source_fails(self, valid_stream_config):
        """Test that missing source fails validation."""
        config = valid_stream_config.copy()
        del config["source"]
        assert validate_stream_config(config) is False

    def test_missing_destinations_fails(self, valid_stream_config):
        """Test that missing destinations fails validation."""
        config = valid_stream_config.copy()
        del config["destinations"]
        assert validate_stream_config(config) is False

    def test_empty_destinations_fails(self, valid_stream_config):
        """Test that empty destinations array fails validation."""
        config = valid_stream_config.copy()
        config["destinations"] = []
        assert validate_stream_config(config) is False

    def test_missing_source_connection_ref_fails(self, valid_stream_config):
        """Test that missing source.connection_ref fails validation."""
        config = valid_stream_config.copy()
        config["source"] = {"endpoint_id": "test-endpoint"}
        assert validate_stream_config(config) is False

    def test_missing_destination_endpoint_fails(self, valid_stream_config):
        """Test that missing destination endpoint_id fails validation."""
        config = valid_stream_config.copy()
        config["destinations"] = [{"connection_ref": "conn_dst", "write": {"mode": "upsert"}}]
        assert validate_stream_config(config) is False


class TestPipelineConfigPrepLocal:
    """Test PipelineConfigPrep class with local filesystem operations."""

    def test_initialization_local_mode(self, temp_config_dir, mock_analitiq_config):
        """Test initialization in local mode."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )

        prep = PipelineConfigPrep(settings)

        assert prep.settings == settings
        assert prep.is_cloud_env is False  # local mode = not cloud
        assert prep.settings is not None

    def test_validate_environment_success(self, temp_config_dir, mock_analitiq_config):
        """Test successful environment validation."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )

        prep = PipelineConfigPrep(settings)
        prep.validate_environment()

    def test_validate_environment_missing_pipeline_id(self, temp_config_dir, mock_analitiq_config):
        """Test environment validation with missing pipeline ID."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id=""
        )

        with pytest.raises(RuntimeError) as exc_info:
            PipelineConfigPrep(settings)

        assert "PIPELINE_ID environment variable is required" in str(exc_info.value)

    def test_validate_environment_missing_pipelines_directory(self, temp_config_dir):
        """Test environment validation with missing pipelines directory."""
        # Create mock config pointing to non-existent pipelines directory
        mock_config = {
            "paths": {
                "pipelines": "/nonexistent/pipelines",
                "secrets": f"{temp_config_dir}/secrets",
            }
        }
        with patch("src.engine.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
            settings = PipelineConfigPrepSettings(
                env="local",
                pipeline_id="test-pipeline"
            )

            with pytest.raises(RuntimeError) as exc_info:
                PipelineConfigPrep(settings)

            assert "Pipelines directory not found" in str(exc_info.value)

class TestConfigurationLoading:
    """Test configuration loading functionality with consolidated file format."""

    def write_consolidated_config(
        self,
        temp_dir: str,
        pipeline_config: Dict[str, Any],
        stream_configs: list = None,
        connection_configs: list = None,
        endpoint_configs: list = None,
        connector_configs: list = None,
        secret_configs: Dict[str, Dict[str, Any]] = None
    ):
        """Helper to write consolidated configuration file.

        Args:
            temp_dir: Temporary directory path
            pipeline_config: Pipeline configuration dict
            stream_configs: List of stream configuration dicts
            connection_configs: List of connection configs
            endpoint_configs: List of endpoint configs
            connector_configs: List of connector configs
            secret_configs: Dict mapping connection_id to secret content
        """
        # Provide default connectors if none specified (validation requires at least 2)
        default_connectors = [
            {"connector_id": "connector-1", "connector_name": "Test Connector 1", "connector_type": "api"},
            {"connector_id": "connector-2", "connector_name": "Test Connector 2", "connector_type": "api"},
        ]

        # Build consolidated config
        consolidated = {
            "pipeline": pipeline_config,
            "connections": connection_configs or [],
            "connectors": connector_configs if connector_configs is not None else default_connectors,
            "endpoints": endpoint_configs or [],
            "streams": stream_configs or [],
        }

        # Write consolidated file
        pipeline_path = Path(temp_dir, "pipelines", f"{pipeline_config['pipeline_id']}.json")
        with open(pipeline_path, "w") as f:
            json.dump(consolidated, f)

        # Write secret configs
        if secret_configs:
            for connection_id, secret_content in secret_configs.items():
                secret_path = Path(temp_dir, "secrets", f"{connection_id}.json")
                with open(secret_path, "w") as f:
                    json.dump(secret_content, f)

    def test_load_pipeline_and_streams(
        self,
        temp_config_dir,
        mock_analitiq_config,
        valid_pipeline_config,
        valid_stream_config
    ):
        """Test loading pipeline and stream configurations from consolidated file."""
        # Create connection configs
        connection_configs = [
            {
                "connection_id": "source-connection-id",
                "connector_id": "connector-1",
                "host": "https://api.source.com",
                "headers": {"Content-Type": "application/json"}
            },
            {
                "connection_id": "dest-connection-id",
                "connector_id": "connector-2",
                "host": "https://api.dest.com",
                "headers": {"Content-Type": "application/json"}
            }
        ]

        endpoint_configs = [
            {
                "endpoint_id": "source-endpoint-id",
                "endpoint": "/v1/data",
                "method": "GET"
            },
            {
                "endpoint_id": "dest-endpoint-id",
                "endpoint": "/v1/write",
                "method": "POST"
            }
        ]

        secret_configs = {
            "source-connection-id": {"token": "source-token"},
            "dest-connection-id": {"token": "dest-token"}
        }

        self.write_consolidated_config(
            temp_config_dir,
            valid_pipeline_config,
            stream_configs=[valid_stream_config],
            connection_configs=connection_configs,
            endpoint_configs=endpoint_configs,
            secret_configs=secret_configs
        )

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline-123"
        )
        prep = PipelineConfigPrep(settings)

        pipeline_config, stream_configs_result = prep.load_pipeline_config()

        # Verify pipeline
        assert pipeline_config.pipeline_id == "test-pipeline-123"
        assert pipeline_config.name == "Test Pipeline"

        # Verify streams
        assert len(stream_configs_result) == 1
        assert stream_configs_result[0].stream_id == "stream-456"

    def test_connection_resolution(
        self,
        temp_config_dir,
        mock_analitiq_config,
        valid_pipeline_config,
        valid_stream_config
    ):
        """Test that connections are resolved from consolidated config."""
        connection_configs = [
            {
                "connection_id": "source-connection-id",
                "connector_id": "connector-1",
                "host": "https://api.source.com"
            },
            {
                "connection_id": "dest-connection-id",
                "connector_id": "connector-2",
                "host": "https://api.dest.com"
            }
        ]

        endpoint_configs = [
            {"endpoint_id": "source-endpoint-id", "endpoint": "/v1/data"},
            {"endpoint_id": "dest-endpoint-id", "endpoint": "/v1/write"}
        ]

        secret_configs = {
            "source-connection-id": {"token": "src-token"},
            "dest-connection-id": {"token": "dst-token"}
        }

        self.write_consolidated_config(
            temp_config_dir,
            valid_pipeline_config,
            stream_configs=[valid_stream_config],
            connection_configs=connection_configs,
            endpoint_configs=endpoint_configs,
            secret_configs=secret_configs
        )

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline-123"
        )
        prep = PipelineConfigPrep(settings)

        pipeline, streams, connections, endpoints, connectors = prep.create_config()

        # Check connections are resolved
        assert "source-connection-id" in connections
        assert connections["source-connection-id"].connection_type == "api"

    def test_endpoint_resolution(
        self,
        temp_config_dir,
        mock_analitiq_config,
        valid_pipeline_config,
        valid_stream_config
    ):
        """Test that endpoints are resolved from consolidated config."""
        connection_configs = [
            {"connection_id": "source-connection-id", "connector_id": "connector-1", "host": "https://src.com"},
            {"connection_id": "dest-connection-id", "connector_id": "connector-2", "host": "https://dst.com"}
        ]

        endpoint_configs = [
            {"endpoint_id": "source-endpoint-id", "endpoint": "/v1/data", "method": "GET"},
            {"endpoint_id": "dest-endpoint-id", "endpoint": "/v1/write", "method": "POST"}
        ]

        secret_configs = {
            "source-connection-id": {"token": "t1"},
            "dest-connection-id": {"token": "t2"}
        }

        self.write_consolidated_config(
            temp_config_dir,
            valid_pipeline_config,
            stream_configs=[valid_stream_config],
            connection_configs=connection_configs,
            endpoint_configs=endpoint_configs,
            secret_configs=secret_configs
        )

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline-123"
        )
        prep = PipelineConfigPrep(settings)

        pipeline, streams, connections, endpoints, connectors = prep.create_config()

        # Check endpoints are resolved
        assert "source-endpoint-id" in endpoints
        assert endpoints["source-endpoint-id"]["endpoint"] == "/v1/data"


class TestConsolidatedConfigLoading:
    """Test loading from consolidated pipeline file."""

    def _minimal_valid_consolidated(self, pipeline_id: str = "test-pipe") -> dict:
        """Return a minimal valid consolidated config that passes validation."""
        return {
            "pipeline": {"pipeline_id": pipeline_id, "name": "Test"},
            "connections": [
                {"connection_id": "conn-1", "connector_id": "connector-1", "host": "https://example1.com"},
                {"connection_id": "conn-2", "connector_id": "connector-2", "host": "https://example2.com"},
            ],
            "connectors": [
                {"connector_id": "connector-1", "connector_name": "Connector 1", "connector_type": "api"},
                {"connector_id": "connector-2", "connector_name": "Connector 2", "connector_type": "api"},
            ],
            "endpoints": [
                {"endpoint_id": "ep-1", "endpoint": "/v1/data", "method": "GET"},
                {"endpoint_id": "ep-2", "endpoint": "/v1/write", "method": "POST"},
            ],
            "streams": [
                {"stream_id": "stream-1", "pipeline_id": pipeline_id},
            ],
        }

    def test_load_consolidated_config_caches_result(self, temp_config_dir, mock_analitiq_config):
        """Verify consolidated config is cached after first load."""
        consolidated = self._minimal_valid_consolidated("test-pipe")
        pipeline_path = Path(temp_config_dir, "pipelines", "test-pipe.json")
        with open(pipeline_path, "w") as f:
            json.dump(consolidated, f)

        settings = PipelineConfigPrepSettings(env="local", pipeline_id="test-pipe")
        prep = PipelineConfigPrep(settings)

        # First call loads from disk
        config1 = prep._load_consolidated_config()
        # Second call returns cached
        config2 = prep._load_consolidated_config()

        assert config1 is config2  # Same object (cached)

    def test_connection_lookup_from_consolidated(self, temp_config_dir, mock_analitiq_config):
        """Verify connections are looked up from consolidated file."""
        consolidated = self._minimal_valid_consolidated("test-pipe")
        pipeline_path = Path(temp_config_dir, "pipelines", "test-pipe.json")
        with open(pipeline_path, "w") as f:
            json.dump(consolidated, f)

        settings = PipelineConfigPrepSettings(env="local", pipeline_id="test-pipe")
        prep = PipelineConfigPrep(settings)

        conn = prep._load_connection_config("conn-1")
        assert conn["connection_id"] == "conn-1"
        assert conn["host"] == "https://example1.com"

    def test_endpoint_lookup_from_consolidated(self, temp_config_dir, mock_analitiq_config):
        """Verify endpoints are looked up from consolidated file."""
        consolidated = self._minimal_valid_consolidated("test-pipe")
        pipeline_path = Path(temp_config_dir, "pipelines", "test-pipe.json")
        with open(pipeline_path, "w") as f:
            json.dump(consolidated, f)

        settings = PipelineConfigPrepSettings(env="local", pipeline_id="test-pipe")
        prep = PipelineConfigPrep(settings)

        ep = prep._load_endpoint_config("ep-1")
        assert ep["endpoint_id"] == "ep-1"
        assert ep["endpoint"] == "/v1/data"

    def test_missing_connection_raises_error(self, temp_config_dir, mock_analitiq_config):
        """Verify FileNotFoundError when connection not in consolidated file."""
        consolidated = self._minimal_valid_consolidated("test-pipe")
        pipeline_path = Path(temp_config_dir, "pipelines", "test-pipe.json")
        with open(pipeline_path, "w") as f:
            json.dump(consolidated, f)

        settings = PipelineConfigPrepSettings(env="local", pipeline_id="test-pipe")
        prep = PipelineConfigPrep(settings)

        with pytest.raises(FileNotFoundError) as exc_info:
            prep._load_connection_config("nonexistent-id")

        assert "not found in consolidated config" in str(exc_info.value)

    def test_missing_endpoint_raises_error(self, temp_config_dir, mock_analitiq_config):
        """Verify FileNotFoundError when endpoint not in consolidated file."""
        consolidated = self._minimal_valid_consolidated("test-pipe")
        pipeline_path = Path(temp_config_dir, "pipelines", "test-pipe.json")
        with open(pipeline_path, "w") as f:
            json.dump(consolidated, f)

        settings = PipelineConfigPrepSettings(env="local", pipeline_id="test-pipe")
        prep = PipelineConfigPrep(settings)

        with pytest.raises(FileNotFoundError) as exc_info:
            prep._load_endpoint_config("nonexistent-ep")

        assert "not found in consolidated config" in str(exc_info.value)
