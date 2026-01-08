"""Comprehensive unit tests for PipelineConfigPrep module - NO MOCKING, real functionality only."""

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from src.core.pipeline_config_prep import (
    PipelineConfigPrep,
    PipelineConfigPrepSettings,
    validate_pipeline_config,
    validate_stream_config,
    expand_required_vars,
    find_unexpanded_placeholders,
    validate_no_unexpanded_placeholders,
)


@pytest.fixture
def temp_config_dir():
    """Create temporary directory with config structure."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create subdirectories for new structure
        Path(temp_dir, "pipelines").mkdir()
        Path(temp_dir, "streams").mkdir()
        Path(temp_dir, "connections").mkdir()
        Path(temp_dir, "endpoints").mkdir()
        Path(temp_dir, "secrets").mkdir()
        # Create connectors directory structure (for path-based endpoints)
        Path(temp_dir, "connectors", "source_api", "endpoints").mkdir(parents=True)
        Path(temp_dir, "connectors", "dest_api", "endpoints").mkdir(parents=True)
        Path(temp_dir, "connectors", "dest_db", "endpoints").mkdir(parents=True)
        yield temp_dir


@pytest.fixture
def mock_analitiq_config(temp_config_dir):
    """Fixture that patches load_analitiq_config to use the temp directory paths."""
    mock_config = {
        "paths": {
            "connectors": f"{temp_config_dir}/connectors",
            "connections": f"{temp_config_dir}/connections",
            "endpoints": f"{temp_config_dir}/endpoints",
            "streams": f"{temp_config_dir}/streams",
            "pipelines": f"{temp_config_dir}/pipelines",
            "secrets": f"{temp_config_dir}/secrets",
        }
    }
    with patch("src.core.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
        yield mock_config


@pytest.fixture
def valid_pipeline_config():
    """Valid pipeline configuration for testing (new ID-based format)."""
    return {
        "version": 1,
        "client_id": "client-123",
        "pipeline_id": "test-pipeline-123",
        "name": "Test Pipeline",
        "status": "active",
        "is_active": True,
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
        "runtime": {
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
    """Valid stream configuration for testing (new path-based format)."""
    return {
        "version": 1,
        "stream_id": "stream-456",
        "pipeline_id": "test-pipeline-123",
        "client_id": "client-123",
        "status": "active",
        "is_enabled": True,
        "source": {
            "connection_ref": "conn_src",
            "endpoint": "source_api/endpoints/data.json",
            "primary_key": ["id"],
            "replication": {
                "method": "incremental",
                "cursor_field": ["created_at"]
            }
        },
        "destinations": [
            {
                "connection_ref": "conn_dst",
                "endpoint": "dest_api/endpoints/data.json",
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
def config_with_env_vars():
    """Config that requires environment variables."""
    return {
        "pipeline_id": "env-test-pipeline",
        "database_url": "${DATABASE_URL}",
        "api_key": "${API_KEY}",
        "nested": {
            "secret": "${NESTED_SECRET}"
        }
    }


@pytest.fixture
def valid_connection_config():
    """Valid connection configuration."""
    return {
        "type": "api",
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
        "endpoint": "/v1/data",
        "method": "GET",
        "response_schema": {
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
        """Test custom settings for local environment."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="custom-pipeline",
            client_id="client-123"
        )

        assert settings.env == "local"
        assert settings.pipeline_id == "custom-pipeline"
        assert settings.client_id == "client-123"

    def test_missing_pipeline_id_raises_validation_error(self):
        """Test that missing pipeline_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineConfigPrepSettings()

        assert "pipeline_id" in str(exc_info.value)
        assert "Field required" in str(exc_info.value)

    def test_empty_pipeline_id_validation(self):
        """Test validation of empty pipeline_id."""
        settings = PipelineConfigPrepSettings(pipeline_id="")
        assert settings.pipeline_id == ""


class TestValidatePipelineConfig:
    """Test validate_pipeline_config function."""

    def test_valid_config_passes(self, valid_pipeline_config):
        """Test that valid configuration passes validation."""
        assert validate_pipeline_config(valid_pipeline_config) is True

    def test_missing_pipeline_id_fails(self, incomplete_pipeline_config):
        """Test that missing pipeline_id fails validation."""
        assert validate_pipeline_config(incomplete_pipeline_config) is False

    def test_empty_pipeline_id_fails(self):
        """Test that empty pipeline_id fails validation."""
        config = {"pipeline_id": ""}
        assert validate_pipeline_config(config) is False

        config = {"pipeline_id": "   "}  # Whitespace only
        assert validate_pipeline_config(config) is False

    def test_non_string_pipeline_id_fails(self):
        """Test that non-string pipeline_id fails validation."""
        config = {"pipeline_id": 123}
        assert validate_pipeline_config(config) is False

        config = {"pipeline_id": None}
        assert validate_pipeline_config(config) is False


class TestValidateStreamConfig:
    """Test validate_stream_config function."""

    def test_valid_config_passes(self, valid_stream_config):
        """Test that valid stream configuration passes validation."""
        assert validate_stream_config(valid_stream_config) is True

    def test_missing_stream_id_fails(self):
        """Test that missing stream_id fails validation."""
        config = {
            "source": {"connection_ref": "conn", "endpoint": "api/endpoints/data.json"},
            "destinations": [{"connection_ref": "conn", "endpoint": "api/endpoints/data.json"}]
        }
        assert validate_stream_config(config) is False

    def test_missing_source_fails(self):
        """Test that missing source fails validation."""
        config = {
            "stream_id": "test",
            "destinations": [{"connection_ref": "conn", "endpoint": "api/endpoints/data.json"}]
        }
        assert validate_stream_config(config) is False

    def test_missing_destinations_fails(self):
        """Test that missing destinations fails validation."""
        config = {
            "stream_id": "test",
            "source": {"connection_ref": "conn", "endpoint": "api/endpoints/data.json"}
        }
        assert validate_stream_config(config) is False

    def test_empty_destinations_fails(self):
        """Test that empty destinations array fails validation."""
        config = {
            "stream_id": "test",
            "source": {"connection_ref": "conn", "endpoint": "api/endpoints/data.json"},
            "destinations": []
        }
        assert validate_stream_config(config) is False

    def test_missing_source_connection_ref_fails(self):
        """Test that missing source.connection_ref fails validation."""
        config = {
            "stream_id": "test",
            "source": {"endpoint": "api/endpoints/data.json"},
            "destinations": [{"connection_ref": "conn", "endpoint": "api/endpoints/data.json"}]
        }
        assert validate_stream_config(config) is False

    def test_missing_destination_endpoint_fails(self):
        """Test that missing destination endpoint fails validation."""
        config = {
            "stream_id": "test",
            "source": {"connection_ref": "conn", "endpoint": "api/endpoints/data.json"},
            "destinations": [{"connection_ref": "conn"}]
        }
        assert validate_stream_config(config) is False


class TestExpandRequiredVars:
    """Test expand_required_vars function."""

    def test_expand_existing_env_vars(self, config_with_env_vars):
        """Test expansion of existing environment variables."""
        os.environ["DATABASE_URL"] = "postgresql://localhost:5432/test"
        os.environ["API_KEY"] = "test-api-key"
        os.environ["NESTED_SECRET"] = "secret-value"

        try:
            expanded = expand_required_vars(config_with_env_vars)

            assert expanded["database_url"] == "postgresql://localhost:5432/test"
            assert expanded["api_key"] == "test-api-key"
            assert expanded["nested"]["secret"] == "secret-value"
            assert expanded["pipeline_id"] == "env-test-pipeline"
        finally:
            for var in ["DATABASE_URL", "API_KEY", "NESTED_SECRET"]:
                os.environ.pop(var, None)

    def test_missing_env_var_raises_error(self):
        """Test that missing environment variable raises EnvironmentError in strict mode."""
        config = {
            "pipeline_id": "test",
            "missing_var": "${NONEXISTENT_VAR}"
        }

        os.environ.pop("NONEXISTENT_VAR", None)

        with pytest.raises(EnvironmentError) as exc_info:
            expand_required_vars(config, strict=True)

        assert "Missing required environment variable: NONEXISTENT_VAR" in str(exc_info.value)

    def test_missing_env_var_kept_non_strict(self):
        """Test that missing environment variable is kept in non-strict mode."""
        config = {
            "pipeline_id": "test",
            "missing_var": "${NONEXISTENT_VAR}"
        }

        os.environ.pop("NONEXISTENT_VAR", None)

        expanded = expand_required_vars(config, strict=False)
        assert expanded["missing_var"] == "${NONEXISTENT_VAR}"

    def test_no_vars_to_expand(self):
        """Test config with no environment variables to expand."""
        config = {
            "pipeline_id": "test",
            "simple_value": "no_vars_here",
            "number": 123,
            "boolean": True,
            "null_value": None
        }

        expanded = expand_required_vars(config)
        assert expanded == config


class TestFindUnexpandedPlaceholders:
    """Test find_unexpanded_placeholders function."""

    def test_finds_simple_placeholder(self):
        """Test finding simple placeholder."""
        config = {"key": "${VALUE}"}
        unexpanded = find_unexpanded_placeholders(config)
        assert len(unexpanded) == 1
        assert unexpanded[0][0] == "key"
        assert unexpanded[0][1] == "${VALUE}"

    def test_finds_nested_placeholder(self):
        """Test finding nested placeholder."""
        config = {"outer": {"inner": "${VALUE}"}}
        unexpanded = find_unexpanded_placeholders(config)
        assert len(unexpanded) == 1
        assert "inner" in unexpanded[0][0]

    def test_finds_multiple_placeholders(self):
        """Test finding multiple placeholders."""
        config = {
            "key1": "${VALUE1}",
            "key2": "${VALUE2}",
            "nested": {"key3": "${VALUE3}"}
        }
        unexpanded = find_unexpanded_placeholders(config)
        assert len(unexpanded) == 3

    def test_no_placeholders_returns_empty(self):
        """Test that config without placeholders returns empty list."""
        config = {"key": "value", "number": 123}
        unexpanded = find_unexpanded_placeholders(config)
        assert len(unexpanded) == 0


class TestValidateNoUnexpandedPlaceholders:
    """Test validate_no_unexpanded_placeholders function."""

    def test_valid_config_passes(self):
        """Test that config without placeholders passes."""
        config = {"key": "value"}
        validate_no_unexpanded_placeholders(config, "test")

    def test_config_with_placeholders_raises(self):
        """Test that config with placeholders raises ValueError."""
        config = {"key": "${VALUE}"}
        with pytest.raises(ValueError) as exc_info:
            validate_no_unexpanded_placeholders(config, "test")
        assert "unexpanded placeholder" in str(exc_info.value).lower()


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
        assert prep.credentials_manager is not None

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
                "connectors": f"{temp_config_dir}/connectors",
                "connections": f"{temp_config_dir}/connections",
                "endpoints": f"{temp_config_dir}/endpoints",
                "streams": f"{temp_config_dir}/streams",
                "pipelines": "/nonexistent/pipelines",
                "secrets": f"{temp_config_dir}/secrets",
            }
        }
        with patch("src.core.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
            settings = PipelineConfigPrepSettings(
                env="local",
                pipeline_id="test-pipeline"
            )

            with pytest.raises(RuntimeError) as exc_info:
                PipelineConfigPrep(settings)

            assert "Pipelines directory not found" in str(exc_info.value)

    def test_load_local_json_success(self, temp_config_dir, mock_analitiq_config):
        """Test successful local JSON loading."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )
        prep = PipelineConfigPrep(settings)

        test_config = {"test": "value", "nested": {"key": "value"}}
        test_file = Path(temp_config_dir, "test.json")
        with open(test_file, "w") as f:
            json.dump(test_config, f)

        result = prep._load_local_json(str(test_file))
        assert result == test_config

    def test_load_local_json_file_not_found(self, temp_config_dir, mock_analitiq_config):
        """Test local JSON loading with missing file."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )
        prep = PipelineConfigPrep(settings)

        with pytest.raises(FileNotFoundError) as exc_info:
            prep._load_local_json("/nonexistent/file.json")

        assert "Configuration file not found" in str(exc_info.value)

    def test_load_local_json_invalid_json(self, temp_config_dir, mock_analitiq_config):
        """Test local JSON loading with invalid JSON."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )
        prep = PipelineConfigPrep(settings)

        test_file = Path(temp_config_dir, "invalid.json")
        with open(test_file, "w") as f:
            f.write("{ invalid json content")

        with pytest.raises(ValueError) as exc_info:
            prep._load_local_json(str(test_file))

        assert "Invalid JSON" in str(exc_info.value)


class TestConfigurationLoading:
    """Test configuration loading functionality with ID-based connections."""

    def write_config_files(
        self,
        temp_dir: str,
        pipeline_config: Dict[str, Any],
        stream_configs: list = None,
        connection_configs: Dict[str, Dict[str, Any]] = None,
        endpoint_configs: Dict[str, Dict[str, Any]] = None,
        secret_configs: Dict[str, Dict[str, Any]] = None
    ):
        """Helper to write configuration files to temporary directory.

        Args:
            temp_dir: Temporary directory path
            pipeline_config: Pipeline configuration dict
            stream_configs: List of stream configuration dicts
            connection_configs: Dict mapping connection_id to connection config
            endpoint_configs: Dict mapping "connector_name/endpoints/file.json" to endpoint config
            secret_configs: Dict mapping connection_id to secret content
        """
        # Write pipeline config
        pipeline_path = Path(temp_dir, "pipelines", f"{pipeline_config['pipeline_id']}.json")
        with open(pipeline_path, "w") as f:
            json.dump(pipeline_config, f)

        # Write stream configs
        if stream_configs:
            for stream in stream_configs:
                stream_path = Path(temp_dir, "streams", f"{stream['stream_id']}.json")
                with open(stream_path, "w") as f:
                    json.dump(stream, f)

        # Write connection configs (connections/{connection_id}.json)
        if connection_configs:
            for connection_id, config in connection_configs.items():
                connection_path = Path(temp_dir, "connections", f"{connection_id}.json")
                with open(connection_path, "w") as f:
                    json.dump(config, f)

        # Write endpoint configs (connectors/{name}/endpoints/{file}.json)
        if endpoint_configs:
            for endpoint_path, config in endpoint_configs.items():
                full_path = Path(temp_dir, "connectors", endpoint_path)
                full_path.parent.mkdir(parents=True, exist_ok=True)
                with open(full_path, "w") as f:
                    json.dump(config, f)

        # Write secret configs (secrets/{connection_id}.json)
        if secret_configs:
            for connection_id, config in secret_configs.items():
                secret_path = Path(temp_dir, "secrets", f"{connection_id}.json")
                with open(secret_path, "w") as f:
                    json.dump(config, f)

    def test_load_pipeline_and_streams(
        self,
        temp_config_dir,
        mock_analitiq_config,
        valid_pipeline_config,
        valid_stream_config,
        valid_connection_config,
        valid_endpoint_config
    ):
        """Test loading pipeline and stream configurations."""
        # Create connection configs (ID-based format)
        connection_configs = {
            "source-connection-id": {
                "connection_id": "source-connection-id",
                "type": "api",
                "host": "https://api.source.com",
                "headers": {"Content-Type": "application/json"}
            },
            "dest-connection-id": {
                "connection_id": "dest-connection-id",
                "type": "api",
                "host": "https://api.dest.com",
                "headers": {"Content-Type": "application/json"}
            }
        }

        endpoint_configs = {
            "source_api/endpoints/data.json": valid_endpoint_config,
            "dest_api/endpoints/data.json": valid_endpoint_config
        }

        secret_configs = {
            "source-connection-id": {"token": "source-token"},
            "dest-connection-id": {"token": "dest-token"}
        }

        self.write_config_files(
            temp_config_dir,
            valid_pipeline_config,
            [valid_stream_config],
            connection_configs,
            endpoint_configs,
            secret_configs
        )

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline-123"
        )
        prep = PipelineConfigPrep(settings)

        pipeline, streams, _, _ = prep.create_config()

        assert pipeline.pipeline_id == "test-pipeline-123"
        assert pipeline.name == "Test Pipeline"
        # Check new nested connections structure
        assert "conn_src" in pipeline.connections.source
        assert any("conn_dst" in d for d in pipeline.connections.destinations)
        assert len(streams) == 1
        assert streams[0].stream_id == "stream-456"

    def test_connection_resolution(self, temp_config_dir, mock_analitiq_config, valid_pipeline_config):
        """Test that connections are properly resolved."""
        connection_configs = {
            "source-connection-id": {
                "connection_id": "source-connection-id",
                "type": "api",
                "host": "https://api.source.com"
            },
            "dest-connection-id": {
                "connection_id": "dest-connection-id",
                "provider": "postgresql",
                "host": "localhost",
                "port": "5432",
                "dbname": "test",
                "username": "user",
                "password": "pass"
            }
        }

        secret_configs = {
            "source-connection-id": {},
            "dest-connection-id": {}
        }

        self.write_config_files(
            temp_config_dir,
            valid_pipeline_config,
            connection_configs=connection_configs,
            secret_configs=secret_configs
        )

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline-123"
        )
        prep = PipelineConfigPrep(settings)

        # Load pipeline (this resolves connections)
        pipeline, _ = prep.load_pipeline_config()

        # Check resolved connections (now keyed by "id:connection_id")
        src_resolved = prep._resolved_connections.get("id:source-connection-id")
        assert src_resolved is not None
        assert src_resolved.connection_type == "api"

        dst_resolved = prep._resolved_connections.get("id:dest-connection-id")
        assert dst_resolved is not None
        assert dst_resolved.connection_type == "database"
        # Check database normalization (provider -> driver)
        assert dst_resolved.config.get("driver") == "postgresql"
        assert dst_resolved.config.get("database") == "test"
        assert dst_resolved.config.get("username") == "user"


class TestEnvironmentVariableExpansion:
    """Test environment variable expansion in configurations."""

    def test_env_var_expansion_in_connection(self, temp_config_dir, mock_analitiq_config):
        """Test that environment variables are expanded in connections."""
        os.environ["TEST_API_TOKEN"] = "expanded-token-123"

        try:
            pipeline_config = {
                "pipeline_id": "test-pipeline",
                "name": "Test",
                "connections": {
                    "source": {
                        "conn": "test-api-connection"
                    },
                    "destinations": []
                }
            }

            connection_config = {
                "connection_id": "test-api-connection",
                "type": "api",
                "host": "https://api.example.com",
                "headers": {"Authorization": "Bearer ${token}"}
            }

            secret_config = {"token": "${TEST_API_TOKEN}"}

            # Write pipeline config
            pipeline_path = Path(temp_config_dir, "pipelines", "test-pipeline.json")
            with open(pipeline_path, "w") as f:
                json.dump(pipeline_config, f)

            # Write connection config
            connection_path = Path(temp_config_dir, "connections", "test-api-connection.json")
            with open(connection_path, "w") as f:
                json.dump(connection_config, f)

            # Write secret config
            with open(Path(temp_config_dir, "secrets", "test-api-connection.json"), "w") as f:
                json.dump(secret_config, f)

            settings = PipelineConfigPrepSettings(
                env="local",
                pipeline_id="test-pipeline"
            )
            prep = PipelineConfigPrep(settings)

            pipeline, _ = prep.load_pipeline_config()

            resolved = prep._resolved_connections.get("id:test-api-connection")
            assert resolved is not None
            assert resolved.config["headers"]["Authorization"] == "Bearer expanded-token-123"

        finally:
            os.environ.pop("TEST_API_TOKEN", None)

    def test_unexpanded_placeholder_warning(self, temp_config_dir, mock_analitiq_config):
        """Test that unexpanded placeholders raise ValueError when create_config validates."""
        pipeline_config = {
            "pipeline_id": "test-pipeline",
            "name": "Test",
            "connections": {
                "source": {
                    "conn": "test-api-connection"
                },
                "destinations": []
            }
        }

        connection_config = {
            "connection_id": "test-api-connection",
            "type": "api",
            "host": "https://api.example.com",
            "headers": {"Authorization": "Bearer ${token}"}
        }

        # Secret with placeholder that won't be expanded
        secret_config = {"token": "${MISSING_VAR}"}

        # Write pipeline config
        pipeline_path = Path(temp_config_dir, "pipelines", "test-pipeline.json")
        with open(pipeline_path, "w") as f:
            json.dump(pipeline_config, f)

        # Write connection config
        connection_path = Path(temp_config_dir, "connections", "test-api-connection.json")
        with open(connection_path, "w") as f:
            json.dump(connection_config, f)

        # Write secret config
        with open(Path(temp_config_dir, "secrets", "test-api-connection.json"), "w") as f:
            json.dump(secret_config, f)

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )
        prep = PipelineConfigPrep(settings)

        # Ensure var is not set
        os.environ.pop("MISSING_VAR", None)

        # This should raise ValueError due to unexpanded placeholder validation
        with pytest.raises(ValueError) as exc_info:
            prep.create_config()

        assert "unexpanded placeholder" in str(exc_info.value).lower()


class TestKeyNormalization:
    """Test that create_config returns clean keys without internal prefixes."""

    @staticmethod
    def write_minimal_config(temp_config_dir):
        """Write minimal valid configuration for testing."""
        pipeline_config = {
            "version": 1,
            "client_id": "client-123",
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "is_active": True,
            "connections": {
                "source": {"conn_src": "src-conn-uuid"},
                "destinations": [{"conn_dst": "dst-conn-uuid"}]
            },
            "streams": [{"stream_id": "stream-uuid", "version": 1}],
        }

        # Use path-based endpoints that reference connectors directory
        stream_config = {
            "version": 1,
            "stream_id": "stream-uuid",
            "pipeline_id": "test-pipeline",
            "client_id": "client-123",
            "is_enabled": True,
            "source": {
                "connection_ref": "conn_src",
                "endpoint": "source_api/endpoints/transfers.json",
                "primary_key": ["id"],
                "replication": {"method": "incremental", "cursor_field": ["created"]}
            },
            "destinations": [{
                "connection_ref": "conn_dst",
                "endpoint": "dest_api/endpoints/records.json",
                "write": {"mode": "upsert"}
            }]
        }

        src_connection = {
            "connection_id": "src-conn-uuid",
            "connector_type": "api",
            "host": "https://api.source.com",
            "headers": {"Authorization": "Bearer test"}
        }

        dst_connection = {
            "connection_id": "dst-conn-uuid",
            "connector_type": "api",
            "host": "https://api.dest.com",
            "headers": {}
        }

        src_endpoint = {
            "endpoint_id": "transfers",
            "endpoint": "/v1/transfers",
            "method": "GET"
        }

        dst_endpoint = {
            "endpoint_id": "records",
            "endpoint": "/v1/records",
            "method": "POST"
        }

        # Write all files
        with open(Path(temp_config_dir, "pipelines", "test-pipeline.json"), "w") as f:
            json.dump(pipeline_config, f)
        with open(Path(temp_config_dir, "streams", "stream-uuid.json"), "w") as f:
            json.dump(stream_config, f)
        with open(Path(temp_config_dir, "connections", "src-conn-uuid.json"), "w") as f:
            json.dump(src_connection, f)
        with open(Path(temp_config_dir, "connections", "dst-conn-uuid.json"), "w") as f:
            json.dump(dst_connection, f)
        with open(Path(temp_config_dir, "secrets", "src-conn-uuid.json"), "w") as f:
            json.dump({}, f)
        with open(Path(temp_config_dir, "secrets", "dst-conn-uuid.json"), "w") as f:
            json.dump({}, f)
        # Write endpoints to connectors directory (path-based endpoint resolution)
        with open(Path(temp_config_dir, "connectors", "source_api", "endpoints", "transfers.json"), "w") as f:
            json.dump(src_endpoint, f)
        with open(Path(temp_config_dir, "connectors", "dest_api", "endpoints", "records.json"), "w") as f:
            json.dump(dst_endpoint, f)

    def test_create_config_strips_id_prefix_from_connections(self, temp_config_dir, mock_analitiq_config):
        """Verify connection keys are plain UUIDs, not prefixed."""
        self.write_minimal_config(temp_config_dir)

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )
        prep = PipelineConfigPrep(settings)
        _, _, resolved_connections, _ = prep.create_config()

        # Keys should NOT have "id:" prefix
        for key in resolved_connections.keys():
            assert not key.startswith("id:"), f"Key should not have 'id:' prefix: {key}"
            assert ":" not in key, f"Key should not contain colon: {key}"

        # Should be able to look up by plain UUID
        assert "src-conn-uuid" in resolved_connections
        assert "dst-conn-uuid" in resolved_connections

    def test_create_config_strips_path_prefix_from_endpoints(self, temp_config_dir, mock_analitiq_config):
        """Verify endpoint keys are plain paths, not prefixed."""
        self.write_minimal_config(temp_config_dir)

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )
        prep = PipelineConfigPrep(settings)
        _, _, _, resolved_endpoints = prep.create_config()

        # Keys should NOT have "path:" prefix
        for key in resolved_endpoints.keys():
            assert not key.startswith("path:"), f"Key should not have 'path:' prefix: {key}"

    def test_internal_cache_uses_prefixed_keys(self, temp_config_dir, mock_analitiq_config):
        """Verify internal caches still use prefixed keys."""
        self.write_minimal_config(temp_config_dir)

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )
        prep = PipelineConfigPrep(settings)
        prep.load_pipeline_config()

        # Internal cache should use "id:" prefix
        assert any(k.startswith("id:") for k in prep._resolved_connections.keys()), (
            "Internal cache should use 'id:' prefix"
        )

    def test_resolved_connection_validates_against_connector_model(self, temp_config_dir, mock_analitiq_config):
        """Verify resolved connection config can be validated by connector models."""
        from src.models.api import APIConnectionConfig

        self.write_minimal_config(temp_config_dir)

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline"
        )
        prep = PipelineConfigPrep(settings)
        _, _, resolved_connections, _ = prep.create_config()

        # Get source connection config
        src_conn = resolved_connections["src-conn-uuid"]

        # Should validate against APIConnectionConfig
        validated = APIConnectionConfig(**src_conn.config)
        assert validated.host == "https://api.source.com"
