"""Tests for PipelineConfigPrep module - Local configuration loading and processing."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from typing import Dict, Any

import pytest

from analitiq_stream.core.pipeline_config_prep import PipelineConfigPrep, PipelineConfigPrepSettings
from analitiq_stream.models.state import PipelineConfig


@pytest.fixture
def temp_config_dir():
    """Create temporary directory with config structure."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create subdirectories
        Path(temp_dir, "pipelines").mkdir()
        Path(temp_dir, "hosts").mkdir()
        Path(temp_dir, "endpoints").mkdir()
        yield temp_dir


@pytest.fixture
def sample_host_configs():
    """Sample host configurations for testing."""
    return {
        "0e8b1731-479a-4bc0-b056-244cc5d6a53c": {  # Wise API host
            "base_url": "https://api.sandbox.transferwise.tech",
            "headers": {
                "Authorization": "Bearer ${WISE_API_TOKEN}",
                "Content-Type": "application/json"
            },
            "rate_limit": {
                "max_requests": 100,
                "time_window": 60
            }
        },
        "7c1a69eb-239f-45d4-b6c2-3ad4c6e89cfa": {  # SevDesk API host
            "base_url": "https://my.sevdesk.de",
            "headers": {
                "Authorization": "${SEVDESK_API_TOKEN}",
                "Content-Type": "application/json"
            },
            "rate_limit": {
                "max_requests": 10,
                "time_window": 60
            }
        },
        "db-host-123": {  # Database host
            "driver": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "analytics",
            "user": "postgres",
            "password": "${DB_PASSWORD}",
            "ssl_mode": "prefer",
            "connection_pool": {
                "min_connections": 2,
                "max_connections": 10
            }
        }
    }


@pytest.fixture
def sample_endpoint_configs():
    """Sample endpoint configurations for testing."""
    return {
        "5a4b9e21-441f-4bc7-9d5e-41917b4357e6": {  # Wise source endpoint
            "endpoint": "/v1/profiles/{profile_id}/transfers",
            "method": "GET",
            "response_schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "created": {"type": "string", "format": "date-time"},
                        "targetValue": {"type": "number"}
                    }
                }
            }
        },
        "1e63d782-4b67-4b7e-b845-4b4de5e4f46e": {  # SevDesk destination endpoint
            "endpoint": "/api/v1/CheckAccountTransaction",
            "method": "POST",
            "response_schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "objects": {"type": "array"}
                }
            }
        },
        "db-endpoint-123": {  # Database endpoint
            "schema": "wise_data",
            "table": "transactions",
            "primary_key": ["wise_id"],
            "write_mode": "upsert",
            "configure": {
                "auto_create_schema": True,
                "auto_create_table": True
            },
            "table_schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "wise_id": {"type": "integer", "database_type": "BIGINT"},
                    "created_at": {"type": "string", "format": "date-time", "database_type": "TIMESTAMPTZ"}
                }
            }
        }
    }


def write_config_files(temp_dir: str, pipeline_config: Dict[str, Any],
                      host_configs: Dict[str, Dict[str, Any]],
                      endpoint_configs: Dict[str, Dict[str, Any]]):
    """Write configuration files to temporary directory."""
    # Write pipeline config
    pipeline_path = Path(temp_dir, "pipelines", f"{pipeline_config['pipeline_id']}.json")
    with open(pipeline_path, "w") as f:
        json.dump(pipeline_config, f)

    # Write host configs
    for host_id, config in host_configs.items():
        host_path = Path(temp_dir, "hosts", f"{host_id}.json")
        with open(host_path, "w") as f:
            json.dump(config, f)

    # Write endpoint configs
    for endpoint_id, config in endpoint_configs.items():
        endpoint_path = Path(temp_dir, "endpoints", f"{endpoint_id}.json")
        with open(endpoint_path, "w") as f:
            json.dump(config, f)


class TestPipelineConfigPrepSettings:
    """Test PipelineConfigPrepSettings validation."""

    def test_default_settings(self):
        """Test default settings creation."""
        settings = PipelineConfigPrepSettings(pipeline_id="test-pipeline")

        assert settings.env == "local"
        assert settings.pipeline_id == "test-pipeline"
        assert settings.aws_region == "eu-central-1"
        assert settings.s3_config_bucket is None
        assert settings.local_config_mount is None  # Default is None, validator sets it when needed
        assert settings.use_secrets_manager is False

    def test_local_mount_explicit_config(self):
        """Test local mount with explicit configuration."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount="/custom/config"
        )
        assert settings.local_config_mount == "/custom/config"


class TestPipelineConfigPrep:
    """Test PipelineConfigPrep class."""

    def test_init_local_mode(self):
        """Test initialization in local mode."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount="/test/config"
        )

        prep = PipelineConfigPrep(settings)

        assert prep.settings == settings
        assert prep.is_local_mode is True
        assert prep.credentials_manager is not None


    @patch.dict(os.environ, {
        "ENV": "local",
        "PIPELINE_ID": "env-pipeline",
        "LOCAL_CONFIG_MOUNT": "/custom/mount"
    })
    def test_load_settings_from_env(self):
        """Test loading settings from environment variables."""
        settings = PipelineConfigPrep._load_settings_from_env()

        assert settings.env == "local"
        assert settings.pipeline_id == "env-pipeline"
        assert settings.local_config_mount == "/custom/mount"


    def test_load_local_json_success(self, temp_config_dir):
        """Test successful local JSON loading."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Create test file
        test_config = {"test": "value", "nested": {"key": "value"}}
        test_file = Path(temp_config_dir, "test.json")
        with open(test_file, "w") as f:
            json.dump(test_config, f)

        result = prep._load_local_json(str(test_file))
        assert result == test_config

    def test_load_local_json_file_not_found(self, temp_config_dir):
        """Test local JSON loading with missing file."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            prep._load_local_json("/nonexistent/file.json")

    def test_load_local_json_invalid_json(self, temp_config_dir):
        """Test local JSON loading with invalid JSON."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Create invalid JSON file
        test_file = Path(temp_config_dir, "invalid.json")
        with open(test_file, "w") as f:
            f.write("{ invalid json")

        with pytest.raises(ValueError, match="Invalid JSON"):
            prep._load_local_json(str(test_file))

    @patch.dict(os.environ, {"TEST_VAR": "expanded_value"})
    def test_load_local_json_with_env_expansion(self, temp_config_dir):
        """Test local JSON loading with environment variable expansion."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Create config with env vars
        test_config = {"password": "${TEST_VAR}", "other": "normal"}
        test_file = Path(temp_config_dir, "test.json")
        with open(test_file, "w") as f:
            json.dump(test_config, f)

        result = prep._load_local_json(str(test_file), expand_env_vars=True)
        assert result == {"password": "expanded_value", "other": "normal"}


    def test_load_pipeline_config_local(self, temp_config_dir, sample_pipeline_config):
        """Test loading pipeline config from local filesystem."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Write pipeline config file
        pipeline_path = Path(temp_config_dir, "pipelines", "a1b2c3d4-e5f6-7890-abcd-ef1234567890.json")
        with open(pipeline_path, "w") as f:
            json.dump(sample_pipeline_config, f)

        result = prep.load_pipeline_config()

        assert isinstance(result, PipelineConfig)
        assert result.pipeline_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert result.name == "Wise Multi-Stream to SevDesk Integration"

    def test_load_host_config_local(self, temp_config_dir, sample_host_configs):
        """Test loading host config from local filesystem."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        host_id = "0e8b1731-479a-4bc0-b056-244cc5d6a53c"
        host_config = sample_host_configs[host_id]

        # Write host config file
        host_path = Path(temp_config_dir, "hosts", f"{host_id}.json")
        with open(host_path, "w") as f:
            json.dump(host_config, f)

        result = prep.load_host_config(host_id)
        assert result == host_config

    def test_load_endpoint_config_local(self, temp_config_dir, sample_endpoint_configs):
        """Test loading endpoint config from local filesystem."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        endpoint_id = "5a4b9e21-441f-4bc7-9d5e-41917b4357e6"
        endpoint_config = sample_endpoint_configs[endpoint_id]

        # Write endpoint config file
        endpoint_path = Path(temp_config_dir, "endpoints", f"{endpoint_id}.json")
        with open(endpoint_path, "w") as f:
            json.dump(endpoint_config, f)

        result = prep.load_endpoint_config(endpoint_id)
        assert result == endpoint_config

    def test_merge_config_success(self, temp_config_dir, sample_pipeline_config,
                                 sample_host_configs, sample_endpoint_configs):
        """Test successful configuration merging."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Write all config files
        write_config_files(temp_config_dir, sample_pipeline_config,
                          sample_host_configs, sample_endpoint_configs)

        # Load pipeline config
        pipeline_config = prep.load_pipeline_config()

        # Test source config merging
        src_config = prep.merge_config(pipeline_config, "src")

        # Should contain endpoint config
        assert "endpoint" in src_config
        assert src_config["endpoint"] == "/v1/profiles/{profile_id}/transfers"

        # Should contain host config (takes precedence)
        assert "base_url" in src_config
        assert src_config["base_url"] == "https://api.sandbox.transferwise.tech"

    def test_merge_config_missing_host_id(self, temp_config_dir, sample_pipeline_config):
        """Test merge config with missing host_id."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Create pipeline config without host_id
        invalid_config = sample_pipeline_config.copy()
        del invalid_config["src"]["host_id"]

        pipeline_path = Path(temp_config_dir, "pipelines", "test-pipeline.json")
        with open(pipeline_path, "w") as f:
            json.dump(invalid_config, f)

        pipeline_config = prep.load_pipeline_config()

        with pytest.raises(ValueError, match="must contain 'host_id'"):
            prep.merge_config(pipeline_config, "src")

    def test_merge_config_no_streams(self, temp_config_dir, sample_pipeline_config):
        """Test merge config with no streams."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Create pipeline config without streams
        invalid_config = sample_pipeline_config.copy()
        invalid_config["streams"] = {}

        pipeline_path = Path(temp_config_dir, "pipelines", "test-pipeline.json")
        with open(pipeline_path, "w") as f:
            json.dump(invalid_config, f)

        pipeline_config = prep.load_pipeline_config()

        with pytest.raises(ValueError, match="No streams configured"):
            prep.merge_config(pipeline_config, "src")


    def test_validate_environment_local_success(self, temp_config_dir):
        """Test successful local environment validation."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Should not raise any exception
        prep.validate_environment()

    def test_validate_environment_local_missing_mount(self):
        """Test local environment validation with missing mount point."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount="/nonexistent/path"
        )
        prep = PipelineConfigPrep(settings)

        with pytest.raises(RuntimeError, match="Local config mount point does not exist"):
            prep.validate_environment()

    def test_validate_environment_missing_pipeline_id(self, temp_config_dir):
        """Test environment validation with missing pipeline ID."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        with pytest.raises(RuntimeError, match="PIPELINE_ID environment variable is required"):
            prep.validate_environment()


    def test_get_pipeline_info_success(self, temp_config_dir, sample_pipeline_config):
        """Test getting pipeline info successfully."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Write pipeline config
        pipeline_path = Path(temp_config_dir, "pipelines", "a1b2c3d4-e5f6-7890-abcd-ef1234567890.json")
        with open(pipeline_path, "w") as f:
            json.dump(sample_pipeline_config, f)

        info = prep.get_pipeline_info()

        assert info["pipeline_id"] == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert info["name"] == "Wise Multi-Stream to SevDesk Integration"
        assert info["version"] == "1.0"
        assert info["stream_count"] == 1
        assert info["environment"] == "local"
        assert info["config_source"] == "local"

    def test_get_pipeline_info_error(self, temp_config_dir):
        """Test getting pipeline info with error."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="nonexistent-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        info = prep.get_pipeline_info()

        assert info["pipeline_id"] == "nonexistent-pipeline"
        assert info["environment"] == "local"
        assert "error" in info

    def test_path_generation_local(self):
        """Test path generation for local mode."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount="/test/config"
        )
        prep = PipelineConfigPrep(settings)

        assert prep._get_pipeline_config_path() == "/test/config/pipelines/test-pipeline.json"
        assert prep._get_host_config_path("host-123") == "/test/config/hosts/host-123.json"
        assert prep._get_endpoint_config_path("ep-456") == "/test/config/endpoints/ep-456.json"
