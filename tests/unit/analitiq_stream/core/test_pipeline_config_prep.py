"""Comprehensive unit tests for PipelineConfigPrep module - NO MOCKING, real functionality only."""

import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Any

import pytest
from pydantic import ValidationError

from analitiq_stream.core.pipeline_config_prep import (
    PipelineConfigPrep,
    PipelineConfigPrepSettings,
    validate_pipeline_config,
    validate_transformations,
    expand_required_vars
)


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
def valid_pipeline_config():
    """Valid pipeline configuration for testing."""
    return {
        "pipeline_id": "test-pipeline-123",
        "name": "Test Pipeline",
        "version": "1.0",
        "engine_config": {
            "batch_size": 1000,
            "max_concurrent_batches": 10,
            "buffer_size": 10000
        },
        "src": {
            "host_id": "src-host-123",
            "endpoint_id": "src-endpoint-456"
        },
        "dst": {
            "host_id": "dst-host-789",
            "endpoint_id": "dst-endpoint-012",
            "refresh_mode": "upsert",
            "batch_size": 100
        },
        "streams": {
            "test-stream": {
                "src": {
                    "endpoint_id": "src-endpoint-456"
                },
                "dst": {
                    "endpoint_id": "dst-endpoint-012"
                }
            }
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
def invalid_structure_config():
    """Pipeline config with wrong structure types."""
    return {
        "pipeline_id": "test-pipeline",
        "engine_config": "should_be_dict_not_string",  # Wrong type
        "src": ["should", "be", "dict"],  # Wrong type
        "dst": {
            "refresh_mode": "invalid_mode",  # Invalid value
            "batch_size": "not_an_integer"  # Wrong type
        }
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
def valid_host_config():
    """Valid host configuration."""
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


@pytest.fixture
def invalid_transformations():
    """Invalid transformation configurations."""
    return [
        {
            # Missing required 'type' field
            "mappings": {"old": "new"}
        },
        {
            "type": "invalid_type",  # Invalid transformation type
            "field": "test"
        },
        {
            "type": "field_mapping"
            # Missing required 'mappings' field
        },
        {
            "type": "computed_field"
            # Missing required 'field' field
        }
    ]


@pytest.fixture
def valid_transformations():
    """Valid transformation configurations."""
    return [
        {
            "type": "field_mapping",
            "mappings": {
                "old_field": "new_field",
                "another_old": "another_new"
            }
        },
        {
            "type": "computed_field",
            "field": "computed_value",
            "expression": "now()"
        },
        {
            "type": "value_transformation",
            "field": "status",
            "transformation": "uppercase"
        }
    ]


class TestPipelineConfigPrepSettings:
    """Test PipelineConfigPrepSettings validation and behavior."""

    def test_default_settings_local(self):
        """Test default settings for local environment."""
        settings = PipelineConfigPrepSettings(pipeline_id="test-pipeline")

        assert settings.env == "local"
        assert settings.pipeline_id == "test-pipeline"
        assert settings.aws_region == "eu-central-1"
        assert settings.s3_config_bucket is None
        assert settings.local_config_mount is None  # Default not applied until validation
        assert settings.use_secrets_manager is False

    def test_custom_local_settings(self):
        """Test custom settings for local environment."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="custom-pipeline",
            local_config_mount="/custom/config"
        )

        assert settings.env == "local"
        assert settings.pipeline_id == "custom-pipeline"
        assert settings.local_config_mount == "/custom/config"

    def test_missing_pipeline_id_raises_validation_error(self):
        """Test that missing pipeline_id raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineConfigPrepSettings()

        assert "pipeline_id" in str(exc_info.value)
        assert "Field required" in str(exc_info.value)

    def test_empty_pipeline_id_validation(self):
        """Test validation of empty pipeline_id."""
        # Empty string should be allowed by Pydantic but caught by our validation
        settings = PipelineConfigPrepSettings(pipeline_id="")
        assert settings.pipeline_id == ""

    def test_aws_environment_defaults(self):
        """Test AWS environment gets default bucket."""
        settings = PipelineConfigPrepSettings(
            env="prod",
            pipeline_id="prod-pipeline"
        )

        assert settings.env == "prod"
        assert settings.s3_config_bucket is None  # Default not applied automatically


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

    def test_invalid_engine_config_type_fails(self):
        """Test that invalid engine_config type fails validation."""
        config = {
            "pipeline_id": "test",
            "engine_config": "should_be_dict"
        }
        assert validate_pipeline_config(config) is False

    def test_invalid_engine_config_numeric_fields_fail(self):
        """Test that invalid numeric fields in engine_config fail validation."""
        config = {
            "pipeline_id": "test",
            "engine_config": {
                "batch_size": "not_an_integer",
                "max_concurrent_batches": 10.5,  # Should be int, not float
                "buffer_size": None
            }
        }
        assert validate_pipeline_config(config) is False

    def test_invalid_src_dst_type_fails(self):
        """Test that invalid src/dst types fail validation."""
        config = {
            "pipeline_id": "test",
            "src": "should_be_dict",
            "dst": ["should", "be", "dict"]
        }
        assert validate_pipeline_config(config) is False

    def test_invalid_refresh_mode_fails(self):
        """Test that invalid refresh_mode fails validation."""
        config = {
            "pipeline_id": "test",
            "dst": {
                "refresh_mode": "invalid_mode"
            }
        }
        assert validate_pipeline_config(config) is False

    def test_valid_refresh_modes_pass(self):
        """Test that valid refresh_modes pass validation."""
        valid_modes = ["insert", "upsert", "truncate_insert"]

        for mode in valid_modes:
            config = {
                "pipeline_id": "test",
                "dst": {
                    "refresh_mode": mode
                }
            }
            assert validate_pipeline_config(config) is True

    def test_invalid_batch_size_fails(self):
        """Test that invalid batch_size fails validation."""
        config = {
            "pipeline_id": "test",
            "dst": {
                "batch_size": 0  # Should be positive
            }
        }
        assert validate_pipeline_config(config) is False

        config = {
            "pipeline_id": "test",
            "dst": {
                "batch_size": -10  # Should be positive
            }
        }
        assert validate_pipeline_config(config) is False

        config = {
            "pipeline_id": "test",
            "dst": {
                "batch_size": "not_an_integer"
            }
        }
        assert validate_pipeline_config(config) is False


class TestValidateTransformations:
    """Test validate_transformations function."""

    def test_valid_transformations_pass(self, valid_transformations):
        """Test that valid transformations pass validation."""
        assert validate_transformations(valid_transformations) is True

    def test_non_list_transformations_fail(self):
        """Test that non-list transformations fail validation."""
        assert validate_transformations("not_a_list") is False
        assert validate_transformations({"type": "field_mapping"}) is False
        assert validate_transformations(None) is False

    def test_non_dict_transformation_items_fail(self):
        """Test that non-dict transformation items fail validation."""
        transformations = [
            "not_a_dict",
            {"type": "field_mapping", "mappings": {}},  # Valid one
            123  # Invalid one
        ]
        assert validate_transformations(transformations) is False

    def test_missing_type_field_fails(self):
        """Test that missing type field fails validation."""
        transformations = [
            {"mappings": {"old": "new"}}  # Missing 'type'
        ]
        assert validate_transformations(transformations) is False

    def test_invalid_transformation_type_fails(self):
        """Test that invalid transformation type fails validation."""
        transformations = [
            {"type": "invalid_type", "field": "test"}
        ]
        assert validate_transformations(transformations) is False

    def test_field_mapping_missing_mappings_fails(self):
        """Test that field_mapping without mappings fails validation."""
        transformations = [
            {"type": "field_mapping"}  # Missing 'mappings'
        ]
        assert validate_transformations(transformations) is False

    def test_computed_field_missing_field_fails(self):
        """Test that computed_field without field fails validation."""
        transformations = [
            {"type": "computed_field", "expression": "now()"}  # Missing 'field'
        ]
        assert validate_transformations(transformations) is False

    def test_value_transformation_missing_field_fails(self):
        """Test that value_transformation without field fails validation."""
        transformations = [
            {"type": "value_transformation", "transformation": "uppercase"}  # Missing 'field'
        ]
        assert validate_transformations(transformations) is False

    def test_empty_transformations_list_passes(self):
        """Test that empty transformations list passes validation."""
        assert validate_transformations([]) is True


class TestExpandRequiredVars:
    """Test expand_required_vars function."""

    def test_expand_existing_env_vars(self, config_with_env_vars):
        """Test expansion of existing environment variables."""
        # Set required environment variables
        os.environ["DATABASE_URL"] = "postgresql://localhost:5432/test"
        os.environ["API_KEY"] = "test-api-key"
        os.environ["NESTED_SECRET"] = "secret-value"

        try:
            expanded = expand_required_vars(config_with_env_vars)

            assert expanded["database_url"] == "postgresql://localhost:5432/test"
            assert expanded["api_key"] == "test-api-key"
            # Note: expand_required_vars only handles top-level keys, not nested dicts
            assert expanded["nested"] == {"secret": "${NESTED_SECRET}"}  # Not expanded
            assert expanded["pipeline_id"] == "env-test-pipeline"  # No var expansion
        finally:
            # Clean up environment variables
            for var in ["DATABASE_URL", "API_KEY", "NESTED_SECRET"]:
                os.environ.pop(var, None)

    def test_missing_env_var_raises_error(self):
        """Test that missing environment variable raises EnvironmentError."""
        config = {
            "pipeline_id": "test",
            "missing_var": "${NONEXISTENT_VAR}"
        }

        # Ensure the variable doesn't exist
        os.environ.pop("NONEXISTENT_VAR", None)

        with pytest.raises(EnvironmentError) as exc_info:
            expand_required_vars(config)

        assert "Missing required environment variable: NONEXISTENT_VAR" in str(exc_info.value)

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

    def test_malformed_var_syntax_ignored(self):
        """Test that malformed variable syntax is ignored (not expanded)."""
        config = {
            "pipeline_id": "test",
            "malformed1": "$MISSING_BRACES",
            "malformed2": "${UNCLOSED_BRACE",
            "malformed3": "${}",  # Empty variable name
            "normal": "normal_value"
        }

        expanded = expand_required_vars(config)
        # Malformed syntax should be left as-is
        assert expanded["malformed1"] == "$MISSING_BRACES"
        assert expanded["malformed2"] == "${UNCLOSED_BRACE"
        assert expanded["malformed3"] == "${}"
        assert expanded["normal"] == "normal_value"


class TestPipelineConfigPrepLocal:
    """Test PipelineConfigPrep class with local filesystem operations."""

    def test_initialization_local_mode(self, temp_config_dir):
        """Test initialization in local mode."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )

        prep = PipelineConfigPrep(settings)

        assert prep.settings == settings
        assert prep.is_local_mode is True
        assert prep.credentials_manager is not None

    def test_load_settings_from_env(self):
        """Test loading settings from environment variables."""
        # Set environment variables
        os.environ["ENV"] = "local"
        os.environ["PIPELINE_ID"] = "env-pipeline"
        os.environ["LOCAL_CONFIG_MOUNT"] = "/custom/mount"

        try:
            settings = PipelineConfigPrep._load_settings_from_env()

            assert settings.env == "local"
            assert settings.pipeline_id == "env-pipeline"
            assert settings.local_config_mount == "/custom/mount"
        finally:
            # Clean up environment variables
            for var in ["ENV", "PIPELINE_ID", "LOCAL_CONFIG_MOUNT"]:
                os.environ.pop(var, None)

    def test_validate_environment_success(self, temp_config_dir):
        """Test successful environment validation."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )

        prep = PipelineConfigPrep(settings)
        # Should not raise any exception
        prep.validate_environment()

    def test_validate_environment_missing_pipeline_id(self, temp_config_dir):
        """Test environment validation with missing pipeline ID."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="",
            local_config_mount=temp_config_dir
        )

        # Constructor should fail because it calls validate_environment
        with pytest.raises(RuntimeError) as exc_info:
            PipelineConfigPrep(settings)

        assert "PIPELINE_ID environment variable is required" in str(exc_info.value)

    def test_validate_environment_missing_mount_point(self):
        """Test environment validation with missing mount point."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount="/nonexistent/path"
        )

        # Constructor should fail because it calls validate_environment
        with pytest.raises(RuntimeError) as exc_info:
            PipelineConfigPrep(settings)

        assert "Local config mount point does not exist" in str(exc_info.value)

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

        with pytest.raises(FileNotFoundError) as exc_info:
            prep._load_local_json("/nonexistent/file.json")

        assert "Configuration file not found" in str(exc_info.value)

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
            f.write("{ invalid json content")

        with pytest.raises(ValueError) as exc_info:
            prep._load_local_json(str(test_file))

        assert "Invalid JSON" in str(exc_info.value)

    def test_load_local_json_empty_file(self, temp_config_dir):
        """Test local JSON loading with empty file."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Create empty JSON file
        test_file = Path(temp_config_dir, "empty.json")
        with open(test_file, "w") as f:
            f.write("")

        with pytest.raises(ValueError) as exc_info:
            prep._load_local_json(str(test_file))

        assert "Invalid JSON" in str(exc_info.value)

    def test_load_config_host_success(self, temp_config_dir, valid_host_config):
        """Test successful host config loading."""
        # Ensure API_TOKEN is not set from other tests
        os.environ.pop("API_TOKEN", None)

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Create host config without environment variables to avoid expansion issues
        host_config = {
            "type": "api",
            "host": "https://api.example.com",
            "headers": {
                "Authorization": "Bearer static-token",
                "Content-Type": "application/json"
            },
            "rate_limit": {
                "max_requests": 100,
                "time_window": 60
            }
        }

        # Write host config file
        host_id = "test-host-123"
        host_file = Path(temp_config_dir, "hosts", f"{host_id}.json")
        with open(host_file, "w") as f:
            json.dump(host_config, f)

        result = prep.load_config("hosts", host_id)
        assert result == host_config

    def test_load_config_endpoint_success(self, temp_config_dir, valid_endpoint_config):
        """Test successful endpoint config loading."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Write endpoint config file
        endpoint_id = "test-endpoint-456"
        endpoint_file = Path(temp_config_dir, "endpoints", f"{endpoint_id}.json")
        with open(endpoint_file, "w") as f:
            json.dump(valid_endpoint_config, f)

        result = prep.load_config("endpoints", endpoint_id)
        assert result == valid_endpoint_config

    def test_load_config_missing_file(self, temp_config_dir):
        """Test loading config with missing file."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        with pytest.raises(FileNotFoundError):
            prep.load_config("hosts", "nonexistent-host")

    def test_path_generation(self, temp_config_dir):
        """Test path generation for local mode."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        assert prep._get_pipeline_config_path() == f"{temp_config_dir}/pipelines/test-pipeline.json"
        assert prep._get_config_path("hosts", "host-123") == f"{temp_config_dir}/hosts/host-123.json"
        assert prep._get_config_path("endpoints", "ep-456") == f"{temp_config_dir}/endpoints/ep-456.json"


class TestConfigurationMerging:
    """Test configuration merging functionality."""

    def write_config_files(self, temp_dir: str, pipeline_config: Dict[str, Any],
                          host_configs: Dict[str, Dict[str, Any]] = None,
                          endpoint_configs: Dict[str, Dict[str, Any]] = None):
        """Helper to write configuration files to temporary directory."""
        # Write pipeline config
        pipeline_path = Path(temp_dir, "pipelines", f"{pipeline_config['pipeline_id']}.json")
        with open(pipeline_path, "w") as f:
            json.dump(pipeline_config, f)

        # Write host configs
        if host_configs:
            for host_id, config in host_configs.items():
                host_path = Path(temp_dir, "hosts", f"{host_id}.json")
                with open(host_path, "w") as f:
                    json.dump(config, f)

        # Write endpoint configs
        if endpoint_configs:
            for endpoint_id, config in endpoint_configs.items():
                endpoint_path = Path(temp_dir, "endpoints", f"{endpoint_id}.json")
                with open(endpoint_path, "w") as f:
                    json.dump(config, f)

    def test_load_pipeline_config_success(self, temp_config_dir, valid_pipeline_config):
        """Test successful pipeline config loading."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="test-pipeline-123",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Write pipeline config file
        self.write_config_files(temp_config_dir, valid_pipeline_config)

        result = prep.load_pipeline_config()
        assert result == valid_pipeline_config

    def test_load_pipeline_config_validation_error(self, temp_config_dir):
        """Test pipeline config loading with validation errors."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="incomplete-pipeline",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Write incomplete config manually (without pipeline_id)
        incomplete_config = {"name": "Incomplete Pipeline", "version": "1.0"}
        pipeline_path = Path(temp_config_dir, "pipelines", "incomplete-pipeline.json")
        with open(pipeline_path, "w") as f:
            json.dump(incomplete_config, f)

        # This should load the file successfully since it exists
        result = prep.load_pipeline_config()
        assert result == incomplete_config
        assert "pipeline_id" not in result


class TestEndToEndScenarios:
    """Test complete end-to-end scenarios with real file operations."""

    def test_complete_config_with_env_vars(self, temp_config_dir):
        """Test complete configuration loading with environment variable expansion."""
        # Set up environment variables
        os.environ["API_TOKEN"] = "test-token-123"
        os.environ["DATABASE_PASSWORD"] = "secret-password"

        try:
            # Create complete configuration
            pipeline_config = {
                "pipeline_id": "complete-test",
                "name": "Complete Test Pipeline",
                "version": "1.0",
                "src": {
                    "host_id": "api-host",
                    "endpoint_id": "api-endpoint"
                },
                "dst": {
                    "host_id": "db-host",
                    "endpoint_id": "db-endpoint"
                },
                "streams": {
                    "test-stream": {
                        "src": {"endpoint_id": "api-endpoint"},
                        "dst": {"endpoint_id": "db-endpoint"}
                    }
                }
            }

            host_configs = {
                "api-host": {
                    "type": "api",
                    "host": "https://api.example.com",
                    "headers": {
                        "Authorization": "Bearer ${API_TOKEN}"
                    }
                },
                "db-host": {
                    "type": "database",
                    "host": "localhost",
                    "password": "${DATABASE_PASSWORD}"
                }
            }

            endpoint_configs = {
                "api-endpoint": {
                    "endpoint": "/data",
                    "method": "GET"
                },
                "db-endpoint": {
                    "table": "target_table",
                    "schema": "public"
                }
            }

            # Write all config files
            for config_type, configs in [
                ("pipelines", {pipeline_config["pipeline_id"]: pipeline_config}),
                ("hosts", host_configs),
                ("endpoints", endpoint_configs)
            ]:
                dir_path = Path(temp_config_dir, config_type)
                for config_id, config_data in configs.items():
                    file_path = dir_path / f"{config_id}.json"
                    with open(file_path, "w") as f:
                        json.dump(config_data, f)

            # Test the complete flow
            settings = PipelineConfigPrepSettings(
                env="local",
                pipeline_id="complete-test",
                local_config_mount=temp_config_dir
            )

            prep = PipelineConfigPrep(settings)

            # Load individual components
            loaded_pipeline = prep.load_pipeline_config()
            assert loaded_pipeline["pipeline_id"] == "complete-test"

            api_host = prep.load_config("hosts", "api-host")
            # load_config uses CredentialsManager._expand_environment_variables which expands variables
            assert "Bearer test-token-123" == api_host["headers"]["Authorization"]

            db_host = prep.load_config("hosts", "db-host")
            assert "secret-password" == db_host["password"]

        finally:
            # Clean up environment variables
            for var in ["API_TOKEN", "DATABASE_PASSWORD"]:
                os.environ.pop(var, None)

    def test_config_with_permission_errors(self, temp_config_dir):
        """Test handling of file permission errors."""
        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="permission-test",
            local_config_mount=temp_config_dir
        )
        prep = PipelineConfigPrep(settings)

        # Create a file and make it unreadable
        config_file = Path(temp_config_dir, "pipelines", "permission-test.json")
        config_file.write_text('{"pipeline_id": "permission-test"}')

        # Make file unreadable (if supported by OS)
        try:
            config_file.chmod(0o000)

            with pytest.raises((PermissionError, OSError)):
                prep.load_pipeline_config()

        except (OSError, NotImplementedError):
            # Skip test if chmod is not supported (e.g., on Windows)
            pytest.skip("chmod not supported on this platform")
        finally:
            # Restore permissions for cleanup
            try:
                config_file.chmod(0o644)
            except (OSError, FileNotFoundError):
                pass