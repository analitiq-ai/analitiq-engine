"""Unit tests for config module."""

import pytest

from src.config import (
    validate_pipeline_config,
    validate_connection_config,
    parse_endpoint_ref,
    resolve_endpoint_ref,
    load_connection,
    load_connector_for_connection,
)
from src.config.exceptions import EndpointNotFoundError, ConnectorNotFoundError, ConnectionConfigError


class TestConfig:
    """Test suite for configuration module."""

    @pytest.mark.unit
    def test_config_module_exports(self):
        """Test that config module exports expected attributes."""
        from src import config

        expected_exports = [
            "validate_pipeline_config",
            "validate_connection_config",
            "parse_endpoint_ref",
            "resolve_endpoint_ref",
            "load_connection",
            "load_connector_for_connection",
            "PathBasedConfigLoader",
        ]

        for attr in expected_exports:
            assert hasattr(config, attr), f"config module should have attribute: {attr}"


class TestPipelineConfigValidator:
    """Test suite for pipeline config validation."""

    @pytest.fixture
    def valid_pipeline(self):
        return {
            "pipeline": {
                "pipeline_id": "test-pipeline",
                "connections": {
                    "source": "my-api",
                    "destinations": ["prod-postgres"],
                },
                "streams": ["stream-1"],
                "engine": {"vcpu": 1, "memory": 8192},
                "runtime": {
                    "buffer_size": 5000,
                    "batching": {"batch_size": 100, "max_concurrent_batches": 3},
                    "logging": {"log_level": "INFO", "metrics_enabled": True},
                    "error_handling": {"strategy": "dlq", "max_retries": 3, "retry_delay": 5},
                },
            },
            "streams": [{"stream_id": "stream-1"}],
        }

    @pytest.mark.unit
    def test_valid_pipeline_passes(self, valid_pipeline):
        result = validate_pipeline_config(valid_pipeline)
        assert "pipeline" in result

    @pytest.mark.unit
    def test_missing_pipeline_key_fails(self, valid_pipeline):
        del valid_pipeline["pipeline"]
        with pytest.raises(ValueError, match="pipeline"):
            validate_pipeline_config(valid_pipeline)

    @pytest.mark.unit
    def test_missing_streams_key_fails(self, valid_pipeline):
        del valid_pipeline["streams"]
        with pytest.raises(ValueError, match="streams"):
            validate_pipeline_config(valid_pipeline)

    @pytest.mark.unit
    def test_missing_source_fails(self, valid_pipeline):
        valid_pipeline["pipeline"]["connections"]["source"] = ""
        with pytest.raises(ValueError, match="source"):
            validate_pipeline_config(valid_pipeline)

    @pytest.mark.unit
    def test_missing_destinations_fails(self, valid_pipeline):
        valid_pipeline["pipeline"]["connections"]["destinations"] = []
        with pytest.raises(ValueError, match="destinations"):
            validate_pipeline_config(valid_pipeline)

    @pytest.mark.unit
    def test_no_stream_ids_fails(self, valid_pipeline):
        valid_pipeline["pipeline"]["streams"] = []
        with pytest.raises(ValueError, match="stream"):
            validate_pipeline_config(valid_pipeline)


class TestConnectionConfigValidator:
    """Test suite for connection config validation."""

    @pytest.mark.unit
    def test_valid_connection_passes(self):
        config = {"connector_slug": "postgresql", "host": "localhost"}
        result = validate_connection_config(config)
        assert result["connector_slug"] == "postgresql"

    @pytest.mark.unit
    def test_missing_connector_slug_fails(self):
        with pytest.raises(ValueError, match="connector_slug"):
            validate_connection_config({"host": "localhost"})


class TestEndpointRefParser:
    """Test suite for endpoint reference parsing."""

    @pytest.mark.unit
    def test_parse_connector_ref(self):
        scope, identifier, name = parse_endpoint_ref("connector:pipedrive/deals")
        assert scope == "connector"
        assert identifier == "pipedrive"
        assert name == "deals"

    @pytest.mark.unit
    def test_parse_connection_ref(self):
        scope, identifier, name = parse_endpoint_ref("connection:prod-postgres/public_users")
        assert scope == "connection"
        assert identifier == "prod-postgres"
        assert name == "public_users"

    @pytest.mark.unit
    def test_missing_scope_raises(self):
        with pytest.raises(ValueError, match="missing scope"):
            parse_endpoint_ref("pipedrive/deals")

    @pytest.mark.unit
    def test_invalid_scope_raises(self):
        with pytest.raises(ValueError, match="Invalid endpoint_ref scope"):
            parse_endpoint_ref("unknown:pipedrive/deals")

    @pytest.mark.unit
    def test_missing_slash_raises(self):
        with pytest.raises(ValueError, match="identifier/endpoint_name"):
            parse_endpoint_ref("connector:pipedrive")

    @pytest.mark.unit
    def test_empty_parts_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            parse_endpoint_ref("connector:/deals")


class TestEndpointRefResolver:
    """Test suite for endpoint reference resolution."""

    @pytest.mark.unit
    def test_resolve_connector_endpoint(self, tmp_path):
        """Test resolving a public connector endpoint."""
        # Set up connector directory structure
        endpoint_dir = tmp_path / "connectors" / "wise" / "definition" / "endpoints"
        endpoint_dir.mkdir(parents=True)
        endpoint_file = endpoint_dir / "transfers.json"
        endpoint_file.write_text('{"endpoint": "/v1/transfers", "method": "GET"}')

        paths = {"connectors": tmp_path / "connectors", "connections": tmp_path / "connections"}
        result = resolve_endpoint_ref("connector:wise/transfers", paths)
        assert result["endpoint"] == "/v1/transfers"

    @pytest.mark.unit
    def test_resolve_connection_endpoint(self, tmp_path):
        """Test resolving a private connection endpoint (under definition/)."""
        endpoint_dir = tmp_path / "connections" / "prod-pg" / "definition" / "endpoints"
        endpoint_dir.mkdir(parents=True)
        endpoint_file = endpoint_dir / "public_users.json"
        endpoint_file.write_text('{"endpoint": "public/users", "method": "DATABASE"}')

        paths = {"connectors": tmp_path / "connectors", "connections": tmp_path / "connections"}
        result = resolve_endpoint_ref("connection:prod-pg/public_users", paths)
        assert result["method"] == "DATABASE"

    @pytest.mark.unit
    def test_resolve_missing_endpoint_raises(self, tmp_path):
        """Test that missing endpoint file raises EndpointNotFoundError."""
        paths = {"connectors": tmp_path / "connectors", "connections": tmp_path / "connections"}
        with pytest.raises(EndpointNotFoundError):
            resolve_endpoint_ref("connector:wise/nonexistent", paths)


class TestConnectionLoader:
    """Test suite for connection loading."""

    @pytest.mark.unit
    def test_load_connection(self, tmp_path):
        conn_dir = tmp_path / "my-api"
        conn_dir.mkdir()
        (conn_dir / "connection.json").write_text(
            '{"connector_slug": "wise", "host": "https://api.wise.com"}'
        )

        result = load_connection("my-api", tmp_path)
        assert result["connector_slug"] == "wise"

    @pytest.mark.unit
    def test_load_missing_connection_raises(self, tmp_path):
        with pytest.raises(ConnectionConfigError):
            load_connection("nonexistent", tmp_path)

    @pytest.mark.unit
    def test_load_connector_for_connection(self, tmp_path):
        connector_dir = tmp_path / "wise" / "definition"
        connector_dir.mkdir(parents=True)
        (connector_dir / "connector.json").write_text(
            '{"connector_type": "api", "slug": "wise"}'
        )

        result = load_connector_for_connection("wise", tmp_path)
        assert result["connector_type"] == "api"

    @pytest.mark.unit
    def test_load_missing_connector_raises(self, tmp_path):
        with pytest.raises(ConnectorNotFoundError):
            load_connector_for_connection("nonexistent", tmp_path)
