"""Unit tests for config module."""

import pytest

from src.config import (
    validate_pipeline_config,
    validate_connection_config,
    resolve_endpoint_ref,
    load_connection,
    load_connector_for_connection,
)
from src.config.exceptions import EndpointNotFoundError, ConnectorNotFoundError, ConnectionConfigError
from src.models.stream import EndpointRef


class TestConfig:
    """Test suite for configuration module."""

    @pytest.mark.unit
    def test_config_module_exports(self):
        """Test that config module exports expected attributes."""
        from src import config

        expected_exports = [
            "validate_pipeline_config",
            "validate_connection_config",
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


class TestEndpointRefModel:
    """Test suite for the EndpointRef dataclass."""

    @pytest.mark.unit
    def test_from_dict_connector(self):
        ref = EndpointRef.from_dict({
            "scope": "connector", "identifier": "pipedrive", "endpoint": "deals",
        })
        assert ref.scope == "connector"
        assert ref.identifier == "pipedrive"
        assert ref.endpoint == "deals"

    @pytest.mark.unit
    def test_from_dict_connection(self):
        ref = EndpointRef.from_dict({
            "scope": "connection", "identifier": "prod-postgres", "endpoint": "public_users",
        })
        assert ref.scope == "connection"
        assert ref.identifier == "prod-postgres"
        assert ref.endpoint == "public_users"

    @pytest.mark.unit
    def test_from_dict_passes_through_existing_instance(self):
        original = EndpointRef(scope="connector", identifier="x", endpoint="y")
        assert EndpointRef.from_dict(original) is original

    @pytest.mark.unit
    def test_invalid_scope_raises(self):
        with pytest.raises(ValueError, match="scope"):
            EndpointRef.from_dict({
                "scope": "unknown", "identifier": "x", "endpoint": "y",
            })

    @pytest.mark.unit
    def test_missing_keys_raises(self):
        with pytest.raises(ValueError, match="missing required keys"):
            EndpointRef.from_dict({"scope": "connector"})

    @pytest.mark.unit
    def test_unknown_keys_raises(self):
        with pytest.raises(ValueError, match="unknown keys"):
            EndpointRef.from_dict({
                "scope": "connector", "identifier": "x", "endpoint": "y", "extra": "z",
            })

    @pytest.mark.unit
    def test_empty_identifier_raises(self):
        with pytest.raises(ValueError, match="identifier cannot be empty"):
            EndpointRef.from_dict({
                "scope": "connector", "identifier": "", "endpoint": "y",
            })

    @pytest.mark.unit
    def test_empty_endpoint_raises(self):
        with pytest.raises(ValueError, match="endpoint cannot be empty"):
            EndpointRef.from_dict({
                "scope": "connector", "identifier": "x", "endpoint": "",
            })

    @pytest.mark.unit
    def test_non_dict_input_raises(self):
        with pytest.raises(TypeError, match="endpoint_ref must be a dict"):
            EndpointRef.from_dict("connector:x/y")

    @pytest.mark.unit
    def test_to_dict_roundtrip(self):
        d = {"scope": "connector", "identifier": "x", "endpoint": "y"}
        assert EndpointRef.from_dict(d).to_dict() == d

    @pytest.mark.unit
    def test_str_canonical_form(self):
        ref = EndpointRef(scope="connection", identifier="alias", endpoint="name")
        assert str(ref) == "connection:alias/name"

    @pytest.mark.unit
    def test_hashable_for_dict_keys(self):
        ref1 = EndpointRef(scope="connector", identifier="x", endpoint="y")
        ref2 = EndpointRef(scope="connector", identifier="x", endpoint="y")
        assert hash(ref1) == hash(ref2)
        cache = {ref1: "value"}
        assert cache[ref2] == "value"


class TestEndpointRefResolver:
    """Test suite for endpoint reference resolution."""

    @pytest.mark.unit
    def test_resolve_connector_endpoint(self, tmp_path):
        """Test resolving a public connector endpoint."""
        endpoint_dir = tmp_path / "connectors" / "wise" / "definition" / "endpoints"
        endpoint_dir.mkdir(parents=True)
        endpoint_file = endpoint_dir / "transfers.json"
        endpoint_file.write_text('{"endpoint": "/v1/transfers", "method": "GET"}')

        paths = {"connectors": tmp_path / "connectors", "connections": tmp_path / "connections"}
        result = resolve_endpoint_ref(
            {"scope": "connector", "identifier": "wise", "endpoint": "transfers"},
            paths,
        )
        assert result["endpoint"] == "/v1/transfers"

    @pytest.mark.unit
    def test_resolve_connection_endpoint(self, tmp_path):
        """Test resolving a private connection endpoint (under definition/)."""
        endpoint_dir = tmp_path / "connections" / "prod-pg" / "definition" / "endpoints"
        endpoint_dir.mkdir(parents=True)
        endpoint_file = endpoint_dir / "public_users.json"
        endpoint_file.write_text('{"endpoint": "public/users", "method": "DATABASE"}')

        paths = {"connectors": tmp_path / "connectors", "connections": tmp_path / "connections"}
        result = resolve_endpoint_ref(
            {"scope": "connection", "identifier": "prod-pg", "endpoint": "public_users"},
            paths,
        )
        assert result["method"] == "DATABASE"

    @pytest.mark.unit
    def test_resolve_accepts_endpoint_ref_instance(self, tmp_path):
        endpoint_dir = tmp_path / "connectors" / "wise" / "definition" / "endpoints"
        endpoint_dir.mkdir(parents=True)
        (endpoint_dir / "transfers.json").write_text('{"endpoint": "/v1/transfers"}')

        paths = {"connectors": tmp_path / "connectors", "connections": tmp_path / "connections"}
        ref = EndpointRef(scope="connector", identifier="wise", endpoint="transfers")
        assert resolve_endpoint_ref(ref, paths)["endpoint"] == "/v1/transfers"

    @pytest.mark.unit
    def test_resolve_missing_endpoint_raises(self, tmp_path):
        """Test that missing endpoint file raises EndpointNotFoundError."""
        paths = {"connectors": tmp_path / "connectors", "connections": tmp_path / "connections"}
        with pytest.raises(EndpointNotFoundError):
            resolve_endpoint_ref(
                {"scope": "connector", "identifier": "wise", "endpoint": "nonexistent"},
                paths,
            )


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
