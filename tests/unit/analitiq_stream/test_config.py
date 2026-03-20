"""Unit tests for config module."""

import pytest

from src.config import (
    load_analitiq_config,
    validate_consolidated_config,
)


class TestConfig:
    """Test suite for configuration module."""

    @pytest.mark.unit
    def test_load_analitiq_config_loads_paths(self):
        """Test that load_analitiq_config loads paths from analitiq.yaml."""
        config = load_analitiq_config()

        assert "paths" in config
        paths = config["paths"]

        assert paths["state"] == "./state"
        assert paths["logs"] == "./logs"
        assert paths["deadletter"] == "./deadletter"
        assert paths["metrics"] == "./metrics"

    @pytest.mark.unit
    def test_load_analitiq_config_loads_defaults(self):
        """Test that load_analitiq_config loads defaults section."""
        config = load_analitiq_config()

        assert "defaults" in config
        defaults = config["defaults"]

        assert defaults["monitoring_interval_seconds"] == 5
        assert isinstance(defaults["monitoring_interval_seconds"], int)

    @pytest.mark.unit
    def test_config_module_exports(self):
        """Test that config module exports expected attributes."""
        from src import config

        expected_exports = [
            "load_analitiq_config",
            "PathBasedConfigLoader",
            "validate_consolidated_config",
        ]

        for attr in expected_exports:
            assert hasattr(config, attr), f"config module should have attribute: {attr}"


class TestConsolidatedConfigValidator:
    """Test suite for consolidated config validation."""

    @pytest.fixture
    def valid_config(self):
        """Return a valid consolidated config with minimum requirements."""
        return {
            "pipeline": {"pipeline_id": "test-pipeline"},
            "connections": [
                {"connection_id": "conn-1", "type": "api"},
                {"connection_id": "conn-2", "type": "database"},
            ],
            "connectors": [
                {"connector_id": "connector-1"},
                {"connector_id": "connector-2"},
            ],
            "endpoints": [
                {"endpoint_id": "endpoint-1"},
                {"endpoint_id": "endpoint-2"},
            ],
            "streams": [
                {"stream_id": "stream-1"},
            ],
        }

    @pytest.mark.unit
    def test_valid_config_passes_validation(self, valid_config):
        """Test that a valid config passes validation."""
        result = validate_consolidated_config(valid_config)
        assert isinstance(result, dict)
        assert len(result["connections"]) == 2
        assert len(result["connectors"]) == 2
        assert len(result["endpoints"]) == 2
        assert len(result["streams"]) == 1

    @pytest.mark.unit
    def test_missing_pipeline_fails_validation(self, valid_config):
        """Test that missing pipeline key fails validation."""
        del valid_config["pipeline"]
        with pytest.raises(ValueError) as exc_info:
            validate_consolidated_config(valid_config)
        assert "pipeline" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_insufficient_connections_fails_validation(self, valid_config):
        """Test that fewer than 2 connections fails validation."""
        valid_config["connections"] = [{"connection_id": "conn-1"}]
        with pytest.raises(ValueError) as exc_info:
            validate_consolidated_config(valid_config)
        assert "connections" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_insufficient_connectors_fails_validation(self, valid_config):
        """Test that fewer than 2 connectors fails validation."""
        valid_config["connectors"] = [{"connector_id": "connector-1"}]
        with pytest.raises(ValueError) as exc_info:
            validate_consolidated_config(valid_config)
        assert "connectors" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_insufficient_endpoints_fails_validation(self, valid_config):
        """Test that fewer than 2 endpoints fails validation."""
        valid_config["endpoints"] = [{"endpoint_id": "endpoint-1"}]
        with pytest.raises(ValueError) as exc_info:
            validate_consolidated_config(valid_config)
        assert "endpoints" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_no_streams_fails_validation(self, valid_config):
        """Test that zero streams fails validation."""
        valid_config["streams"] = []
        with pytest.raises(ValueError) as exc_info:
            validate_consolidated_config(valid_config)
        assert "streams" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_empty_connections_fails_validation(self, valid_config):
        """Test that empty connections list fails validation."""
        valid_config["connections"] = []
        with pytest.raises(ValueError) as exc_info:
            validate_consolidated_config(valid_config)
        assert "connections" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_more_than_minimum_passes_validation(self, valid_config):
        """Test that having more than minimum requirements passes."""
        valid_config["connections"].append({"connection_id": "conn-3"})
        valid_config["connectors"].append({"connector_id": "connector-3"})
        valid_config["endpoints"].append({"endpoint_id": "endpoint-3"})
        valid_config["streams"].append({"stream_id": "stream-2"})

        result = validate_consolidated_config(valid_config)
        assert len(result["connections"]) == 3
        assert len(result["connectors"]) == 3
        assert len(result["endpoints"]) == 3
        assert len(result["streams"]) == 2