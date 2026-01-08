"""Unit tests for config module."""

import pytest

from src.config import load_analitiq_config


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
            "ConnectorSync",
            "ConnectorConfig",
            "PathBasedConfigLoader",
        ]

        for attr in expected_exports:
            assert hasattr(config, attr), f"config module should have attribute: {attr}"

    @pytest.mark.unit
    def test_connectors_section_exists(self):
        """Test that connectors section exists in config."""
        config = load_analitiq_config()

        assert "connectors" in config
        connectors = config["connectors"]

        assert "repository" in connectors
        assert "sync" in connectors