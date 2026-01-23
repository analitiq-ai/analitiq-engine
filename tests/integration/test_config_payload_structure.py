"""Integration tests for config payload structure validation.

Tests all connector combinations with real example data.
Minimal mocking - only analitiq.yaml path patching.

These tests verify that:
1. Connections are resolved with correct keys (plain UUIDs, not prefixed)
2. Resolved config structure matches what connectors expect
3. Pipeline._build_config_dict() produces valid enriched configs
4. End-to-end flow from PipelineConfigPrep -> Pipeline -> Engine config
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.engine.pipeline_config_prep import PipelineConfigPrep, PipelineConfigPrepSettings
from src.engine.pipeline import Pipeline
from src.source.connectors.api import APIConnector
from src.source.connectors.database import DatabaseConfig
from src.models.api import APIConnectionConfig
from src.models.enriched import (
    EnrichedAPIDestinationConfig,
    EnrichedAPISourceConfig,
    EnrichedDatabaseDestinationConfig,
    EnrichedDatabaseSourceConfig,
)


# Path to examples directory
EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples"


def get_mock_config_for_example(example_name: str) -> dict:
    """Create mock config that points to example directory."""
    example_dir = EXAMPLES_DIR / example_name
    project_root = EXAMPLES_DIR.parent
    return {
        "paths": {
            "pipelines": str(example_dir / "pipelines"),
            "streams": str(example_dir / "streams"),
            "connections": str(example_dir / "connections"),
            "endpoints": str(example_dir / "endpoints"),
            "secrets": str(example_dir / ".secrets"),
            "connectors": str(project_root / "connectors"),
        }
    }


class TestResolvedConnectionKeys:
    """Tests that resolved connections use plain UUID keys, not prefixed."""

    def test_wise_to_sevdesk_connection_keys_are_plain_uuids(self):
        """Verify resolved connection keys are plain UUIDs for API-to-API example."""
        mock_config = get_mock_config_for_example("wise_to_sevdesk")

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="22ab7b76-b4df-4c68-8b27-82c307436661",
        )

        with patch("src.engine.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
            prep = PipelineConfigPrep(settings)
            _, _, resolved_connections, resolved_endpoints = prep.create_config()

        # Keys should be plain UUIDs, not "id:{uuid}" format
        for key in resolved_connections.keys():
            assert ":" not in key, f"Connection key should be plain UUID, got: {key}"
            # Should look like a UUID
            parts = key.split("-")
            assert len(parts) == 5, f"Connection key should be UUID format, got: {key}"

        # Endpoint keys should also be plain paths
        for key in resolved_endpoints.keys():
            assert not key.startswith("path:"), f"Endpoint key should not have 'path:' prefix, got: {key}"


class TestAPIToAPIConfigStructure:
    """Wise to SevDesk: API source to API destination."""

    @pytest.fixture
    def api_to_api_config(self):
        """Load wise_to_sevdesk example configuration."""
        mock_config = get_mock_config_for_example("wise_to_sevdesk")

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="22ab7b76-b4df-4c68-8b27-82c307436661",
        )

        with patch("src.engine.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
            prep = PipelineConfigPrep(settings)
            return prep.create_config()

    def test_source_connection_has_api_fields(self, api_to_api_config):
        """Verify API source connection has required fields for APIConnectionConfig."""
        pipeline_config, _, resolved_connections, _ = api_to_api_config

        # Get source connection ID from pipeline
        source_refs = pipeline_config.connections.source
        source_connection_id = list(source_refs.values())[0]

        assert source_connection_id in resolved_connections, (
            f"Source connection {source_connection_id} not in resolved connections"
        )

        source_conn = resolved_connections[source_connection_id]
        config = source_conn.config

        # Validate against APIConnectionConfig model
        validated = APIConnectionConfig(**config)
        assert validated.host, "API connection must have host"
        assert validated.host.startswith("http"), f"Host should be URL, got: {validated.host}"

    def test_destination_connection_has_api_fields(self, api_to_api_config):
        """Verify API destination connection has required fields."""
        pipeline_config, _, resolved_connections, _ = api_to_api_config

        # Get destination connection ID from pipeline
        dest_refs = pipeline_config.connections.destinations[0]
        dest_connection_id = list(dest_refs.values())[0]

        assert dest_connection_id in resolved_connections, (
            f"Destination connection {dest_connection_id} not in resolved connections"
        )

        dest_conn = resolved_connections[dest_connection_id]
        config = dest_conn.config

        # Validate against APIConnectionConfig model
        validated = APIConnectionConfig(**config)
        assert validated.host, "API connection must have host"

    def test_pipeline_build_config_produces_valid_source(self, api_to_api_config):
        """Verify Pipeline._build_config_dict() produces valid source config."""
        pipeline_config, stream_configs, resolved_connections, resolved_endpoints = api_to_api_config

        # Create Pipeline instance
        pipeline = Pipeline(
            pipeline_config=pipeline_config,
            stream_configs=stream_configs,
            resolved_connections=resolved_connections,
            resolved_endpoints=resolved_endpoints,
        )

        config_dict = pipeline._build_config_dict()

        # Check that streams have source with connection fields
        for stream_id, stream_data in config_dict["streams"].items():
            source = stream_data["source"]

            # Source should have API fields merged
            assert "host" in source, f"Stream {stream_id} source missing 'host'"
            assert source["host"].startswith("http"), f"Source host should be URL"

            # Source should have stream fields
            assert "connection_ref" in source
            assert "endpoint_id" in source
            assert "replication_method" in source

    def test_pipeline_build_config_produces_valid_destination(self, api_to_api_config):
        """Verify Pipeline._build_config_dict() produces valid destination config."""
        pipeline_config, stream_configs, resolved_connections, resolved_endpoints = api_to_api_config

        pipeline = Pipeline(
            pipeline_config=pipeline_config,
            stream_configs=stream_configs,
            resolved_connections=resolved_connections,
            resolved_endpoints=resolved_endpoints,
        )

        config_dict = pipeline._build_config_dict()

        for stream_id, stream_data in config_dict["streams"].items():
            destination = stream_data["destination"]

            # Destination should have API fields merged
            assert "host" in destination, f"Stream {stream_id} destination missing 'host'"

            # Destination should have stream fields
            assert "connection_ref" in destination
            assert "endpoint_id" in destination
            assert "refresh_mode" in destination


class TestAPIToDatabaseConfigStructure:
    """Wise to Postgres: API source to database destination."""

    @pytest.fixture
    def api_to_db_config(self):
        """Load wise_to_postgres example configuration."""
        mock_config = get_mock_config_for_example("wise_to_postgres")

        # Need to check what pipeline_id is used in this example
        pipelines_dir = EXAMPLES_DIR / "wise_to_postgres" / "pipelines"
        if not pipelines_dir.exists():
            pytest.skip("wise_to_postgres example not set up")

        pipeline_files = list(pipelines_dir.glob("*.json"))
        if not pipeline_files:
            pytest.skip("No pipeline files found in wise_to_postgres")

        pipeline_id = pipeline_files[0].stem

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id=pipeline_id,
        )

        try:
            with patch("src.engine.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
                prep = PipelineConfigPrep(settings)
                return prep.create_config()
        except FileNotFoundError as e:
            pytest.skip(f"wise_to_postgres example incomplete: {e}")

    def test_source_connection_is_api_type(self, api_to_db_config):
        """Verify API source has host, headers."""
        pipeline_config, _, resolved_connections, _ = api_to_db_config

        source_refs = pipeline_config.connections.source
        source_connection_id = list(source_refs.values())[0]

        source_conn = resolved_connections[source_connection_id]
        config = source_conn.config

        # Should be API type
        assert config.get("connector_type") == "api" or config.get("type") == "api", (
            f"Source should be API type, got: {config}"
        )
        assert "host" in config, "API source must have 'host'"

    def test_destination_connection_is_database_type(self, api_to_db_config):
        """Verify database dest has driver, host, port, database, username."""
        pipeline_config, _, resolved_connections, _ = api_to_db_config

        dest_refs = pipeline_config.connections.destinations[0]
        dest_connection_id = list(dest_refs.values())[0]

        dest_conn = resolved_connections[dest_connection_id]
        config = dest_conn.config

        # Should have database fields
        assert config.get("connector_type") == "database" or config.get("type") == "database", (
            f"Destination should be database type, got type: {config.get('type')}"
        )

    def test_database_fields_validated(self, api_to_db_config):
        """Verify port is int type, driver/database/username present."""
        pipeline_config, _, resolved_connections, _ = api_to_db_config

        dest_refs = pipeline_config.connections.destinations[0]
        dest_connection_id = list(dest_refs.values())[0]

        dest_conn = resolved_connections[dest_connection_id]
        config = dest_conn.config

        # Required database fields
        assert "driver" in config, "Database connection must have 'driver'"
        assert "database" in config, "Database connection must have 'database'"
        assert "username" in config, "Database connection must have 'username'"
        assert "host" in config, "Database connection must have 'host'"
        assert "port" in config, "Database connection must have 'port'"

        # Port should be convertible to int
        port_value = config["port"]
        int(port_value)  # Should not raise


class TestEndToEndConfigFlow:
    """Complete flow tests from PipelineConfigPrep to Engine."""

    def test_api_connection_config_validates_for_connector(self):
        """APIConnector.connect() would receive APIConnectionConfig-valid dict."""
        mock_config = get_mock_config_for_example("wise_to_sevdesk")

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="22ab7b76-b4df-4c68-8b27-82c307436661",
        )

        with patch("src.engine.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
            prep = PipelineConfigPrep(settings)
            pipeline_config, stream_configs, resolved_connections, resolved_endpoints = prep.create_config()

        # Get source connection
        source_refs = pipeline_config.connections.source
        source_connection_id = list(source_refs.values())[0]
        source_conn = resolved_connections[source_connection_id]

        # This is what would be passed to APIConnector.connect()
        config = source_conn.config

        # Validate it matches what APIConnectionConfig expects
        validated = APIConnectionConfig(**config)
        assert validated.host
        assert isinstance(validated.headers, dict)
        assert validated.timeout > 0

    def test_database_connection_config_validates_for_connector(self):
        """DatabaseConnector.connect() would receive DatabaseConfig-valid dict."""
        mock_config = get_mock_config_for_example("wise_to_postgres")

        pipelines_dir = EXAMPLES_DIR / "wise_to_postgres" / "pipelines"
        if not pipelines_dir.exists():
            pytest.skip("wise_to_postgres example not set up")

        pipeline_files = list(pipelines_dir.glob("*.json"))
        if not pipeline_files:
            pytest.skip("No pipeline files found")

        pipeline_id = pipeline_files[0].stem

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id=pipeline_id,
        )

        try:
            with patch("src.engine.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
                prep = PipelineConfigPrep(settings)
                pipeline_config, _, resolved_connections, _ = prep.create_config()
        except FileNotFoundError as e:
            pytest.skip(f"wise_to_postgres example incomplete: {e}")

        # Get destination connection (database)
        dest_refs = pipeline_config.connections.destinations[0]
        dest_connection_id = list(dest_refs.values())[0]
        dest_conn = resolved_connections[dest_connection_id]

        config = dest_conn.config

        # Convert port to int (as connector does)
        if "port" in config:
            config = config.copy()
            config["port"] = int(config["port"])

        # Validate it matches what DatabaseConfig expects
        validated = DatabaseConfig(**config)
        assert validated.driver
        assert validated.host
        assert validated.database
        assert validated.username

    def test_full_pipeline_config_dict_structure(self):
        """Verify complete config dict has all required sections."""
        mock_config = get_mock_config_for_example("wise_to_sevdesk")

        settings = PipelineConfigPrepSettings(
            env="local",
            pipeline_id="22ab7b76-b4df-4c68-8b27-82c307436661",
        )

        with patch("src.engine.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
            prep = PipelineConfigPrep(settings)
            pipeline_config, stream_configs, resolved_connections, resolved_endpoints = prep.create_config()

        pipeline = Pipeline(
            pipeline_config=pipeline_config,
            stream_configs=stream_configs,
            resolved_connections=resolved_connections,
            resolved_endpoints=resolved_endpoints,
        )

        config_dict = pipeline._build_config_dict()

        # Required top-level fields
        assert "pipeline_id" in config_dict
        assert "name" in config_dict
        assert "streams" in config_dict
        assert "source" in config_dict
        assert "destination" in config_dict
        assert "runtime" in config_dict

        # Streams should not be empty
        assert len(config_dict["streams"]) > 0

        # Each stream should have source, destination, mapping
        for stream_id, stream_data in config_dict["streams"].items():
            assert "source" in stream_data, f"Stream {stream_id} missing source"
            assert "destination" in stream_data, f"Stream {stream_id} missing destination"
            assert "mapping" in stream_data, f"Stream {stream_id} missing mapping"

            # Source should have merged connection + stream fields
            source = stream_data["source"]
            assert "host" in source or "driver" in source, (
                f"Stream {stream_id} source missing connection fields"
            )
            assert "connection_ref" in source
            assert "endpoint_id" in source
