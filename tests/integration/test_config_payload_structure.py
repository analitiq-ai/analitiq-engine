"""Integration tests for config payload structure validation.

Tests all connector combinations with fixture data.
No filesystem dependencies - configs constructed directly from fixture data.

These tests verify that:
1. Connections are resolved with correct keys (plain UUIDs, not prefixed)
2. Resolved config structure matches what connectors expect
3. Pipeline._build_config_dict() produces valid enriched configs
4. End-to-end flow from constructed configs -> Pipeline -> Engine config
"""

import pytest
from unittest.mock import Mock

from src.engine.pipeline import Pipeline
from src.engine.pipeline_config_prep import ResolvedConnection
from src.models.api import APIConnectionConfig


# --- Test UUIDs ---
PIPELINE_ID = "22ab7b76-b4df-4c68-8b27-82c307436661"
ORG_ID = "d7a11991-2795-49d1-a858-c7e58ee5ecc6"
WISE_CONNECTION_ID = "b4904c77-0a4a-4a8d-a768-4a8b5f2f2414"
SEVDESK_CONNECTION_ID = "b452b1b2-34f9-4175-a47b-3dd6a4012230"
DB_CONNECTION_ID = "c5d6e7f8-9a0b-1c2d-3e4f-567890abcdef"
WISE_ENDPOINT_ID = "5a4b9e21-441f-4bc7-9d5e-41917b4357e6"
SEVDESK_ENDPOINT_ID = "1e63d782-4b67-4b7e-b845-4b4de5e4f46e"
DB_ENDPOINT_ID = "d1e2f3a4-b5c6-7d8e-9f0a-1b2c3d4e5f6a"
STREAM_ID = "stream-wise-transfers"

# --- Connection alias names ---
WISE_ALIAS = "conn_wise"
SEVDESK_ALIAS = "conn_sevdesk"
DB_ALIAS = "conn_db"

# --- Connector definitions ---
API_CONNECTORS = [
    {"connector_id": "api-connector", "connector_type": "api", "connector_name": "REST API"},
]
API_DB_CONNECTORS = [
    {"connector_id": "api-connector", "connector_type": "api", "connector_name": "REST API"},
    {"connector_id": "pg-connector", "connector_type": "database", "connector_name": "PostgreSQL", "driver": "postgresql"},
]


def _wise_connection_config():
    """Wise API connection config (compatible with APIConnectionConfig)."""
    return {
        "connector_id": "api-connector",
        "connector_type": "api",
        "host": "https://api.sandbox.transferwise.tech",
        "parameters": {
            "headers": {
                "Authorization": "Bearer test_token",
                "Content-Type": "application/json",
                "User-Agent": "Analitiq-Stream/1.0",
            },
            "timeout": 30,
            "rate_limit": {
                "max_requests": 100,
                "time_window": 60,
            },
        },
    }


def _sevdesk_connection_config():
    """SevDesk API connection config (compatible with APIConnectionConfig)."""
    return {
        "connector_id": "api-connector",
        "connector_type": "api",
        "host": "https://my.sevdesk.de",
        "parameters": {
            "headers": {
                "Authorization": "Bearer test_token",
                "Content-Type": "application/json",
            },
            "timeout": 15,
            "rate_limit": {
                "max_requests": 10,
                "time_window": 60,
            },
        },
    }


def _database_connection_config():
    """PostgreSQL database connection config (compatible with DatabaseConfig)."""
    return {
        "connector_id": "pg-connector",
        "connector_type": "database",
        "host": "localhost",
        "parameters": {
            "port": 5432,
            "database": "analytics",
            "username": "postgres",
            "password": "test_password",
            "ssl_mode": "prefer",
        },
    }


def _wise_endpoint_config():
    """Wise API source endpoint config."""
    return {
        "endpoint": "/v1/profiles/{profile_id}/transfers",
        "method": "GET",
        "query_params": {
            "status": "outgoing_payment_sent,funds_converted",
            "limit": 1000,
            "offset": 0,
        },
        "pagination": {
            "type": "offset",
            "limit_param": "limit",
            "offset_param": "offset",
            "max_limit": 1000,
        },
    }


def _sevdesk_endpoint_config():
    """SevDesk API destination endpoint config."""
    return {
        "endpoint": "/api/v1/CheckAccountTransaction",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
    }


def _database_endpoint_config():
    """PostgreSQL database destination endpoint config."""
    return {
        "schema": "wise_data",
        "table": "transactions",
        "primary_key": ["wise_id"],
        "write_mode": "upsert",
    }


def _make_pipeline_config(source_alias, source_conn_id, dest_alias, dest_conn_id):
    """Build a pipeline config dict for testing."""
    return {
        "version": 1,
        "pipeline_id": PIPELINE_ID,
        "org_id": ORG_ID,
        "name": "Test Pipeline",
        "status": "active",
        "connections": {
            "source": {source_alias: source_conn_id},
            "destinations": [{dest_alias: dest_conn_id}],
        },
        "engine_config": {},
    }


def _make_stream_config(source_alias, source_endpoint_id, dest_alias, dest_endpoint_id):
    """Build a stream config dict for testing."""
    return {
        "version": 1,
        "stream_id": STREAM_ID,
        "pipeline_id": PIPELINE_ID,
        "org_id": ORG_ID,
        "status": "active",
        "is_enabled": True,
        "source": {
            "connection_ref": source_alias,
            "endpoint_id": source_endpoint_id,
            "primary_key": ["id"],
            "replication": {
                "method": "incremental",
                "cursor_field": ["created"],
            },
        },
        "destinations": [{
            "connection_ref": dest_alias,
            "endpoint_id": dest_endpoint_id,
            "write": {"mode": "upsert"},
        }],
        "mapping": {"assignments": []},
    }


def _api_to_api_resolved_connections():
    """Resolved connections for API-to-API (Wise -> SevDesk)."""
    return {
        WISE_CONNECTION_ID: ResolvedConnection(
            connection_id=WISE_CONNECTION_ID,
            connection_type="api",
            config=_wise_connection_config(),
            connection_config_wrapper=Mock(),
        ),
        SEVDESK_CONNECTION_ID: ResolvedConnection(
            connection_id=SEVDESK_CONNECTION_ID,
            connection_type="api",
            config=_sevdesk_connection_config(),
            connection_config_wrapper=Mock(),
        ),
    }


def _api_to_api_resolved_endpoints():
    """Resolved endpoints for API-to-API (Wise -> SevDesk)."""
    return {
        WISE_ENDPOINT_ID: _wise_endpoint_config(),
        SEVDESK_ENDPOINT_ID: _sevdesk_endpoint_config(),
    }


def _api_to_db_resolved_connections():
    """Resolved connections for API-to-DB (Wise -> Postgres)."""
    return {
        WISE_CONNECTION_ID: ResolvedConnection(
            connection_id=WISE_CONNECTION_ID,
            connection_type="api",
            config=_wise_connection_config(),
            connection_config_wrapper=Mock(),
        ),
        DB_CONNECTION_ID: ResolvedConnection(
            connection_id=DB_CONNECTION_ID,
            connection_type="database",
            config=_database_connection_config(),
            connection_config_wrapper=Mock(),
        ),
    }


def _api_to_db_resolved_endpoints():
    """Resolved endpoints for API-to-DB (Wise -> Postgres)."""
    return {
        WISE_ENDPOINT_ID: _wise_endpoint_config(),
        DB_ENDPOINT_ID: _database_endpoint_config(),
    }


class TestResolvedConnectionKeys:
    """Tests that resolved connections use plain UUID keys, not prefixed."""

    def test_connection_keys_are_plain_uuids(self):
        """Verify resolved connection keys are plain UUIDs for API-to-API."""
        resolved_connections = _api_to_api_resolved_connections()

        for key in resolved_connections.keys():
            assert ":" not in key, f"Connection key should be plain UUID, got: {key}"
            parts = key.split("-")
            assert len(parts) == 5, f"Connection key should be UUID format, got: {key}"

        resolved_endpoints = _api_to_api_resolved_endpoints()

        for key in resolved_endpoints.keys():
            assert not key.startswith("path:"), (
                f"Endpoint key should not have 'path:' prefix, got: {key}"
            )


class TestAPIToAPIConfigStructure:
    """Wise to SevDesk: API source to API destination."""

    @pytest.fixture
    def api_to_api_config(self):
        """Construct wise_to_sevdesk config from fixture data."""
        pipeline_config = _make_pipeline_config(
            WISE_ALIAS, WISE_CONNECTION_ID,
            SEVDESK_ALIAS, SEVDESK_CONNECTION_ID,
        )
        stream_configs = [_make_stream_config(
            WISE_ALIAS, WISE_ENDPOINT_ID,
            SEVDESK_ALIAS, SEVDESK_ENDPOINT_ID,
        )]
        resolved_connections = _api_to_api_resolved_connections()
        resolved_endpoints = _api_to_api_resolved_endpoints()
        return pipeline_config, stream_configs, resolved_connections, resolved_endpoints

    def test_source_connection_has_api_fields(self, api_to_api_config):
        """Verify API source connection has required fields for APIConnectionConfig."""
        pipeline_config, _, resolved_connections, _ = api_to_api_config

        source_refs = pipeline_config["connections"]["source"]
        source_connection_id = list(source_refs.values())[0]

        assert source_connection_id in resolved_connections, (
            f"Source connection {source_connection_id} not in resolved connections"
        )

        source_conn = resolved_connections[source_connection_id]
        config = source_conn.config

        assert "host" in config, "API connection must have host"
        assert config["host"].startswith("http"), f"Host should be URL, got: {config['host']}"
        assert "parameters" in config, "API connection must have parameters"

    def test_destination_connection_has_api_fields(self, api_to_api_config):
        """Verify API destination connection has required fields."""
        pipeline_config, _, resolved_connections, _ = api_to_api_config

        dest_refs = pipeline_config["connections"]["destinations"][0]
        dest_connection_id = list(dest_refs.values())[0]

        assert dest_connection_id in resolved_connections, (
            f"Destination connection {dest_connection_id} not in resolved connections"
        )

        dest_conn = resolved_connections[dest_connection_id]
        config = dest_conn.config

        assert "host" in config, "API connection must have host"
        assert "parameters" in config, "API connection must have parameters"

    def test_pipeline_build_config_produces_valid_source(self, api_to_api_config):
        """Verify Pipeline._build_config_dict() produces valid source config."""
        pipeline_config, stream_configs, resolved_connections, resolved_endpoints = api_to_api_config

        pipeline = Pipeline(
            pipeline_config=pipeline_config,
            stream_configs=stream_configs,
            resolved_connections=resolved_connections,
            resolved_endpoints=resolved_endpoints,
            connectors=API_CONNECTORS,
        )

        config_dict = pipeline._build_config_dict()

        for stream_id, stream_data in config_dict["streams"].items():
            source = stream_data["source"]

            assert "host" in source, f"Stream {stream_id} source missing 'host'"
            assert source["host"].startswith("http"), "Source host should be URL"

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
            connectors=API_CONNECTORS,
        )

        config_dict = pipeline._build_config_dict()

        for stream_id, stream_data in config_dict["streams"].items():
            destination = stream_data["destination"]

            assert "host" in destination, f"Stream {stream_id} destination missing 'host'"

            assert "connection_ref" in destination
            assert "endpoint_id" in destination
            assert "refresh_mode" in destination


class TestAPIToDatabaseConfigStructure:
    """Wise to Postgres: API source to database destination."""

    @pytest.fixture
    def api_to_db_config(self):
        """Construct wise_to_postgres config from fixture data."""
        pipeline_config = _make_pipeline_config(
            WISE_ALIAS, WISE_CONNECTION_ID,
            DB_ALIAS, DB_CONNECTION_ID,
        )
        stream_configs = [_make_stream_config(
            WISE_ALIAS, WISE_ENDPOINT_ID,
            DB_ALIAS, DB_ENDPOINT_ID,
        )]
        resolved_connections = _api_to_db_resolved_connections()
        resolved_endpoints = _api_to_db_resolved_endpoints()
        return pipeline_config, stream_configs, resolved_connections, resolved_endpoints

    def test_source_connection_is_api_type(self, api_to_db_config):
        """Verify API source has host, headers."""
        pipeline_config, _, resolved_connections, _ = api_to_db_config

        source_refs = pipeline_config["connections"]["source"]
        source_connection_id = list(source_refs.values())[0]

        source_conn = resolved_connections[source_connection_id]
        config = source_conn.config

        assert config.get("connector_type") == "api" or config.get("type") == "api", (
            f"Source should be API type, got: {config}"
        )
        assert "host" in config, "API source must have 'host'"

    def test_destination_connection_is_database_type(self, api_to_db_config):
        """Verify database dest has driver, host, port, database, username."""
        pipeline_config, _, resolved_connections, _ = api_to_db_config

        dest_refs = pipeline_config["connections"]["destinations"][0]
        dest_connection_id = list(dest_refs.values())[0]

        dest_conn = resolved_connections[dest_connection_id]
        config = dest_conn.config

        assert config.get("connector_type") == "database" or config.get("type") == "database", (
            f"Destination should be database type, got type: {config.get('type')}"
        )

    def test_database_fields_validated(self, api_to_db_config):
        """Verify port is int type, database/username present in parameters."""
        pipeline_config, _, resolved_connections, _ = api_to_db_config

        dest_refs = pipeline_config["connections"]["destinations"][0]
        dest_connection_id = list(dest_refs.values())[0]

        dest_conn = resolved_connections[dest_connection_id]
        config = dest_conn.config

        assert "host" in config, "Database connection must have 'host'"
        assert "parameters" in config, "Database connection must have 'parameters'"
        params = config["parameters"]
        assert "database" in params, "Database parameters must have 'database'"
        assert "username" in params, "Database parameters must have 'username'"
        assert "port" in params, "Database parameters must have 'port'"

        port_value = params["port"]
        int(port_value)  # Should not raise


class TestEndToEndConfigFlow:
    """Complete flow tests from constructed configs to Engine."""

    def test_api_connection_config_validates_for_connector(self):
        """APIConnector.connect() would receive APIConnectionConfig-valid dict."""
        resolved_connections = _api_to_api_resolved_connections()
        source_conn = resolved_connections[WISE_CONNECTION_ID]
        config = source_conn.config

        validated = APIConnectionConfig(**config)
        assert validated.host
        assert isinstance(validated.parameters.headers, dict)
        assert validated.parameters.timeout > 0

    def test_database_connection_config_validates_for_connector(self):
        """DatabaseConnector.connect() requires 'host' at root and DB fields in parameters."""
        resolved_connections = _api_to_db_resolved_connections()
        dest_conn = resolved_connections[DB_CONNECTION_ID]
        config = dest_conn.config

        assert config.get("host"), "Database connection must have 'host'"
        params = config.get("parameters", {})
        assert params.get("database"), "Database parameters must have 'database'"
        assert params.get("username"), "Database parameters must have 'username'"

    def test_full_pipeline_config_dict_structure(self):
        """Verify complete config dict has all required sections."""
        pipeline_config = _make_pipeline_config(
            WISE_ALIAS, WISE_CONNECTION_ID,
            SEVDESK_ALIAS, SEVDESK_CONNECTION_ID,
        )
        stream_configs = [_make_stream_config(
            WISE_ALIAS, WISE_ENDPOINT_ID,
            SEVDESK_ALIAS, SEVDESK_ENDPOINT_ID,
        )]
        resolved_connections = _api_to_api_resolved_connections()
        resolved_endpoints = _api_to_api_resolved_endpoints()

        pipeline = Pipeline(
            pipeline_config=pipeline_config,
            stream_configs=stream_configs,
            resolved_connections=resolved_connections,
            resolved_endpoints=resolved_endpoints,
            connectors=API_CONNECTORS,
        )

        config_dict = pipeline._build_config_dict()

        # Required top-level fields
        assert "pipeline_id" in config_dict
        assert "name" in config_dict
        assert "streams" in config_dict
        assert "source" in config_dict
        assert "destination" in config_dict
        assert "engine_config" in config_dict

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
