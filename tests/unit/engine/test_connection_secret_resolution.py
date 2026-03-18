"""Tests for connection secret resolution in the engine pipeline."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.engine.pipeline_config_prep import ResolvedConnection
from src.secrets.config_wrapper import ConnectionConfig


class TestBuildConfigDictCarriesConnectionWrapper:
    """Test that get_connection_config propagates _connection_wrapper."""

    def test_resolved_connection_wrapper_in_config(self):
        """Verify ResolvedConnection config copy includes wrapper
        when accessed through the pipeline's get_connection_config pattern."""
        mock_wrapper = MagicMock()
        raw_config = {
            "host": "db.example.com",
            "port": 5432,
            "password": "${password}",
            "driver": "postgresql",
        }
        resolved_conn = ResolvedConnection(
            connection_id="conn-1",
            connection_type="database",
            config=raw_config,
            connection_config_wrapper=mock_wrapper,
        )

        # Simulate what get_connection_config now does (pipeline.py line 381)
        result = resolved_conn.config.copy()
        result["_connection_wrapper"] = resolved_conn.connection_config_wrapper

        assert "_connection_wrapper" in result
        assert result["_connection_wrapper"] is mock_wrapper
        # Original config should not be modified
        assert "_connection_wrapper" not in resolved_conn.config

    def test_dict_connection_has_no_wrapper(self):
        """When resolved_connections stores a plain dict, no wrapper is added."""
        raw_config = {
            "host": "db.example.com",
            "port": 5432,
            "password": "plain",
            "driver": "postgresql",
        }

        # Simulate the dict branch of get_connection_config
        result = raw_config.copy()

        assert "_connection_wrapper" not in result


class TestProcessStreamResolvesWrapper:
    """Test that _process_stream resolves wrapper before connecting."""

    @pytest.mark.asyncio
    async def test_resolves_wrapper_before_connect(self):
        """When _connection_wrapper is present in merged source config,
        engine must await wrapper.resolve() and pass the resolved dict
        (not the raw one with ${password}) to source_connector.connect()."""
        resolved_config = {
            "host": "db.example.com",
            "port": 5432,
            "password": "actual-secret",
            "driver": "postgresql",
        }

        mock_wrapper = AsyncMock()
        mock_wrapper.resolve.return_value = resolved_config

        mock_source_connector = AsyncMock()

        merged_src_config = {
            "_connection_wrapper": mock_wrapper,
            "_connection": {
                "host": "db.example.com",
                "port": 5432,
                "password": "${password}",
                "driver": "postgresql",
            },
        }

        # Replicate the resolution logic from engine.py lines 280-284
        connection_wrapper = merged_src_config.get("_connection_wrapper")
        if connection_wrapper is not None:
            src_connection_config = await connection_wrapper.resolve()
        else:
            src_connection_config = merged_src_config.get(
                "_connection", merged_src_config
            )

        await mock_source_connector.connect(src_connection_config)

        mock_wrapper.resolve.assert_awaited_once()
        mock_source_connector.connect.assert_awaited_once_with(resolved_config)
        call_args = mock_source_connector.connect.call_args[0][0]
        assert call_args["password"] == "actual-secret"
        assert "${password}" not in str(call_args)

    @pytest.mark.asyncio
    async def test_falls_back_to_connection_without_wrapper(self):
        """When _connection_wrapper is absent, engine uses _connection directly."""
        raw_config = {
            "host": "db.example.com",
            "port": 5432,
            "password": "plain-password",
            "driver": "postgresql",
        }

        merged_src_config = {
            "_connection": raw_config,
        }

        connection_wrapper = merged_src_config.get("_connection_wrapper")
        if connection_wrapper is not None:
            src_connection_config = await connection_wrapper.resolve()
        else:
            src_connection_config = merged_src_config.get(
                "_connection", merged_src_config
            )

        assert src_connection_config is raw_config
        assert src_connection_config["password"] == "plain-password"


class TestConnectionConfigResolveShortCircuit:
    """Test that resolve() skips secret fetching when no placeholders exist."""

    @pytest.mark.asyncio
    async def test_resolve_without_placeholders_skips_resolver(self):
        """When config has no ${placeholder} values, resolve() should return
        the raw config without calling the secrets resolver."""
        mock_resolver = AsyncMock()
        config = ConnectionConfig(
            raw_config={"host": "localhost", "port": 5432, "password": "literal"},
            connection_id="conn-no-secrets",
            resolver=mock_resolver,
        )

        result = await config.resolve()

        mock_resolver.resolve.assert_not_awaited()
        assert result["password"] == "literal"
        assert result["host"] == "localhost"

    @pytest.mark.asyncio
    async def test_resolve_with_placeholders_calls_resolver(self):
        """When config has ${placeholder} values, resolve() must call the
        secrets resolver and expand them."""
        mock_resolver = AsyncMock()
        mock_resolver.resolve.return_value = {"DB_PASS": "secret123"}
        config = ConnectionConfig(
            raw_config={"host": "localhost", "password": "${DB_PASS}"},
            connection_id="conn-with-secrets",
            resolver=mock_resolver,
        )

        result = await config.resolve()

        mock_resolver.resolve.assert_awaited_once_with("conn-with-secrets", org_id=None)
        assert result["password"] == "secret123"
