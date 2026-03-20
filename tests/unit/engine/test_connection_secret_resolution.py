"""Tests for ConnectionRuntime secret resolution and lifecycle."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.shared.connection_runtime import ConnectionRuntime


class TestConnectionRuntimeMetadata:
    """Test that ConnectionRuntime exposes enriched metadata before materialize."""

    def test_connector_type_available(self):
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost"},
            connection_id="conn-1",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )
        assert runtime.connector_type == "database"
        assert runtime.driver == "postgresql"
        assert runtime.connection_id == "conn-1"

    def test_raw_config_available(self):
        config = {"host": "localhost", "parameters": {"port": 5432}}
        runtime = ConnectionRuntime(
            raw_config=config,
            connection_id="conn-1",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )
        assert runtime.raw_config is config


class TestConnectionRuntimeResolveSecrets:
    """Test secret resolution inside materialize."""

    @pytest.mark.asyncio
    async def test_resolve_without_placeholders_skips_resolver(self):
        mock_resolver = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "parameters": {"port": 5432, "password": "literal"}},
            connection_id="conn-no-secrets",
            connector_type="file",
            driver=None,
            resolver=mock_resolver,
        )

        await runtime.materialize()

        mock_resolver.resolve.assert_not_awaited()
        assert runtime.resolved_config["host"] == "localhost"

    @pytest.mark.asyncio
    async def test_resolve_with_placeholders_calls_resolver(self):
        mock_resolver = AsyncMock()
        mock_resolver.resolve.return_value = {"DB_PASS": "secret123"}
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "parameters": {"password": "${DB_PASS}"}},
            connection_id="conn-with-secrets",
            connector_type="file",
            driver=None,
            resolver=mock_resolver,
        )

        await runtime.materialize()

        mock_resolver.resolve.assert_awaited_once_with("conn-with-secrets", org_id=None)
        assert runtime.resolved_config["parameters"]["password"] == "secret123"


class TestConnectionRuntimeMaterialize:
    """Test materialize creates correct transport per connector_type."""

    @pytest.mark.asyncio
    async def test_materialize_is_idempotent(self):
        mock_resolver = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "file_format": "jsonl"},
            connection_id="conn-1",
            connector_type="stdout",
            driver=None,
            resolver=mock_resolver,
        )

        await runtime.materialize()
        first_config = runtime.resolved_config

        # Second call is a no-op
        await runtime.materialize()
        assert runtime.resolved_config is first_config

    @pytest.mark.asyncio
    async def test_transport_accessors_raise_before_materialize(self):
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost"},
            connection_id="conn-1",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )

        with pytest.raises(RuntimeError, match="call materialize"):
            _ = runtime.engine

        with pytest.raises(RuntimeError, match="call materialize"):
            _ = runtime.session

    @pytest.mark.asyncio
    async def test_materialize_database(self):
        mock_resolver = AsyncMock()
        mock_engine = AsyncMock()

        runtime = ConnectionRuntime(
            raw_config={
                "host": "localhost",
                "driver": "postgresql",
                "parameters": {"port": 5432, "database": "test", "username": "user", "password": "pass"},
            },
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=mock_resolver,
        )

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(mock_engine, "postgresql")) as mock_create:
            await runtime.materialize(require_port=True)

            mock_create.assert_awaited_once()
            assert runtime.engine is mock_engine

    @pytest.mark.asyncio
    async def test_materialize_api(self):
        mock_resolver = AsyncMock()
        mock_session = AsyncMock()
        mock_rate_limiter = MagicMock()

        runtime = ConnectionRuntime(
            raw_config={
                "host": "https://api.example.com",
                "parameters": {"timeout": 30},
            },
            connection_id="conn-api",
            connector_type="api",
            driver=None,
            resolver=mock_resolver,
        )

        with patch(
            "src.shared.connection_runtime.create_api_session",
            return_value=(mock_session, "https://api.example.com", mock_rate_limiter),
        ):
            await runtime.materialize()

            assert runtime.session is mock_session
            assert runtime.base_url == "https://api.example.com"
            assert runtime.rate_limiter is mock_rate_limiter

    @pytest.mark.asyncio
    async def test_secrets_scrubbed_after_materialize_database(self):
        mock_resolver = AsyncMock()
        mock_resolver.resolve.return_value = {"DB_PASS": "secret"}

        runtime = ConnectionRuntime(
            raw_config={
                "host": "localhost",
                "driver": "postgresql",
                "parameters": {"password": "${DB_PASS}"},
            },
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=mock_resolver,
        )

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(AsyncMock(), "postgresql")):
            await runtime.materialize()

        # Secrets should be scrubbed
        assert runtime._secrets is None
        # For database, resolved config should also be scrubbed
        assert runtime._resolved_config is None


class TestConnectionRuntimeClose:
    """Test close disposes resources."""

    @pytest.mark.asyncio
    async def test_close_disposes_engine(self):
        mock_engine = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "driver": "postgresql", "parameters": {"port": 5432, "database": "test", "username": "u", "password": "p"}},
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(mock_engine, "postgresql")):
            await runtime.materialize()

        await runtime.close()
        mock_engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_double_close_is_safe(self):
        mock_engine = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "driver": "postgresql", "parameters": {"port": 5432, "database": "test", "username": "u", "password": "p"}},
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(mock_engine, "postgresql")):
            await runtime.materialize()

        await runtime.close()
        await runtime.close()  # Should not raise
