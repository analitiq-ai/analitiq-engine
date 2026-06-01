"""Unit tests for DatabaseConnector."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, List

from src.source.connectors.database import DatabaseConnector
from src.source.connectors.base import ConnectionError, ReadError
from cdk.connection_runtime import ConnectionRuntime
from cdk.transport_factory import SqlAlchemyTransport


@pytest.fixture
def database_config():
    """Sample database configuration for unit tests."""
    return {
        "connector_slug": "postgres",
        "parameters": {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
        },
        "secret_refs": {"password": "tests/postgres/password"},
    }


@pytest.fixture
def database_connector_def():
    """Minimal connector definition with a sqlalchemy transport."""
    return {
        "slug": "postgres",
        "connector_type": "database",
        "default_transport": "database",
        "transports": {
            "database": {
                "kind": "sqlalchemy",
                "driver": "postgresql+asyncpg",
                "dsn": {"template": "postgresql+asyncpg://u:p@h:5432/d"},
            }
        },
    }


@pytest.fixture
def database_runtime(database_config, database_connector_def):
    """ConnectionRuntime for database tests, wired for the new transport factory."""
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value={"password": "test_password"})
    return ConnectionRuntime(
        raw_config=database_config,
        connection_id="test-conn",
        connector_type="database",
        resolver=resolver,
        connector_definition=database_connector_def,
    )


@pytest.fixture
def endpoint_config():
    """Sample endpoint configuration."""
    return {
        "endpoint": "test_schema/test_table",
        "primary_key": ["id"],
        "unique_constraints": ["email"],
        "endpoint_schema": {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "email": {"type": "string", "maxLength": 255},
                "created_at": {"type": "string", "format": "date-time"}
            }
        },
        "write_mode": "upsert",
        "conflict_resolution": {
            "on_conflict": "id",
            "action": "update",
            "update_columns": ["email", "created_at"]
        },
    }


@pytest.fixture
def connector():
    """DatabaseConnector instance."""
    return DatabaseConnector("TestConnector")


@pytest.fixture
def mock_state_manager():
    """Mock state manager for read_batches tests."""
    state_manager = AsyncMock()
    state_manager.get_cursor = AsyncMock(return_value=None)
    state_manager.update_cursor = AsyncMock()
    return state_manager


class TestDatabaseConnectorInit:
    """Test DatabaseConnector initialization."""

    def test_init(self):
        """Test connector initialization."""
        connector = DatabaseConnector("TestConnector")
        assert connector.name == "TestConnector"
        assert connector._engine is None
        assert connector._driver == ""
        assert connector._initialized is False
        assert connector.is_connected is False


class TestDatabaseConnectorConnection:
    """Test DatabaseConnector connection management."""

    @pytest.mark.asyncio
    async def test_connect_success(self, connector, database_runtime):
        """Test successful database connection."""
        mock_engine = AsyncMock()
        transport = SqlAlchemyTransport(
            engine=mock_engine,
            driver="postgresql+asyncpg",
            dialect="postgresql",
        )

        with patch(
            'cdk.connection_runtime.build_transport',
            new=AsyncMock(return_value=transport),
        ):
            await connector.connect(database_runtime)

            assert connector.is_connected is True
            assert connector._initialized is True
            assert connector._engine is mock_engine
            assert connector._driver == "postgresql"

    @pytest.mark.asyncio
    async def test_connect_engine_error(self, connector, database_runtime):
        """Test connection failure at engine level."""
        with patch(
            'cdk.connection_runtime.build_transport',
            new=AsyncMock(side_effect=Exception("Engine creation failed")),
        ):
            with pytest.raises(ConnectionError) as exc_info:
                await connector.connect(database_runtime)

            assert "Database connection failed" in str(exc_info.value)
            assert connector.is_connected is False
            assert connector._initialized is False

    @pytest.mark.asyncio
    async def test_disconnect(self, connector):
        """Test database disconnection."""
        mock_engine = AsyncMock()
        mock_runtime = AsyncMock()
        connector._runtime = mock_runtime
        connector._engine = mock_engine
        connector.is_connected = True
        connector._initialized = True

        await connector.disconnect()

        mock_runtime.close.assert_awaited_once()
        assert connector._engine is None
        assert connector.is_connected is False
        assert connector._initialized is False

    @pytest.mark.asyncio
    async def test_disconnect_no_engine(self, connector):
        """Test disconnect when engine is None."""
        connector.is_connected = True
        connector._initialized = True

        await connector.disconnect()

        assert connector.is_connected is False
        assert connector._initialized is False


    """Test DatabaseConnector write_batch raises NotImplementedError."""

    @pytest.mark.asyncio
    async def test_write_batch_raises(self, connector):
        """Source connector write_batch should raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="read-only"):
            await connector.write_batch([{"id": 1}], {})


class TestDatabaseConnectorContextManager:
    """Test DatabaseConnector context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_usage(self, database_runtime):
        """Test connector as context manager."""
        mock_engine = AsyncMock()
        transport = SqlAlchemyTransport(
            engine=mock_engine,
            driver="postgresql+asyncpg",
            dialect="postgresql",
        )

        with patch(
            'cdk.connection_runtime.build_transport',
            new=AsyncMock(return_value=transport),
        ):
            connector = DatabaseConnector("ContextTestConnector")
            await connector.connect(database_runtime)

            assert connector.is_connected is True

            await connector.disconnect()

            assert connector.is_connected is False


class TestDatabaseConnectorImports:
    """Test DatabaseConnector imports and initialization."""

    def test_import_database_connector(self):
        """Test importing DatabaseConnector."""
        from src.source.connectors.database import DatabaseConnector

        assert DatabaseConnector is not None


