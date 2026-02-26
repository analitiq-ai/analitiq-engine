"""Unit tests for DatabaseConnector."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, List

from src.source.connectors.database import (
    DatabaseConnector,
    EndpointConfig,
    ConfigureConfig
)
from src.source.connectors.base import ConnectionError, ReadError, WriteError


@pytest.fixture
def database_config():
    """Sample database configuration for unit tests."""
    return {
        "driver": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "username": "test_user",
        "password": "test_password",
    }


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
        "configure": {
            "auto_create_schema": True,
            "auto_create_table": True,
            "auto_create_indexes": [
                {
                    "name": "idx_email",
                    "columns": ["email"],
                    "type": "btree"
                }
            ]
        }
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


class TestDatabaseConnectorConnectValidation:
    """Test DatabaseConnector connect() validation."""

    @pytest.mark.asyncio
    async def test_connect_missing_driver(self):
        """Test connect raises error when driver is missing."""
        connector = DatabaseConnector("TestConnector")
        with pytest.raises(ConnectionError, match="missing required 'driver' field"):
            await connector.connect({"host": "localhost", "port": 5432})

    @pytest.mark.asyncio
    async def test_connect_works_without_explicit_credential_keys(self):
        """Test connect works without host/port/username/password/database keys.

        extract_connection_params in the driver provides defaults, so the
        Pydantic gate should not block connections that omit these fields.
        """
        mock_driver = AsyncMock()
        with patch('src.source.connectors.database.DriverFactory.create_driver') as mock_factory:
            mock_factory.return_value = mock_driver
            connector = DatabaseConnector("TestConnector")
            # Only driver is required at the connector level
            await connector.connect({"driver": "postgresql"})
            mock_factory.assert_called_once_with("postgresql")
            mock_driver.create_connection_pool.assert_called_once()
            assert connector.is_connected is True


class TestConfigureConfig:
    """Test ConfigureConfig Pydantic model."""

    def test_default_values(self):
        """Test default configuration values."""
        config = ConfigureConfig()
        assert config.auto_create_schema is False
        assert config.auto_create_table is False
        assert config.auto_create_indexes == []

    def test_custom_values(self):
        """Test custom configuration values."""
        config = ConfigureConfig(
            auto_create_schema=True,
            auto_create_table=True,
            auto_create_indexes=[{"name": "idx_test", "columns": ["test_col"]}]
        )
        assert config.auto_create_schema is True
        assert config.auto_create_table is True
        assert len(config.auto_create_indexes) == 1

    def test_extra_fields_forbidden(self):
        """Test that extra fields are forbidden."""
        with pytest.raises(Exception):  # ValidationError from Pydantic
            ConfigureConfig(invalid_field="value")


class TestEndpointConfig:
    """Test EndpointConfig Pydantic model."""

    def test_default_values(self):
        """Test default endpoint configuration values."""
        config = EndpointConfig(endpoint="test_table")
        assert config.endpoint == "test_table"
        assert config.primary_key == []
        assert config.unique_constraints == []
        assert config.endpoint_schema == {}
        assert config.write_mode == "insert"
        assert config.conflict_resolution == {}
        assert config.configure is None

    def test_full_configuration(self, endpoint_config):
        """Test full endpoint configuration."""
        config = EndpointConfig(**endpoint_config)
        assert config.endpoint == "test_schema/test_table"
        assert config.primary_key == ["id"]
        assert config.unique_constraints == ["email"]
        assert config.write_mode == "upsert"
        assert config.configure.auto_create_schema is True

    def test_endpoint_config_extra_fields(self):
        """Test that EndpointConfig allows extra fields."""
        config_data = {
            "endpoint": "public/test_table",
            # Extra field should be allowed
            "custom_setting": "value"
        }

        config = EndpointConfig(**config_data)
        assert config.endpoint == "public/test_table"


class TestDatabaseConnectorInit:
    """Test DatabaseConnector initialization."""

    def test_init(self):
        """Test connector initialization."""
        connector = DatabaseConnector("TestConnector")
        assert connector.name == "TestConnector"
        assert connector.driver is None
        assert connector.table_info_cache == {}
        assert connector._initialized is False
        assert connector.is_connected is False


class TestDatabaseConnectorConnection:
    """Test DatabaseConnector connection management."""

    @pytest.mark.asyncio
    async def test_connect_success(self, connector, database_config):
        """Test successful database connection."""
        mock_driver = AsyncMock()
        mock_driver.name = "PostgreSQL"

        with patch('src.source.connectors.database.DriverFactory.create_driver') as mock_factory:
            mock_factory.return_value = mock_driver

            await connector.connect(database_config)

            mock_factory.assert_called_once_with("postgresql")
            mock_driver.create_connection_pool.assert_called_once_with(database_config)
            assert connector.is_connected is True
            assert connector._initialized is True
            assert connector.driver == mock_driver

    @pytest.mark.asyncio
    async def test_connect_missing_driver_field(self, connector):
        """Test connection with missing driver field."""
        invalid_config = {"host": "localhost"}  # Missing driver

        with pytest.raises(ConnectionError, match="missing required 'driver' field"):
            await connector.connect(invalid_config)

    @pytest.mark.asyncio
    async def test_connect_driver_error(self, connector, database_config):
        """Test connection failure at driver level."""
        with patch('src.source.connectors.database.DriverFactory.create_driver') as mock_factory:
            mock_factory.side_effect = Exception("Driver creation failed")

            with pytest.raises(ConnectionError) as exc_info:
                await connector.connect(database_config)

            assert "Database connection failed" in str(exc_info.value)
            assert connector.is_connected is False
            assert connector._initialized is False

    @pytest.mark.asyncio
    async def test_disconnect(self, connector):
        """Test database disconnection."""
        mock_driver = AsyncMock()
        connector.driver = mock_driver
        connector.is_connected = True
        connector._initialized = True

        await connector.disconnect()

        mock_driver.close_connection_pool.assert_called_once()
        assert connector.is_connected is False
        assert connector._initialized is False


class TestDatabaseConnectorConfigure:
    """Test DatabaseConnector configure method."""

    @pytest.mark.asyncio
    async def test_configure_success(self, connector, endpoint_config):
        """Test successful database configuration."""
        mock_driver = AsyncMock()
        connector.driver = mock_driver
        connector._initialized = True

        await connector.configure(endpoint_config)

        mock_driver.create_schema_if_not_exists.assert_called_once_with("test_schema")
        mock_driver.create_table_if_not_exists.assert_called_once()
        mock_driver.create_indexes_if_not_exist.assert_called_once()

    @pytest.mark.asyncio
    async def test_configure_no_configure_section(self, connector):
        """Test configuration without configure section."""
        config = {"endpoint": "test_schema/test_table"}
        connector._initialized = True

        await connector.configure(config)
        # Should complete without errors, just log that no config section found

    @pytest.mark.asyncio
    async def test_configure_not_initialized(self, connector, endpoint_config):
        """Test configuration without initialization."""
        with pytest.raises(RuntimeError) as exc_info:
            await connector.configure(endpoint_config)

        assert "not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_configure_validation_error(self, connector):
        """Test configuration with invalid endpoint config."""
        invalid_config = {}  # Missing required table field
        connector._initialized = True

        with pytest.raises(ConnectionError):
            await connector.configure(invalid_config)


class TestDatabaseConnectorReadBatches:
    """Test DatabaseConnector read_batches method."""

    @pytest.mark.asyncio
    async def test_read_batches_success(self, connector, endpoint_config, mock_state_manager):
        """Test successful batch reading."""
        mock_driver = MagicMock()
        mock_connection = AsyncMock()

        # Mock acquire_connection as async context manager
        mock_acm = AsyncMock()
        mock_acm.__aenter__.return_value = mock_connection
        mock_acm.__aexit__.return_value = False
        mock_driver.acquire_connection.return_value = mock_acm

        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.build_incremental_query.return_value = ("SELECT * FROM test", [])

        # Mock query results - first batch with data, second batch empty
        mock_driver.execute_query = AsyncMock()
        mock_driver.execute_query.side_effect = [
            [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}],
            []  # Empty result to end iteration
        ]

        batches = []
        async for batch in connector.read_batches(
            endpoint_config,
            state_manager=mock_state_manager,
            stream_name="test_stream",
            batch_size=2
        ):
            batches.append(batch)

        assert len(batches) == 1
        assert len(batches[0]) == 2
        assert batches[0][0]["id"] == 1
        assert connector.metrics["records_read"] == 2
        assert connector.metrics["batches_read"] == 1

    @pytest.mark.asyncio
    async def test_read_batches_not_initialized(self, connector, endpoint_config, mock_state_manager):
        """Test read batches without initialization."""
        with pytest.raises(RuntimeError) as exc_info:
            async for batch in connector.read_batches(
                endpoint_config,
                state_manager=mock_state_manager,
                stream_name="test_stream"
            ):
                pass

        assert "not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_read_batches_error(self, connector, endpoint_config, mock_state_manager):
        """Test read batches with database error."""
        mock_driver = AsyncMock()
        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.build_incremental_query.side_effect = Exception("Query build failed")

        with pytest.raises(ReadError) as exc_info:
            async for batch in connector.read_batches(
                endpoint_config,
                state_manager=mock_state_manager,
                stream_name="test_stream"
            ):
                pass

        assert "Database read failed" in str(exc_info.value)
        assert connector.metrics["errors"] == 1

    @pytest.mark.asyncio
    async def test_read_batches_pagination_edge_case(self, connector, endpoint_config, mock_state_manager):
        """Test read_batches pagination with different result sizes."""
        mock_driver = MagicMock()
        mock_connection = AsyncMock()

        mock_acm = AsyncMock()
        mock_acm.__aenter__.return_value = mock_connection
        mock_acm.__aexit__.return_value = False
        mock_driver.acquire_connection.return_value = mock_acm

        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.build_incremental_query.return_value = ("SELECT * FROM test", [])

        # Return exactly batch_size records, then fewer, then empty
        mock_driver.execute_query = AsyncMock()
        mock_driver.execute_query.side_effect = [
            [{"id": 1}, {"id": 2}],
            [{"id": 3}],  # Fewer than batch_size
            []  # Empty
        ]

        batches = []
        async for batch in connector.read_batches(
            endpoint_config,
            state_manager=mock_state_manager,
            stream_name="test_stream",
            batch_size=2
        ):
            batches.append(batch)

        assert len(batches) == 2
        assert connector.metrics["records_read"] == 3


class TestDatabaseConnectorWriteBatch:
    """Test DatabaseConnector write_batch method."""

    @pytest.mark.asyncio
    async def test_write_batch_success_upsert(self, connector, endpoint_config):
        """Test successful batch write with upsert."""
        mock_driver = MagicMock()
        mock_connection = AsyncMock()

        mock_acm = AsyncMock()
        mock_acm.__aenter__.return_value = mock_connection
        mock_acm.__aexit__.return_value = False
        mock_driver.acquire_connection.return_value = mock_acm

        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.execute_upsert = AsyncMock()

        batch = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

        await connector.write_batch(batch, endpoint_config)

        mock_driver.execute_upsert.assert_called_once()
        assert connector.metrics["records_written"] == 2
        assert connector.metrics["batches_written"] == 1

    @pytest.mark.asyncio
    async def test_write_batch_success_insert(self, connector, endpoint_config):
        """Test successful batch write with insert."""
        endpoint_config["write_mode"] = "insert"

        mock_driver = MagicMock()
        mock_connection = AsyncMock()

        mock_acm = AsyncMock()
        mock_acm.__aenter__.return_value = mock_connection
        mock_acm.__aexit__.return_value = False
        mock_driver.acquire_connection.return_value = mock_acm

        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.execute_insert = AsyncMock()

        batch = [{"id": 1, "name": "test1"}]

        await connector.write_batch(batch, endpoint_config)

        mock_driver.execute_insert.assert_called_once()
        assert connector.metrics["records_written"] == 1

    @pytest.mark.asyncio
    async def test_write_batch_empty(self, connector, endpoint_config):
        """Test write batch with empty batch."""
        connector._initialized = True

        # Empty batch should not raise an error
        await connector.write_batch([], endpoint_config)
        assert connector.metrics["records_written"] == 0

    @pytest.mark.asyncio
    async def test_write_batch_not_initialized(self, connector, endpoint_config):
        """Test write batch without initialization."""
        with pytest.raises(RuntimeError) as exc_info:
            await connector.write_batch([{"id": 1}], endpoint_config)

        assert "not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_write_batch_error(self, connector, endpoint_config):
        """Test write batch with database error."""
        mock_driver = MagicMock()
        mock_connection = AsyncMock()

        mock_acm = AsyncMock()
        mock_acm.__aenter__.return_value = mock_connection
        mock_acm.__aexit__.return_value = False
        mock_driver.acquire_connection.return_value = mock_acm

        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.execute_upsert = AsyncMock(side_effect=Exception("DB error"))

        with pytest.raises(WriteError) as exc_info:
            await connector.write_batch([{"id": 1}], endpoint_config)

        assert "Database write failed" in str(exc_info.value)
        assert connector.metrics["errors"] == 1


class TestDatabaseConnectorCapabilities:
    """Test DatabaseConnector capability methods."""

    def test_supports_incremental_read(self, connector):
        """Test incremental read support."""
        mock_driver = MagicMock()
        mock_driver.supports_incremental = True
        connector.driver = mock_driver

        assert connector.supports_incremental_read() is True

    def test_supports_upsert(self, connector):
        """Test upsert support."""
        mock_driver = MagicMock()
        mock_driver.supports_upsert = True
        connector.driver = mock_driver

        assert connector.supports_upsert() is True

    def test_supports_schema_evolution(self, connector):
        """Test schema evolution support."""
        mock_driver = MagicMock()
        mock_driver.supports_schema_evolution = True
        connector.driver = mock_driver

        assert connector.supports_schema_evolution() is True

    @pytest.mark.asyncio
    async def test_evolve_schema_not_initialized(self, connector):
        """Test evolve schema without initialization."""
        with pytest.raises(RuntimeError) as exc_info:
            await connector.evolve_schema({}, {})

        assert "not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_evolve_schema_initialized(self, connector):
        """Test evolve schema with initialization."""
        mock_driver = AsyncMock()
        connector.driver = mock_driver
        connector._initialized = True

        # Should not raise an error when initialized
        await connector.evolve_schema({"table": "test"}, {"new_column": {"type": "string"}})


class TestDatabaseConnectorContextManager:
    """Test DatabaseConnector context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_usage(self, database_config):
        """Test connector as context manager."""
        mock_driver = AsyncMock()
        mock_driver.name = "PostgreSQL"

        with patch('src.source.connectors.database.DriverFactory.create_driver') as mock_factory:
            mock_factory.return_value = mock_driver

            connector = DatabaseConnector("ContextTestConnector")
            await connector.connect(database_config)

            assert connector.is_connected is True

            await connector.disconnect()

            mock_driver.close_connection_pool.assert_called_once()


class TestDatabaseConnectorImports:
    """Test DatabaseConnector imports and initialization."""

    def test_import_database_connector(self):
        """Test importing DatabaseConnector and related components."""
        from src.source.connectors.database import (
            DatabaseConnector,
            EndpointConfig,
            ConfigureConfig
        )

        assert DatabaseConnector is not None
        assert EndpointConfig is not None
        assert ConfigureConfig is not None
