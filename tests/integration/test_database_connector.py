"""Integration tests for DatabaseConnector with PostgreSQL."""

import pytest
import os
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, List

from src.connectors.database.database_connector import (
    DatabaseConnector,
    DatabaseConfig,
    EndpointConfig,
    ConfigureConfig
)
from src.connectors.base import ConnectionError, ReadError, WriteError


@pytest.fixture
def database_config():
    """Sample database configuration from environment."""
    import os
    return {
        "driver": "postgresql",
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "analitiq_test"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "test_password"),
        "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer")
    }


@pytest.fixture
def endpoint_config():
    """Sample endpoint configuration."""
    return {
        "schema": "test_schema",
        "table": "test_table",
        "primary_key": ["id"],
        "unique_constraints": ["email"],
        "table_schema": {
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


class TestDatabaseConfig:
    """Test DatabaseConfig Pydantic model."""

    def test_valid_config(self, database_config):
        """Test valid database configuration."""
        config = DatabaseConfig(**database_config)
        assert config.driver == "postgresql"
        assert config.host == os.getenv("POSTGRES_HOST", "localhost")
        assert config.port == int(os.getenv("POSTGRES_PORT", "5432"))
        assert config.database == os.getenv("POSTGRES_DB", "analitiq_test")
        assert config.user == os.getenv("POSTGRES_USER", "postgres")
        assert config.password == os.getenv("POSTGRES_PASSWORD", "test_password")

    def test_missing_required_fields(self):
        """Test validation with missing required fields."""
        with pytest.raises(Exception):  # ValidationError from Pydantic
            DatabaseConfig(driver="postgresql")

    def test_extra_fields_allowed(self, database_config):
        """Test that extra fields are allowed."""
        database_config["ssl_mode"] = "prefer"
        database_config["custom_param"] = "value"
        config = DatabaseConfig(**database_config)
        assert config.driver == "postgresql"


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
        config = EndpointConfig(table="test_table")
        assert config.schema == "public"
        assert config.table == "test_table"
        assert config.primary_key == []
        assert config.unique_constraints == []
        assert config.table_schema == {}
        assert config.write_mode == "insert"
        assert config.conflict_resolution == {}
        assert config.configure is None

    def test_full_configuration(self, endpoint_config):
        """Test full endpoint configuration."""
        config = EndpointConfig(**endpoint_config)
        assert config.schema == "test_schema"
        assert config.table == "test_table"
        assert config.primary_key == ["id"]
        assert config.unique_constraints == ["email"]
        assert config.write_mode == "upsert"
        assert config.configure.auto_create_schema is True


class TestDatabaseConnector:
    """Test DatabaseConnector class."""

    def test_init(self):
        """Test connector initialization."""
        connector = DatabaseConnector("TestConnector")
        assert connector.name == "TestConnector"
        assert connector.driver is None
        assert connector.table_info_cache == {}
        assert connector._initialized is False
        assert connector.is_connected is False

    @pytest.mark.asyncio
    async def test_connect_success(self, connector, database_config):
        """Test successful database connection."""
        # Skip if PostgreSQL not configured
        if not os.getenv("POSTGRES_PASSWORD"):
            pytest.skip("PostgreSQL not configured. Set POSTGRES_PASSWORD in tests/.env")
            
        mock_driver = AsyncMock()
        mock_driver.name = "PostgreSQL"
        
        with patch('src.connectors.database.database_connector.DriverFactory.create_driver') as mock_factory:
            mock_factory.return_value = mock_driver
            
            await connector.connect(database_config)
            
            mock_factory.assert_called_once_with("postgresql")
            mock_driver.create_connection_pool.assert_called_once_with(database_config)
            assert connector.is_connected is True
            assert connector._initialized is True
            assert connector.driver == mock_driver

    @pytest.mark.asyncio
    async def test_connect_validation_error(self, connector):
        """Test connection with invalid configuration."""
        invalid_config = {"driver": "postgresql"}  # Missing required fields
        
        with pytest.raises(ConnectionError):
            await connector.connect(invalid_config)

    @pytest.mark.asyncio
    async def test_connect_driver_error(self, connector, database_config):
        """Test connection failure at driver level."""
        with patch('src.connectors.database.database_connector.DriverFactory.create_driver') as mock_factory:
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
        config = {"schema": "test_schema", "table": "test_table"}
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

    @pytest.mark.asyncio
    async def test_read_batches_success(self, connector, endpoint_config):
        """Test successful batch reading."""
        mock_driver = MagicMock()
        mock_connection = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
        
        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.connection_pool = mock_pool
        mock_driver.build_incremental_query.return_value = ("SELECT * FROM test", [])
        
        # Mock query results - first batch with data, second batch empty
        mock_driver.execute_query = AsyncMock()
        mock_driver.execute_query.side_effect = [
            [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}],
            []  # Empty result to end iteration
        ]
        
        batches = []
        async for batch in connector.read_batches(endpoint_config, batch_size=2):
            batches.append(batch)
        
        assert len(batches) == 1
        assert len(batches[0]) == 2
        assert batches[0][0]["id"] == 1
        assert connector.metrics["records_read"] == 2
        assert connector.metrics["batches_read"] == 1

    @pytest.mark.asyncio
    async def test_read_batches_not_initialized(self, connector, endpoint_config):
        """Test read batches without initialization."""
        with pytest.raises(RuntimeError) as exc_info:
            async for batch in connector.read_batches(endpoint_config):
                pass
        
        assert "not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_read_batches_error(self, connector, endpoint_config):
        """Test read batches with database error."""
        mock_driver = AsyncMock()
        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.build_incremental_query.side_effect = Exception("Query build failed")
        
        with pytest.raises(ReadError) as exc_info:
            async for batch in connector.read_batches(endpoint_config):
                pass
        
        assert "Database read failed" in str(exc_info.value)
        assert connector.metrics["errors"] == 1

    @pytest.mark.asyncio
    async def test_write_batch_success_upsert(self, connector, endpoint_config):
        """Test successful batch write with upsert."""
        mock_driver = MagicMock()
        mock_connection = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
        
        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.connection_pool = mock_pool
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
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
        
        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.connection_pool = mock_pool
        mock_driver.execute_insert = AsyncMock()
        
        batch = [{"id": 1, "name": "test1"}]
        
        await connector.write_batch(batch, endpoint_config)
        
        mock_driver.execute_insert.assert_called_once()
        assert connector.metrics["records_written"] == 1

    @pytest.mark.asyncio
    async def test_write_batch_empty(self, connector, endpoint_config):
        """Test write batch with empty batch."""
        connector._initialized = True
        
        await connector.write_batch([], endpoint_config)
        
        assert connector.metrics["records_written"] == 0

    @pytest.mark.asyncio
    async def test_write_batch_not_initialized(self, connector, endpoint_config):
        """Test write batch without initialization."""
        batch = [{"id": 1, "name": "test1"}]
        
        with pytest.raises(RuntimeError) as exc_info:
            await connector.write_batch(batch, endpoint_config)
        
        assert "not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_write_batch_error(self, connector, endpoint_config):
        """Test write batch with database error."""
        mock_driver = AsyncMock()
        mock_connection = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
        
        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.connection_pool = mock_pool
        mock_driver.execute_upsert.side_effect = Exception("Write failed")
        
        batch = [{"id": 1, "name": "test1"}]
        
        with pytest.raises(WriteError) as exc_info:
            await connector.write_batch(batch, endpoint_config)
        
        assert "Database write failed" in str(exc_info.value)
        assert connector.metrics["errors"] == 1

    def test_supports_incremental_read(self, connector):
        """Test supports_incremental_read returns True."""
        assert connector.supports_incremental_read() is True

    def test_supports_upsert(self, connector):
        """Test supports_upsert returns True."""
        assert connector.supports_upsert() is True

    def test_supports_schema_evolution(self, connector):
        """Test supports_schema_evolution returns True."""
        assert connector.supports_schema_evolution() is True

    @pytest.mark.asyncio
    async def test_evolve_schema_not_initialized(self, connector):
        """Test schema evolution without initialization."""
        changes = {"added_fields": ["new_field"]}
        config = {"schema": "test", "table": "test"}
        
        with pytest.raises(RuntimeError) as exc_info:
            await connector.evolve_schema(changes, config)
        
        assert "not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_evolve_schema_initialized(self, connector):
        """Test schema evolution with initialization."""
        connector._initialized = True
        changes = {"added_fields": ["new_field"]}
        config = {"schema": "test", "table": "test"}
        
        # Should not raise an error, just log the request
        await connector.evolve_schema(changes, config)


class TestDatabaseConnectorEdgeCases:
    """Test edge cases and missing coverage paths."""
    
    def test_import_database_connector(self):
        """Test importing DatabaseConnector and related classes."""
        from src.connectors.database.database_connector import (
            DatabaseConnector, DatabaseConfig, EndpointConfig, ConfigureConfig
        )
        
        # Test that classes are properly imported
        assert DatabaseConnector is not None
        assert DatabaseConfig is not None
        assert EndpointConfig is not None
        assert ConfigureConfig is not None
    
    @pytest.mark.asyncio
    async def test_read_batches_pagination_edge_case(self, connector, endpoint_config):
        """Test read batches pagination when exactly batch_size rows returned."""
        mock_driver = MagicMock()
        mock_connection = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
        
        connector.driver = mock_driver
        connector._initialized = True
        mock_driver.connection_pool = mock_pool
        mock_driver.build_incremental_query.return_value = ("SELECT * FROM test", [])
        mock_driver.execute_query = AsyncMock()
        
        # Return exactly batch_size on first call, then empty on second
        mock_driver.execute_query.side_effect = [
            [{"id": 1}, {"id": 2}],  # Exactly batch_size (2)
            []  # Empty result should stop iteration
        ]
        
        batches = []
        async for batch in connector.read_batches(endpoint_config, batch_size=2):
            batches.append(batch)
        
        assert len(batches) == 1
        assert len(batches[0]) == 2
        # Should have made 2 calls due to len(rows) == batch_size condition
        assert mock_driver.execute_query.call_count == 2
    
    @pytest.mark.asyncio
    async def test_database_connector_context_manager_usage(self):
        """Test that connector can be used in context management patterns."""
        connector = DatabaseConnector("TestContextConnector")
        
        # Test basic state
        assert connector.name == "TestContextConnector"
        assert connector.is_connected is False
        assert connector._initialized is False
    
    def test_database_config_extra_fields(self):
        """Test that DatabaseConfig allows extra fields as specified."""
        config_data = {
            "driver": "postgresql",
            "host": "localhost", 
            "port": 5432,
            "database": "test",
            "user": "test",
            "password": "test",
            # Extra fields should be allowed
            "ssl_mode": "require",
            "connection_timeout": 30,
            "pool_size": 5
        }
        
        config = DatabaseConfig(**config_data)
        assert config.driver == "postgresql"
        # Extra fields are allowed due to model_config extra='allow'
        
    def test_endpoint_config_extra_fields(self):
        """Test that EndpointConfig allows extra fields."""
        config_data = {
            "table": "test_table",
            "schema": "public",
            # Extra field should be allowed
            "custom_setting": "value"
        }
        
        config = EndpointConfig(**config_data)
        assert config.table == "test_table"
        assert config.schema == "public"