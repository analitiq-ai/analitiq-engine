"""Unit tests for DatabaseConnector."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, List

from src.source.connectors.database import DatabaseConnector
from src.source.connectors.base import ConnectionError, ReadError
from src.shared.connection_runtime import ConnectionRuntime


@pytest.fixture
def database_config():
    """Sample database configuration for unit tests."""
    return {
        "driver": "postgresql",
        "host": "localhost",
        "parameters": {
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
        },
    }


@pytest.fixture
def database_runtime(database_config):
    """ConnectionRuntime for database tests."""
    return ConnectionRuntime(
        raw_config=database_config,
        connection_id="test-conn",
        connector_type="database",
        driver="postgresql",
        resolver=AsyncMock(),
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
        assert connector.table_info_cache == {}
        assert connector._initialized is False
        assert connector.is_connected is False


class TestDatabaseConnectorConnection:
    """Test DatabaseConnector connection management."""

    @pytest.mark.asyncio
    async def test_connect_success(self, connector, database_runtime):
        """Test successful database connection."""
        mock_engine = AsyncMock()

        with patch(
            'src.shared.connection_runtime.create_database_engine',
            return_value=(mock_engine, "postgresql"),
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
            'src.shared.connection_runtime.create_database_engine',
            side_effect=Exception("Engine creation failed"),
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


class TestDatabaseConnectorReadBatches:
    """Test DatabaseConnector read_batches method."""

    @pytest.mark.asyncio
    async def test_read_batches_success(self, connector, endpoint_config, mock_state_manager):
        """Test successful batch reading."""
        mock_engine = AsyncMock()
        mock_conn = AsyncMock()

        # Mock result rows
        mock_row1 = MagicMock()
        mock_row1._mapping = {"id": 1, "name": "test1"}
        mock_row2 = MagicMock()
        mock_row2._mapping = {"id": 2, "name": "test2"}
        mock_result_batch1 = MagicMock()
        mock_result_batch1.__iter__ = lambda self: iter([mock_row1, mock_row2])
        mock_result_empty = MagicMock()
        mock_result_empty.__iter__ = lambda self: iter([])

        mock_conn.exec_driver_sql = AsyncMock(
            side_effect=[mock_result_batch1, mock_result_empty]
        )

        connector._engine = mock_engine
        connector._initialized = True
        connector._driver = "postgresql"

        with patch(
            'src.source.connectors.database.acquire_connection',
        ) as mock_acquire, patch(
            'src.source.connectors.database.build_select_query',
            return_value=("SELECT * FROM test", []),
        ):
            mock_acm = AsyncMock()
            mock_acm.__aenter__.return_value = mock_conn
            mock_acm.__aexit__.return_value = False
            mock_acquire.return_value = mock_acm

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
        """Test read batches with query build error."""
        connector._engine = AsyncMock()
        connector._initialized = True
        connector._driver = "postgresql"

        with patch(
            'src.source.connectors.database.build_select_query',
            side_effect=Exception("Query build failed"),
        ):
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
        mock_engine = AsyncMock()
        mock_conn = AsyncMock()

        # Batch 1: exactly batch_size records
        row1 = MagicMock(); row1._mapping = {"id": 1}
        row2 = MagicMock(); row2._mapping = {"id": 2}
        result1 = MagicMock(); result1.__iter__ = lambda self: iter([row1, row2])

        # Batch 2: fewer than batch_size
        row3 = MagicMock(); row3._mapping = {"id": 3}
        result2 = MagicMock(); result2.__iter__ = lambda self: iter([row3])

        mock_conn.exec_driver_sql = AsyncMock(side_effect=[result1, result2])

        connector._engine = mock_engine
        connector._initialized = True
        connector._driver = "postgresql"

        with patch(
            'src.source.connectors.database.acquire_connection',
        ) as mock_acquire, patch(
            'src.source.connectors.database.build_select_query',
            return_value=("SELECT * FROM test", []),
        ):
            mock_acm = AsyncMock()
            mock_acm.__aenter__.return_value = mock_conn
            mock_acm.__aexit__.return_value = False
            mock_acquire.return_value = mock_acm

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

        with patch(
            'src.shared.connection_runtime.create_database_engine',
            return_value=(mock_engine, "postgresql"),
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


class TestParseEndpoint:
    """Test _parse_endpoint method."""

    def test_schema_slash_table(self, connector):
        schema, table = connector._parse_endpoint("myschema/mytable")
        assert schema == "myschema"
        assert table == "mytable"

    def test_table_only(self, connector):
        schema, table = connector._parse_endpoint("mytable")
        assert schema == "public"
        assert table == "mytable"

    def test_empty_schema_part(self, connector):
        schema, table = connector._parse_endpoint("/mytable")
        assert schema == "public"
        assert table == "mytable"
