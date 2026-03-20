"""Integration tests for DatabaseConnector with real PostgreSQL database."""

import os
import uuid
import pytest
from unittest.mock import AsyncMock

from sqlalchemy import text
from src.source.connectors.database import DatabaseConnector
from src.shared.connection_runtime import ConnectionRuntime
from src.shared.database_utils import acquire_connection
from src.secrets.resolvers.memory import InMemorySecretsResolver


def _postgres_configured() -> bool:
    """Check if PostgreSQL is configured for testing."""
    return bool(os.getenv("POSTGRES_PASSWORD") or os.getenv("TEST_POSTGRES_URL"))


pytestmark = pytest.mark.skipif(
    not _postgres_configured(),
    reason="PostgreSQL not configured (set POSTGRES_PASSWORD)"
)


@pytest.fixture
def database_config():
    """Connection configuration for real database tests."""
    return {
        "driver": "postgresql",
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "parameters": {
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "analitiq_test"),
            "username": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
            "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
        },
    }


@pytest.fixture
def database_runtime(database_config):
    """ConnectionRuntime for real database tests."""
    resolver = InMemorySecretsResolver({})
    return ConnectionRuntime(
        raw_config=database_config,
        connection_id="test-conn",
        connector_type="database",
        driver="postgresql",
        resolver=resolver,
    )


@pytest.fixture
def unique_table_name():
    """Generate unique table name for test isolation."""
    return f"test_connector_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def mock_state_manager():
    """Mock state manager for read_batches tests."""
    state_manager = AsyncMock()
    state_manager.get_cursor = AsyncMock(return_value=None)
    state_manager.save_cursor = AsyncMock()
    return state_manager


class TestDatabaseConnectorRealIntegration:
    """Integration tests using real PostgreSQL database."""

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, database_runtime):
        """Test real database connection and disconnection."""
        connector = DatabaseConnector("RealTestConnector")

        await connector.connect(database_runtime)

        assert connector.is_connected is True
        assert connector._initialized is True
        assert connector._engine is not None
        assert connector._driver == "postgresql"

        await connector.disconnect()

        assert connector.is_connected is False
        assert connector._initialized is False
        assert connector._engine is None

    @pytest.mark.asyncio
    async def test_read_batches_pagination(
        self, database_runtime, unique_table_name, mock_state_manager
    ):
        """Test that read_batches correctly paginates through large datasets."""
        connector = DatabaseConnector("RealPaginationConnector")

        try:
            await connector.connect(database_runtime)

            # Create table and insert data directly via the engine
            async with connector._engine.begin() as conn:
                await conn.execute(text(
                    f"CREATE TABLE public.{unique_table_name} ("
                    f"  id INTEGER PRIMARY KEY,"
                    f"  name VARCHAR(255),"
                    f"  email VARCHAR(255)"
                    f")"
                ))

            # Insert 25 records
            async with connector._engine.begin() as conn:
                values = ", ".join(
                    f"({i}, 'User{i}', 'user{i}@test.com')" for i in range(1, 26)
                )
                await conn.execute(text(
                    f"INSERT INTO public.{unique_table_name} (id, name, email) VALUES {values}"
                ))

            # Read with small batch size
            read_config = {
                "endpoint": f"public/{unique_table_name}",
                "columns": ["id", "name", "email"],
                "cursor_field": "id",
                "driver": "postgresql",
            }

            all_records = []
            batch_count = 0
            async for read_batch in connector.read_batches(
                read_config,
                state_manager=mock_state_manager,
                stream_name="test_stream",
                batch_size=10
            ):
                all_records.extend(read_batch)
                batch_count += 1

            assert len(all_records) == 25
            assert batch_count >= 2  # Should have multiple batches

            # Verify all records are present
            ids = {r["id"] for r in all_records}
            assert ids == set(range(1, 26))

        finally:
            if connector._engine:
                async with connector._engine.begin() as conn:
                    await conn.execute(text(f"DROP TABLE IF EXISTS public.{unique_table_name}"))
            await connector.disconnect()
