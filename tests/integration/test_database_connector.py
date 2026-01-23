"""Integration tests for DatabaseConnector with real PostgreSQL database."""

import os
import uuid
import pytest
from unittest.mock import AsyncMock

from src.source.connectors.database import DatabaseConnector


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
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "analitiq_test"),
        "username": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
    }


@pytest.fixture
def unique_table_name():
    """Generate unique table name for test isolation."""
    return f"test_connector_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def endpoint_config(unique_table_name):
    """Endpoint configuration for integration tests with unique table."""
    return {
        "schema": "public",
        "table": unique_table_name,
        "primary_key": ["id"],
        "endpoint_schema": {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "name": {"type": "string", "maxLength": 255},
                "email": {"type": "string", "maxLength": 255},
                "created_at": {"type": "string", "format": "date-time"}
            }
        },
        "write_mode": "upsert",
        "conflict_resolution": {
            "on_conflict": "id",
            "update_columns": ["name", "email"]
        },
        "configure": {
            "auto_create_schema": False,
            "auto_create_table": True,
            "auto_create_indexes": []
        }
    }


@pytest.fixture
def mock_state_manager():
    """Mock state manager for read_batches tests."""
    state_manager = AsyncMock()
    state_manager.get_cursor = AsyncMock(return_value=None)
    state_manager.update_cursor = AsyncMock()
    return state_manager


class TestDatabaseConnectorRealIntegration:
    """Integration tests using real PostgreSQL database."""

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, database_config):
        """Test real database connection and disconnection."""
        connector = DatabaseConnector("RealTestConnector")

        await connector.connect(database_config)

        assert connector.is_connected is True
        assert connector._initialized is True
        assert connector.driver is not None
        assert connector.driver.name == "PostgreSQL"

        await connector.disconnect()

        assert connector.is_connected is False
        assert connector._initialized is False

    @pytest.mark.asyncio
    async def test_configure_creates_table(self, database_config, endpoint_config, unique_table_name):
        """Test that configure creates a table in real database."""
        connector = DatabaseConnector("RealConfigureConnector")

        try:
            await connector.connect(database_config)
            await connector.configure(endpoint_config)

            # Verify table was created by querying it
            async with connector.driver.connection_pool.acquire() as conn:
                result = await conn.fetch(
                    """SELECT table_name FROM information_schema.tables
                       WHERE table_schema = 'public' AND table_name = $1""",
                    unique_table_name
                )
                assert len(result) == 1
                assert result[0]['table_name'] == unique_table_name
        finally:
            if connector.driver and connector.driver.connection_pool:
                async with connector.driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
            await connector.disconnect()

    @pytest.mark.asyncio
    async def test_write_batch(self, database_config, endpoint_config, unique_table_name):
        """Test writing data with real database."""
        connector = DatabaseConnector("RealWriteConnector")

        try:
            await connector.connect(database_config)
            await connector.configure(endpoint_config)

            # Write test data
            batch = [
                {"id": 1, "name": "Alice", "email": "alice@test.com"},
                {"id": 2, "name": "Bob", "email": "bob@test.com"},
                {"id": 3, "name": "Charlie", "email": "charlie@test.com"}
            ]

            await connector.write_batch(batch, endpoint_config)

            assert connector.metrics["records_written"] == 3
            assert connector.metrics["batches_written"] == 1

            # Verify data was written
            async with connector.driver.connection_pool.acquire() as conn:
                result = await conn.fetch(
                    f"SELECT COUNT(*) as cnt FROM public.{unique_table_name}"
                )
                assert result[0]['cnt'] == 3

        finally:
            if connector.driver and connector.driver.connection_pool:
                async with connector.driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
            await connector.disconnect()

    @pytest.mark.asyncio
    async def test_write_and_read_batch(
        self, database_config, endpoint_config, unique_table_name, mock_state_manager
    ):
        """Test writing and reading data with real database."""
        connector = DatabaseConnector("RealWriteReadConnector")

        try:
            await connector.connect(database_config)
            await connector.configure(endpoint_config)

            # Write test data
            batch = [
                {"id": 1, "name": "Alice", "email": "alice@test.com"},
                {"id": 2, "name": "Bob", "email": "bob@test.com"},
                {"id": 3, "name": "Charlie", "email": "charlie@test.com"}
            ]

            await connector.write_batch(batch, endpoint_config)

            # Read data back
            read_config = {
                **endpoint_config,
                "columns": ["id", "name", "email"],
                "cursor_field": "id"
            }

            batches = []
            async for read_batch in connector.read_batches(
                read_config,
                state_manager=mock_state_manager,
                stream_name="test_stream",
                batch_size=10
            ):
                batches.append(read_batch)

            assert len(batches) == 1
            assert len(batches[0]) == 3

            # Verify data
            records_by_id = {r["id"]: r for r in batches[0]}
            assert records_by_id[1]["name"] == "Alice"
            assert records_by_id[2]["email"] == "bob@test.com"

        finally:
            if connector.driver and connector.driver.connection_pool:
                async with connector.driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
            await connector.disconnect()

    @pytest.mark.asyncio
    async def test_upsert_updates_existing_records(
        self, database_config, endpoint_config, unique_table_name, mock_state_manager
    ):
        """Test that upsert correctly updates existing records."""
        connector = DatabaseConnector("RealUpsertConnector")

        try:
            await connector.connect(database_config)
            await connector.configure(endpoint_config)

            # Insert initial data
            initial_batch = [
                {"id": 1, "name": "Alice", "email": "alice@test.com"},
                {"id": 2, "name": "Bob", "email": "bob@test.com"}
            ]
            await connector.write_batch(initial_batch, endpoint_config)

            # Upsert with updated and new records
            upsert_batch = [
                {"id": 1, "name": "Alice Updated", "email": "alice.new@test.com"},
                {"id": 3, "name": "Charlie", "email": "charlie@test.com"}
            ]
            await connector.write_batch(upsert_batch, endpoint_config)

            # Read all data
            read_config = {
                **endpoint_config,
                "columns": ["id", "name", "email"],
                "cursor_field": "id"
            }

            all_records = []
            async for batch in connector.read_batches(
                read_config,
                state_manager=mock_state_manager,
                stream_name="test_stream",
                batch_size=10
            ):
                all_records.extend(batch)

            assert len(all_records) == 3

            records_by_id = {r["id"]: r for r in all_records}
            # Verify update happened
            assert records_by_id[1]["name"] == "Alice Updated"
            assert records_by_id[1]["email"] == "alice.new@test.com"
            # Verify Bob unchanged
            assert records_by_id[2]["name"] == "Bob"
            # Verify new record inserted
            assert records_by_id[3]["name"] == "Charlie"

        finally:
            if connector.driver and connector.driver.connection_pool:
                async with connector.driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
            await connector.disconnect()

    @pytest.mark.asyncio
    async def test_insert_mode(self, database_config, endpoint_config, unique_table_name):
        """Test insert mode (not upsert) with real database."""
        connector = DatabaseConnector("RealInsertConnector")

        # Modify config for insert mode
        insert_config = {
            **endpoint_config,
            "write_mode": "insert"
        }

        try:
            await connector.connect(database_config)
            await connector.configure(insert_config)

            batch = [
                {"id": 1, "name": "Alice", "email": "alice@test.com"},
                {"id": 2, "name": "Bob", "email": "bob@test.com"}
            ]

            await connector.write_batch(batch, insert_config)

            assert connector.metrics["records_written"] == 2

            # Verify data was inserted
            async with connector.driver.connection_pool.acquire() as conn:
                result = await conn.fetch(f"SELECT COUNT(*) as cnt FROM public.{unique_table_name}")
                assert result[0]['cnt'] == 2

        finally:
            if connector.driver and connector.driver.connection_pool:
                async with connector.driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
            await connector.disconnect()

    @pytest.mark.asyncio
    async def test_read_batches_pagination(
        self, database_config, endpoint_config, unique_table_name, mock_state_manager
    ):
        """Test that read_batches correctly paginates through large datasets."""
        connector = DatabaseConnector("RealPaginationConnector")

        try:
            await connector.connect(database_config)
            await connector.configure(endpoint_config)

            # Insert more records than batch size
            batch = [{"id": i, "name": f"User{i}", "email": f"user{i}@test.com"} for i in range(1, 26)]
            await connector.write_batch(batch, endpoint_config)

            # Read with small batch size
            read_config = {
                **endpoint_config,
                "columns": ["id", "name", "email"],
                "cursor_field": "id"
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
            if connector.driver and connector.driver.connection_pool:
                async with connector.driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
            await connector.disconnect()
