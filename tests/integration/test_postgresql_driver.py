"""Integration tests for shared database engine with real PostgreSQL database."""

import os
import uuid
import pytest

from sqlalchemy import text
from src.shared.database_utils import create_database_engine, acquire_connection


def _postgres_configured() -> bool:
    """Check if PostgreSQL is configured for testing."""
    return bool(os.getenv("POSTGRES_PASSWORD") or os.getenv("TEST_POSTGRES_URL"))


pytestmark = pytest.mark.skipif(
    not _postgres_configured(),
    reason="PostgreSQL not configured (set POSTGRES_PASSWORD)"
)


@pytest.fixture
def connection_config():
    """Connection configuration for real database tests."""
    return {
        "driver": "postgresql",
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "username": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
        "database": os.getenv("POSTGRES_DB", "analitiq_test"),
        "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
        "connection_pool": {
            "min_connections": 2,
            "max_connections": 10
        }
    }


@pytest.fixture
def unique_table_name():
    """Generate unique table name for test isolation."""
    return f"test_engine_{uuid.uuid4().hex[:8]}"


class TestSharedEngineRealIntegration:
    """Integration tests using real PostgreSQL database via shared engine."""

    @pytest.mark.asyncio
    async def test_create_and_dispose_engine(self, connection_config):
        """Test creating and disposing a real engine."""
        engine, driver = await create_database_engine(connection_config, require_port=True)

        assert engine is not None
        assert driver == "postgresql"

        await engine.dispose()

    @pytest.mark.asyncio
    async def test_acquire_connection_and_query(self, connection_config, unique_table_name):
        """Test acquiring a connection and running queries."""
        engine, _ = await create_database_engine(connection_config, require_port=True)

        try:
            # Create table
            async with engine.begin() as conn:
                await conn.execute(text(
                    f"CREATE TABLE public.{unique_table_name} ("
                    f"  id INTEGER PRIMARY KEY,"
                    f"  name VARCHAR(255),"
                    f"  email VARCHAR(100)"
                    f")"
                ))

            # Insert data
            async with engine.begin() as conn:
                await conn.execute(text(
                    f"INSERT INTO public.{unique_table_name} (id, name, email) VALUES "
                    f"(1, 'Alice', 'alice@test.com'), (2, 'Bob', 'bob@test.com')"
                ))

            # Query data using acquire_connection
            async with acquire_connection(engine) as conn:
                result = await conn.execute(
                    text(f"SELECT * FROM public.{unique_table_name} ORDER BY id")
                )
                rows = [dict(row._mapping) for row in result]

            assert len(rows) == 2
            assert rows[0]["id"] == 1
            assert rows[0]["name"] == "Alice"
            assert rows[1]["id"] == 2
            assert rows[1]["email"] == "bob@test.com"
        finally:
            async with engine.begin() as conn:
                await conn.execute(text(f"DROP TABLE IF EXISTS public.{unique_table_name}"))
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_parameterized_query(self, connection_config, unique_table_name):
        """Test parameterized queries via exec_driver_sql."""
        engine, _ = await create_database_engine(connection_config, require_port=True)

        try:
            async with engine.begin() as conn:
                await conn.execute(text(
                    f"CREATE TABLE public.{unique_table_name} ("
                    f"  id INTEGER PRIMARY KEY,"
                    f"  name VARCHAR(255),"
                    f"  status VARCHAR(50)"
                    f")"
                ))

            async with engine.begin() as conn:
                await conn.execute(text(
                    f"INSERT INTO public.{unique_table_name} (id, name, status) VALUES "
                    f"(1, 'Alice', 'active'), (2, 'Bob', 'inactive'), (3, 'Charlie', 'active')"
                ))

            async with acquire_connection(engine) as conn:
                result = await conn.exec_driver_sql(
                    f"SELECT * FROM public.{unique_table_name} WHERE status = $1 ORDER BY id",
                    ("active",)
                )
                rows = [dict(row._mapping) for row in result]

            assert len(rows) == 2
            assert rows[0]["name"] == "Alice"
            assert rows[1]["name"] == "Charlie"
        finally:
            async with engine.begin() as conn:
                await conn.execute(text(f"DROP TABLE IF EXISTS public.{unique_table_name}"))
            await engine.dispose()
