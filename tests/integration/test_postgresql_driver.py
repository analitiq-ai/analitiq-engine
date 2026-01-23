"""Integration tests for PostgreSQLDriver with real PostgreSQL database."""

import os
import uuid
import pytest

from src.source.drivers.postgresql import PostgreSQLDriver


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
    return f"test_driver_{uuid.uuid4().hex[:8]}"


class TestPostgreSQLDriverRealIntegration:
    """Integration tests using real PostgreSQL database."""

    @pytest.mark.asyncio
    async def test_create_connection_pool(self, connection_config):
        """Test creating a real connection pool."""
        driver = PostgreSQLDriver()

        await driver.create_connection_pool(connection_config)

        assert driver.connection_pool is not None
        assert driver.asyncpg is not None

        await driver.close_connection_pool()
        assert driver.connection_pool is None

    @pytest.mark.asyncio
    async def test_create_schema(self, connection_config):
        """Test creating a schema in real database."""
        driver = PostgreSQLDriver()
        schema_name = f"test_schema_{uuid.uuid4().hex[:8]}"

        try:
            await driver.create_connection_pool(connection_config)
            await driver.create_schema_if_not_exists(schema_name)

            # Verify schema exists
            async with driver.connection_pool.acquire() as conn:
                result = await conn.fetch(
                    """SELECT schema_name FROM information_schema.schemata
                       WHERE schema_name = $1""",
                    schema_name
                )
                assert len(result) == 1
        finally:
            if driver.connection_pool:
                async with driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                await driver.close_connection_pool()

    @pytest.mark.asyncio
    async def test_create_table(self, connection_config, unique_table_name):
        """Test creating a table in real database."""
        driver = PostgreSQLDriver()
        endpoint_schema = {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "name": {"type": "string", "maxLength": 255},
                "email": {"type": "string", "maxLength": 100},
                "created_at": {"type": "string", "format": "date-time"}
            }
        }

        try:
            await driver.create_connection_pool(connection_config)
            await driver.create_table_if_not_exists(
                "public",
                unique_table_name,
                endpoint_schema,
                primary_key=["id"],
                unique_constraints=["email"]
            )

            # Verify table exists and has correct columns
            async with driver.connection_pool.acquire() as conn:
                result = await conn.fetch(
                    """SELECT column_name, data_type FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name = $1
                       ORDER BY ordinal_position""",
                    unique_table_name
                )

                columns = {row['column_name']: row['data_type'] for row in result}
                assert 'id' in columns
                assert 'name' in columns
                assert 'email' in columns
                assert 'created_at' in columns
        finally:
            if driver.connection_pool:
                async with driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
                await driver.close_connection_pool()

    @pytest.mark.asyncio
    async def test_create_indexes(self, connection_config, unique_table_name):
        """Test creating indexes in real database."""
        driver = PostgreSQLDriver()
        endpoint_schema = {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "email": {"type": "string", "maxLength": 100},
                "status": {"type": "string", "maxLength": 50}
            }
        }
        indexes = [
            {"name": f"idx_{unique_table_name}_email", "columns": ["email"], "type": "btree"},
            {"name": f"idx_{unique_table_name}_status", "columns": ["status"], "type": "btree"}
        ]

        try:
            await driver.create_connection_pool(connection_config)
            await driver.create_table_if_not_exists(
                "public", unique_table_name, endpoint_schema, ["id"], []
            )
            await driver.create_indexes_if_not_exist("public", unique_table_name, indexes)

            # Verify indexes exist
            async with driver.connection_pool.acquire() as conn:
                result = await conn.fetch(
                    """SELECT indexname FROM pg_indexes
                       WHERE schemaname = 'public' AND tablename = $1""",
                    unique_table_name
                )
                index_names = {row['indexname'] for row in result}
                assert f"idx_{unique_table_name}_email" in index_names
                assert f"idx_{unique_table_name}_status" in index_names
        finally:
            if driver.connection_pool:
                async with driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
                await driver.close_connection_pool()

    @pytest.mark.asyncio
    async def test_execute_insert_and_query(self, connection_config, unique_table_name):
        """Test inserting and querying data in real database."""
        driver = PostgreSQLDriver()
        endpoint_schema = {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "name": {"type": "string", "maxLength": 255},
                "email": {"type": "string", "maxLength": 100}
            }
        }

        try:
            await driver.create_connection_pool(connection_config)
            await driver.create_table_if_not_exists(
                "public", unique_table_name, endpoint_schema, ["id"], []
            )

            # Insert data
            batch = [
                {"id": 1, "name": "Alice", "email": "alice@test.com"},
                {"id": 2, "name": "Bob", "email": "bob@test.com"}
            ]

            async with driver.connection_pool.acquire() as conn:
                await driver.execute_insert(conn, "public", unique_table_name, batch)

            # Query data
            async with driver.connection_pool.acquire() as conn:
                result = await driver.execute_query(
                    conn,
                    f"SELECT * FROM public.{unique_table_name} ORDER BY id"
                )

            assert len(result) == 2
            assert result[0]["id"] == 1
            assert result[0]["name"] == "Alice"
            assert result[1]["id"] == 2
            assert result[1]["email"] == "bob@test.com"
        finally:
            if driver.connection_pool:
                async with driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
                await driver.close_connection_pool()

    @pytest.mark.asyncio
    async def test_execute_upsert(self, connection_config, unique_table_name):
        """Test upsert operations in real database."""
        driver = PostgreSQLDriver()
        endpoint_schema = {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "name": {"type": "string", "maxLength": 255},
                "status": {"type": "string", "maxLength": 50}
            }
        }

        try:
            await driver.create_connection_pool(connection_config)
            await driver.create_table_if_not_exists(
                "public", unique_table_name, endpoint_schema, ["id"], []
            )

            # Initial insert
            initial_batch = [
                {"id": 1, "name": "Alice", "status": "active"},
                {"id": 2, "name": "Bob", "status": "active"}
            ]
            conflict_config = {"on_conflict": "id", "update_columns": ["name", "status"]}

            async with driver.connection_pool.acquire() as conn:
                await driver.execute_upsert(conn, "public", unique_table_name, initial_batch, conflict_config)

            # Upsert - update existing and add new
            upsert_batch = [
                {"id": 1, "name": "Alice Updated", "status": "inactive"},
                {"id": 3, "name": "Charlie", "status": "active"}
            ]

            async with driver.connection_pool.acquire() as conn:
                await driver.execute_upsert(conn, "public", unique_table_name, upsert_batch, conflict_config)

            # Verify results
            async with driver.connection_pool.acquire() as conn:
                result = await driver.execute_query(
                    conn,
                    f"SELECT * FROM public.{unique_table_name} ORDER BY id"
                )

            assert len(result) == 3
            # Verify update
            assert result[0]["name"] == "Alice Updated"
            assert result[0]["status"] == "inactive"
            # Verify Bob unchanged
            assert result[1]["name"] == "Bob"
            assert result[1]["status"] == "active"
            # Verify new record
            assert result[2]["name"] == "Charlie"
        finally:
            if driver.connection_pool:
                async with driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
                await driver.close_connection_pool()

    @pytest.mark.asyncio
    async def test_execute_query_with_params(self, connection_config, unique_table_name):
        """Test parameterized queries in real database."""
        driver = PostgreSQLDriver()
        endpoint_schema = {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "name": {"type": "string", "maxLength": 255},
                "status": {"type": "string", "maxLength": 50}
            }
        }

        try:
            await driver.create_connection_pool(connection_config)
            await driver.create_table_if_not_exists(
                "public", unique_table_name, endpoint_schema, ["id"], []
            )

            # Insert test data
            batch = [
                {"id": 1, "name": "Alice", "status": "active"},
                {"id": 2, "name": "Bob", "status": "inactive"},
                {"id": 3, "name": "Charlie", "status": "active"}
            ]

            async with driver.connection_pool.acquire() as conn:
                await driver.execute_insert(conn, "public", unique_table_name, batch)

            # Query with parameters
            async with driver.connection_pool.acquire() as conn:
                result = await driver.execute_query(
                    conn,
                    f"SELECT * FROM public.{unique_table_name} WHERE status = $1 ORDER BY id",
                    ["active"]
                )

            assert len(result) == 2
            assert result[0]["name"] == "Alice"
            assert result[1]["name"] == "Charlie"
        finally:
            if driver.connection_pool:
                async with driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
                await driver.close_connection_pool()

    @pytest.mark.asyncio
    async def test_json_data_handling(self, connection_config, unique_table_name):
        """Test JSONB data handling in real database."""
        import json
        driver = PostgreSQLDriver()
        endpoint_schema = {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "metadata": {"type": "object", "database_type": "JSONB"},
                "tags": {"type": "array", "database_type": "JSONB"}
            }
        }
        driver._build_column_type_mapping(endpoint_schema)

        try:
            await driver.create_connection_pool(connection_config)
            await driver.create_table_if_not_exists(
                "public", unique_table_name, endpoint_schema, ["id"], []
            )

            # Insert JSON data
            batch = [
                {"id": 1, "metadata": {"key": "value", "nested": {"inner": 42}}, "tags": ["tag1", "tag2"]}
            ]
            conflict_config = {"on_conflict": "id"}

            async with driver.connection_pool.acquire() as conn:
                await driver.execute_upsert(conn, "public", unique_table_name, batch, conflict_config)

            # Query and verify JSON data using driver's execute_query (handles type conversion)
            async with driver.connection_pool.acquire() as conn:
                result = await driver.execute_query(
                    conn, f"SELECT * FROM public.{unique_table_name}"
                )

            assert len(result) == 1
            # Driver may return JSONB as strings or dicts depending on configuration
            metadata = result[0]["metadata"]
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            tags = result[0]["tags"]
            if isinstance(tags, str):
                tags = json.loads(tags)

            assert metadata["key"] == "value"
            assert metadata["nested"]["inner"] == 42
            assert "tag1" in tags
        finally:
            if driver.connection_pool:
                async with driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
                await driver.close_connection_pool()

    @pytest.mark.asyncio
    async def test_incremental_query_execution(self, connection_config, unique_table_name):
        """Test incremental query building with real database execution."""
        from datetime import datetime
        driver = PostgreSQLDriver()
        endpoint_schema = {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "name": {"type": "string", "maxLength": 255},
                "updated_at": {"type": "string", "format": "date-time"}
            }
        }

        try:
            await driver.create_connection_pool(connection_config)
            await driver.create_table_if_not_exists(
                "public", unique_table_name, endpoint_schema, ["id"], []
            )

            # Insert test data with different timestamps
            async with driver.connection_pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO public.{unique_table_name} (id, name, updated_at) VALUES
                    (1, 'Alice', '2023-01-01 00:00:00'),
                    (2, 'Bob', '2023-06-15 12:00:00'),
                    (3, 'Charlie', '2023-12-31 23:59:59')
                """)

            # Build and execute incremental query
            # Use datetime object for cursor_value as asyncpg requires datetime for TIMESTAMPTZ
            cursor_datetime = datetime(2023, 6, 1, 0, 0, 0)
            config = {
                "columns": ["id", "name"],
                "cursor_field": "updated_at",
                "cursor_value": cursor_datetime,
                "cursor_mode": "exclusive"
            }

            query, params = driver.build_incremental_query("public", unique_table_name, config)

            async with driver.connection_pool.acquire() as conn:
                result = await driver.execute_query(conn, query, params)

            # Should only return records after June 1, 2023
            assert len(result) == 2
            names = {r["name"] for r in result}
            assert "Bob" in names
            assert "Charlie" in names
            assert "Alice" not in names
        finally:
            if driver.connection_pool:
                async with driver.connection_pool.acquire() as conn:
                    await conn.execute(f"DROP TABLE IF EXISTS public.{unique_table_name}")
                await driver.close_connection_pool()
