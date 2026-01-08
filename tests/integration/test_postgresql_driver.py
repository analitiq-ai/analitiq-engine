"""Unit tests for PostgreSQLDriver."""

import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, List

from src.connectors.database.postgresql_driver import PostgreSQLDriver


@pytest.fixture
def driver():
    """PostgreSQLDriver instance."""
    return PostgreSQLDriver()


@pytest.fixture
def connection_config():
    """Sample connection configuration."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_password",
        "ssl_mode": "prefer",
        "connection_pool": {
            "min_connections": 2,
            "max_connections": 10,
            "max_overflow": 20,
            "pool_recycle": 3600
        },
        "command_timeout": 300
    }


@pytest.fixture
def table_schema():
    """Sample table schema definition."""
    return {
        "properties": {
            "id": {
                "type": "integer",
                "database_type": "BIGINT",
                "nullable": False,
                "primary_key": True
            },
            "name": {
                "type": "string",
                "maxLength": 255,
                "nullable": False
            },
            "email": {
                "type": "string",
                "maxLength": 100,
                "nullable": True
            },
            "balance": {
                "type": "number",
                "precision": 10,
                "scale": 2,
                "nullable": True
            },
            "is_active": {
                "type": "boolean",
                "default": True,
                "nullable": False
            },
            "metadata": {
                "type": "object",
                "nullable": True
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "nullable": True
            },
            "created_at": {
                "type": "string",
                "format": "date-time",
                "default": "CURRENT_TIMESTAMP",
                "nullable": False
            },
            "birth_date": {
                "type": "string",
                "format": "date",
                "nullable": True
            }
        }
    }


class TestPostgreSQLDriverInit:
    """Test PostgreSQLDriver initialization."""
    
    def test_init(self):
        """Test driver initialization."""
        driver = PostgreSQLDriver()
        assert driver.name == "PostgreSQL"
        assert driver.asyncpg is None
        assert driver.connection_pool is None


class TestPostgreSQLDriverConnection:
    """Test PostgreSQLDriver connection management."""
    
    @pytest.mark.asyncio
    async def test_create_connection_pool_success(self, driver, connection_config):
        """Test successful connection pool creation."""
        mock_asyncpg = MagicMock()
        mock_pool = AsyncMock()
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)
        
        with patch('builtins.__import__') as mock_import:
            mock_import.return_value = mock_asyncpg
            driver.asyncpg = mock_asyncpg
            
            await driver.create_connection_pool(connection_config)
            
            assert driver.connection_pool == mock_pool
            mock_asyncpg.create_pool.assert_called_once()
            call_args = mock_asyncpg.create_pool.call_args[1]
            assert call_args["host"] == "localhost"
            assert call_args["port"] == 5432
            assert call_args["database"] == "test_db"
            assert call_args["min_size"] == 2
            assert call_args["max_size"] == 10
            assert call_args["command_timeout"] == 300
    
    @pytest.mark.asyncio
    async def test_create_connection_pool_missing_asyncpg(self, driver, connection_config):
        """Test connection pool creation without asyncpg package."""
        with patch('builtins.__import__', side_effect=ImportError("No module named 'asyncpg'")):
            with pytest.raises(ImportError) as exc_info:
                await driver.create_connection_pool(connection_config)
            
            assert "asyncpg package required" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_close_connection_pool(self, driver):
        """Test connection pool closure."""
        mock_pool = AsyncMock()
        driver.connection_pool = mock_pool
        
        await driver.close_connection_pool()
        
        mock_pool.close.assert_called_once()
        assert driver.connection_pool is None
    
    def test_get_connection_params_full_config(self, driver):
        """Test connection parameter extraction with full config."""
        config = {
            "host": "testhost",
            "port": 3306,
            "user": "testuser",
            "password": "testpass",
            "database": "testdb",
            "ssl_mode": "require"
        }
        
        params = driver.get_connection_params(config)
        
        assert params["host"] == "testhost"
        assert params["port"] == 3306
        assert params["user"] == "testuser"
        assert params["password"] == "testpass"
        assert params["database"] == "testdb"
        assert params["ssl"] == "require"
    
    def test_get_connection_params_defaults(self, driver):
        """Test connection parameter extraction with defaults."""
        config = {}
        
        params = driver.get_connection_params(config)
        
        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["user"] == "postgres"
        assert params["password"] == ""
        assert params["database"] == "postgres"
        assert params["ssl"] == "prefer"


class TestPostgreSQLDriverSchemaOperations:
    """Test PostgreSQLDriver schema operations."""
    
    @pytest.mark.asyncio
    async def test_create_schema_if_not_exists(self, driver):
        """Test schema creation."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        driver.connection_pool = mock_pool
        
        await driver.create_schema_if_not_exists("test_schema")
        
        mock_conn.execute.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS test_schema")
    
    @pytest.mark.asyncio
    async def test_create_schema_invalid_name(self, driver):
        """Test schema creation with invalid name."""
        with pytest.raises(ValueError) as exc_info:
            await driver.create_schema_if_not_exists("invalid-schema-name!")
        
        assert "Invalid schema name" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_create_table_if_not_exists(self, driver, table_schema):
        """Test table creation."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        driver.connection_pool = mock_pool
        
        primary_key = ["id"]
        unique_constraints = ["email"]
        
        await driver.create_table_if_not_exists(
            "test_schema",
            "test_table", 
            table_schema,
            primary_key,
            unique_constraints
        )
        
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0][0]
        
        # Verify the SQL contains expected elements
        assert "CREATE TABLE IF NOT EXISTS test_schema.test_table" in call_args
        assert "id BIGINT NOT NULL" in call_args
        assert "name VARCHAR(255) NOT NULL" in call_args
        assert "email VARCHAR(100) NULL" in call_args
        assert "balance DECIMAL(10,2) NULL" in call_args
        assert "is_active BOOLEAN NOT NULL DEFAULT True" in call_args
        assert "metadata JSONB NULL" in call_args
        assert "tags TEXT[] NULL" in call_args
        assert "created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP" in call_args
        assert "birth_date DATE NULL" in call_args
        assert "PRIMARY KEY (id)" in call_args
        assert "UNIQUE (email)" in call_args
    
    @pytest.mark.asyncio
    async def test_create_table_invalid_name(self, driver, table_schema):
        """Test table creation with invalid name."""
        with pytest.raises(ValueError) as exc_info:
            await driver.create_table_if_not_exists(
                "test_schema",
                "invalid table!",
                table_schema,
                []
            )
        
        assert "Invalid table name" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_create_indexes_if_not_exist(self, driver):
        """Test index creation."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        driver.connection_pool = mock_pool
        
        indexes = [
            {
                "name": "idx_email",
                "columns": ["email"],
                "type": "btree"
            },
            {
                "name": "idx_created_at",
                "columns": ["created_at"],
                "type": "hash"
            }
        ]
        
        await driver.create_indexes_if_not_exist("test_schema", "test_table", indexes)
        
        assert mock_conn.execute.call_count == 2
        
        # Check first index
        first_call = mock_conn.execute.call_args_list[0][0][0]
        assert "CREATE INDEX IF NOT EXISTS idx_email" in first_call
        assert "ON test_schema.test_table" in first_call
        assert "USING BTREE (email)" in first_call
        
        # Check second index
        second_call = mock_conn.execute.call_args_list[1][0][0]
        assert "CREATE INDEX IF NOT EXISTS idx_created_at" in second_call
        assert "USING HASH (created_at)" in second_call
    
    @pytest.mark.asyncio
    async def test_create_indexes_error_handling(self, driver):
        """Test index creation with errors."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.execute.side_effect = [Exception("Index creation failed")]
        driver.connection_pool = mock_pool
        
        indexes = [{"name": "idx_test", "columns": ["test_col"]}]
        
        # Should not raise exception, just log warning
        await driver.create_indexes_if_not_exist("test_schema", "test_table", indexes)


class TestPostgreSQLDriverTypeMapping:
    """Test PostgreSQLDriver type mapping."""
    
    def test_map_json_schema_explicit_database_type(self, driver):
        """Test type mapping with explicit database_type."""
        field_def = {"type": "string", "database_type": "CUSTOM_TYPE"}
        result = driver.map_json_schema_to_sql_type(field_def)
        assert result == "CUSTOM_TYPE"
    
    def test_map_json_schema_string_types(self, driver):
        """Test string type mapping."""
        # Regular string
        field_def = {"type": "string", "maxLength": 100}
        assert driver.map_json_schema_to_sql_type(field_def) == "VARCHAR(100)"
        
        # Large string -> TEXT
        field_def = {"type": "string", "maxLength": 20000000}
        assert driver.map_json_schema_to_sql_type(field_def) == "TEXT"
        
        # Default string
        field_def = {"type": "string"}
        assert driver.map_json_schema_to_sql_type(field_def) == "VARCHAR(255)"
        
        # Date-time string
        field_def = {"type": "string", "format": "date-time"}
        assert driver.map_json_schema_to_sql_type(field_def) == "TIMESTAMPTZ"
        
        # Date string
        field_def = {"type": "string", "format": "date"}
        assert driver.map_json_schema_to_sql_type(field_def) == "DATE"
    
    def test_map_json_schema_numeric_types(self, driver):
        """Test numeric type mapping."""
        # Integer
        field_def = {"type": "integer"}
        assert driver.map_json_schema_to_sql_type(field_def) == "BIGINT"
        
        # Number with precision/scale
        field_def = {"type": "number", "precision": 8, "scale": 3}
        assert driver.map_json_schema_to_sql_type(field_def) == "DECIMAL(8,3)"
        
        # Number with defaults
        field_def = {"type": "number"}
        assert driver.map_json_schema_to_sql_type(field_def) == "DECIMAL(15,2)"
    
    def test_map_json_schema_other_types(self, driver):
        """Test other type mappings."""
        # Boolean
        field_def = {"type": "boolean"}
        assert driver.map_json_schema_to_sql_type(field_def) == "BOOLEAN"
        
        # Object -> JSONB
        field_def = {"type": "object"}
        assert driver.map_json_schema_to_sql_type(field_def) == "JSONB"
        
        # String array
        field_def = {"type": "array", "items": {"type": "string"}}
        assert driver.map_json_schema_to_sql_type(field_def) == "TEXT[]"
        
        # Integer array
        field_def = {"type": "array", "items": {"type": "integer"}}
        assert driver.map_json_schema_to_sql_type(field_def) == "BIGINT[]"
        
        # Complex array -> JSONB
        field_def = {"type": "array", "items": {"type": "object"}}
        assert driver.map_json_schema_to_sql_type(field_def) == "JSONB"
        
        # Unknown type
        field_def = {"type": "unknown"}
        assert driver.map_json_schema_to_sql_type(field_def) == "TEXT"
    
    def test_map_json_schema_nullable_types(self, driver):
        """Test nullable type handling."""
        # Nullable string
        field_def = {"type": ["string", "null"], "maxLength": 50}
        assert driver.map_json_schema_to_sql_type(field_def) == "VARCHAR(50)"
        
        # Nullable integer
        field_def = {"type": ["integer", "null"]}
        assert driver.map_json_schema_to_sql_type(field_def) == "BIGINT"
    
    def test_get_default_clause(self, driver):
        """Test default clause generation."""
        # No default
        assert driver._get_default_clause({}) == ""
        assert driver._get_default_clause({"default": None}) == ""
        
        # CURRENT_TIMESTAMP
        assert driver._get_default_clause({"default": "CURRENT_TIMESTAMP"}) == " DEFAULT CURRENT_TIMESTAMP"
        
        # String default
        assert driver._get_default_clause({"default": "test"}) == " DEFAULT 'test'"
        
        # Numeric default
        assert driver._get_default_clause({"default": 42}) == " DEFAULT 42"
        assert driver._get_default_clause({"default": True}) == " DEFAULT True"


class TestPostgreSQLDriverDataOperations:
    """Test PostgreSQLDriver data operations."""
    
    @pytest.mark.asyncio
    async def test_execute_upsert(self, driver):
        """Test upsert execution."""
        mock_conn = AsyncMock()
        
        batch = [
            {"id": 1, "name": "Alice", "email": "alice@test.com"},
            {"id": 2, "name": "Bob", "email": "bob@test.com"}
        ]
        
        conflict_config = {
            "on_conflict": "id",
            "update_columns": ["name", "email"]
        }
        
        await driver.execute_upsert(mock_conn, "test_schema", "test_table", batch, conflict_config)
        
        mock_conn.executemany.assert_called_once()
        query = mock_conn.executemany.call_args[0][0]
        values = mock_conn.executemany.call_args[0][1]
        
        assert "INSERT INTO test_schema.test_table" in query
        assert "ON CONFLICT (id)" in query
        assert "DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email" in query
        assert len(values) == 2
        assert values[0] == [1, "Alice", "alice@test.com"]
    
    @pytest.mark.asyncio
    async def test_execute_upsert_with_json_data(self, driver):
        """Test upsert with JSON data."""
        mock_conn = AsyncMock()

        # Set up schema information in driver for JSON conversion
        table_schema = {
            "properties": {
                "id": {"type": "integer", "database_type": "BIGINT"},
                "metadata": {"type": "object", "database_type": "JSONB"},
                "tags": {"type": "array", "database_type": "JSONB"}
            }
        }
        driver._build_column_type_mapping(table_schema)

        batch = [
            {"id": 1, "metadata": {"key": "value"}, "tags": ["tag1", "tag2"]}
        ]

        conflict_config = {"on_conflict": "id"}

        await driver.execute_upsert(mock_conn, "schema", "table", batch, conflict_config)
        
        values = mock_conn.executemany.call_args[0][1]
        # JSON objects should be converted to strings
        assert values[0][1] == '{"key": "value"}'
        assert values[0][2] == '["tag1", "tag2"]'
    
    @pytest.mark.asyncio
    async def test_execute_upsert_auto_update_columns(self, driver):
        """Test upsert with automatic update column detection."""
        mock_conn = AsyncMock()
        
        batch = [{"id": 1, "name": "Alice", "status": "active"}]
        conflict_config = {"on_conflict": "id"}  # No update_columns specified
        
        await driver.execute_upsert(mock_conn, "schema", "table", batch, conflict_config)
        
        query = mock_conn.executemany.call_args[0][0]
        # Should update all columns except the conflict column
        assert "name = EXCLUDED.name, status = EXCLUDED.status" in query
        assert "id = EXCLUDED.id" not in query
    
    @pytest.mark.asyncio
    async def test_execute_upsert_empty_batch(self, driver):
        """Test upsert with empty batch."""
        mock_conn = AsyncMock()
        
        await driver.execute_upsert(mock_conn, "schema", "table", [], {})
        
        mock_conn.executemany.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_execute_insert(self, driver):
        """Test insert execution."""
        mock_conn = AsyncMock()
        
        batch = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        
        await driver.execute_insert(mock_conn, "test_schema", "test_table", batch)
        
        mock_conn.executemany.assert_called_once()
        query = mock_conn.executemany.call_args[0][0]
        values = mock_conn.executemany.call_args[0][1]
        
        assert query == "INSERT INTO test_schema.test_table (id, name) VALUES ($1, $2)"
        assert len(values) == 2
        assert values[0] == [1, "Alice"]
    
    @pytest.mark.asyncio
    async def test_execute_insert_empty_batch(self, driver):
        """Test insert with empty batch."""
        mock_conn = AsyncMock()
        
        await driver.execute_insert(mock_conn, "schema", "table", [])
        
        mock_conn.executemany.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_execute_query_with_params(self, driver):
        """Test query execution with parameters."""
        mock_conn = AsyncMock()
        mock_rows = [
            {"id": 1, "name": "Alice", "created_at": datetime(2023, 1, 1, 12, 0, 0)},
            {"id": 2, "name": "Bob", "created_at": datetime(2023, 1, 2, 13, 0, 0)}
        ]
        mock_conn.fetch.return_value = mock_rows
        
        query = "SELECT * FROM users WHERE id > $1"
        params = [0]
        
        result = await driver.execute_query(mock_conn, query, params)
        
        mock_conn.fetch.assert_called_once_with(query, 0)
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Alice"
        # Datetime should be converted to ISO string
        assert result[0]["created_at"] == "2023-01-01T12:00:00"
    
    @pytest.mark.asyncio
    async def test_execute_query_without_params(self, driver):
        """Test query execution without parameters."""
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []
        
        query = "SELECT COUNT(*) FROM users"
        
        result = await driver.execute_query(mock_conn, query)
        
        mock_conn.fetch.assert_called_once_with(query)
        assert result == []


class TestPostgreSQLDriverQueryBuilding:
    """Test PostgreSQLDriver query building."""
    
    def test_build_incremental_query_basic(self, driver):
        """Test basic incremental query building."""
        config = {
            "columns": ["id", "name", "email"],
            "cursor_field": "updated_at",
            "cursor_value": "2023-01-01T00:00:00Z",
            "cursor_mode": "exclusive"
        }
        
        query, params = driver.build_incremental_query("public", "users", config)
        
        expected_query = "SELECT id, name, email FROM public.users WHERE updated_at > $1 ORDER BY updated_at"
        assert query == expected_query
        assert params == ["2023-01-01T00:00:00Z"]
    
    def test_build_incremental_query_inclusive(self, driver):
        """Test incremental query with inclusive cursor mode."""
        config = {
            "cursor_field": "id",
            "cursor_value": 100,
            "cursor_mode": "inclusive"
        }
        
        query, params = driver.build_incremental_query("schema", "table", config)
        
        assert "WHERE id >= $1" in query
        assert params == [100]
    
    def test_build_incremental_query_with_where_clause(self, driver):
        """Test incremental query with additional WHERE clause."""
        config = {
            "where": "status = 'active'",
            "cursor_field": "created_at",
            "cursor_value": "2023-01-01",
            "cursor_mode": "exclusive"
        }
        
        query, params = driver.build_incremental_query("schema", "table", config)
        
        assert "WHERE status = 'active' AND created_at > $1" in query
        assert params == ["2023-01-01"]
    
    def test_build_incremental_query_with_order_by(self, driver):
        """Test incremental query with custom ORDER BY."""
        config = {
            "order_by": "name ASC, id DESC",
            "cursor_field": "updated_at",
            "cursor_value": "2023-01-01"
        }
        
        query, params = driver.build_incremental_query("schema", "table", config)
        
        assert query.endswith("ORDER BY name ASC, id DESC")
    
    def test_build_incremental_query_defaults(self, driver):
        """Test incremental query with default values."""
        config = {}
        
        query, params = driver.build_incremental_query("public", "users", config)
        
        expected_query = "SELECT * FROM public.users ORDER BY updated_at"
        assert query == expected_query
        assert params == []
    
    def test_build_incremental_query_no_cursor_value(self, driver):
        """Test incremental query without cursor value."""
        config = {
            "cursor_field": "updated_at",
            "cursor_mode": "exclusive"
            # No cursor_value
        }
        
        query, params = driver.build_incremental_query("schema", "table", config)
        
        # Should not add WHERE clause for cursor
        assert "WHERE" not in query
        assert "ORDER BY updated_at" in query
        assert params == []


class TestPostgreSQLDriverEdgeCases:
    """Test PostgreSQLDriver edge cases."""
    
    def test_get_full_table_name_inheritance(self, driver):
        """Test that get_full_table_name is inherited correctly."""
        assert driver.get_full_table_name("schema", "table") == "schema.table"
        assert driver.get_full_table_name("", "table") == "table"
    
    def test_validate_identifier_inheritance(self, driver):
        """Test that validate_identifier is inherited correctly."""
        assert driver.validate_identifier("valid_name") is True
        assert driver.validate_identifier("123invalid") is False
    
    @pytest.mark.asyncio
    async def test_acquire_connection_inheritance(self, driver):
        """Test that acquire_connection is inherited correctly."""
        mock_pool = AsyncMock()
        driver.connection_pool = mock_pool
        
        await driver.acquire_connection()
        
        mock_pool.acquire.assert_called_once()
    
    def test_connection_params_type_conversion(self, driver):
        """Test connection parameter type conversion."""
        config = {
            "host": "localhost",
            "port": "5432",  # String port
            "user": "testuser",
            "password": "testpass",
            "database": "testdb"
        }
        
        params = driver.get_connection_params(config)
        
        # Port should be converted to int
        assert params["port"] == 5432
        assert isinstance(params["port"], int)
    
    @pytest.mark.asyncio
    async def test_create_table_with_complex_constraints(self, driver):
        """Test table creation with complex constraints."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        driver.connection_pool = mock_pool
        
        table_schema = {
            "properties": {
                "id": {"type": "integer", "nullable": False},
                "email": {"type": "string", "maxLength": 255}
            }
        }
        
        # Multiple unique constraints including composite ones
        unique_constraints = [
            "email",  # Single column
            ["first_name", "last_name"]  # Composite constraint
        ]
        
        await driver.create_table_if_not_exists(
            "schema",
            "table",
            table_schema,
            ["id"],
            unique_constraints
        )
        
        call_args = mock_conn.execute.call_args[0][0]
        assert "UNIQUE (email)" in call_args
        assert "UNIQUE (first_name, last_name)" in call_args


class TestPostgreSQLDriverImports:
    """Test PostgreSQL driver imports and initialization."""
    
    def test_import_postgresql_driver(self):
        """Test importing PostgreSQL driver and related components."""
        from src.connectors.database.postgresql_driver import PostgreSQLDriver
        
        assert PostgreSQLDriver is not None
        
        # Test that driver can be instantiated
        driver = PostgreSQLDriver()
        assert driver.name == "PostgreSQL"
        assert driver.asyncpg is None


class TestPostgreSQLDriverMissingCoverage:
    """Test missing coverage paths in PostgreSQL driver."""
    
    @pytest.mark.asyncio
    async def test_close_connection_pool_with_none(self, driver):
        """Test closing connection pool when it's None."""
        driver.connection_pool = None
        
        # Should not raise an error
        await driver.close_connection_pool()
        
        assert driver.connection_pool is None
    
    def test_connection_params_with_partial_config(self, driver):
        """Test connection parameters with minimal config."""
        minimal_config = {"database": "custom_db"}
        
        params = driver.get_connection_params(minimal_config)
        
        # Should use defaults for missing values
        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["user"] == "postgres"
        assert params["password"] == ""
        assert params["database"] == "custom_db"  # Custom value
        assert params["ssl"] == "prefer"
    
    def test_map_json_schema_edge_cases(self, driver):
        """Test type mapping edge cases."""
        # Test empty field definition
        empty_field = {}
        result = driver.map_json_schema_to_sql_type(empty_field)
        assert result == "VARCHAR(255)"  # Default for string type (default type)
        
        # Test field with only format
        format_only = {"format": "date-time"}
        result = driver.map_json_schema_to_sql_type(format_only)
        assert result == "TIMESTAMPTZ"
        
        # Test complex nullable type
        nullable_complex = {"type": ["object", "null"]}
        result = driver.map_json_schema_to_sql_type(nullable_complex)
        assert result == "JSONB"
    
    def test_get_default_clause_edge_cases(self, driver):
        """Test default clause generation edge cases."""
        # Test with numeric default - note: 0 and False are falsy, so they return empty
        assert driver._get_default_clause({"default": 0}) == ""
        assert driver._get_default_clause({"default": False}) == ""
        
        # Test with truthy numeric values
        assert driver._get_default_clause({"default": 1}) == " DEFAULT 1"
        assert driver._get_default_clause({"default": True}) == " DEFAULT True"
        
        # Test with null default (should return empty string)
        assert driver._get_default_clause({"default": None}) == ""
    
    @pytest.mark.asyncio
    async def test_create_schema_validation(self, driver):
        """Test schema creation with various invalid names."""
        mock_pool = MagicMock()
        driver.connection_pool = mock_pool
        
        # Test with numbers at start (should fail validation)
        with pytest.raises(ValueError) as exc_info:
            await driver.create_schema_if_not_exists("123schema")
        assert "Invalid schema name" in str(exc_info.value)
        
        # Test with special characters (should fail validation)
        with pytest.raises(ValueError):
            await driver.create_schema_if_not_exists("schema!")
    
    @pytest.mark.asyncio
    async def test_create_table_field_filtering(self, driver):
        """Test table creation filters invalid field names."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        driver.connection_pool = mock_pool
        
        # Schema with mix of valid and invalid field names
        mixed_schema = {
            "properties": {
                "valid_field": {"type": "string"},
                "123invalid": {"type": "integer"},  # Invalid: starts with digit
                "field!": {"type": "boolean"},  # Invalid: special character
                "another_valid": {"type": "number"}
            }
        }
        
        await driver.create_table_if_not_exists(
            "test_schema", "test_table", mixed_schema, []
        )
        
        # Check that SQL was executed
        mock_conn.execute.assert_called_once()
        sql = mock_conn.execute.call_args[0][0]
        
        # Should include valid fields
        assert "valid_field" in sql
        assert "another_valid" in sql
        
        # Should exclude invalid fields
        assert "123invalid" not in sql
        assert "field!" not in sql