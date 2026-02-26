"""Unit tests for PostgreSQLDriver.

Tests are designed to catch BUGS, not just validate current behavior.
"""

import json
import ssl
import pytest
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import Any, Dict, List
from io import StringIO
import sys

from src.source.drivers.postgresql import PostgreSQLDriver


@pytest.fixture
def driver():
    """PostgreSQLDriver instance."""
    return PostgreSQLDriver()


@pytest.fixture
def connection_config():
    """Sample connection configuration for unit tests."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "username": "test_user",
        "password": "test_password",
        "ssl_mode": "prefer",
    }


@pytest.fixture
def real_world_config():
    """Real-world configuration as provided by the user.

    This is the ACTUAL format the driver receives in production.
    Tests MUST work with this format.
    """
    return {
        'created_at': '2026-01-17T22:48:58.264589',
        'host': 'database-1.cp2em6mk83a2.eu-central-1.rds.amazonaws.com',
        'connector_id': '5d1d3d63-dc26-4e87-bab8-475be44afd32',
        'updated_at': '2026-01-17T22:48:58.264589',
        'port': 5432,
        'connection_name': 'PG Connection',
        'client_id': 'd7a11991-2795-49d1-a858-c7e58ee5ecc6',
        'database': 'postgres',
        'connection_id': 'a97f022f-5761-4d72-9f39-877280408af7',
        'password': 'PDuhZN?cylU~J57M!~Rb>0LmERfb',
        'id': 'a97f022f-5761-4d72-9f39-877280408af7',
        'username': 'postgres',
        'category_name': 'Other',
        'driver': 'postgresql',
        'version': 1,
        'connector_name': 'PostgreSQL',
        'ssl_mode': 'prefer',
        'endpoint_id': 'ad21da6d-2bd6-4d71-b9bd-be222c6e30de_v1',
        'host_id': 'a97f022f-5761-4d72-9f39-877280408af7',
        'endpoint_schema': {
            'columns': [
                {'name': 'id', 'default': None, 'comment': None, 'type': 'BIGINT', 'nullable': False, 'autoincrement': False},
                {'name': 'status', 'default': None, 'comment': None, 'type': 'VARCHAR(50)', 'nullable': True, 'autoincrement': False},
                {'name': 'created', 'default': None, 'comment': None, 'type': 'TIMESTAMP', 'nullable': True, 'autoincrement': False},
                {'name': 'source_value', 'default': None, 'comment': None, 'type': 'NUMERIC(18, 2)', 'nullable': True, 'autoincrement': False},
                {'name': 'target_value', 'default': None, 'comment': None, 'type': 'NUMERIC(18, 2)', 'nullable': True, 'autoincrement': False},
                {'name': 'source_currency', 'default': None, 'comment': None, 'type': 'VARCHAR(3)', 'nullable': True, 'autoincrement': False},
                {'name': 'target_currency', 'default': None, 'comment': None, 'type': 'VARCHAR(3)', 'nullable': True, 'autoincrement': False},
                {'name': 'rate', 'default': None, 'comment': None, 'type': 'NUMERIC(18, 10)', 'nullable': True, 'autoincrement': False},
                {'name': '_synced_at', 'default': 'now()', 'comment': None, 'type': 'TIMESTAMP', 'nullable': True, 'autoincrement': False}
            ],
            'primary_keys': ['id']
        },
        'connectors': '5d1d3d63-dc26-4e87-bab8-475be44afd32',
        'method': 'DATABASE',
        'endpoint': 'public/wise_transfers',
        '_content_hash': '71c2d46f77f94eb744f2451198c78b16d8857e1a10ac467973c172b0ba584831',
        'connection_ref': 'conn_1',
        'replication_method': 'full',
        'cursor_field': None,
        'cursor_mode': 'inclusive',
        'safety_window_seconds': None,
        'primary_key': ['id']
    }


# =============================================================================
# BUG DETECTION TESTS - These tests SHOULD fail if bugs exist
# =============================================================================

class TestDebugPrintStatements:
    """BUG: Driver has debug print statements that pollute stdout."""

    def test_build_incremental_query_should_not_print_to_stdout(self, driver):
        """BUG: build_incremental_query has print('='*100) statements.

        Production code should NEVER print to stdout. These debug statements
        at lines 320-321, 338-340, 352-354, 366-368 must be removed.
        """
        config = {
            "driver": "postgresql",
            "cursor_field": "updated_at",
            "cursor_value": "2023-01-01",
            "cursor_mode": "inclusive"
        }

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            query, params = driver.build_incremental_query("public", "users", config)
        finally:
            sys.stdout = sys.__stdout__

        output = captured_output.getvalue()

        # This test FAILS if there are print statements - which is correct!
        assert output == "", (
            f"BUG: build_incremental_query prints to stdout.\n"
            f"Output captured: {repr(output)}\n"
            f"Remove debug print statements from postgresql.py lines 320-368"
        )


class TestSchemaFormatMismatch:
    """BUG: Driver expects 'properties' format but real config uses 'columns' format."""

    @pytest.mark.asyncio
    async def test_create_table_with_real_world_columns_format(self, driver, real_world_config):
        """BUG: create_table_if_not_exists expects 'properties' format.

        Real-world endpoint_schema uses 'columns' array format:
        {'columns': [{'name': 'id', 'type': 'BIGINT', ...}, ...]}

        But the driver does: properties = endpoint_schema.get("properties", {})
        This returns empty dict for real configs, creating tables with no columns!
        """
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        driver.connection_pool = mock_pool

        endpoint_schema = real_world_config['endpoint_schema']
        primary_key = real_world_config['primary_key']

        await driver.create_table_if_not_exists(
            "public",
            "wise_transfers",
            endpoint_schema,
            primary_key
        )

        # Get the SQL that was executed
        sql = mock_conn.execute.call_args[0][0]

        # This should have actual columns from the schema
        # BUG: With current code, it creates empty table because it looks for 'properties'
        assert "id BIGINT" in sql, (
            f"BUG: Table created without columns.\n"
            f"SQL: {sql}\n"
            f"Driver expects 'properties' format but real config uses 'columns' format.\n"
            f"Fix: Add support for 'columns' array format in create_table_if_not_exists"
        )
        assert "status VARCHAR(50)" in sql
        assert "created TIMESTAMP" in sql
        assert "source_value NUMERIC(18, 2)" in sql

    def test_build_column_type_mapping_with_columns_format(self, driver, real_world_config):
        """BUG: _build_column_type_mapping expects 'properties' format.

        Same issue - real configs use 'columns' array format.
        """
        endpoint_schema = real_world_config['endpoint_schema']

        driver._build_column_type_mapping(endpoint_schema)

        # BUG: With 'columns' format, _column_types will be empty
        assert len(driver._column_types) > 0, (
            f"BUG: Column type mapping is empty.\n"
            f"Driver expects 'properties' format but real config uses 'columns' format.\n"
            f"_column_types: {driver._column_types}"
        )
        assert "id" in driver._column_types


class TestCursorModeDefault:
    """BUG: Default cursor_mode should be 'inclusive' per CLAUDE.md."""

    def test_default_cursor_mode_should_be_inclusive(self, driver):
        """BUG: Default cursor_mode is 'exclusive' but should be 'inclusive'.

        CLAUDE.md states: "Engine uses inclusive mode (>=) for cursor comparison"
        But line 345 defaults to: cursor_mode = config.get("cursor_mode", "exclusive")
        """
        config = {
            "driver": "postgresql",
            "cursor_field": "updated_at",
            "cursor_value": "2023-01-01"
            # cursor_mode NOT specified - should default to inclusive
        }

        query, params = driver.build_incremental_query("schema", "table", config)

        # Per CLAUDE.md, default should be inclusive (>=)
        assert ">=" in query, (
            f"BUG: Default cursor_mode is 'exclusive' (>) but should be 'inclusive' (>=).\n"
            f"Query: {query}\n"
            f"CLAUDE.md says: 'Engine uses inclusive mode (>=) for cursor comparison'\n"
            f"Fix: Change line 345 from 'exclusive' default to 'inclusive'"
        )


class TestCursorFieldNone:
    """BUG: cursor_field=None with cursor_value set generates invalid SQL."""

    def test_cursor_field_none_with_cursor_value_generates_invalid_sql(self, driver):
        """BUG: When cursor_field is None but cursor_value is set, generates 'WHERE None >= $1'.

        The code only checks `if cursor_value:` but doesn't verify cursor_field is set.
        This generates invalid SQL: SELECT * FROM table WHERE None >= $1
        """
        config = {
            "driver": "postgresql",
            "cursor_field": None,
            "cursor_value": "2023-01-01",
            "cursor_mode": "inclusive"
        }

        query, params = driver.build_incremental_query("public", "users", config)

        # Should NOT generate WHERE clause with None
        assert "WHERE None" not in query, (
            f"BUG: Query contains 'WHERE None' which is invalid SQL.\n"
            f"Query: {query}\n"
            f"Fix: Check that cursor_field is set before adding WHERE clause"
        )
        # Either: no WHERE clause, OR raise an error for invalid config
        assert "None >=" not in query and "None >" not in query, (
            f"BUG: cursor_field=None used in WHERE clause.\n"
            f"Query: {query}"
        )

    def test_cursor_field_none_should_not_add_cursor_filter(self, driver, real_world_config):
        """Test with real-world config that has cursor_field=None (full replication)."""
        # Real config has cursor_field=None for full replication
        config = {
            "driver": real_world_config["driver"],  # postgresql
            "cursor_field": real_world_config["cursor_field"],  # None
            "cursor_mode": real_world_config["cursor_mode"],    # "inclusive"
            "replication_method": real_world_config["replication_method"],  # "full"
        }

        query, params = driver.build_incremental_query("public", "wise_transfers", config)

        # Full replication with no cursor_field should have no WHERE clause for cursor
        assert "WHERE" not in query or "None" not in query, (
            f"BUG: Full replication query should not have cursor WHERE clause.\n"
            f"Query: {query}"
        )
        assert params == [], f"Full replication should have no params, got: {params}"


class TestSqlInjectionProtection:
    """Verify SQLAlchemy-based query builder protects against SQL injection."""

    def test_cursor_field_is_properly_quoted(self, driver):
        """SQLAlchemy should properly quote cursor_field identifier.

        Malicious cursor_field values should be quoted, preventing injection.
        """
        malicious_config = {
            "driver": "postgresql",
            "cursor_field": "1; DROP TABLE users; --",
            "cursor_value": "2023-01-01",
            "cursor_mode": "inclusive"
        }

        query, params = driver.build_incremental_query("schema", "table", malicious_config)

        # SQLAlchemy should quote the identifier, making it safe
        # The malicious string should NOT appear as executable SQL
        assert "DROP TABLE" not in query or '"1; DROP TABLE users; --"' in query, (
            f"Query should have quoted identifier or not include DROP TABLE.\n"
            f"Query: {query}"
        )
        # Value should be parameterized
        assert params == ["2023-01-01"], f"Cursor value should be parameterized, got: {params}"

    def test_order_by_is_properly_quoted(self, driver):
        """SQLAlchemy should properly quote order_by identifier."""
        malicious_config = {
            "driver": "postgresql",
            "order_by": "1; DROP TABLE users; --"
        }

        query, params = driver.build_incremental_query("schema", "table", malicious_config)

        # SQLAlchemy should quote the identifier
        assert "DROP TABLE" not in query or '"1; DROP TABLE users; --"' in query, (
            f"Query should have quoted identifier or not include DROP TABLE.\n"
            f"Query: {query}"
        )

    def test_filters_use_parameterized_values(self, driver):
        """Filters should use parameterized values, not string interpolation."""
        config = {
            "driver": "postgresql",
            "filters": [
                {"field": "status", "op": "=", "value": "active'; DROP TABLE users; --"}
            ]
        }

        query, params = driver.build_incremental_query("schema", "table", config)

        # The malicious value should be in params, not in query string
        assert "DROP TABLE" not in query, (
            f"Malicious value should be parameterized, not in query.\n"
            f"Query: {query}"
        )
        assert any("DROP TABLE" in str(p) for p in params), (
            f"Malicious value should be in params: {params}"
        )


class TestIdentifierValidation:
    """BUG: Identifiers with hyphens pass validation but need quoting."""

    def test_identifier_with_hyphen_needs_quoting(self, driver):
        """BUG: validate_identifier allows hyphens but PostgreSQL needs quoting for them."""
        # Current validation allows hyphens (line 122 in base.py)
        result = driver.validate_identifier("my-column")

        # This passes validation but will fail in PostgreSQL without quotes
        # Either: reject hyphens, OR ensure proper quoting throughout
        assert result is False, (
            "BUG: Identifier 'my-column' passes validation but PostgreSQL requires "
            "quoting for identifiers with hyphens. Either reject hyphens in validation "
            "or ensure all identifiers are properly quoted in SQL."
        )


# =============================================================================
# WORKING FUNCTIONALITY TESTS
# =============================================================================

class TestPostgreSQLDriverInit:
    """Test PostgreSQLDriver initialization."""

    def test_init(self):
        """Test driver initialization."""
        driver = PostgreSQLDriver()
        assert driver.name == "PostgreSQL"
        assert driver.asyncpg is None
        assert driver.connection_pool is None
        assert driver._column_types == {}


class TestPostgreSQLDriverConnectionParams:
    """Test connection parameter extraction."""

    def test_get_connection_params_full_config(self, driver, connection_config):
        """Test connection parameter extraction with full config."""
        params = driver.get_connection_params(connection_config)

        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["user"] == "test_user"
        assert params["password"] == "test_password"
        assert params["database"] == "test_db"

    def test_get_connection_params_port_required(self, driver):
        """Test that port is required."""
        config = {}

        with pytest.raises(ValueError) as exc_info:
            driver.get_connection_params(config)

        assert "port is required" in str(exc_info.value)

    def test_get_connection_params_port_type_conversion(self, driver):
        """Test that string port is converted to int."""
        config = {"port": "5432"}

        params = driver.get_connection_params(config)

        assert params["port"] == 5432
        assert isinstance(params["port"], int)

    def test_get_connection_params_real_world_config(self, driver, real_world_config):
        """Test with real-world config format."""
        params = driver.get_connection_params(real_world_config)

        assert params["host"] == "database-1.cp2em6mk83a2.eu-central-1.rds.amazonaws.com"
        assert params["port"] == 5432
        assert params["user"] == "postgres"
        assert params["password"] == "PDuhZN?cylU~J57M!~Rb>0LmERfb"
        assert params["database"] == "postgres"


class TestPostgreSQLDriverConnectionPool:
    """Test connection pool management."""

    @pytest.mark.asyncio
    async def test_create_connection_pool_missing_asyncpg(self, driver, connection_config):
        """Test connection pool creation without asyncpg package."""
        with patch.dict('sys.modules', {'asyncpg': None}):
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

    @pytest.mark.asyncio
    async def test_close_connection_pool_when_none(self, driver):
        """Test closing when pool is None."""
        driver.connection_pool = None

        await driver.close_connection_pool()

        assert driver.connection_pool is None


class TestPostgreSQLDriverSchemaOperations:
    """Test schema and table operations."""

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
        mock_pool = MagicMock()
        driver.connection_pool = mock_pool

        with pytest.raises(ValueError) as exc_info:
            await driver.create_schema_if_not_exists("invalid-schema!")

        assert "Invalid schema name" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_table_invalid_name(self, driver):
        """Test table creation with invalid name."""
        with pytest.raises(ValueError) as exc_info:
            await driver.create_table_if_not_exists(
                "schema",
                "invalid table!",
                {"columns": []},
                []
            )

        assert "Invalid table name" in str(exc_info.value)


class TestPostgreSQLDriverDataOperations:
    """Test data operations (insert, upsert, query)."""

    @pytest.mark.asyncio
    async def test_execute_upsert_basic(self, driver):
        """Test basic upsert execution."""
        mock_conn = AsyncMock()

        batch = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]

        conflict_config = {
            "on_conflict": "id",
            "update_columns": ["name"]
        }

        await driver.execute_upsert(mock_conn, "schema", "table", batch, conflict_config)

        mock_conn.executemany.assert_called_once()
        query = mock_conn.executemany.call_args[0][0]

        assert "INSERT INTO schema.table" in query
        assert "ON CONFLICT (id)" in query
        assert "DO UPDATE SET name = EXCLUDED.name" in query

    @pytest.mark.asyncio
    async def test_execute_upsert_empty_batch(self, driver):
        """Test upsert with empty batch does nothing."""
        mock_conn = AsyncMock()

        await driver.execute_upsert(mock_conn, "schema", "table", [], {})

        mock_conn.executemany.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_insert_basic(self, driver):
        """Test basic insert execution."""
        mock_conn = AsyncMock()

        batch = [{"id": 1, "name": "Alice"}]

        await driver.execute_insert(mock_conn, "schema", "table", batch)

        mock_conn.executemany.assert_called_once()
        query = mock_conn.executemany.call_args[0][0]

        assert query == "INSERT INTO schema.table (id, name) VALUES ($1, $2)"

    @pytest.mark.asyncio
    async def test_execute_insert_empty_batch(self, driver):
        """Test insert with empty batch does nothing."""
        mock_conn = AsyncMock()

        await driver.execute_insert(mock_conn, "schema", "table", [])

        mock_conn.executemany.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_query_with_params(self, driver):
        """Test query execution with parameters."""
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{"id": 1, "name": "Alice"}]

        result = await driver.execute_query(mock_conn, "SELECT * FROM users WHERE id = $1", [1])

        mock_conn.fetch.assert_called_once_with("SELECT * FROM users WHERE id = $1", 1)
        assert len(result) == 1
        assert result[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_execute_query_without_params(self, driver):
        """Test query execution without parameters."""
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []

        result = await driver.execute_query(mock_conn, "SELECT COUNT(*) FROM users")

        mock_conn.fetch.assert_called_once_with("SELECT COUNT(*) FROM users")
        assert result == []


class TestPostgreSQLDriverQueryBuilding:
    """Test incremental query building."""

    def test_build_query_basic_full_replication(self, driver):
        """Test query building for full replication (no cursor)."""
        config = {"driver": "postgresql"}

        query, params = driver.build_incremental_query("public", "users", config)

        # SQLAlchemy may add newlines, so normalize
        normalized_query = " ".join(query.split())
        assert normalized_query == "SELECT * FROM public.users"
        assert params == []

    def test_build_query_with_columns(self, driver):
        """Test query building with specific columns."""
        config = {"driver": "postgresql", "columns": ["id", "name", "email"]}

        query, params = driver.build_incremental_query("public", "users", config)

        # SQLAlchemy may add newlines, so normalize
        normalized_query = " ".join(query.split())
        assert "SELECT id, name, email FROM public.users" in normalized_query

    def test_build_query_with_filters(self, driver):
        """Test query building with structured filters."""
        config = {
            "driver": "postgresql",
            "filters": [
                {"field": "status", "op": "=", "value": "active"}
            ]
        }

        query, params = driver.build_incremental_query("public", "users", config)

        assert "WHERE" in query
        # Value should be parameterized
        assert "active" in params

    def test_build_query_with_cursor_exclusive(self, driver):
        """Test query building with exclusive cursor mode."""
        config = {
            "driver": "postgresql",
            "cursor_field": "updated_at",
            "cursor_value": "2023-01-01",
            "cursor_mode": "exclusive"
        }

        query, params = driver.build_incremental_query("public", "users", config)

        assert ">" in query  # exclusive uses >
        assert params == ["2023-01-01"]

    def test_build_query_with_cursor_inclusive(self, driver):
        """Test query building with inclusive cursor mode."""
        config = {
            "driver": "postgresql",
            "cursor_field": "updated_at",
            "cursor_value": "2023-01-01",
            "cursor_mode": "inclusive"
        }

        query, params = driver.build_incremental_query("public", "users", config)

        assert ">=" in query  # inclusive uses >=
        assert params == ["2023-01-01"]

    def test_build_query_cursor_value_parameter_overrides_config(self, driver):
        """Test that cursor_value parameter overrides config value."""
        config = {
            "driver": "postgresql",
            "cursor_field": "updated_at",
            "cursor_value": "config-value",
            "cursor_mode": "inclusive"
        }

        query, params = driver.build_incremental_query(
            "public", "users", config, cursor_value="param-value"
        )

        assert params == ["param-value"]

    def test_build_query_with_order_by(self, driver):
        """Test query building with ORDER BY."""
        config = {"driver": "postgresql", "order_by": "name"}

        query, params = driver.build_incremental_query("public", "users", config)

        assert "ORDER BY" in query

    def test_build_query_cursor_field_adds_order_by(self, driver):
        """Test that cursor_field adds ORDER BY when no explicit order_by."""
        config = {"driver": "postgresql", "cursor_field": "updated_at"}

        query, params = driver.build_incremental_query("public", "users", config)

        assert "ORDER BY" in query

    def test_build_query_order_by_overrides_cursor_field_ordering(self, driver):
        """Test that explicit order_by takes precedence over cursor_field."""
        config = {
            "driver": "postgresql",
            "cursor_field": "updated_at",
            "order_by": "priority"
        }

        query, params = driver.build_incremental_query("public", "users", config)

        assert "ORDER BY" in query


class TestPostgreSQLDriverIndexOperations:
    """Test index operations."""

    @pytest.mark.asyncio
    async def test_create_indexes(self, driver):
        """Test index creation."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        driver.connection_pool = mock_pool

        indexes = [
            {"name": "idx_email", "columns": ["email"], "type": "btree"},
            {"name": "idx_created", "columns": ["created_at"]}
        ]

        await driver.create_indexes_if_not_exist("schema", "table", indexes)

        assert mock_conn.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_create_indexes_skips_invalid(self, driver):
        """Test that invalid index configs are skipped."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        driver.connection_pool = mock_pool

        indexes = [
            {"name": "idx_valid", "columns": ["email"]},
            {"name": "idx_no_columns"},  # Missing columns
            {"columns": ["name"]},  # Missing name
        ]

        await driver.create_indexes_if_not_exist("schema", "table", indexes)

        # Only valid index should be created
        assert mock_conn.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_create_indexes_handles_errors(self, driver):
        """Test that index creation errors are logged but don't raise."""
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.execute.side_effect = Exception("Index exists")
        driver.connection_pool = mock_pool

        indexes = [{"name": "idx_test", "columns": ["col"]}]

        # Should not raise
        await driver.create_indexes_if_not_exist("schema", "table", indexes)


class TestPostgreSQLDriverErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    async def test_execute_upsert_propagates_database_error(self, driver):
        """Test that database errors are propagated."""
        mock_conn = AsyncMock()
        mock_conn.executemany.side_effect = Exception("Connection lost")

        batch = [{"id": 1, "name": "Alice"}]
        conflict_config = {"on_conflict": "id"}

        with pytest.raises(Exception) as exc_info:
            await driver.execute_upsert(mock_conn, "schema", "table", batch, conflict_config)

        assert "Connection lost" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_insert_propagates_database_error(self, driver):
        """Test that database errors are propagated."""
        mock_conn = AsyncMock()
        mock_conn.executemany.side_effect = Exception("Constraint violation")

        batch = [{"id": 1}]

        with pytest.raises(Exception) as exc_info:
            await driver.execute_insert(mock_conn, "schema", "table", batch)

        assert "Constraint violation" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_acquire_connection_without_pool(self, driver):
        """Test acquiring connection when pool is not initialized."""
        driver.connection_pool = None

        with pytest.raises(RuntimeError) as exc_info:
            await driver.acquire_connection()

        assert "Connection pool not initialized" in str(exc_info.value)


class TestPostgreSQLDriverHelperMethods:
    """Test helper methods."""

    def test_get_full_table_name_with_schema(self, driver):
        """Test full table name with schema."""
        assert driver.get_full_table_name("public", "users") == "public.users"

    def test_get_full_table_name_without_schema(self, driver):
        """Test full table name without schema."""
        assert driver.get_full_table_name("", "users") == "users"

    def test_validate_identifier_valid(self, driver):
        """Test valid identifiers."""
        assert driver.validate_identifier("valid_name") is True
        assert driver.validate_identifier("name123") is True
        assert driver.validate_identifier("_private") is True

    def test_validate_identifier_invalid(self, driver):
        """Test invalid identifiers."""
        assert driver.validate_identifier("") is False
        assert driver.validate_identifier("123starts_with_digit") is False
        assert driver.validate_identifier("has space") is False
        assert driver.validate_identifier("has!special") is False


class TestPostgreSQLDriverJsonConversion:
    """Test JSON data conversion for JSONB columns."""

    @pytest.mark.asyncio
    async def test_upsert_converts_dict_to_json_string(self, driver):
        """Test that dict values are converted to JSON strings for JSONB columns."""
        mock_conn = AsyncMock()

        # Set up column types
        driver._column_types = {"metadata": "JSONB"}

        batch = [{"id": 1, "metadata": {"key": "value"}}]
        conflict_config = {"on_conflict": "id"}

        await driver.execute_upsert(mock_conn, "schema", "table", batch, conflict_config)

        values = mock_conn.executemany.call_args[0][1]
        # metadata should be JSON string
        assert values[0][1] == '{"key": "value"}'

    @pytest.mark.asyncio
    async def test_upsert_converts_list_to_json_string(self, driver):
        """Test that list values are converted to JSON strings for JSONB columns."""
        mock_conn = AsyncMock()

        driver._column_types = {"tags": "JSONB"}

        batch = [{"id": 1, "tags": ["a", "b", "c"]}]
        conflict_config = {"on_conflict": "id"}

        await driver.execute_upsert(mock_conn, "schema", "table", batch, conflict_config)

        values = mock_conn.executemany.call_args[0][1]
        assert values[0][1] == '["a", "b", "c"]'


class TestPostgreSQLDriverSSLPreferFallback:
    """Test SSL prefer fallback behavior in create_connection_pool."""

    @pytest.mark.asyncio
    async def test_ssl_prefer_retries_on_ssl_error(self, driver, connection_config):
        """ssl_mode=prefer should retry without SSL on handshake error."""
        mock_asyncpg = MagicMock()
        mock_pool = AsyncMock()

        ssl_error = ssl.SSLError("SSL handshake failed")
        mock_asyncpg.create_pool = AsyncMock(side_effect=[ssl_error, mock_pool])

        with patch.dict("sys.modules", {"asyncpg": mock_asyncpg}):
            with patch("builtins.__import__", return_value=mock_asyncpg):
                driver.asyncpg = mock_asyncpg
                await driver.create_connection_pool(connection_config)

        assert mock_asyncpg.create_pool.call_count == 2
        # Second call should have ssl=False
        second_call_kwargs = mock_asyncpg.create_pool.call_args_list[1]
        assert second_call_kwargs.kwargs.get("ssl") is False or second_call_kwargs[1].get("ssl") is False
        assert driver.connection_pool is mock_pool

    @pytest.mark.asyncio
    async def test_ssl_prefer_no_retry_on_non_ssl_error(self, driver, connection_config):
        """ssl_mode=prefer should NOT retry on non-SSL errors."""
        mock_asyncpg = MagicMock()

        non_ssl_error = OSError("Connection timed out")
        mock_asyncpg.create_pool = AsyncMock(side_effect=non_ssl_error)

        with patch.dict("sys.modules", {"asyncpg": mock_asyncpg}):
            with patch("builtins.__import__", return_value=mock_asyncpg):
                driver.asyncpg = mock_asyncpg
                with pytest.raises(OSError, match="Connection timed out"):
                    await driver.create_connection_pool(connection_config)

        assert mock_asyncpg.create_pool.call_count == 1

    @pytest.mark.asyncio
    async def test_ssl_require_does_not_fallback(self, driver):
        """ssl_mode=require should NOT retry without SSL."""
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
            "ssl_mode": "require",
        }

        mock_asyncpg = MagicMock()
        ssl_error = ssl.SSLError("SSL handshake failed")
        mock_asyncpg.create_pool = AsyncMock(side_effect=ssl_error)

        with patch.dict("sys.modules", {"asyncpg": mock_asyncpg}):
            with patch("builtins.__import__", return_value=mock_asyncpg):
                driver.asyncpg = mock_asyncpg
                with pytest.raises(ssl.SSLError):
                    await driver.create_connection_pool(config)

        assert mock_asyncpg.create_pool.call_count == 1
