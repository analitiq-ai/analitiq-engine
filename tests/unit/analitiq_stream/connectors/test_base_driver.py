"""Unit tests for BaseDatabaseDriver."""

import pytest
from unittest.mock import AsyncMock
from typing import Any, Dict, List

from src.source.drivers.base import BaseDatabaseDriver


class MockDatabaseDriver(BaseDatabaseDriver):
    """Mock implementation of BaseDatabaseDriver for testing."""
    
    def __init__(self):
        super().__init__("MockDriver")
        self._connection_pool = None
    
    async def create_connection_pool(self, config: Dict[str, Any]):
        """Mock connection pool creation."""
        self._connection_pool = AsyncMock()
        self.connection_pool = self._connection_pool
    
    async def close_connection_pool(self):
        """Mock connection pool closure."""
        self._connection_pool = None
        self.connection_pool = None
    
    async def create_schema_if_not_exists(self, schema_name: str):
        """Mock schema creation."""
        pass
    
    async def create_table_if_not_exists(
        self, 
        schema_name: str, 
        table_name: str, 
        endpoint_schema: Dict[str, Any],
        primary_key: List[str],
        unique_constraints: List[str] = None
    ):
        """Mock table creation."""
        pass
    
    async def create_indexes_if_not_exist(
        self,
        schema_name: str,
        table_name: str, 
        indexes: List[Dict[str, Any]]
    ):
        """Mock index creation."""
        pass
    
    def map_json_schema_to_sql_type(self, field_def: Dict[str, Any]) -> str:
        """Mock type mapping."""
        return "TEXT"
    
    async def execute_upsert(
        self,
        conn,
        schema_name: str,
        table_name: str,
        batch: List[Dict[str, Any]],
        conflict_config: Dict[str, Any]
    ):
        """Mock upsert execution."""
        pass
    
    async def execute_insert(
        self,
        conn, 
        schema_name: str,
        table_name: str,
        batch: List[Dict[str, Any]]
    ):
        """Mock insert execution."""
        pass
    
    async def execute_query(
        self,
        conn,
        query: str,
        params: List[Any] = None
    ) -> List[Dict[str, Any]]:
        """Mock query execution."""
        return []
    
    def build_incremental_query(
        self,
        schema_name: str,
        table_name: str,
        config: Dict[str, Any]
    ):
        """Mock query building."""
        return "SELECT * FROM table", []
    
    def get_connection_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Mock connection parameters extraction."""
        return {
            "host": config.get("host", "localhost"),
            "port": config.get("port", 5432),
            "database": config.get("database", "test")
        }


@pytest.fixture
def driver():
    """Mock driver instance."""
    return MockDatabaseDriver()


class TestBaseDatabaseDriver:
    """Test BaseDatabaseDriver abstract base class."""
    
    def test_init(self):
        """Test driver initialization."""
        driver = MockDatabaseDriver()
        assert driver.name == "MockDriver"
        assert driver.connection_pool is None
    
    def test_abstract_methods(self):
        """Test that abstract methods exist."""
        # Verify that the abstract methods are defined
        abstract_methods = {
            'create_connection_pool',
            'close_connection_pool',
            'create_schema_if_not_exists',
            'create_table_if_not_exists', 
            'create_indexes_if_not_exist',
            'map_json_schema_to_sql_type',
            'execute_upsert',
            'execute_insert',
            'execute_query',
            'build_incremental_query',
            'get_connection_params'
        }
        
        driver_methods = set(dir(BaseDatabaseDriver))
        for method in abstract_methods:
            assert method in driver_methods
    
    def test_get_full_table_name_with_schema(self, driver):
        """Test get_full_table_name with schema."""
        result = driver.get_full_table_name("test_schema", "test_table")
        assert result == "test_schema.test_table"
    
    def test_get_full_table_name_without_schema(self, driver):
        """Test get_full_table_name without schema."""
        result = driver.get_full_table_name("", "test_table")
        assert result == "test_table"
        
        result = driver.get_full_table_name(None, "test_table")
        assert result == "test_table"
    
    def test_validate_identifier_valid_cases(self, driver):
        """Test validate_identifier with valid identifiers."""
        valid_identifiers = [
            "table_name",
            "TableName",
            "table123",
            "t",
            "test_table_name",
            "schema-name",  # Allows hyphens
            "CamelCase",
            "lowercase"
        ]
        
        for identifier in valid_identifiers:
            assert driver.validate_identifier(identifier) is True, f"Failed for: {identifier}"
    
    def test_validate_identifier_invalid_cases(self, driver):
        """Test validate_identifier with invalid identifiers."""
        invalid_identifiers = [
            "",  # Empty
            None,  # None
            "123table",  # Starts with digit
            "table name",  # Contains space
            "table@name",  # Contains special character
            "table.name",  # Contains dot
            "table;name",  # Contains semicolon
            "table'name",  # Contains quote
            "table\"name",  # Contains double quote
            "table(name)",  # Contains parentheses
            "table[name]",  # Contains brackets
        ]
        
        for identifier in invalid_identifiers:
            assert driver.validate_identifier(identifier) is False, f"Should fail for: {identifier}"
    
    @pytest.mark.asyncio
    async def test_acquire_connection_with_pool(self, driver):
        """Test acquire_connection when pool exists."""
        await driver.create_connection_pool({})
        
        connection = await driver.acquire_connection()
        
        # Should call acquire on the pool
        driver.connection_pool.acquire.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_acquire_connection_without_pool(self, driver):
        """Test acquire_connection when pool doesn't exist."""
        with pytest.raises(RuntimeError) as exc_info:
            await driver.acquire_connection()
        
        assert "Connection pool not initialized" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_connection_lifecycle(self, driver):
        """Test connection pool lifecycle."""
        config = {"host": "localhost", "port": 5432}
        
        # Initially no pool
        assert driver.connection_pool is None
        
        # Create pool
        await driver.create_connection_pool(config)
        assert driver.connection_pool is not None
        
        # Close pool
        await driver.close_connection_pool()
        assert driver.connection_pool is None
    
    def test_concrete_implementation_methods(self, driver):
        """Test that concrete implementation works."""
        # Test connection params extraction
        config = {"host": "testhost", "port": 3306, "database": "testdb"}
        params = driver.get_connection_params(config)
        
        assert params["host"] == "testhost"
        assert params["port"] == 3306
        assert params["database"] == "testdb"
        
        # Test type mapping
        field_def = {"type": "string", "maxLength": 100}
        sql_type = driver.map_json_schema_to_sql_type(field_def)
        assert sql_type == "TEXT"
        
        # Test query building
        query, params = driver.build_incremental_query("schema", "table", {})
        assert query == "SELECT * FROM table"
        assert params == []


class TestBaseDatabaseDriverCannotInstantiate:
    """Test that BaseDatabaseDriver cannot be instantiated directly."""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that abstract class cannot be instantiated."""
        with pytest.raises(TypeError):
            BaseDatabaseDriver("TestDriver")
    
    def test_must_implement_abstract_methods(self):
        """Test that subclass must implement all abstract methods."""
        
        class IncompleteDriver(BaseDatabaseDriver):
            """Incomplete driver missing abstract methods."""
            def __init__(self):
                super().__init__("IncompleteDriver")
            
            # Missing all abstract method implementations
        
        with pytest.raises(TypeError):
            IncompleteDriver()
    
    def test_partial_implementation_fails(self):
        """Test that partial implementation still fails."""
        
        class PartialDriver(BaseDatabaseDriver):
            """Partial driver with some abstract methods implemented."""
            def __init__(self):
                super().__init__("PartialDriver")
            
            async def create_connection_pool(self, config):
                pass
            
            # Missing other abstract methods
        
        with pytest.raises(TypeError):
            PartialDriver()


class TestBaseDatabaseDriverEdgeCases:
    """Test edge cases for BaseDatabaseDriver."""
    
    def test_validate_identifier_edge_cases(self, driver):
        """Test validate_identifier edge cases."""
        # Test with numeric strings that aren't identifiers
        assert driver.validate_identifier("123") is False
        assert driver.validate_identifier("0table") is False
        
        # Test with mixed valid characters
        assert driver.validate_identifier("a_b-c123") is True
        
        # Test minimum length
        assert driver.validate_identifier("a") is True
        assert driver.validate_identifier("_") is False  # Only underscore fails isalnum()
        assert driver.validate_identifier("-") is False  # Only hyphen fails isalnum()
        
        # Test special cases with underscores and hyphens
        assert driver.validate_identifier("_table") is True
        assert driver.validate_identifier("-table") is True
        assert driver.validate_identifier("table_") is True
        assert driver.validate_identifier("table-") is True
    
    def test_get_full_table_name_edge_cases(self, driver):
        """Test get_full_table_name edge cases."""
        # Test with empty strings
        assert driver.get_full_table_name("", "") == ""
        assert driver.get_full_table_name("schema", "") == "schema."
        
        # Test with None values  
        assert driver.get_full_table_name(None, None) is None  # None schema returns table_name (None)
        
        # Test with whitespace
        assert driver.get_full_table_name(" ", "table") == " .table"  # Space is truthy, so forms full name
        assert driver.get_full_table_name("schema", " ") == "schema. "
    
    @pytest.mark.asyncio 
    async def test_mock_methods_work(self, driver):
        """Test that mock implementation methods work correctly."""
        config = {}
        
        # Test connection lifecycle
        await driver.create_connection_pool(config)
        assert driver.connection_pool is not None
        
        # Test schema operations
        await driver.create_schema_if_not_exists("test_schema")
        await driver.create_table_if_not_exists(
            "test_schema", 
            "test_table", 
            {}, 
            ["id"], 
            ["email"]
        )
        await driver.create_indexes_if_not_exist("test_schema", "test_table", [])
        
        # Test data operations
        mock_conn = AsyncMock()
        await driver.execute_insert(mock_conn, "schema", "table", [])
        await driver.execute_upsert(mock_conn, "schema", "table", [], {})
        
        results = await driver.execute_query(mock_conn, "SELECT 1")
        assert results == []
        
        # Test cleanup
        await driver.close_connection_pool()
        assert driver.connection_pool is None