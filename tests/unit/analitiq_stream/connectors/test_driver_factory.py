"""Unit tests for DriverFactory."""

import pytest
from unittest.mock import MagicMock

from src.connectors.database.driver_factory import DriverFactory
from src.connectors.database.base_driver import BaseDatabaseDriver
from src.connectors.database.postgresql_driver import PostgreSQLDriver


class MockDriver(BaseDatabaseDriver):
    """Mock driver for testing registration."""
    
    def __init__(self):
        super().__init__("MockDriver")
    
    async def create_connection_pool(self, config):
        pass
    
    async def close_connection_pool(self):
        pass
    
    async def create_schema_if_not_exists(self, schema_name):
        pass
    
    async def create_table_if_not_exists(self, schema_name, table_name, table_schema, primary_key, unique_constraints=None):
        pass
    
    async def create_indexes_if_not_exist(self, schema_name, table_name, indexes):
        pass
    
    def map_json_schema_to_sql_type(self, field_def):
        return "TEXT"
    
    async def execute_upsert(self, conn, schema_name, table_name, batch, conflict_config):
        pass
    
    async def execute_insert(self, conn, schema_name, table_name, batch):
        pass
    
    async def execute_query(self, conn, query, params=None):
        return []
    
    def build_incremental_query(self, schema_name, table_name, config):
        return "SELECT * FROM table", []
    
    def get_connection_params(self, config):
        return {}


class TestDriverFactory:
    """Test DriverFactory class."""
    
    def test_factory_has_default_drivers(self):
        """Test that factory has default PostgreSQL drivers."""
        available = DriverFactory.get_available_drivers()
        
        assert "postgresql" in available
        assert "postgres" in available  # Alias
        assert len(available) >= 2
    
    def test_factory_imports_and_registration(self):
        """Test that factory can be imported and used for registration."""
        from src.connectors.database.driver_factory import DriverFactory
        from src.connectors.database.base_driver import BaseDatabaseDriver
        
        # Test importing the factory
        assert DriverFactory is not None
        assert hasattr(DriverFactory, '_drivers')
        assert hasattr(DriverFactory, 'register_driver')
        assert hasattr(DriverFactory, 'get_available_drivers')
        
        # Test registration function exists and works
        original_drivers = DriverFactory._drivers.copy()
        try:
            # This should work without error
            initial_count = len(DriverFactory.get_available_drivers())
            assert initial_count >= 2  # At least postgresql and postgres
        finally:
            DriverFactory._drivers = original_drivers
    
    def test_create_postgresql_driver(self):
        """Test creating PostgreSQL driver."""
        driver = DriverFactory.create_driver("postgresql")
        
        assert isinstance(driver, PostgreSQLDriver)
        assert driver.name == "PostgreSQL"
    
    def test_create_postgres_alias_driver(self):
        """Test creating driver using postgres alias."""
        driver = DriverFactory.create_driver("postgres")
        
        assert isinstance(driver, PostgreSQLDriver)
        assert driver.name == "PostgreSQL"
    
    def test_create_driver_case_insensitive(self):
        """Test that driver creation is case-insensitive."""
        # Test various case combinations
        test_cases = ["PostgreSQL", "POSTGRESQL", "PostGreSQL", "postgresql"]
        
        for driver_name in test_cases:
            driver = DriverFactory.create_driver(driver_name)
            assert isinstance(driver, PostgreSQLDriver)
    
    def test_create_unsupported_driver(self):
        """Test creating unsupported driver raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DriverFactory.create_driver("unsupported_db")
        
        error_msg = str(exc_info.value)
        assert "Unsupported database driver 'unsupported_db'" in error_msg
        assert "Available:" in error_msg
        assert "postgresql" in error_msg
    
    def test_create_driver_empty_string(self):
        """Test creating driver with empty string."""
        with pytest.raises(ValueError) as exc_info:
            DriverFactory.create_driver("")
        
        assert "Unsupported database driver ''" in str(exc_info.value)
    
    def test_register_new_driver(self):
        """Test registering a new driver."""
        # Store original drivers to restore later
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Register new driver
            DriverFactory.register_driver("mock", MockDriver)
            
            # Verify it's registered
            available = DriverFactory.get_available_drivers()
            assert "mock" in available
            
            # Verify it can be created
            driver = DriverFactory.create_driver("mock")
            assert isinstance(driver, MockDriver)
            assert driver.name == "MockDriver"
        
        finally:
            # Restore original drivers
            DriverFactory._drivers = original_drivers
    
    def test_register_driver_case_insensitive(self):
        """Test that driver registration is case-insensitive."""
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Register with uppercase name
            DriverFactory.register_driver("MOCK", MockDriver)
            
            # Should be stored in lowercase
            assert "mock" in DriverFactory._drivers
            assert "MOCK" not in DriverFactory._drivers
            
            # Should be accessible via various cases
            driver1 = DriverFactory.create_driver("mock")
            driver2 = DriverFactory.create_driver("MOCK")
            driver3 = DriverFactory.create_driver("Mock")
            
            assert all(isinstance(d, MockDriver) for d in [driver1, driver2, driver3])
        
        finally:
            DriverFactory._drivers = original_drivers
    
    def test_register_driver_overwrite_existing(self):
        """Test that registering a driver overwrites existing one."""
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Register a driver with same name as existing
            DriverFactory.register_driver("postgresql", MockDriver)
            
            # Should now return MockDriver instead of PostgreSQLDriver
            driver = DriverFactory.create_driver("postgresql")
            assert isinstance(driver, MockDriver)
            assert not isinstance(driver, PostgreSQLDriver)
        
        finally:
            DriverFactory._drivers = original_drivers
    
    def test_register_driver_invalid_class(self):
        """Test registering invalid driver class raises TypeError."""
        class InvalidDriver:
            """Invalid driver that doesn't inherit from BaseDatabaseDriver."""
            pass
        
        with pytest.raises(TypeError) as exc_info:
            DriverFactory.register_driver("invalid", InvalidDriver)
        
        assert "must inherit from BaseDatabaseDriver" in str(exc_info.value)
    
    def test_register_driver_none_class(self):
        """Test registering None as driver class raises TypeError."""
        with pytest.raises(TypeError):
            DriverFactory.register_driver("none", None)
    
    def test_get_available_drivers_returns_list(self):
        """Test that get_available_drivers returns a list."""
        available = DriverFactory.get_available_drivers()
        
        assert isinstance(available, list)
        assert len(available) > 0
        assert all(isinstance(name, str) for name in available)
    
    def test_get_available_drivers_includes_registered(self):
        """Test that get_available_drivers includes newly registered drivers."""
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Get initial list
            initial_drivers = DriverFactory.get_available_drivers()
            
            # Register new driver
            DriverFactory.register_driver("test_driver", MockDriver)
            
            # Get updated list
            updated_drivers = DriverFactory.get_available_drivers()
            
            # Should include the new driver
            assert len(updated_drivers) == len(initial_drivers) + 1
            assert "test_driver" in updated_drivers
            assert all(driver in updated_drivers for driver in initial_drivers)
        
        finally:
            DriverFactory._drivers = original_drivers
    
    def test_factory_is_stateless(self):
        """Test that factory methods don't maintain state between calls."""
        # Create multiple instances
        driver1 = DriverFactory.create_driver("postgresql")
        driver2 = DriverFactory.create_driver("postgresql")
        
        # Should be different instances
        assert driver1 is not driver2
        assert isinstance(driver1, PostgreSQLDriver)
        assert isinstance(driver2, PostgreSQLDriver)
    
    def test_factory_registry_is_shared(self):
        """Test that driver registry is shared across factory usage."""
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Register driver
            DriverFactory.register_driver("shared_test", MockDriver)
            
            # Should be available in subsequent calls
            available1 = DriverFactory.get_available_drivers()
            available2 = DriverFactory.get_available_drivers()
            
            assert "shared_test" in available1
            assert "shared_test" in available2
            
            # Should be creatable multiple times
            driver1 = DriverFactory.create_driver("shared_test")
            driver2 = DriverFactory.create_driver("shared_test")
            
            assert isinstance(driver1, MockDriver)
            assert isinstance(driver2, MockDriver)
            assert driver1 is not driver2  # Different instances
        
        finally:
            DriverFactory._drivers = original_drivers


class TestDriverFactoryEdgeCases:
    """Test edge cases for DriverFactory."""
    
    def test_register_driver_with_special_characters(self):
        """Test registering driver with special characters in name."""
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Try various names with special characters
            special_names = ["driver-with-dashes", "driver_with_underscores"]
            
            for name in special_names:
                DriverFactory.register_driver(name, MockDriver)
                
                # Should be stored in lowercase
                assert name.lower() in DriverFactory._drivers
                
                # Should be creatable
                driver = DriverFactory.create_driver(name)
                assert isinstance(driver, MockDriver)
        
        finally:
            DriverFactory._drivers = original_drivers
    
    def test_create_driver_whitespace_handling(self):
        """Test driver creation with whitespace in names."""
        # Leading/trailing whitespace should be handled by lower()
        with pytest.raises(ValueError):
            DriverFactory.create_driver(" postgresql ")
        
        with pytest.raises(ValueError):
            DriverFactory.create_driver("\tpostgresql\n")
    
    def test_registry_modification_during_iteration(self):
        """Test that registry can be safely modified during operations."""
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Get initial state
            initial_count = len(DriverFactory.get_available_drivers())
            
            # Register multiple drivers
            for i in range(5):
                DriverFactory.register_driver(f"test_driver_{i}", MockDriver)
            
            # Verify all were registered
            final_drivers = DriverFactory.get_available_drivers()
            assert len(final_drivers) == initial_count + 5
            
            # Verify all can be created
            for i in range(5):
                driver = DriverFactory.create_driver(f"test_driver_{i}")
                assert isinstance(driver, MockDriver)
        
        finally:
            DriverFactory._drivers = original_drivers
    
    def test_error_message_formatting(self):
        """Test that error messages are properly formatted."""
        with pytest.raises(ValueError) as exc_info:
            DriverFactory.create_driver("nonexistent")
        
        error_msg = str(exc_info.value)
        
        # Should include the attempted driver name
        assert "nonexistent" in error_msg
        
        # Should include available drivers
        assert "Available:" in error_msg
        
        # Should list actual available drivers
        available = DriverFactory.get_available_drivers()
        for driver_name in available:
            assert driver_name in error_msg
    
    def test_driver_instantiation_isolation(self):
        """Test that driver instances are properly isolated."""
        # Create multiple drivers
        drivers = []
        for _ in range(3):
            driver = DriverFactory.create_driver("postgresql")
            drivers.append(driver)
        
        # All should be different instances
        for i in range(len(drivers)):
            for j in range(i + 1, len(drivers)):
                assert drivers[i] is not drivers[j]
        
        # All should be the same type
        assert all(isinstance(d, PostgreSQLDriver) for d in drivers)
        
        # All should have the same name but be independent
        assert all(d.name == "PostgreSQL" for d in drivers)


class TestDriverFactoryInheritance:
    """Test DriverFactory with inheritance scenarios."""
    
    def test_register_driver_subclass(self):
        """Test registering a driver that's a subclass of existing driver."""
        
        class ExtendedPostgreSQLDriver(PostgreSQLDriver):
            """Extended PostgreSQL driver for testing."""
            
            def __init__(self):
                super().__init__()
                self.name = "ExtendedPostgreSQL"
        
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Register extended driver
            DriverFactory.register_driver("extended_pg", ExtendedPostgreSQLDriver)
            
            # Should be creatable
            driver = DriverFactory.create_driver("extended_pg")
            assert isinstance(driver, ExtendedPostgreSQLDriver)
            assert isinstance(driver, PostgreSQLDriver)  # Should also be instance of parent
            assert driver.name == "ExtendedPostgreSQL"
        
        finally:
            DriverFactory._drivers = original_drivers
    
    def test_register_multiple_inheritance_driver(self):
        """Test registering driver with multiple inheritance."""
        
        class MixinClass:
            """Mixin class for testing."""
            def mixin_method(self):
                return "mixin"
        
        class MultipleInheritanceDriver(BaseDatabaseDriver, MixinClass):
            """Driver with multiple inheritance."""
            
            def __init__(self):
                BaseDatabaseDriver.__init__(self, "MultipleInheritance")
                MixinClass.__init__(self)
            
            async def create_connection_pool(self, config):
                pass
            
            async def close_connection_pool(self):
                pass
            
            async def create_schema_if_not_exists(self, schema_name):
                pass
            
            async def create_table_if_not_exists(self, schema_name, table_name, table_schema, primary_key, unique_constraints=None):
                pass
            
            async def create_indexes_if_not_exist(self, schema_name, table_name, indexes):
                pass
            
            def map_json_schema_to_sql_type(self, field_def):
                return "TEXT"
            
            async def execute_upsert(self, conn, schema_name, table_name, batch, conflict_config):
                pass
            
            async def execute_insert(self, conn, schema_name, table_name, batch):
                pass
            
            async def execute_query(self, conn, query, params=None):
                return []
            
            def build_incremental_query(self, schema_name, table_name, config):
                return "SELECT * FROM table", []
            
            def get_connection_params(self, config):
                return {}
        
        original_drivers = DriverFactory._drivers.copy()
        
        try:
            # Should register successfully
            DriverFactory.register_driver("multi_inherit", MultipleInheritanceDriver)
            
            # Should be creatable
            driver = DriverFactory.create_driver("multi_inherit")
            assert isinstance(driver, MultipleInheritanceDriver)
            assert isinstance(driver, BaseDatabaseDriver)
            assert hasattr(driver, 'mixin_method')
            assert driver.mixin_method() == "mixin"
        
        finally:
            DriverFactory._drivers = original_drivers