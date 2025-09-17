"""Unit tests for database package initialization."""

import pytest


def test_database_package_imports():
    """Test that database package imports work correctly."""
    from analitiq_stream.connectors.database import BaseDatabaseDriver, DriverFactory, DatabaseConnector
    
    # Verify classes are importable
    assert BaseDatabaseDriver is not None
    assert DriverFactory is not None
    assert DatabaseConnector is not None


def test_database_package_all_exports():
    """Test that __all__ exports are correctly defined."""
    import analitiq_stream.connectors.database as db_package
    
    # Check that __all__ is defined
    assert hasattr(db_package, '__all__')
    
    # Check that all exported items exist
    for item in db_package.__all__:
        assert hasattr(db_package, item), f"Missing export: {item}"
    
    # Check that specific classes are in __all__
    expected_exports = ["BaseDatabaseDriver", "DriverFactory", "DatabaseConnector"]
    for expected in expected_exports:
        assert expected in db_package.__all__, f"Missing in __all__: {expected}"


def test_database_package_class_availability():
    """Test that main database classes are available."""
    from analitiq_stream.connectors.database import BaseDatabaseDriver, DriverFactory, DatabaseConnector
    
    # Test that classes have expected attributes
    assert hasattr(BaseDatabaseDriver, '__abstractmethods__')  # Abstract class
    assert hasattr(DriverFactory, 'create_driver')  # Factory method
    assert hasattr(DatabaseConnector, 'connect')  # Connector method