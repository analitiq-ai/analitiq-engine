"""Factory for creating database-specific drivers."""

from typing import Dict, Any

from .base import BaseDatabaseDriver
from .postgresql import PostgreSQLDriver


class DriverFactory:
    """Factory for creating database-specific drivers."""
    
    # Registry of available drivers
    _drivers = {
        "postgresql": PostgreSQLDriver,
        "postgres": PostgreSQLDriver,  # Alias
    }
    
    @classmethod
    def create_driver(cls, driver_name: str) -> BaseDatabaseDriver:
        """
        Create a database driver instance.
        
        Args:
            driver_name: Name of the database driver (postgresql, mysql, etc.)
            
        Returns:
            Instance of the appropriate database driver
            
        Raises:
            ValueError: If driver is not supported
        """
        driver_name = driver_name.lower()
        
        if driver_name not in cls._drivers:
            available = ", ".join(cls._drivers.keys())
            raise ValueError(f"Unsupported database driver '{driver_name}'. Available: {available}")
        
        driver_class = cls._drivers[driver_name]
        return driver_class()
    
    @classmethod
    def register_driver(cls, name: str, driver_class):
        """
        Register a new database driver.
        
        Args:
            name: Driver name to register
            driver_class: Driver class that inherits from BaseDatabaseDriver
        """
        if not issubclass(driver_class, BaseDatabaseDriver):
            raise TypeError("Driver class must inherit from BaseDatabaseDriver")
            
        cls._drivers[name.lower()] = driver_class
    
    @classmethod
    def get_available_drivers(cls) -> list:
        """Get list of available driver names."""
        return list(cls._drivers.keys())


def get_driver(driver_name: str) -> BaseDatabaseDriver:
    """Convenience function to create a database driver.

    Args:
        driver_name: Name of the database driver (postgresql, mysql, etc.)

    Returns:
        Instance of the appropriate database driver
    """
    return DriverFactory.create_driver(driver_name)