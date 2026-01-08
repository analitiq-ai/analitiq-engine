"""Database connector package with driver-based architecture."""

from .base_driver import BaseDatabaseDriver
from .driver_factory import DriverFactory
from .database_connector import DatabaseConnector

# Note: Specific drivers (PostgreSQLDriver, etc.) are dynamically loaded by DriverFactory
# and should not be directly imported to maintain loose coupling

__all__ = ["BaseDatabaseDriver", "DriverFactory", "DatabaseConnector"]