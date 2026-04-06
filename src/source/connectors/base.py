"""Base connector interface for data sources and destinations."""

import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract base class for all data connectors.

    Defines the interface that all connectors must implement for
    reading from and writing to data sources.
    """

    def __init__(self, name: str = None):
        """
        Initialize base connector.

        Args:
            name: Optional name for the connector
        """
        self.name = name or self.__class__.__name__
        self.connection = None
        self.is_connected = False
        self.metrics = {
            "records_read": 0,
            "records_written": 0,
            "batches_read": 0,
            "batches_written": 0,
            "errors": 0,
        }

    @abstractmethod
    async def connect(self, runtime: "ConnectionRuntime"):
        """
        Establish connection to the data source.

        Args:
            runtime: ConnectionRuntime that manages connection lifecycle
        """
        pass

    @abstractmethod
    async def disconnect(self):
        """Close connection to the data source."""
        pass


    @abstractmethod
    async def read_batches(
        self,
        config: Dict[str, Any],
        *,
        state_manager: "StateManager",
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """
        Read data in batches from the source with state management.

        Args:
            config: Read configuration
            state_manager: State manager for incremental replication
            stream_name: Name of the stream for state tracking
            partition: Optional partition identifier for sharded streams
            batch_size: Number of records per batch

        Yields:
            Batches of records as dictionaries
        """
        pass

    @abstractmethod
    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        """
        Write a batch of records to the destination.

        Args:
            batch: List of records to write
            config: Write configuration
        """
        pass

    async def prepare_schema(self, schema: Dict[str, Any], config: Dict[str, Any]):
        """
        Prepare destination schema (create tables, validate endpoints, etc.).

        Args:
            schema: Schema to prepare
            config: Preparation configuration
        """
        # Default implementation - can be overridden by specific connectors
        logger.info(f"Preparing schema for {self.name}")

    def supports_schema_evolution(self) -> bool:
        """
        Check if connector supports schema evolution.

        Returns:
            True if schema evolution is supported
        """
        return False

    async def evolve_schema(self, changes: Dict[str, Any], config: Dict[str, Any]):
        """
        Evolve schema based on detected changes.

        Args:
            changes: Schema changes from SchemaManager
            config: Evolution configuration
        """
        if not self.supports_schema_evolution():
            raise NotImplementedError(f"Schema evolution not supported by {self.name}")

    async def create_versioned_target(
        self, changes: Dict[str, Any], config: Dict[str, Any]
    ):
        """
        Create a new versioned target for breaking schema changes.

        Args:
            changes: Schema changes from SchemaManager
            config: Target creation configuration
        """
        # Default implementation - can be overridden by specific connectors
        logger.warning(f"Versioned target creation not implemented for {self.name}")

    def supports_incremental_read(self) -> bool:
        """
        Check if connector supports incremental reading.

        Returns:
            True if incremental reading is supported
        """
        return False

    def supports_upsert(self) -> bool:
        """
        Check if connector supports upsert operations.

        Returns:
            True if upsert is supported
        """
        return False

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on the connector.

        Returns:
            Health status dictionary
        """
        return {
            "connector": self.name,
            "status": "healthy" if self.is_connected else "disconnected",
            "metrics": self.metrics.copy(),
        }

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get connector metrics.

        Returns:
            Metrics dictionary
        """
        return self.metrics.copy()

    def reset_metrics(self):
        """Reset connector metrics."""
        self.metrics = {
            "records_read": 0,
            "records_written": 0,
            "batches_read": 0,
            "batches_written": 0,
            "errors": 0,
        }

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

    def __repr__(self):
        """String representation of connector."""
        return f"{self.__class__.__name__}(name={self.name}, connected={self.is_connected})"


class ConnectorError(Exception):
    """Base exception for connector errors."""

    pass


class ConnectionError(ConnectorError):
    """Exception raised when connection fails."""

    pass


class SchemaError(ConnectorError):
    """Exception raised for schema-related errors."""

    pass


class ReadError(ConnectorError):
    """Exception raised during read operations."""

    pass


class WriteError(ConnectorError):
    """Exception raised during write operations."""

    pass
