"""Base connector interface for data sources and destinations."""

import logging
from abc import ABC, abstractmethod
from types import TracebackType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cdk.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Shared lifecycle, metrics, and schema-prep helpers for in-engine connectors.

    The connector I/O contract is the published ``Readable`` / ``Writable``
    protocols in ``cdk.contract`` -- subclasses implement those directly. This
    base only carries the cross-cutting machinery (connect/disconnect lifecycle,
    metrics, schema preparation, context-manager support); it does not redeclare
    the read/write contract, which would be a second grammar free to drift.
    """

    def __init__(self, name: str | None = None):
        """
        Initialize base connector.

        Args:
            name: Optional name for the connector
        """
        self.name = name or self.__class__.__name__
        self.connection: Any = None
        self.is_connected = False
        self.metrics: dict[str, int] = {
            "records_read": 0,
            "records_written": 0,
            "batches_read": 0,
            "batches_written": 0,
            "errors": 0,
        }

    @abstractmethod
    async def connect(self, runtime: "ConnectionRuntime") -> None:
        """
        Establish connection to the data source.

        Args:
            runtime: ConnectionRuntime that manages connection lifecycle
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the data source."""
        pass

    async def prepare_schema(
        self, schema: dict[str, Any], config: dict[str, Any]
    ) -> None:
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

    async def evolve_schema(
        self, changes: dict[str, Any], config: dict[str, Any]
    ) -> None:
        """
        Evolve schema based on detected changes.

        Args:
            changes: Schema changes from SchemaManager
            config: Evolution configuration
        """
        if not self.supports_schema_evolution():
            raise NotImplementedError(f"Schema evolution not supported by {self.name}")

    async def create_versioned_target(
        self, changes: dict[str, Any], config: dict[str, Any]
    ) -> None:
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

    async def health_check(self) -> dict[str, Any]:
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

    def get_metrics(self) -> dict[str, Any]:
        """
        Get connector metrics.

        Returns:
            Metrics dictionary
        """
        return self.metrics.copy()

    def reset_metrics(self) -> None:
        """Reset connector metrics."""
        self.metrics = {
            "records_read": 0,
            "records_written": 0,
            "batches_read": 0,
            "batches_written": 0,
            "errors": 0,
        }

    async def __aenter__(self) -> "BaseConnector":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.disconnect()

    def __repr__(self) -> str:
        """Return the string representation of connector."""
        return (
            f"{self.__class__.__name__}"
            f"(name={self.name}, connected={self.is_connected})"
        )


class ConnectorError(Exception):
    """Base exception for connector errors."""

    pass


class ConnectorConnectionError(ConnectorError):
    """Exception raised when connection fails.

    Deliberately not named ``ConnectionError``: that shadows the builtin
    (an ``OSError`` subclass) for every module importing from this package,
    so an ``except ConnectionError`` there would catch connector failures
    and miss real socket errors.
    """

    pass


class SchemaError(ConnectorError):
    """Exception raised for schema-related errors."""

    pass


class ReadError(ConnectorError):
    """Exception raised during read operations.

    Reserved for failures retrying cannot heal: contract/configuration
    problems and non-transient HTTP statuses. The worker classifies it
    deterministic (the stream fails fatally instead of retrying).
    """

    pass


class TransientReadError(ConnectorError):
    """A read failure that retrying can heal (rate limit, upstream outage).

    Deliberately NOT a ``ReadError`` subclass: the worker classifies
    ``ReadError`` as deterministic/fatal, and this must classify retryable.
    """

    pass


class WriteError(ConnectorError):
    """Exception raised during write operations."""

    pass
