"""Base connector interface for data sources."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ...engine.resolved import ResolvedSource
    from ...shared.connection_runtime import ConnectionRuntime
    from ...state.state_manager import StateManager

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Abstract base class for source connectors.

    Source connectors are read-only. Destination writes happen via the
    `BaseDestinationHandler` hierarchy in `src/destination/`.
    """

    def __init__(self, name: str | None = None):
        self.name = name or self.__class__.__name__
        self.is_connected = False
        self.metrics = {
            "records_read": 0,
            "batches_read": 0,
            "errors": 0,
        }

    @abstractmethod
    async def connect(self, runtime: "ConnectionRuntime") -> None:
        """Establish connection. Implementations must call `runtime.materialize()`."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection. Must be safe to call multiple times."""

    @abstractmethod
    async def read_batches(
        self,
        source: "ResolvedSource",
        *,
        state_manager: "StateManager",
        stream_id: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Read records in batches.

        Args:
            source: Resolved source spec (typed). Provides endpoint, replication,
                primary_keys, filters, etc.
            state_manager: State manager for incremental cursor tracking.
            stream_id: Stream identifier (used as state-partition key).
            partition: Optional partition key for sharded reads.
            batch_size: Records per batch.
        """

    def supports_incremental_read(self) -> bool:
        return False

    async def health_check(self) -> Dict[str, Any]:
        return {
            "connector": self.name,
            "status": "healthy" if self.is_connected else "disconnected",
            "metrics": self.metrics.copy(),
        }

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.copy()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, connected={self.is_connected})"


class ConnectorError(Exception):
    """Base exception for connector errors."""


class ConnectionError(ConnectorError):
    """Raised when connection fails."""


class SchemaError(ConnectorError):
    """Raised for schema-related errors."""


class ReadError(ConnectorError):
    """Raised during read operations."""


class WriteError(ConnectorError):
    """Raised during write operations (legacy: source connectors are read-only)."""
