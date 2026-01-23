"""Destination handler implementations and registry."""

from typing import Any, Dict, Type

from ..base_handler import BaseDestinationHandler
from .database import DatabaseDestinationHandler
from .stream import StreamDestinationHandler
from .file import FileDestinationHandler
from .api import ApiDestinationHandler


__all__ = [
    "DatabaseDestinationHandler",
    "StreamDestinationHandler",
    "FileDestinationHandler",
    "ApiDestinationHandler",
    "HandlerRegistry",
    "get_handler",
]


class HandlerRegistry:
    """
    Central registry for destination handlers.

    Maps connector_type values from configuration to handler classes.
    Supports registration of new handlers at runtime.
    """

    # Handler classes mapped by connector_type
    _handlers: Dict[str, Type[BaseDestinationHandler]] = {
        # SQLAlchemy-based database handler (unified for all SQL databases)
        # Dialect is selected based on the 'driver' field in connection config
        "db": DatabaseDestinationHandler,
        "database": DatabaseDestinationHandler,
        "postgresql": DatabaseDestinationHandler,
        "postgres": DatabaseDestinationHandler,
        "mysql": DatabaseDestinationHandler,
        "mariadb": DatabaseDestinationHandler,
        "sqlite": DatabaseDestinationHandler,
        # API handler
        "api": ApiDestinationHandler,
        # Stream handlers
        "stdout": StreamDestinationHandler,
        # File handlers
        "file": FileDestinationHandler,
        "s3": FileDestinationHandler,  # S3 uses FileDestinationHandler with S3 storage backend
    }

    @classmethod
    def register(cls, connector_type: str, handler_class: Type[BaseDestinationHandler]) -> None:
        """
        Register a handler class for a connector type.

        Args:
            connector_type: The connector_type value from configuration
            handler_class: Handler class that implements BaseDestinationHandler
        """
        cls._handlers[connector_type.lower()] = handler_class

    @classmethod
    def get_handler_class(cls, connector_type: str) -> Type[BaseDestinationHandler]:
        """
        Get the handler class for a connector type.

        Args:
            connector_type: The connector_type value from configuration

        Returns:
            Handler class

        Raises:
            ValueError: If connector type is not supported
        """
        handler_class = cls._handlers.get(connector_type.lower())
        if handler_class is None:
            supported = ", ".join(sorted(cls._handlers.keys()))
            raise ValueError(
                f"Unsupported connector type: {connector_type}. "
                f"Supported types: {supported}"
            )
        return handler_class

    @classmethod
    def create_handler(
        cls,
        connector_type: str,
        config: Dict[str, Any] | None = None,
    ) -> BaseDestinationHandler:
        """
        Create a handler instance for a connector type.

        Args:
            connector_type: The connector_type value from configuration
            config: Optional configuration (not used for instantiation,
                   handler.connect() takes config)

        Returns:
            Handler instance
        """
        handler_class = cls.get_handler_class(connector_type)
        return handler_class()

    @classmethod
    def get_supported_types(cls) -> list[str]:
        """Get list of supported connector types."""
        return sorted(cls._handlers.keys())


def get_handler(connector_type: str) -> BaseDestinationHandler:
    """
    Factory function to get handler by connector type.

    This is the main entry point for creating handlers.
    Uses HandlerRegistry internally.

    Args:
        connector_type: Type of connector (db, api, file, s3, stdout, etc.)

    Returns:
        Instance of the appropriate handler

    Raises:
        ValueError: If connector type is not supported
    """
    return HandlerRegistry.create_handler(connector_type)
