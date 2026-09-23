"""Configuration management for Analitiq Stream."""

from .endpoint_resolver import ConnectionLookup, resolve_endpoint_path
from .exceptions import (
    ConfigError,
    ConfigNotFoundError,
    ConfigValidationError,
    ConnectorNotFoundError,
    EndpointNotFoundError,
)
from .loader import PathBasedConfigLoader

__all__ = [
    # Endpoint resolver
    "ConnectionLookup",
    "resolve_endpoint_path",
    # Connection loader
    "PathBasedConfigLoader",
    # Exceptions
    "ConfigError",
    "ConfigNotFoundError",
    "ConfigValidationError",
    "ConnectorNotFoundError",
    "EndpointNotFoundError",
]
