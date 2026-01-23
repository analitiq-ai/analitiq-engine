"""Configuration management for Analitiq Stream."""

# Export config loader and validators
from .analitiq_config import (
    load_analitiq_config,
    validate_consolidated_config,
    ConsolidatedConfigValidator,
)

# Export path-based config loader
from .loader import PathBasedConfigLoader

# Export exceptions
from .exceptions import (
    ConfigError,
    ConfigNotFoundError,
    ConfigValidationError,
    ConnectorNotFoundError,
    EndpointNotFoundError,
    ConnectionConfigError,
)

__all__ = [
    # Config loader
    "load_analitiq_config",
    # Consolidated config validation
    "validate_consolidated_config",
    "ConsolidatedConfigValidator",
    # Path-based loader
    "PathBasedConfigLoader",
    # Exceptions
    "ConfigError",
    "ConfigNotFoundError",
    "ConfigValidationError",
    "ConnectorNotFoundError",
    "EndpointNotFoundError",
    "ConnectionConfigError",
]
