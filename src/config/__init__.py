"""Configuration management for Analitiq Stream."""

# Export config validators
from .analitiq_config import (
    validate_pipeline_config,
    validate_connection_config,
)

# Export endpoint resolver
from .endpoint_resolver import (
    resolve_endpoint_ref,
    resolve_endpoint_path,
)

# Export connection loader
from .connection_loader import (
    load_connection,
    load_connector_for_connection,
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
    # Pipeline and connection validation
    "validate_pipeline_config",
    "validate_connection_config",
    # Endpoint resolver
    "resolve_endpoint_ref",
    "resolve_endpoint_path",
    # Connection loader
    "load_connection",
    "load_connector_for_connection",
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
