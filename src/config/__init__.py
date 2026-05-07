"""Configuration management for Analitiq Stream."""

from .analitiq_config import (
    validate_pipeline_config,
    validate_connection_config,
)
from .schema_validator import (
    ContractValidationError,
    validate as validate_artifact,
    validate_file as validate_artifact_file,
    ARTIFACT_KINDS,
)
from .endpoint_resolver import (
    ConnectionLookup,
    resolve_endpoint_path,
    resolve_endpoint_ref,
)
from .connection_loader import (
    load_connection,
    load_connection_file,
    load_connector_definition,
    load_connector_for_connection,  # legacy alias
)
from .loader import PathBasedConfigLoader
from .exceptions import (
    ConfigError,
    ConfigNotFoundError,
    ConfigValidationError,
    ConnectorNotFoundError,
    EndpointNotFoundError,
    ConnectionConfigError,
)

__all__ = [
    # Schema validation
    "ContractValidationError",
    "validate_artifact",
    "validate_artifact_file",
    "ARTIFACT_KINDS",
    "validate_pipeline_config",
    "validate_connection_config",
    # Endpoint resolver
    "ConnectionLookup",
    "resolve_endpoint_path",
    "resolve_endpoint_ref",
    # Connection loader
    "load_connection",
    "load_connection_file",
    "load_connector_definition",
    "load_connector_for_connection",
    # Path-based loader (legacy)
    "PathBasedConfigLoader",
    # Exceptions
    "ConfigError",
    "ConfigNotFoundError",
    "ConfigValidationError",
    "ConnectorNotFoundError",
    "EndpointNotFoundError",
    "ConnectionConfigError",
]
