"""Configuration management for Analitiq Stream."""

from .connection_loader import (
    load_connection,
    load_connection_file,
    load_connector_definition,
)
from .endpoint_resolver import (
    ConnectionLookup,
    resolve_endpoint_path,
    resolve_endpoint_ref,
)
from .exceptions import (
    ConfigError,
    ConfigNotFoundError,
    ConfigValidationError,
    ConnectionConfigError,
    ConnectorNotFoundError,
    EndpointNotFoundError,
)
from .loader import PathBasedConfigLoader
from .schema_validator import ARTIFACT_KINDS, ContractValidationError
from .schema_validator import validate as validate_artifact
from .schema_validator import validate_file as validate_artifact_file

__all__ = [
    # Schema validation
    "ContractValidationError",
    "validate_artifact",
    "validate_artifact_file",
    "ARTIFACT_KINDS",
    # Endpoint resolver
    "ConnectionLookup",
    "resolve_endpoint_path",
    "resolve_endpoint_ref",
    # Connection loader
    "load_connection",
    "load_connection_file",
    "load_connector_definition",
    "PathBasedConfigLoader",
    # Exceptions
    "ConfigError",
    "ConfigNotFoundError",
    "ConfigValidationError",
    "ConnectorNotFoundError",
    "EndpointNotFoundError",
    "ConnectionConfigError",
]
