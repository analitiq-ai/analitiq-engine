"""
Exceptions for configuration loading.
"""


class ConfigError(Exception):
    """Base exception for configuration errors."""

    pass


class ConfigNotFoundError(ConfigError):
    """Raised when a configuration file is not found."""

    def __init__(self, path: str, detail: str = ""):
        self.path = path
        message = f"Configuration not found: {path}"
        if detail:
            message = f"{message} ({detail})"
        super().__init__(message)


class ConfigValidationError(ConfigError):
    """Raised when configuration validation fails."""

    def __init__(self, message: str, field: str = ""):
        self.field = field
        super().__init__(message)


class ConnectorNotFoundError(ConfigError):
    """Raised when a connector is not found."""

    def __init__(self, connector_name: str, detail: str = ""):
        self.connector_name = connector_name
        message = f"Connector not found: {connector_name}"
        if detail:
            message = f"{message}. {detail}"
        super().__init__(message)


class EndpointNotFoundError(ConfigError):
    """Raised when an endpoint is not found."""

    def __init__(self, endpoint_ref: str, detail: str = ""):
        self.endpoint_ref = endpoint_ref
        message = f"Endpoint not found: {endpoint_ref}"
        if detail:
            message = f"{message}. {detail}"
        super().__init__(message)


class ConnectionConfigError(ConfigError):
    """Raised when connection configuration is invalid."""

    def __init__(self, connection_ref: str, detail: str = ""):
        self.connection_ref = connection_ref
        message = f"Invalid connection configuration: {connection_ref}"
        if detail:
            message = f"{message} ({detail})"
        super().__init__(message)
