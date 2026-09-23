"""Exceptions for configuration loading."""


class ConfigError(Exception):
    """Base exception for configuration errors."""

    pass


class ConfigValidationError(ConfigError):
    """Raised when configuration validation fails."""

    def __init__(self, message: str, field: str = ""):
        self.field = field
        super().__init__(message)
