"""
Exceptions for secrets resolution.
"""


class SecretResolutionError(Exception):
    """Base exception for secrets resolution failures."""

    def __init__(self, message: str, connection_id: str = ""):
        self.connection_id = connection_id
        super().__init__(message)


class SecretNotFoundError(SecretResolutionError):
    """Raised when no secrets exist for a connection."""

    def __init__(self, connection_id: str, detail: str = ""):
        message = f"No secrets found for connection: {connection_id}"
        if detail:
            message = f"{message} ({detail})"
        super().__init__(message, connection_id)


class SecretAccessDeniedError(SecretResolutionError):
    """Raised when access to secrets is denied."""

    def __init__(self, connection_id: str, detail: str = ""):
        message = f"Access denied to secrets for connection: {connection_id}"
        if detail:
            message = f"{message} ({detail})"
        super().__init__(message, connection_id)


class PlaceholderExpansionError(SecretResolutionError):
    """Raised when placeholder expansion fails."""

    def __init__(self, placeholder: str, connection_id: str = "", detail: str = ""):
        message = f"Failed to expand placeholder '{placeholder}'"
        if detail:
            message = f"{message}: {detail}"
        super().__init__(message, connection_id)
        self.placeholder = placeholder
