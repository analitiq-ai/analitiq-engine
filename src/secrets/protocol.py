"""
SecretsResolver protocol definition.

Defines the interface that all secrets resolvers must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional


class SecretsResolver(ABC):
    """
    Abstract base class for secrets resolution.

    Secrets resolvers are responsible for fetching secret values
    from various backends (local files, S3, Secrets Manager, etc.)
    and returning them as a dictionary.

    Implementations should:
    - Return secrets as a flat dictionary of key-value pairs
    - Raise SecretNotFoundError if the connection has no secrets
    - Raise SecretAccessDeniedError for permission issues
    - Raise SecretResolutionError for other failures
    """

    @abstractmethod
    async def resolve(
        self,
        connection_id: str,
        *,
        client_id: Optional[str] = None,
        keys: Optional[list[str]] = None,
    ) -> Dict[str, str]:
        """
        Resolve secrets for a connection.

        Args:
            connection_id: Identifier for the connection (e.g., connector name or UUID)
            client_id: Optional client identifier (for multi-tenant deployments)
            keys: Optional list of specific keys to retrieve (if None, return all)

        Returns:
            Dictionary mapping secret keys to their values

        Raises:
            SecretNotFoundError: If no secrets exist for the connection
            SecretAccessDeniedError: If access to secrets is denied
            SecretResolutionError: For other resolution failures
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """
        Clean up any resources held by the resolver.

        Should be called when the resolver is no longer needed.
        """
        ...

    async def __aenter__(self) -> "SecretsResolver":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
