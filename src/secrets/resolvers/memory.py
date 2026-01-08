"""
In-memory secrets resolver for testing.

Stores secrets in memory, useful for unit tests and development.
"""

import logging
from typing import Dict, Optional

from src.secrets.protocol import SecretsResolver
from src.secrets.exceptions import SecretNotFoundError

logger = logging.getLogger(__name__)


class InMemorySecretsResolver(SecretsResolver):
    """
    In-memory secrets resolver for testing.

    Stores secrets in a dictionary, useful for:
    - Unit tests
    - Integration tests
    - Development without external secrets storage

    Usage:
        resolver = InMemorySecretsResolver({
            "my_api": {"token": "test-token"},
            "my_db": {"password": "test-password"},
        })

        secrets = await resolver.resolve("my_api")
        # {"token": "test-token"}
    """

    def __init__(
        self,
        secrets: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        """
        Initialize in-memory secrets resolver.

        Args:
            secrets: Optional dictionary mapping connection_id to secrets dict
        """
        self._secrets: Dict[str, Dict[str, str]] = secrets or {}

    def set_secrets(self, connection_id: str, secrets: Dict[str, str]) -> None:
        """
        Set secrets for a connection.

        Args:
            connection_id: Connection identifier
            secrets: Dictionary of secret key-value pairs
        """
        self._secrets[connection_id] = secrets

    def clear_secrets(self, connection_id: Optional[str] = None) -> None:
        """
        Clear secrets.

        Args:
            connection_id: If provided, clear only this connection's secrets.
                          If None, clear all secrets.
        """
        if connection_id:
            self._secrets.pop(connection_id, None)
        else:
            self._secrets.clear()

    async def resolve(
        self,
        connection_id: str,
        *,
        client_id: Optional[str] = None,
        keys: Optional[list[str]] = None,
    ) -> Dict[str, str]:
        """
        Resolve secrets for a connection from memory.

        Args:
            connection_id: Identifier for the connection
            client_id: Ignored for in-memory resolver
            keys: Optional list of specific keys to retrieve

        Returns:
            Dictionary mapping secret keys to their values

        Raises:
            SecretNotFoundError: If no secrets exist for the connection
        """
        # Try with client_id prefix first if provided
        lookup_keys = []
        if client_id:
            lookup_keys.append(f"{client_id}/{connection_id}")
        lookup_keys.append(connection_id)

        secrets = None
        for key in lookup_keys:
            if key in self._secrets:
                secrets = self._secrets[key]
                break

        if secrets is None:
            raise SecretNotFoundError(
                connection_id=connection_id,
                detail="No secrets registered in memory",
            )

        # Convert all values to strings
        result = {k: str(v) for k, v in secrets.items()}

        # Filter to specific keys if requested
        if keys:
            result = {k: v for k, v in result.items() if k in keys}

        logger.debug(f"Resolved in-memory secrets for {connection_id} ({len(result)} keys)")
        return result

    async def close(self) -> None:
        """Clear all secrets from memory."""
        self._secrets.clear()

    def __repr__(self) -> str:
        """String representation."""
        return f"InMemorySecretsResolver({len(self._secrets)} connections)"
