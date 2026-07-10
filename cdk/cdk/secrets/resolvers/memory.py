"""
In-memory secrets resolver for testing.

Stores secrets in memory, useful for unit tests and development.
"""

import logging
from collections.abc import Mapping

from cdk.secrets.exceptions import SecretNotFoundError
from cdk.secrets.protocol import SecretsResolver

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
        secrets: dict[str, dict[str, str]] | None = None,
    ):
        """
        Initialize in-memory secrets resolver.

        Args:
            secrets: Optional dictionary mapping connection_id to secrets dict
        """
        self._secrets: dict[str, dict[str, str]] = secrets or {}

    def set_secrets(self, connection_id: str, secrets: dict[str, str]) -> None:
        """
        Set secrets for a connection.

        Args:
            connection_id: Connection identifier
            secrets: Dictionary of secret key-value pairs
        """
        self._secrets[connection_id] = secrets

    def clear_secrets(self, connection_id: str | None = None) -> None:
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
        secret_refs: Mapping[str, str],
    ) -> dict[str, str]:
        """
        Resolve a connection's declared refs from the in-memory store.

        The stored per-connection dict holds already-resolved values keyed by
        ref name; this returns the values for the declared refs and fails loud
        when a declared ref has no stored value (mirroring a real resolver).

        Args:
            connection_id: Identifier for the connection
            secret_refs: Map of ref name -> locator (only the keys are used)

        Returns:
            Dictionary mapping each declared ref name to its stored value

        Raises:
            SecretNotFoundError: If the connection or a declared ref is unknown
        """
        secrets = self._secrets.get(connection_id)

        if secrets is None:
            raise SecretNotFoundError(
                connection_id=connection_id,
                detail="No secrets registered in memory",
            )

        result: dict[str, str] = {}
        missing = []
        for name in secret_refs:
            if name in secrets:
                result[name] = str(secrets[name])
            else:
                missing.append(name)
        if missing:
            raise SecretNotFoundError(
                connection_id=connection_id,
                detail=f"no in-memory value for declared secret_refs {missing!r}",
            )

        logger.debug(
            f"Resolved in-memory secrets for {connection_id} ({len(result)} keys)"
        )
        return result

    async def close(self) -> None:
        """Clear all secrets from memory."""
        self._secrets.clear()

    def __repr__(self) -> str:
        """Return the string representation."""
        return f"InMemorySecretsResolver({len(self._secrets)} connections)"
