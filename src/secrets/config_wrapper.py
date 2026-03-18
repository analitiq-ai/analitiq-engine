"""
ConnectionConfig wrapper for lazy secret resolution.

Wraps connection configuration with lazy (just-in-time) secret resolution,
ensuring secrets are only fetched when actually needed.
"""

import re
import logging
from typing import Any, Dict, Optional

from src.secrets.protocol import SecretsResolver
from src.secrets.exceptions import PlaceholderExpansionError

logger = logging.getLogger(__name__)


class ConnectionConfig:
    """
    Wraps connection config with lazy secret resolution.

    Secrets are resolved at the first call to resolve(), not at initialization.
    This minimizes the time secrets spend in memory.

    Usage:
        config = ConnectionConfig(
            raw_config={"host": "api.example.com", "token": "${API_TOKEN}"},
            connection_id="my_api",
            resolver=LocalFileSecretsResolver("/path/to/secrets")
        )

        # Secrets are NOT resolved yet

        # Later, when actually connecting:
        resolved = await config.resolve()
        # Now secrets are resolved: {"host": "api.example.com", "token": "actual-token"}
    """

    # Pattern for ${placeholder} syntax
    PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")

    def __init__(
        self,
        raw_config: Dict[str, Any],
        connection_id: str,
        resolver: SecretsResolver,
        *,
        org_id: Optional[str] = None,
    ):
        """
        Initialize ConnectionConfig wrapper.

        Args:
            raw_config: Configuration dict with ${placeholder} values
            connection_id: Identifier for the connection
            resolver: SecretsResolver instance for fetching secrets
            org_id: Optional org identifier for multi-tenant deployments
        """
        self._raw_config = raw_config
        self._connection_id = connection_id
        self._resolver = resolver
        self._org_id = org_id
        self._resolved_config: Optional[Dict[str, Any]] = None
        self._secrets: Optional[Dict[str, str]] = None

    @property
    def connection_id(self) -> str:
        """Get the connection identifier."""
        return self._connection_id

    @property
    def raw_config(self) -> Dict[str, Any]:
        """Get the raw (unresolved) configuration."""
        return self._raw_config

    @property
    def is_resolved(self) -> bool:
        """Check if secrets have been resolved."""
        return self._resolved_config is not None

    async def resolve(self) -> Dict[str, Any]:
        """
        Resolve all ${placeholder} values just-in-time.

        If the raw config contains no placeholders, returns it as-is
        without calling the secrets resolver.

        Returns:
            Fully resolved configuration dictionary

        Raises:
            SecretNotFoundError: If secrets cannot be found
            PlaceholderExpansionError: If a placeholder cannot be expanded
        """
        if self._resolved_config is not None:
            return self._resolved_config

        # Short-circuit: no placeholders means no secrets to fetch
        if not self.has_placeholders():
            self._resolved_config = self._raw_config.copy()
            logger.debug(f"No placeholders in connection {self._connection_id}, skipping secret resolution")
            return self._resolved_config

        # Fetch secrets from resolver
        self._secrets = await self._resolver.resolve(
            self._connection_id,
            org_id=self._org_id,
        )

        # Expand placeholders
        self._resolved_config = self._expand_placeholders(
            self._raw_config,
            self._secrets,
        )

        logger.debug(f"Resolved secrets for connection: {self._connection_id}")
        return self._resolved_config

    def _expand_placeholders(
        self,
        value: Any,
        secrets: Dict[str, str],
    ) -> Any:
        """
        Recursively expand ${placeholder} values using secrets.

        Args:
            value: Value to expand (can be dict, list, str, or primitive)
            secrets: Dictionary of secret key-value pairs

        Returns:
            Value with all placeholders expanded

        Raises:
            PlaceholderExpansionError: If a placeholder cannot be expanded
        """
        if isinstance(value, str):
            return self._expand_string(value, secrets)
        elif isinstance(value, dict):
            return {k: self._expand_placeholders(v, secrets) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._expand_placeholders(item, secrets) for item in value]
        return value

    def _expand_string(self, value: str, secrets: Dict[str, str]) -> str:
        """
        Expand placeholders in a string value.

        Args:
            value: String that may contain ${placeholder} patterns
            secrets: Dictionary of secret key-value pairs

        Returns:
            String with placeholders replaced

        Raises:
            PlaceholderExpansionError: If a required placeholder is missing
        """
        def replace_placeholder(match: re.Match) -> str:
            key = match.group(1)
            if key in secrets:
                return str(secrets[key])
            raise PlaceholderExpansionError(
                placeholder=f"${{{key}}}",
                connection_id=self._connection_id,
                detail=f"Secret key '{key}' not found",
            )

        return self.PLACEHOLDER_PATTERN.sub(replace_placeholder, value)

    def has_placeholders(self) -> bool:
        """Check if the raw config contains any unresolved placeholders."""
        return self._find_placeholders(self._raw_config) != []

    def get_placeholder_keys(self) -> list[str]:
        """Get list of placeholder keys that need to be resolved."""
        return self._find_placeholders(self._raw_config)

    def _find_placeholders(self, value: Any) -> list[str]:
        """Recursively find all placeholder keys in a value."""
        placeholders: list[str] = []

        if isinstance(value, str):
            matches = self.PLACEHOLDER_PATTERN.findall(value)
            placeholders.extend(matches)
        elif isinstance(value, dict):
            for v in value.values():
                placeholders.extend(self._find_placeholders(v))
        elif isinstance(value, list):
            for item in value:
                placeholders.extend(self._find_placeholders(item))

        return placeholders

    def clear_resolved(self) -> None:
        """
        Clear resolved configuration and secrets from memory.

        Call this after connection is established to minimize
        secret exposure time.
        """
        self._resolved_config = None
        self._secrets = None

    def __repr__(self) -> str:
        """String representation."""
        status = "resolved" if self.is_resolved else "unresolved"
        return f"ConnectionConfig({self._connection_id}, {status})"
