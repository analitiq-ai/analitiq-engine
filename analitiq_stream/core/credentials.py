"""Credentials management for secure authentication."""

import json
import logging
import os
from string import Template
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class CredentialsManager:
    """
    Manages credentials for data sources and destinations.

    Features:
    - Secure loading of credentials from external files
    - Environment variable expansion
    - Validation of required credential fields
    - Support for different authentication types
    """

    def __init__(self):
        self.credentials_cache = {}

    def load_credentials(self, credentials_path: str) -> Dict[str, Any]:
        """
        Load credentials from JSON file.

        Args:
            credentials_path: Path to credentials JSON file

        Returns:
            Dictionary containing credentials

        Raises:
            FileNotFoundError: If credentials file doesn't exist
            ValueError: If credentials file is invalid
        """
        try:
            if credentials_path in self.credentials_cache:
                return self.credentials_cache[credentials_path]

            if not Path(credentials_path).exists():
                raise FileNotFoundError(
                    f"Credentials file not found: {credentials_path}"
                )

            with open(credentials_path, "r") as f:
                credentials = json.load(f)

            # Expand environment variables
            expanded_credentials = self._expand_environment_variables(credentials)

            # Cache the credentials
            self.credentials_cache[credentials_path] = expanded_credentials

            logger.info(f"Loaded credentials from {credentials_path}")
            return expanded_credentials

        except json.JSONDecodeError as e:
            logger.error(
                f"Invalid JSON in credentials file {credentials_path}: {str(e)}"
            )
            raise ValueError(f"Invalid JSON in credentials file: {str(e)}")
        except Exception as e:
            logger.error(
                f"Failed to load credentials from {credentials_path}: {str(e)}"
            )
            raise

    def _expand_environment_variables(
        self, credentials: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Expand environment variables in credentials.

        Args:
            credentials: Raw credentials dictionary

        Returns:
            Credentials with environment variables expanded
        """
        def expand_env(obj, *, strict=False, warn_missing=False):
            def sub(s: str) -> str:
                if strict:
                    # raises KeyError if any placeholder is missing
                    return Template(s).substitute(os.environ)
                val = Template(s).safe_substitute(os.environ)
                if warn_missing and val != Template(s).substitute({**os.environ, **{k:'${'+k+'}' for k in os.environ}}):
                    logger.warning("Some environment variables in %r were not found", s)
                return val

            if isinstance(obj, str):
                return sub(obj)
            if isinstance(obj, Mapping):
                return {k: expand_env(v, strict=strict, warn_missing=warn_missing) for k, v in obj.items()}
            if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
                return type(obj)(expand_env(x, strict=strict, warn_missing=warn_missing) for x in obj)
            return obj

        return expand_env(credentials)

    def validate_database_credentials(self, credentials: Dict[str, Any]) -> bool:
        """
        Validate database credentials.

        Args:
            credentials: Database credentials dictionary

        Returns:
            True if credentials are valid
        """
        required_fields = ["host", "database", "user", "password"]

        for field in required_fields:
            if field not in credentials:
                logger.error(f"Missing required database credential field: {field}")
                return False

        # Validate port if provided
        if "port" in credentials:
            try:
                port = int(credentials["port"])
                if port < 1 or port > 65535:
                    logger.error(f"Invalid port number: {port}")
                    return False
            except ValueError:
                logger.error(f"Port must be a number, got: {credentials['port']}")
                return False

        logger.info("Database credentials validation passed")
        return True

    def validate_api_credentials(self, credentials: Dict[str, Any]) -> bool:
        """
        Validate API credentials.

        Args:
            credentials: API credentials dictionary

        Returns:
            True if credentials are valid
        """
        required_fields = ["base_url"]

        for field in required_fields:
            if field not in credentials:
                logger.error(f"Missing required API credential field: {field}")
                return False

        # Validate authentication configuration
        if "auth" in credentials:
            auth = credentials["auth"]
            auth_type = auth.get("type")

            if auth_type == "bearer_token":
                if "token" not in auth:
                    logger.error("Bearer token authentication requires 'token' field")
                    return False
            elif auth_type == "api_key":
                if "api_key" not in auth:
                    logger.error("API key authentication requires 'api_key' field")
                    return False
            elif auth_type == "basic":
                if "username" not in auth or "password" not in auth:
                    logger.error(
                        "Basic authentication requires 'username' and 'password' fields"
                    )
                    return False

        logger.info("API credentials validation passed")
        return True

    def merge_credentials_with_config(
        self, config: Dict[str, Any], credentials: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge credentials into configuration.

        Args:
            config: Original configuration
            credentials: Credentials to merge

        Returns:
            Configuration with credentials merged
        """
        merged_config = config.copy()

        # Merge credentials into connection section
        if "connection" not in merged_config:
            merged_config["connection"] = {}

        # Deep merge credentials
        merged_config["connection"].update(credentials)

        # Special handling for rate_limit: move from connection to root level
        # so APIConnector can read it directly
        if "rate_limit" in credentials:
            merged_config["rate_limit"] = credentials["rate_limit"]

        return merged_config

    def create_template_credentials(self, credential_type: str, output_path: str):
        """
        Create template credentials file.

        Args:
            credential_type: Type of credentials (database, api)
            output_path: Path where template should be created
        """
        templates = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "your_database",
                "user": "your_username",
                "password": "${DB_PASSWORD}",
                "driver": "postgresql",
                "ssl_mode": "prefer",
                "connection_timeout": 30,
                "max_connections": 10,
                "min_connections": 1,
            },
            "api": {
                "base_url": "https://api.example.com",
                "auth": {"type": "bearer_token", "token": "${API_TOKEN}"},
                "headers": {
                    "User-Agent": "AnalitiqStream/1.0",
                    "Content-Type": "application/json",
                },
                "timeout": 30,
                "max_connections": 10,
                "rate_limit": {"max_requests": 100, "time_window": 60},
            },
        }

        if credential_type not in templates:
            raise ValueError(f"Unknown credential type: {credential_type}")

        template = templates[credential_type]

        try:
            with open(output_path, "w") as f:
                json.dump(template, f, indent=2)

            logger.info(
                f"Created {credential_type} credentials template at {output_path}"
            )

        except Exception as e:
            logger.error(f"Failed to create credentials template: {str(e)}")
            raise

    def clear_cache(self):
        """Clear credentials cache."""
        self.credentials_cache.clear()
        logger.info("Credentials cache cleared")


# Global instance
credentials_manager = CredentialsManager()
