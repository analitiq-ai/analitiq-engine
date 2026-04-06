"""
Local file-based secrets resolver.

Reads secrets from JSON files in a local directory.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional

from src.secrets.protocol import SecretsResolver
from src.secrets.exceptions import SecretNotFoundError, SecretResolutionError

logger = logging.getLogger(__name__)


class LocalFileSecretsResolver(SecretsResolver):
    """
    Resolves secrets from local JSON files.

    Secrets are stored in a directory structure:
    - {secrets_dir}/{connection_id}.json

    Each file contains a JSON object with key-value pairs:
    {"token": "secret-value", "password": "secret-password"}

    Alternative path patterns supported:
    - {secrets_dir}/{connection_id}  (without .json extension)
    """

    def __init__(self, secrets_dir: str | Path):
        """
        Initialize local file secrets resolver.

        Args:
            secrets_dir: Path to directory containing secrets files
        """
        self._secrets_dir = Path(secrets_dir)
        if not self._secrets_dir.exists():
            logger.warning(f"Secrets directory does not exist: {self._secrets_dir}")

    @property
    def secrets_dir(self) -> Path:
        """Get the secrets directory path."""
        return self._secrets_dir

    async def resolve(
        self,
        connection_id: str,
        *,
        keys: Optional[list[str]] = None,
    ) -> Dict[str, str]:
        """
        Resolve secrets for a connection from local files.

        Args:
            connection_id: Identifier for the connection
            keys: Optional list of specific keys to retrieve

        Returns:
            Dictionary mapping secret keys to their values

        Raises:
            SecretNotFoundError: If no secrets file exists
            SecretResolutionError: If file cannot be parsed
        """
        secrets_path = self._find_secrets_file(connection_id)

        if not secrets_path:
            raise SecretNotFoundError(
                connection_id=connection_id,
                detail=f"No secrets file found in {self._secrets_dir}",
            )

        try:
            with open(secrets_path) as f:
                secrets = json.load(f)

            if not isinstance(secrets, dict):
                raise SecretResolutionError(
                    f"Invalid secrets format in {secrets_path}: expected object",
                    connection_id=connection_id,
                )

            # Convert all values to strings
            result = {k: str(v) for k, v in secrets.items()}

            # Filter to specific keys if requested
            if keys:
                result = {k: v for k, v in result.items() if k in keys}

            logger.debug(
                f"Loaded secrets for {connection_id} from {secrets_path} "
                f"({len(result)} keys)"
            )
            return result

        except json.JSONDecodeError as e:
            raise SecretResolutionError(
                f"Invalid JSON in secrets file {secrets_path}: {e}",
                connection_id=connection_id,
            )
        except PermissionError as e:
            raise SecretResolutionError(
                f"Permission denied reading secrets file {secrets_path}: {e}",
                connection_id=connection_id,
            )
        except OSError as e:
            raise SecretResolutionError(
                f"Error reading secrets file {secrets_path}: {e}",
                connection_id=connection_id,
            )

    def _find_secrets_file(
        self,
        connection_id: str,
    ) -> Optional[Path]:
        """
        Find the secrets file for a connection.

        Checks multiple path patterns in order:
        1. {secrets_dir}/{connection_id}.json
        2. {secrets_dir}/{connection_id}  (no extension)
        3. {secrets_dir}/credentials.json  (standard name for colocated secrets)

        Args:
            connection_id: Connection identifier

        Returns:
            Path to secrets file if found, None otherwise
        """
        candidates = [
            self._secrets_dir / f"{connection_id}.json",
            self._secrets_dir / connection_id,
            self._secrets_dir / "credentials.json",
        ]

        for path in candidates:
            if path.is_file():
                return path

        return None

    async def close(self) -> None:
        """No cleanup needed for file-based resolver."""
        pass

    def __repr__(self) -> str:
        """String representation."""
        return f"LocalFileSecretsResolver({self._secrets_dir})"
