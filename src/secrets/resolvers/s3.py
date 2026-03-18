"""
AWS S3-based secrets resolver.

Reads secrets from S3 for cloud deployments.
"""

import json
import logging
from typing import Any, Dict, Optional

from src.secrets.protocol import SecretsResolver
from src.secrets.exceptions import (
    SecretNotFoundError,
    SecretAccessDeniedError,
    SecretResolutionError,
)

logger = logging.getLogger(__name__)


class S3SecretsResolver(SecretsResolver):
    """
    Resolves secrets from AWS S3.

    Secrets are stored in S3 with the path pattern:
    s3://{bucket}/connections/{org_id}/{connection_id}

    Each file contains a JSON object with key-value pairs:
    {"token": "secret-value", "password": "secret-password"}
    """

    def __init__(
        self,
        bucket: str,
        *,
        region: str = "eu-central-1",
        prefix: str = "connections",
        s3_client: Optional[Any] = None,
    ):
        """
        Initialize S3 secrets resolver.

        Args:
            bucket: S3 bucket name
            region: AWS region
            prefix: Path prefix within the bucket (default: "connections")
            s3_client: Optional pre-configured boto3 S3 client
        """
        self._bucket = bucket
        self._region = region
        self._prefix = prefix
        self._s3_client = s3_client
        self._owns_client = s3_client is None

    @property
    def bucket(self) -> str:
        """Get the S3 bucket name."""
        return self._bucket

    def _get_client(self) -> Any:
        """Get or create S3 client."""
        if self._s3_client is None:
            try:
                import boto3
                self._s3_client = boto3.client("s3", region_name=self._region)
            except ImportError:
                raise SecretResolutionError(
                    "boto3 package not installed. Install with: pip install boto3",
                    connection_id="",
                )
        return self._s3_client

    def _build_key(self, connection_id: str, org_id: Optional[str]) -> str:
        """Build S3 object key."""
        if org_id:
            return f"{self._prefix}/{org_id}/{connection_id}"
        return f"{self._prefix}/{connection_id}"

    async def resolve(
        self,
        connection_id: str,
        *,
        org_id: Optional[str] = None,
        keys: Optional[list[str]] = None,
    ) -> Dict[str, str]:
        """
        Resolve secrets for a connection from S3.

        Args:
            connection_id: Identifier for the connection
            org_id: Org identifier (required for S3)
            keys: Optional list of specific keys to retrieve

        Returns:
            Dictionary mapping secret keys to their values

        Raises:
            SecretNotFoundError: If no secrets exist in S3
            SecretAccessDeniedError: If access to S3 is denied
            SecretResolutionError: For other S3 failures
        """
        s3_key = self._build_key(connection_id, org_id)
        s3_uri = f"s3://{self._bucket}/{s3_key}"

        try:
            from botocore.exceptions import ClientError, NoCredentialsError

            client = self._get_client()
            response = client.get_object(Bucket=self._bucket, Key=s3_key)
            content = response["Body"].read().decode("utf-8")
            secrets = json.loads(content)

            if not isinstance(secrets, dict):
                raise SecretResolutionError(
                    f"Invalid secrets format at {s3_uri}: expected object",
                    connection_id=connection_id,
                )

            # Convert all values to strings
            result = {k: str(v) for k, v in secrets.items()}

            # Filter to specific keys if requested
            if keys:
                result = {k: v for k, v in result.items() if k in keys}

            logger.debug(
                f"Loaded secrets for {connection_id} from {s3_uri} "
                f"({len(result)} keys)"
            )
            return result

        except ImportError:
            raise SecretResolutionError(
                "botocore package not installed. Install with: pip install boto3",
                connection_id=connection_id,
            )
        except NoCredentialsError:
            raise SecretAccessDeniedError(
                connection_id=connection_id,
                detail="AWS credentials not configured",
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchKey":
                raise SecretNotFoundError(
                    connection_id=connection_id,
                    detail=f"Object not found at {s3_uri}",
                )
            elif error_code == "NoSuchBucket":
                raise SecretNotFoundError(
                    connection_id=connection_id,
                    detail=f"Bucket not found: {self._bucket}",
                )
            elif error_code in ("AccessDenied", "403"):
                raise SecretAccessDeniedError(
                    connection_id=connection_id,
                    detail=f"Access denied to {s3_uri}",
                )
            else:
                raise SecretResolutionError(
                    f"S3 error fetching secrets from {s3_uri}: {e}",
                    connection_id=connection_id,
                )
        except json.JSONDecodeError as e:
            raise SecretResolutionError(
                f"Invalid JSON in secrets at {s3_uri}: {e}",
                connection_id=connection_id,
            )

    async def close(self) -> None:
        """Clean up S3 client if we own it."""
        if self._owns_client and self._s3_client is not None:
            # boto3 clients don't need explicit cleanup
            self._s3_client = None

    def __repr__(self) -> str:
        """String representation."""
        return f"S3SecretsResolver(s3://{self._bucket}/{self._prefix})"
