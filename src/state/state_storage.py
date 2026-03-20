"""Storage backends for state management."""

import json
import logging
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class StateStorageSettings(BaseModel):
    """Settings for state storage configuration."""

    env: str = Field(default="local", description="Environment: local, dev, or prod")
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    org_id: Optional[str] = Field(default=None, description="Org ID for S3 path partitioning")
    aws_region: str = Field(default="eu-central-1", description="AWS region")
    state_bucket: Optional[str] = Field(default=None, description="S3 bucket for state storage")
    local_base_dir: str = Field(default="state", description="Local base directory for state")

    @classmethod
    def from_env(cls) -> "StateStorageSettings":
        """Create settings from environment variables."""
        env = os.getenv("ENV", "local")
        pipeline_id = os.getenv("PIPELINE_ID", "")
        org_id = os.getenv("ORG_ID")
        aws_region = os.getenv("AWS_REGION", "eu-central-1")
        state_bucket = os.getenv("STATE_BUCKET") or f"analitiq-client-pipeline-state-{env}"
        local_base_dir = os.getenv("STATE_DIR", "state")

        return cls(
            env=env,
            pipeline_id=pipeline_id,
            org_id=org_id,
            aws_region=aws_region,
            state_bucket=state_bucket,
            local_base_dir=local_base_dir,
        )

    @property
    def is_cloud_mode(self) -> bool:
        """Check if running in cloud mode (dev or prod)."""
        return self.env in ("dev", "prod")


class StateStorageBackend(ABC):
    """Abstract base class for state storage backends."""

    @abstractmethod
    def ensure_directories(self, paths: List[str]) -> None:
        """Ensure directory structure exists for given paths."""
        pass

    @abstractmethod
    def read_json(self, path: str) -> Optional[Dict[str, Any]]:
        """Read JSON file from storage."""
        pass

    @abstractmethod
    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """Write JSON data to storage atomically."""
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete a file from storage."""
        pass

    @abstractmethod
    def delete_recursive(self, path: str) -> None:
        """Delete a directory and all contents recursively."""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        pass

    @abstractmethod
    def list_files(self, path: str) -> List[str]:
        """List files in a directory."""
        pass


class LocalStateStorage(StateStorageBackend):
    """Local filesystem storage backend."""

    def __init__(self, base_dir: str = "state"):
        """Initialize local storage with base directory."""
        self.base_dir = Path(base_dir)

    def _resolve_path(self, path: str) -> Path:
        """Resolve relative path to absolute path."""
        return self.base_dir / path

    def ensure_directories(self, paths: List[str]) -> None:
        """Create directory structure for given paths."""
        for path in paths:
            full_path = self._resolve_path(path)
            full_path.mkdir(parents=True, exist_ok=True)

    def read_json(self, path: str) -> Optional[Dict[str, Any]]:
        """Read JSON file from local filesystem."""
        full_path = self._resolve_path(path)
        try:
            if full_path.exists():
                with open(full_path, "r") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read {full_path}: {e}")
        return None

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """Write JSON data atomically using temp file."""
        full_path = self._resolve_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        temp_file = full_path.with_suffix(".tmp")

        try:
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2, default=str)
            temp_file.replace(full_path)
            logger.debug(f"Wrote state to {full_path}")
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise e

    def delete(self, path: str) -> None:
        """Delete a file from local filesystem."""
        full_path = self._resolve_path(path)
        if full_path.exists():
            full_path.unlink()

    def delete_recursive(self, path: str) -> None:
        """Delete directory and all contents."""
        full_path = self._resolve_path(path)
        if full_path.exists():
            shutil.rmtree(full_path)

    def exists(self, path: str) -> bool:
        """Check if path exists on local filesystem."""
        return self._resolve_path(path).exists()

    def list_files(self, path: str) -> List[str]:
        """List files in directory."""
        full_path = self._resolve_path(path)
        if not full_path.exists():
            return []
        return [f.name for f in full_path.iterdir() if f.is_file()]


class S3StateStorage(StateStorageBackend):
    """S3 storage backend for cloud environments."""

    def __init__(
        self,
        bucket: str,
        org_id: str,
        pipeline_id: str,
        aws_region: str = "eu-central-1",
    ):
        """
        Initialize S3 storage backend.

        Args:
            bucket: S3 bucket name (e.g., analitiq-client-pipeline-state-dev)
            org_id: Org identifier for path partitioning
            pipeline_id: Pipeline identifier
            aws_region: AWS region for S3 client
        """
        self.bucket = bucket
        self.org_id = org_id
        self.pipeline_id = pipeline_id
        self.aws_region = aws_region
        self._s3_client = None

    @property
    def s3_client(self):
        """Lazy-initialize S3 client."""
        if self._s3_client is None:
            import boto3

            self._s3_client = boto3.client("s3", region_name=self.aws_region)
        return self._s3_client

    def _build_s3_key(self, path: str) -> str:
        """Build full S3 key with client/pipeline prefix."""
        # path is relative like "index.json" or "streams/stream1/partition-default.json"
        return f"{self.org_id}/{self.pipeline_id}/{path}"

    def ensure_directories(self, paths: List[str]) -> None:
        """No-op for S3 - directories are implicit."""
        pass

    def read_json(self, path: str) -> Optional[Dict[str, Any]]:
        """Read JSON from S3."""
        from botocore.exceptions import ClientError

        key = self._build_s3_key(path)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                logger.debug(f"No state found at s3://{self.bucket}/{key}")
                return None
            elif error_code == "NoSuchBucket":
                raise RuntimeError(f"State bucket not found: {self.bucket}")
            raise RuntimeError(f"Failed to read state from S3: {e}")
        except Exception as e:
            logger.warning(f"Failed to read {key} from S3: {e}")
            return None

    def write_json(self, path: str, data: Dict[str, Any]) -> None:
        """Write JSON to S3."""
        from botocore.exceptions import ClientError, NoCredentialsError

        key = self._build_s3_key(path)
        try:
            content = json.dumps(data, indent=2, default=str)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="application/json",
            )
            logger.debug(f"Wrote state to s3://{self.bucket}/{key}")
        except NoCredentialsError:
            raise RuntimeError("AWS credentials not configured for S3 access")
        except ClientError as e:
            raise RuntimeError(f"Failed to write state to S3: {e}")

    def delete(self, path: str) -> None:
        """Delete a single object from S3."""
        from botocore.exceptions import ClientError

        key = self._build_s3_key(path)
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)
            logger.debug(f"Deleted s3://{self.bucket}/{key}")
        except ClientError as e:
            logger.warning(f"Failed to delete {key}: {e}")

    def delete_recursive(self, path: str) -> None:
        """Delete all objects under a prefix."""
        from botocore.exceptions import ClientError

        prefix = self._build_s3_key(path)
        if not prefix.endswith("/"):
            prefix += "/"

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue

                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    self.s3_client.delete_objects(
                        Bucket=self.bucket, Delete={"Objects": objects}
                    )
                    logger.debug(f"Deleted {len(objects)} objects under {prefix}")
        except ClientError as e:
            logger.warning(f"Failed to delete prefix {prefix}: {e}")

    def exists(self, path: str) -> bool:
        """Check if S3 object exists."""
        from botocore.exceptions import ClientError

        key = self._build_s3_key(path)
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def list_files(self, path: str) -> List[str]:
        """List objects under a prefix."""
        from botocore.exceptions import ClientError

        prefix = self._build_s3_key(path)
        if not prefix.endswith("/"):
            prefix += "/"

        files = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    # Extract filename from full key
                    key = obj["Key"]
                    if key != prefix:  # Skip the prefix itself
                        filename = key[len(prefix) :].split("/")[0]
                        if filename and filename not in files:
                            files.append(filename)
        except ClientError as e:
            logger.warning(f"Failed to list {prefix}: {e}")

        return files


def create_storage_backend(settings: StateStorageSettings) -> StateStorageBackend:
    """Factory function to create appropriate storage backend."""
    if settings.is_cloud_mode:
        if not settings.org_id:
            raise ValueError("org_id is required for cloud state storage")
        if not settings.state_bucket:
            raise ValueError("state_bucket is required for cloud state storage")

        logger.info(
            f"Using S3 state storage: s3://{settings.state_bucket}/{settings.org_id}/{settings.pipeline_id}/"
        )
        return S3StateStorage(
            bucket=settings.state_bucket,
            org_id=settings.org_id,
            pipeline_id=settings.pipeline_id,
            aws_region=settings.aws_region,
        )
    else:
        logger.info(f"Using local state storage: {settings.local_base_dir}/{settings.pipeline_id}/")
        return LocalStateStorage(base_dir=settings.local_base_dir)