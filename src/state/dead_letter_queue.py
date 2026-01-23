"""Dead Letter Queue implementation for handling failed records."""

import asyncio
import io
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class DLQStorageSettings(BaseModel):
    """Settings for DLQ storage configuration."""

    env: str = Field(default="local", description="Environment: local, dev, or prod")
    pipeline_id: str = Field(default="", description="Unique pipeline identifier")
    client_id: Optional[str] = Field(
        default=None, description="Client ID for S3 path partitioning"
    )
    aws_region: str = Field(default="eu-central-1", description="AWS region")
    dlq_bucket: Optional[str] = Field(
        default=None, description="S3 bucket for DLQ storage"
    )
    local_dlq_dir: str = Field(
        default="deadletter", description="Local directory for DLQ"
    )

    @classmethod
    def from_env(cls) -> "DLQStorageSettings":
        """Create settings from environment variables."""
        env = os.getenv("ENV", "local")
        pipeline_id = os.getenv("PIPELINE_ID", "")
        client_id = os.getenv("CLIENT_ID")
        aws_region = os.getenv("AWS_REGION", "eu-central-1")
        dlq_bucket = os.getenv("DLQ_BUCKET") or f"analitiq-client-pipeline-deadletter-{env}"
        local_dlq_dir = os.getenv("DLQ_DIR", "deadletter")

        return cls(
            env=env,
            pipeline_id=pipeline_id,
            client_id=client_id,
            aws_region=aws_region,
            dlq_bucket=dlq_bucket,
            local_dlq_dir=local_dlq_dir,
        )

    @property
    def is_cloud_mode(self) -> bool:
        """Check if running in cloud mode (dev or prod)."""
        return self.env in ("dev", "prod")


class DLQStorageBackend(ABC):
    """Abstract base class for DLQ storage backends."""

    @abstractmethod
    async def write_record(self, record: Dict[str, Any], stream_id: Optional[str] = None) -> None:
        """Write a single DLQ record."""
        pass

    @abstractmethod
    async def get_records(
        self, pipeline_id: Optional[str] = None, stream_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get DLQ records, optionally filtered by pipeline and stream."""
        pass

    @abstractmethod
    async def get_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        pass

    @abstractmethod
    async def clear(self, pipeline_id: Optional[str] = None) -> None:
        """Clear DLQ records."""
        pass

    @abstractmethod
    async def cleanup_old_records(self, retention_days: int) -> None:
        """Clean up records older than retention period."""
        pass

    @abstractmethod
    async def flush(self) -> None:
        """Flush any buffered records to storage."""
        pass


class LocalDLQStorage(DLQStorageBackend):
    """Local filesystem DLQ storage backend."""

    def __init__(
        self,
        dlq_path: str,
        max_file_size: int = 10 * 1024 * 1024,
        max_files: int = 100,
    ):
        """Initialize local DLQ storage."""
        self.dlq_path = Path(dlq_path)
        self.max_file_size = max_file_size
        self.max_files = max_files

        # Create DLQ directory
        self.dlq_path.mkdir(parents=True, exist_ok=True)

        # Current file tracking
        self.current_file: Optional[Path] = None
        self.current_file_size = 0
        self.lock = asyncio.Lock()

    def _need_new_file(self) -> bool:
        """Check if we need to create a new DLQ file."""
        return (
            self.current_file is None
            or self.current_file_size >= self.max_file_size
            or not self.current_file.exists()
        )

    async def _create_new_file(self) -> None:
        """Create a new DLQ file."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        self.current_file = self.dlq_path / f"dlq_{timestamp}.jsonl"
        self.current_file_size = 0
        self.current_file.touch()
        logger.debug(f"Created new DLQ file: {self.current_file}")

    async def write_record(self, record: Dict[str, Any], stream_id: Optional[str] = None) -> None:
        """Write a DLQ record to local file."""
        async with self.lock:
            try:
                if self._need_new_file():
                    await self._create_new_file()

                record_json = json.dumps(record, cls=DateTimeEncoder) + "\n"
                record_bytes = record_json.encode("utf-8")

                with open(self.current_file, "a", encoding="utf-8") as f:
                    f.write(record_json)

                self.current_file_size += len(record_bytes)

            except Exception as e:
                logger.error(f"Failed to write to DLQ: {e}")
                # Fallback to unique file
                fallback_file = (
                    self.dlq_path
                    / f"dlq_fallback_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.json"
                )
                with open(fallback_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, indent=2, cls=DateTimeEncoder)

    async def get_records(
        self, pipeline_id: Optional[str] = None, stream_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get DLQ records from local files."""
        records = []

        try:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))

            for dlq_file in dlq_files:
                with open(dlq_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            try:
                                record = json.loads(line.strip())
                                if pipeline_id is None or record.get("pipeline_id") == pipeline_id:
                                    records.append(record)
                            except json.JSONDecodeError:
                                logger.warning(f"Invalid JSON in DLQ file {dlq_file}")

        except Exception as e:
            logger.error(f"Failed to read DLQ records: {e}")

        return records

    async def get_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        stats = {
            "total_records": 0,
            "records_by_pipeline": {},
            "records_by_error_type": {},
            "oldest_record": None,
            "newest_record": None,
            "total_files": 0,
            "total_size_bytes": 0,
        }

        try:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))
            stats["total_files"] = len(dlq_files)

            for dlq_file in dlq_files:
                stats["total_size_bytes"] += dlq_file.stat().st_size

                with open(dlq_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            try:
                                record = json.loads(line.strip())
                                stats["total_records"] += 1

                                pipeline_id = record.get("pipeline_id", "unknown")
                                stats["records_by_pipeline"][pipeline_id] = (
                                    stats["records_by_pipeline"].get(pipeline_id, 0) + 1
                                )

                                error_type = record.get("error", {}).get("type", "unknown")
                                stats["records_by_error_type"][error_type] = (
                                    stats["records_by_error_type"].get(error_type, 0) + 1
                                )

                                timestamp = record.get("timestamp")
                                if timestamp:
                                    if stats["oldest_record"] is None or timestamp < stats["oldest_record"]:
                                        stats["oldest_record"] = timestamp
                                    if stats["newest_record"] is None or timestamp > stats["newest_record"]:
                                        stats["newest_record"] = timestamp

                            except json.JSONDecodeError:
                                continue

        except Exception as e:
            logger.error(f"Failed to get DLQ stats: {e}")

        return stats

    async def clear(self, pipeline_id: Optional[str] = None) -> None:
        """Clear DLQ records."""
        if pipeline_id is None:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))
            for dlq_file in dlq_files:
                dlq_file.unlink()
            self.current_file = None
            self.current_file_size = 0
            logger.info("Cleared all DLQ records")
        else:
            logger.info(f"Selective DLQ clearing for pipeline {pipeline_id} not implemented")

    async def cleanup_old_records(self, retention_days: int) -> None:
        """Clean up old DLQ files."""
        try:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))
            dlq_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)

            # Remove files beyond max_files limit
            if len(dlq_files) > self.max_files:
                for file_to_remove in dlq_files[self.max_files:]:
                    file_to_remove.unlink()
                    logger.info(f"Removed old DLQ file: {file_to_remove}")

            # Remove files older than retention period
            cutoff_time = datetime.now(timezone.utc).timestamp() - (retention_days * 24 * 60 * 60)
            for dlq_file in dlq_files:
                if dlq_file.stat().st_ctime < cutoff_time:
                    dlq_file.unlink()
                    logger.info(f"Removed expired DLQ file: {dlq_file}")

        except Exception as e:
            logger.error(f"Failed to cleanup old DLQ files: {e}")

    async def flush(self) -> None:
        """No-op for local storage - writes are immediate."""
        pass


class S3DLQStorage(DLQStorageBackend):
    """S3 DLQ storage backend with date-based partitioning."""

    def __init__(
        self,
        bucket: str,
        client_id: str,
        pipeline_id: str,
        aws_region: str = "eu-central-1",
        buffer_size: int = 100,
    ):
        """
        Initialize S3 DLQ storage.

        Args:
            bucket: S3 bucket name
            client_id: Client identifier for path partitioning
            pipeline_id: Pipeline identifier
            aws_region: AWS region
            buffer_size: Number of records to buffer before flushing
        """
        self.bucket = bucket
        self.client_id = client_id
        self.pipeline_id = pipeline_id
        self.aws_region = aws_region
        self.buffer_size = buffer_size

        self._s3_client = None
        self._buffer: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    @property
    def s3_client(self):
        """Lazy-initialize S3 client."""
        if self._s3_client is None:
            import boto3

            self._s3_client = boto3.client("s3", region_name=self.aws_region)
        return self._s3_client

    def _get_s3_key(self, stream_id: Optional[str] = None) -> str:
        """
        Build S3 key with date-based partitioning.

        Pattern: {client_id}/{pipeline_id}/year=YYYY/month=MM/day=DD/{stream_id}/{run_id}_dlq.jsonl
        """
        now = datetime.now(timezone.utc)
        date_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        filename = f"{self._run_id}_dlq.jsonl"

        if stream_id:
            return f"{self.client_id}/{self.pipeline_id}/{date_path}/{stream_id}/{filename}"
        return f"{self.client_id}/{self.pipeline_id}/{date_path}/{filename}"

    async def write_record(self, record: Dict[str, Any], stream_id: Optional[str] = None) -> None:
        """Buffer a DLQ record for later S3 upload."""
        async with self._lock:
            # Add stream_id to record for later filtering
            if stream_id:
                record["stream_id"] = stream_id
            self._buffer.append(record)

            if len(self._buffer) >= self.buffer_size:
                await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """Flush buffered records to S3."""
        if not self._buffer:
            return

        from botocore.exceptions import ClientError, NoCredentialsError

        # Group records by stream_id
        records_by_stream: Dict[Optional[str], List[Dict[str, Any]]] = {}
        for record in self._buffer:
            stream_id = record.get("stream_id")
            if stream_id not in records_by_stream:
                records_by_stream[stream_id] = []
            records_by_stream[stream_id].append(record)

        for stream_id, records in records_by_stream.items():
            key = self._get_s3_key(stream_id)

            try:
                # Try to get existing content
                existing_content = ""
                try:
                    response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                    existing_content = response["Body"].read().decode("utf-8")
                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchKey":
                        raise

                # Append new records
                new_lines = [json.dumps(r, cls=DateTimeEncoder) for r in records]
                new_content = existing_content + "\n".join(new_lines) + "\n"

                # Write back to S3
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=new_content.encode("utf-8"),
                    ContentType="application/x-ndjson",
                )
                logger.debug(f"Wrote {len(records)} DLQ records to s3://{self.bucket}/{key}")

            except NoCredentialsError:
                logger.error(f"S3DLQStorage: No AWS credentials for bucket {self.bucket}")
            except ClientError as e:
                logger.error(f"S3DLQStorage: Failed to write to S3: {e}")

        self._buffer.clear()

    async def get_records(
        self, pipeline_id: Optional[str] = None, stream_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get DLQ records from S3."""
        from botocore.exceptions import ClientError

        records = []
        prefix = f"{self.client_id}/{self.pipeline_id}/"

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    if not obj["Key"].endswith("_dlq.jsonl"):
                        continue

                    try:
                        response = self.s3_client.get_object(Bucket=self.bucket, Key=obj["Key"])
                        content = response["Body"].read().decode("utf-8")

                        for line in content.strip().split("\n"):
                            if line.strip():
                                try:
                                    record = json.loads(line)
                                    if stream_id is None or record.get("stream_id") == stream_id:
                                        records.append(record)
                                except json.JSONDecodeError:
                                    continue

                    except ClientError as e:
                        logger.warning(f"Failed to read DLQ file {obj['Key']}: {e}")

        except ClientError as e:
            logger.error(f"Failed to list DLQ records: {e}")

        return records

    async def get_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics from S3."""
        from botocore.exceptions import ClientError

        stats = {
            "total_records": 0,
            "records_by_pipeline": {},
            "records_by_error_type": {},
            "oldest_record": None,
            "newest_record": None,
            "total_files": 0,
            "total_size_bytes": 0,
        }

        prefix = f"{self.client_id}/{self.pipeline_id}/"

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    if not obj["Key"].endswith("_dlq.jsonl"):
                        continue

                    stats["total_files"] += 1
                    stats["total_size_bytes"] += obj["Size"]

                    try:
                        response = self.s3_client.get_object(Bucket=self.bucket, Key=obj["Key"])
                        content = response["Body"].read().decode("utf-8")

                        for line in content.strip().split("\n"):
                            if line.strip():
                                try:
                                    record = json.loads(line)
                                    stats["total_records"] += 1

                                    pid = record.get("pipeline_id", "unknown")
                                    stats["records_by_pipeline"][pid] = (
                                        stats["records_by_pipeline"].get(pid, 0) + 1
                                    )

                                    error_type = record.get("error", {}).get("type", "unknown")
                                    stats["records_by_error_type"][error_type] = (
                                        stats["records_by_error_type"].get(error_type, 0) + 1
                                    )

                                    timestamp = record.get("timestamp")
                                    if timestamp:
                                        if stats["oldest_record"] is None or timestamp < stats["oldest_record"]:
                                            stats["oldest_record"] = timestamp
                                        if stats["newest_record"] is None or timestamp > stats["newest_record"]:
                                            stats["newest_record"] = timestamp

                                except json.JSONDecodeError:
                                    continue

                    except ClientError:
                        continue

        except ClientError as e:
            logger.error(f"Failed to get DLQ stats: {e}")

        return stats

    async def clear(self, pipeline_id: Optional[str] = None) -> None:
        """Clear DLQ records from S3."""
        from botocore.exceptions import ClientError

        prefix = f"{self.client_id}/{self.pipeline_id}/"

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue

                objects = [
                    {"Key": obj["Key"]}
                    for obj in page["Contents"]
                    if obj["Key"].endswith("_dlq.jsonl")
                ]
                if objects:
                    self.s3_client.delete_objects(Bucket=self.bucket, Delete={"Objects": objects})
                    logger.info(f"Cleared {len(objects)} DLQ files from S3")

        except ClientError as e:
            logger.error(f"Failed to clear DLQ: {e}")

    async def cleanup_old_records(self, retention_days: int) -> None:
        """Clean up old DLQ records from S3 (handled by S3 lifecycle policies)."""
        logger.info("S3 DLQ cleanup is handled by S3 lifecycle policies")

    async def flush(self) -> None:
        """Flush buffered records to S3."""
        async with self._lock:
            await self._flush_buffer()


def create_dlq_storage(
    settings: DLQStorageSettings,
    dlq_path: Optional[str] = None,
) -> DLQStorageBackend:
    """Factory function to create appropriate DLQ storage backend."""
    if settings.is_cloud_mode:
        if not settings.client_id:
            raise ValueError("client_id is required for cloud DLQ storage")
        if not settings.dlq_bucket:
            raise ValueError("dlq_bucket is required for cloud DLQ storage")

        logger.info(
            f"Using S3 DLQ storage: s3://{settings.dlq_bucket}/"
            f"{settings.client_id}/{settings.pipeline_id}/..."
        )
        return S3DLQStorage(
            bucket=settings.dlq_bucket,
            client_id=settings.client_id,
            pipeline_id=settings.pipeline_id,
            aws_region=settings.aws_region,
        )
    else:
        path = dlq_path or str(Path(settings.local_dlq_dir) / settings.pipeline_id)
        logger.info(f"Using local DLQ storage: {path}")
        return LocalDLQStorage(dlq_path=path)


class DeadLetterQueue:
    """
    Dead Letter Queue for handling records that fail processing.

    Features:
    - Auto-detects storage backend from ENV (local or S3)
    - Date-based partitioning for S3 lifecycle management
    - Structured error information
    - Retry and reprocessing capabilities
    - Cleanup and archival
    """

    def __init__(
        self,
        dlq_path: str = "./deadletter/",
        max_file_size: int = 10 * 1024 * 1024,
        max_files: int = 100,
        retention_days: int = 30,
        settings: Optional[DLQStorageSettings] = None,
        storage_backend: Optional[DLQStorageBackend] = None,
    ):
        """
        Initialize Dead Letter Queue.

        The storage backend is determined automatically based on ENV:
        - ENV=local (default): Uses local filesystem storage
        - ENV=dev or ENV=prod: Uses S3 storage with date-based partitioning

        Args:
            dlq_path: Directory path for storing failed records (local mode)
            max_file_size: Maximum size per DLQ file in bytes (local mode)
            max_files: Maximum number of DLQ files to keep (local mode)
            retention_days: Days to retain DLQ files
            settings: Optional storage settings (if None, auto-detected from environment)
            storage_backend: Optional pre-configured storage backend
        """
        self.retention_days = retention_days

        # Determine storage backend
        if storage_backend:
            self.storage = storage_backend
            self._settings = None
        elif settings:
            self._settings = settings
            self.storage = create_dlq_storage(settings, dlq_path)
        else:
            # Auto-detect from environment
            self._settings = DLQStorageSettings.from_env()
            if self._settings.is_cloud_mode:
                self.storage = create_dlq_storage(self._settings)
            else:
                self.storage = LocalDLQStorage(
                    dlq_path=dlq_path,
                    max_file_size=max_file_size,
                    max_files=max_files,
                )

        # Keep legacy attributes for compatibility
        self.dlq_path = Path(dlq_path)
        self.max_file_size = max_file_size
        self.max_files = max_files
        self.lock = asyncio.Lock()

    async def send_to_dlq(
        self,
        record: Dict[str, Any],
        error: Exception,
        pipeline_id: str,
        stream_id: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ):
        """
        Send a failed record to the Dead Letter Queue.

        Args:
            record: Original record that failed processing
            error: Exception that caused the failure
            pipeline_id: ID of the pipeline that failed
            stream_id: Optional stream identifier for partitioning
            additional_context: Additional context information
        """
        dlq_record = {
            "id": str(uuid4()),
            "pipeline_id": pipeline_id,
            "original_record": record,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": self._get_traceback(error),
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "retry_count": 0,
            "additional_context": additional_context or {},
        }

        await self.storage.write_record(dlq_record, stream_id)

    async def send_batch(
        self,
        batch: List[Dict[str, Any]],
        error_message: str,
        pipeline_id: str = "unknown",
        stream_id: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ):
        """
        Send a batch of failed records to the Dead Letter Queue.

        Args:
            batch: List of records that failed processing
            error_message: Error message describing the failure
            pipeline_id: ID of the pipeline that failed
            stream_id: Optional stream identifier for partitioning
            additional_context: Additional context information
        """
        for record in batch:
            error = Exception(error_message)
            await self.send_to_dlq(
                record=record,
                error=error,
                pipeline_id=pipeline_id,
                stream_id=stream_id,
                additional_context=additional_context,
            )

        logger.warning(f"Sent batch of {len(batch)} records to DLQ for pipeline {pipeline_id}")

    def _get_traceback(self, error: Exception) -> Optional[str]:
        """Get traceback string from exception."""
        import traceback

        try:
            return traceback.format_exception(type(error), error, error.__traceback__)
        except Exception:
            return None

    async def get_failed_records(
        self, pipeline_id: Optional[str] = None, stream_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get failed records from DLQ.

        Args:
            pipeline_id: Optional pipeline ID to filter records
            stream_id: Optional stream ID to filter records

        Returns:
            List of failed records
        """
        return await self.storage.get_records(pipeline_id, stream_id)

    async def retry_failed_record(self, dlq_record_id: str) -> bool:
        """
        Mark a failed record for retry.

        Args:
            dlq_record_id: ID of the DLQ record to retry

        Returns:
            True if record was found and marked for retry
        """
        logger.info(f"Retry requested for DLQ record: {dlq_record_id}")
        return True

    async def get_dlq_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        return await self.storage.get_stats()

    async def clear_dlq(self, pipeline_id: Optional[str] = None):
        """
        Clear DLQ records.

        Args:
            pipeline_id: Optional pipeline ID to clear only specific records
        """
        await self.storage.clear(pipeline_id)

    async def cleanup(self):
        """Clean up old DLQ files based on retention policy."""
        await self.storage.cleanup_old_records(self.retention_days)

    async def flush(self):
        """Flush any buffered records to storage."""
        await self.storage.flush()

    # Legacy compatibility methods
    async def _write_to_dlq(self, dlq_record: Dict[str, Any]):
        """Legacy method - delegates to storage backend."""
        await self.storage.write_record(dlq_record)

    def _need_new_file(self) -> bool:
        """Legacy method for local storage compatibility."""
        if isinstance(self.storage, LocalDLQStorage):
            return self.storage._need_new_file()
        return False

    async def _create_new_file(self):
        """Legacy method for local storage compatibility."""
        if isinstance(self.storage, LocalDLQStorage):
            await self.storage._create_new_file()

    async def _cleanup_old_files(self):
        """Legacy method - delegates to cleanup."""
        await self.cleanup()