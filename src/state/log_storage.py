"""S3 logging handler with date-based partitioning for lifecycle management."""

import io
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class LogStorageSettings(BaseModel):
    """Settings for log storage configuration."""

    env: str = Field(default="local", description="Environment: local, dev, or prod")
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    org_id: Optional[str] = Field(default=None, description="Org ID for S3 path partitioning")
    aws_region: str = Field(default="eu-central-1", description="AWS region")
    logs_bucket: Optional[str] = Field(default=None, description="S3 bucket for log storage")
    local_logs_dir: str = Field(default="logs", description="Local directory for logs")

    @classmethod
    def from_env(cls) -> "LogStorageSettings":
        """Create settings from environment variables."""
        env = os.getenv("ENV", "local")
        pipeline_id = os.getenv("PIPELINE_ID", "")
        org_id = os.getenv("ORG_ID")
        aws_region = os.getenv("AWS_REGION", "eu-central-1")
        logs_bucket = os.getenv("LOGS_BUCKET") or f"analitiq-client-pipeline-logs-{env}"
        local_logs_dir = os.getenv("LOGS_DIR", "logs")

        return cls(
            env=env,
            pipeline_id=pipeline_id,
            org_id=org_id,
            aws_region=aws_region,
            logs_bucket=logs_bucket,
            local_logs_dir=local_logs_dir,
        )

    @property
    def is_cloud_mode(self) -> bool:
        """Check if running in cloud mode (dev or prod)."""
        return self.env in ("dev", "prod")


class S3LogHandler(logging.Handler):
    """
    Logging handler that writes logs to S3 with date-based partitioning.

    Path structure for lifecycle management:
    s3://{bucket}/{org_id}/{pipeline_id}/year=YYYY/month=MM/day=DD/{log_name}.log

    Features:
    - Buffered writes to reduce S3 API calls
    - Date-based partitioning for S3 lifecycle rules
    - Thread-safe buffer management
    - Automatic flush on buffer size or close
    """

    def __init__(
        self,
        bucket: str,
        org_id: str,
        pipeline_id: str,
        log_name: str = "pipeline",
        stream_id: Optional[str] = None,
        aws_region: str = "eu-central-1",
        buffer_size: int = 100,
        flush_interval_seconds: int = 60,
    ):
        """
        Initialize S3 log handler.

        Args:
            bucket: S3 bucket name
            org_id: Org identifier for path partitioning
            pipeline_id: Pipeline identifier
            log_name: Name of the log file (e.g., "pipeline" or "stream")
            stream_id: Optional stream identifier for stream-specific logs
            aws_region: AWS region for S3 client
            buffer_size: Number of log records to buffer before flushing
            flush_interval_seconds: Maximum time between flushes
        """
        super().__init__()
        self.bucket = bucket
        self.org_id = org_id
        self.pipeline_id = pipeline_id
        self.log_name = log_name
        self.stream_id = stream_id
        self.aws_region = aws_region
        self.buffer_size = buffer_size
        self.flush_interval_seconds = flush_interval_seconds

        self._s3_client = None
        self._buffer: list[str] = []
        self._lock = threading.Lock()
        self._last_flush = datetime.now(timezone.utc)
        self._current_date: Optional[str] = None

    @property
    def s3_client(self):
        """Lazy-initialize S3 client."""
        if self._s3_client is None:
            import boto3
            self._s3_client = boto3.client("s3", region_name=self.aws_region)
        return self._s3_client

    def _get_s3_key(self, date: datetime) -> str:
        """
        Build S3 key with date-based partitioning.

        Pattern: {org_id}/{pipeline_id}/year=YYYY/month=MM/day=DD/{stream_id}/{log_name}.log
        """
        date_path = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"

        if self.stream_id:
            return f"{self.org_id}/{self.pipeline_id}/{date_path}/{self.stream_id}/{self.log_name}.log"
        return f"{self.org_id}/{self.pipeline_id}/{date_path}/{self.log_name}.log"

    def _should_flush(self) -> bool:
        """Check if buffer should be flushed."""
        if len(self._buffer) >= self.buffer_size:
            return True

        elapsed = (datetime.now(timezone.utc) - self._last_flush).total_seconds()
        if elapsed >= self.flush_interval_seconds and self._buffer:
            return True

        return False

    def _flush_buffer(self) -> None:
        """Flush buffered logs to S3."""
        if not self._buffer:
            return

        from botocore.exceptions import ClientError, NoCredentialsError

        now = datetime.now(timezone.utc)
        key = self._get_s3_key(now)

        try:
            # Try to get existing log content
            existing_content = ""
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                existing_content = response["Body"].read().decode("utf-8")
            except ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchKey":
                    raise

            # Append new logs
            new_content = existing_content + "".join(self._buffer)

            # Write back to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=new_content.encode("utf-8"),
                ContentType="text/plain",
            )

            self._buffer.clear()
            self._last_flush = now
            self._current_date = now.strftime("%Y-%m-%d")

        except NoCredentialsError:
            # Log to stderr if S3 credentials aren't available
            import sys
            print(f"S3LogHandler: No AWS credentials for bucket {self.bucket}", file=sys.stderr)
        except ClientError as e:
            import sys
            print(f"S3LogHandler: Failed to write to S3: {e}", file=sys.stderr)

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to the buffer."""
        try:
            msg = self.format(record)
            with self._lock:
                self._buffer.append(msg + "\n")

                # Check if date changed (new partition)
                current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if self._current_date and self._current_date != current_date:
                    self._flush_buffer()
                    self._current_date = current_date

                if self._should_flush():
                    self._flush_buffer()

        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Force flush the buffer to S3."""
        with self._lock:
            self._flush_buffer()

    def close(self) -> None:
        """Close the handler and flush remaining logs."""
        self.flush()
        super().close()


class BufferedS3LogHandler(logging.Handler):
    """
    Alternative S3 log handler that writes complete log files at the end of pipeline execution.

    This is more efficient for short-running pipelines as it minimizes S3 API calls.
    Logs are kept in memory and written to S3 when close() is called.
    """

    def __init__(
        self,
        bucket: str,
        org_id: str,
        pipeline_id: str,
        log_name: str = "pipeline",
        stream_id: Optional[str] = None,
        aws_region: str = "eu-central-1",
        run_id: Optional[str] = None,
    ):
        """
        Initialize buffered S3 log handler.

        Args:
            bucket: S3 bucket name
            org_id: Org identifier for path partitioning
            pipeline_id: Pipeline identifier
            log_name: Name of the log file
            stream_id: Optional stream identifier
            aws_region: AWS region
            run_id: Optional run ID to include in filename for uniqueness
        """
        super().__init__()
        self.bucket = bucket
        self.org_id = org_id
        self.pipeline_id = pipeline_id
        self.log_name = log_name
        self.stream_id = stream_id
        self.aws_region = aws_region
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        self._s3_client = None
        self._buffer = io.StringIO()
        self._lock = threading.Lock()
        self._start_time = datetime.now(timezone.utc)

    @property
    def s3_client(self):
        """Lazy-initialize S3 client."""
        if self._s3_client is None:
            import boto3
            self._s3_client = boto3.client("s3", region_name=self.aws_region)
        return self._s3_client

    def _get_s3_key(self) -> str:
        """
        Build S3 key with date-based partitioning.

        Pattern: {org_id}/{pipeline_id}/year=YYYY/month=MM/day=DD/{stream_id}/{run_id}_{log_name}.log
        """
        date = self._start_time
        date_path = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"
        filename = f"{self.run_id}_{self.log_name}.log"

        if self.stream_id:
            return f"{self.org_id}/{self.pipeline_id}/{date_path}/{self.stream_id}/{filename}"
        return f"{self.org_id}/{self.pipeline_id}/{date_path}/{filename}"

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to the buffer."""
        try:
            msg = self.format(record)
            with self._lock:
                self._buffer.write(msg + "\n")
        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Flush is a no-op for buffered handler - use close() to write to S3."""
        pass

    def close(self) -> None:
        """Write all buffered logs to S3 and close the handler."""
        from botocore.exceptions import ClientError, NoCredentialsError
        import sys

        with self._lock:
            content = self._buffer.getvalue()
            if content:
                key = self._get_s3_key()
                try:
                    self.s3_client.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=content.encode("utf-8"),
                        ContentType="text/plain",
                    )
                    # Use print instead of logger to avoid recursive logging
                    # during handler close (this handler may be the target)
                    print(f"BufferedS3LogHandler: Wrote logs to s3://{self.bucket}/{key}", file=sys.stderr)
                except NoCredentialsError:
                    print(f"BufferedS3LogHandler: No AWS credentials", file=sys.stderr)
                except ClientError as e:
                    print(f"BufferedS3LogHandler: S3 write failed: {e}", file=sys.stderr)

            self._buffer.close()

        super().close()


def create_log_handler(
    settings: LogStorageSettings,
    log_name: str = "pipeline",
    stream_id: Optional[str] = None,
    run_id: Optional[str] = None,
    buffered: bool = True,
) -> logging.Handler:
    """
    Factory function to create appropriate log handler based on settings.

    Args:
        settings: Log storage settings
        log_name: Name of the log file
        stream_id: Optional stream identifier for stream-specific logs
        run_id: Optional run ID for log file naming
        buffered: If True, use BufferedS3LogHandler (writes on close)

    Returns:
        Appropriate logging handler for the environment
    """
    if settings.is_cloud_mode:
        if not settings.org_id:
            raise ValueError("org_id is required for cloud log storage")
        if not settings.logs_bucket:
            raise ValueError("logs_bucket is required for cloud log storage")

        if buffered:
            return BufferedS3LogHandler(
                bucket=settings.logs_bucket,
                org_id=settings.org_id,
                pipeline_id=settings.pipeline_id,
                log_name=log_name,
                stream_id=stream_id,
                aws_region=settings.aws_region,
                run_id=run_id,
            )
        else:
            return S3LogHandler(
                bucket=settings.logs_bucket,
                org_id=settings.org_id,
                pipeline_id=settings.pipeline_id,
                log_name=log_name,
                stream_id=stream_id,
                aws_region=settings.aws_region,
            )
    else:
        # Local file handler
        logs_dir = Path(settings.local_logs_dir) / settings.pipeline_id
        if stream_id:
            logs_dir = logs_dir / stream_id
        logs_dir.mkdir(parents=True, exist_ok=True)

        log_file = logs_dir / f"{log_name}.log"
        return logging.FileHandler(log_file)