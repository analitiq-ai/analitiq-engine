"""S3 metrics storage with partitioning optimized for Athena queries.

Stores pipeline execution metrics (row counts, duration, etc.) in a format
optimized for querying with AWS Athena.

Partition structure:
    s3://{bucket}/client_id={client_id}/year=YYYY/month=MM/day=DD/{run_id}.json

This partitioning scheme enables efficient queries:
    - Filter by client_id (partition pruning)
    - Filter by date range (partition pruning)
    - Aggregate by pipeline_id within results
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


class MetricsStorageSettings(BaseModel):
    """Settings for metrics storage configuration."""

    env: str = Field(default="local", description="Environment: local, dev, or prod")
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    pipeline_name: Optional[str] = Field(default=None, description="Human-readable pipeline name")
    client_id: Optional[str] = Field(default=None, description="Client ID for S3 path partitioning")
    aws_region: str = Field(default="eu-central-1", description="AWS region")
    row_count_bucket: Optional[str] = Field(default=None, description="S3 bucket for row count metrics")
    local_metrics_dir: str = Field(default="metrics", description="Local directory for metrics")

    @classmethod
    def from_env(cls) -> "MetricsStorageSettings":
        """Create settings from environment variables."""
        env = os.getenv("ENV", "local")
        pipeline_id = os.getenv("PIPELINE_ID", "")
        pipeline_name = os.getenv("PIPELINE_NAME")
        client_id = os.getenv("CLIENT_ID")
        aws_region = os.getenv("AWS_REGION", "eu-central-1")
        row_count_bucket = os.getenv("ROW_COUNT_BUCKET") or f"analitiq-client-pipeline-row-count-{env}"
        local_metrics_dir = os.getenv("METRICS_DIR", "metrics")

        return cls(
            env=env,
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            client_id=client_id,
            aws_region=aws_region,
            row_count_bucket=row_count_bucket,
            local_metrics_dir=local_metrics_dir,
        )

    @property
    def is_cloud_mode(self) -> bool:
        """Check if running in cloud mode (dev or prod)."""
        return self.env in ("dev", "prod")


class PipelineMetricsRecord(BaseModel):
    """
    Metrics record for a single pipeline execution.

    This schema is designed for Athena queries with the following considerations:
    - All fields are flat (no nested objects) for easy SQL querying
    - Timestamps are ISO 8601 strings for Athena compatibility
    - Numeric fields use appropriate types for aggregation
    """

    # Identifiers
    run_id: str = Field(..., description="Unique identifier for this pipeline run")
    pipeline_id: str = Field(..., description="Pipeline identifier")
    pipeline_name: Optional[str] = Field(default=None, description="Human-readable pipeline name")
    client_id: str = Field(..., description="Client identifier")

    # Timing
    start_time: str = Field(..., description="Pipeline start time (ISO 8601)")
    end_time: str = Field(..., description="Pipeline end time (ISO 8601)")
    duration_seconds: float = Field(..., ge=0, description="Total execution duration in seconds")

    # Record counts
    records_processed: int = Field(default=0, ge=0, description="Number of records successfully processed")
    records_failed: int = Field(default=0, ge=0, description="Number of records that failed processing")
    records_total: int = Field(default=0, ge=0, description="Total records attempted (processed + failed)")

    # Batch metrics
    batches_processed: int = Field(default=0, ge=0, description="Number of batches processed")

    # Status
    status: str = Field(..., description="Pipeline execution status: success, failed, partial")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")

    # Performance metrics
    records_per_second: Optional[float] = Field(default=None, ge=0, description="Processing throughput")

    # Metadata
    environment: str = Field(..., description="Execution environment: local, dev, prod")


class MetricsStorage:
    """
    Stores pipeline execution metrics to S3 or local filesystem.

    For Athena compatibility, metrics are stored as JSON files with partition structure:
        s3://{bucket}/client_id={client_id}/year=YYYY/month=MM/day=DD/{run_id}.json

    Typical Athena queries supported:

    1. Get all rows exported by day over a time period, for all pipelines:
        SELECT
            year, month, day,
            SUM(records_processed) as total_records,
            COUNT(*) as pipeline_runs
        FROM pipeline_metrics
        WHERE client_id = 'xxx'
          AND year = '2025' AND month = '01'
        GROUP BY year, month, day
        ORDER BY year, month, day

    2. Get all rows exported by a pipeline over a time period:
        SELECT
            year, month, day,
            records_processed,
            duration_seconds,
            status
        FROM pipeline_metrics
        WHERE client_id = 'xxx'
          AND pipeline_id = 'yyy'
          AND year = '2025'
        ORDER BY start_time
    """

    def __init__(self, settings: MetricsStorageSettings):
        """
        Initialize metrics storage.

        Args:
            settings: Metrics storage configuration
        """
        self.settings = settings
        self._s3_client = None

    @property
    def s3_client(self):
        """Lazy-initialize S3 client."""
        if self._s3_client is None:
            import boto3
            self._s3_client = boto3.client("s3", region_name=self.settings.aws_region)
        return self._s3_client

    def _get_s3_key(self, record: PipelineMetricsRecord, execution_date: datetime) -> str:
        """
        Build S3 key with Athena-compatible partitioning.

        Pattern: client_id={client_id}/year=YYYY/month=MM/day=DD/{run_id}.json

        Using Hive-style partitioning (key=value) for automatic partition discovery in Athena.
        """
        return (
            f"client_id={record.client_id}/"
            f"year={execution_date.year}/"
            f"month={execution_date.month:02d}/"
            f"day={execution_date.day:02d}/"
            f"{record.run_id}.json"
        )

    def _get_local_path(self, record: PipelineMetricsRecord, execution_date: datetime) -> Path:
        """Build local file path mirroring S3 structure."""
        base_dir = Path(self.settings.local_metrics_dir)
        return (
            base_dir
            / f"client_id={record.client_id}"
            / f"year={execution_date.year}"
            / f"month={execution_date.month:02d}"
            / f"day={execution_date.day:02d}"
            / f"{record.run_id}.json"
        )

    def save(self, record: PipelineMetricsRecord) -> str:
        """
        Save metrics record to storage.

        Args:
            record: Pipeline metrics record to save

        Returns:
            Storage location (S3 URI or local path)
        """
        # Parse execution date from start_time
        execution_date = datetime.fromisoformat(record.start_time.replace("Z", "+00:00"))

        # Convert record to JSON
        record_json = record.model_dump_json(indent=2)

        if self.settings.is_cloud_mode:
            return self._save_to_s3(record, record_json, execution_date)
        else:
            return self._save_to_local(record, record_json, execution_date)

    def _save_to_s3(
        self,
        record: PipelineMetricsRecord,
        record_json: str,
        execution_date: datetime
    ) -> str:
        """Save metrics to S3."""
        from botocore.exceptions import ClientError, NoCredentialsError

        if not self.settings.row_count_bucket:
            raise ValueError("row_count_bucket is required for cloud metrics storage")
        if not self.settings.client_id:
            raise ValueError("client_id is required for cloud metrics storage")

        key = self._get_s3_key(record, execution_date)
        s3_uri = f"s3://{self.settings.row_count_bucket}/{key}"

        try:
            self.s3_client.put_object(
                Bucket=self.settings.row_count_bucket,
                Key=key,
                Body=record_json.encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(f"Saved metrics to {s3_uri}")
            return s3_uri

        except NoCredentialsError:
            logger.error("AWS credentials not configured for metrics storage")
            raise RuntimeError("AWS credentials not configured for metrics storage")
        except ClientError as e:
            logger.error(f"Failed to save metrics to S3: {e}")
            raise RuntimeError(f"Failed to save metrics to S3: {e}")

    def _save_to_local(
        self,
        record: PipelineMetricsRecord,
        record_json: str,
        execution_date: datetime
    ) -> str:
        """Save metrics to local filesystem."""
        # Use pipeline_id as client_id for local mode if client_id not set
        if not record.client_id:
            record = record.model_copy(update={"client_id": self.settings.pipeline_id})

        file_path = self._get_local_path(record, execution_date)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_path.write_text(record_json, encoding="utf-8")
        logger.info(f"Saved metrics to {file_path}")
        return str(file_path)


def create_metrics_record(
    settings: MetricsStorageSettings,
    run_id: str,
    start_time: datetime,
    end_time: datetime,
    records_processed: int = 0,
    records_failed: int = 0,
    batches_processed: int = 0,
    status: str = "success",
    error_message: Optional[str] = None,
) -> PipelineMetricsRecord:
    """
    Factory function to create a metrics record from pipeline execution data.

    Args:
        settings: Metrics storage settings
        run_id: Unique run identifier
        start_time: Pipeline start time
        end_time: Pipeline end time
        records_processed: Number of successfully processed records
        records_failed: Number of failed records
        batches_processed: Number of batches processed
        status: Execution status (success, failed, partial)
        error_message: Error message if failed

    Returns:
        PipelineMetricsRecord ready for storage
    """
    duration_seconds = (end_time - start_time).total_seconds()
    records_total = records_processed + records_failed

    # Calculate throughput
    records_per_second = None
    if duration_seconds > 0 and records_processed > 0:
        records_per_second = round(records_processed / duration_seconds, 2)

    return PipelineMetricsRecord(
        run_id=run_id,
        pipeline_id=settings.pipeline_id,
        pipeline_name=settings.pipeline_name,
        client_id=settings.client_id or settings.pipeline_id,
        start_time=start_time.isoformat(),
        end_time=end_time.isoformat(),
        duration_seconds=round(duration_seconds, 2),
        records_processed=records_processed,
        records_failed=records_failed,
        records_total=records_total,
        batches_processed=batches_processed,
        status=status,
        error_message=error_message,
        records_per_second=records_per_second,
        environment=settings.env,
    )


def save_pipeline_metrics(
    run_id: str,
    start_time: datetime,
    end_time: datetime,
    records_processed: int = 0,
    records_failed: int = 0,
    batches_processed: int = 0,
    status: str = "success",
    error_message: Optional[str] = None,
    settings: Optional[MetricsStorageSettings] = None,
) -> str:
    """
    Convenience function to save pipeline metrics.

    Args:
        run_id: Unique run identifier
        start_time: Pipeline start time
        end_time: Pipeline end time
        records_processed: Number of successfully processed records
        records_failed: Number of failed records
        batches_processed: Number of batches processed
        status: Execution status (success, failed, partial)
        error_message: Error message if failed
        settings: Optional settings (defaults to from_env())

    Returns:
        Storage location (S3 URI or local path)
    """
    if settings is None:
        settings = MetricsStorageSettings.from_env()

    record = create_metrics_record(
        settings=settings,
        run_id=run_id,
        start_time=start_time,
        end_time=end_time,
        records_processed=records_processed,
        records_failed=records_failed,
        batches_processed=batches_processed,
        status=status,
        error_message=error_message,
    )

    storage = MetricsStorage(settings)
    return storage.save(record)
