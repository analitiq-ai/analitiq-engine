"""Log-based metrics emission for pipeline execution tracking.

Emits pipeline execution metrics (row counts, duration, etc.) as structured JSON
logs with a marker prefix for easy extraction from CloudWatch Logs.

Log format:
    ANALITIQ_METRICS::{"type":"batch","run_id":"...","records_written":1000,...}

CloudWatch extraction query:
    fields @timestamp, @message
    | filter @message like /ANALITIQ_METRICS::/
    | parse @message /ANALITIQ_METRICS::(?<metrics>.*)$/
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)

METRICS_MARKER = "ANALITIQ_METRICS::"


class PipelineMetricsRecord(BaseModel):
    """
    Metrics record for a single pipeline execution.

    This schema is designed for structured log queries with the following considerations:
    - All fields are flat (no nested objects) for easy querying
    - Timestamps are ISO 8601 strings
    - Numeric fields use appropriate types for aggregation
    """

    # Identifiers
    run_id: str = Field(..., description="Unique identifier for this pipeline run")
    pipeline_id: str = Field(..., description="Pipeline identifier")
    pipeline_name: Optional[str] = Field(default=None, description="Human-readable pipeline name")

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


def emit_metrics_log(data: Dict[str, Any]) -> None:
    """
    Emit metrics as structured JSON log with marker for extraction.

    The marker prefix allows easy filtering and extraction from CloudWatch Logs
    or other log aggregation systems.

    Args:
        data: Dictionary of metrics to emit
    """
    logger.info(f"{METRICS_MARKER}{json.dumps(data)}")


def create_metrics_record(
    run_id: str,
    pipeline_id: str,
    start_time: datetime,
    end_time: datetime,
    records_processed: int = 0,
    records_failed: int = 0,
    batches_processed: int = 0,
    status: str = "success",
    error_message: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    environment: Optional[str] = None,
) -> PipelineMetricsRecord:
    """
    Factory function to create a metrics record from pipeline execution data.

    Args:
        run_id: Unique run identifier
        pipeline_id: Pipeline identifier
        start_time: Pipeline start time
        end_time: Pipeline end time
        records_processed: Number of successfully processed records
        records_failed: Number of failed records
        batches_processed: Number of batches processed
        status: Execution status (success, failed, partial)
        error_message: Error message if failed
        pipeline_name: Human-readable pipeline name

    Returns:
        PipelineMetricsRecord ready for emission
    """
    duration_seconds = (end_time - start_time).total_seconds()
    records_total = records_processed + records_failed

    # Calculate throughput
    records_per_second = None
    if duration_seconds > 0 and records_processed > 0:
        records_per_second = round(records_processed / duration_seconds, 2)

    env = environment or os.getenv("ENV", "local")

    return PipelineMetricsRecord(
        run_id=run_id,
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
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
        environment=env,
    )


def save_pipeline_metrics(
    run_id: str,
    pipeline_id: str,
    start_time: datetime,
    end_time: datetime,
    records_processed: int = 0,
    records_failed: int = 0,
    batches_processed: int = 0,
    status: str = "success",
    error_message: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    environment: Optional[str] = None,
) -> None:
    """
    Emit pipeline metrics to logs.

    Emits a structured JSON log entry with the ANALITIQ_METRICS:: prefix
    for easy extraction from CloudWatch Logs or other log systems.

    Args:
        run_id: Unique run identifier
        pipeline_id: Pipeline identifier
        start_time: Pipeline start time
        end_time: Pipeline end time
        records_processed: Number of successfully processed records
        records_failed: Number of failed records
        batches_processed: Number of batches processed
        status: Execution status (success, failed, partial)
        error_message: Error message if failed
        pipeline_name: Human-readable pipeline name
    """
    record = create_metrics_record(
        run_id=run_id,
        pipeline_id=pipeline_id,
        start_time=start_time,
        end_time=end_time,
        records_processed=records_processed,
        records_failed=records_failed,
        batches_processed=batches_processed,
        status=status,
        error_message=error_message,
        pipeline_name=pipeline_name,
        environment=environment,
    )

    emit_metrics_log({
        "type": "pipeline",
        **record.model_dump(),
    })
