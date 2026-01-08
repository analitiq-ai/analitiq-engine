"""Pydantic models for engine configuration and validation."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    ConfigDict,
)


class EngineConfig(BaseModel):
    """Engine configuration with validation."""
    
    model_config = ConfigDict(extra='forbid', validate_assignment=True)
    
    batch_size: int = Field(1000, ge=1, le=100000, description="Batch size for processing")
    max_concurrent_batches: int = Field(10, ge=1, le=1000, description="Maximum concurrent batches")
    buffer_size: int = Field(10000, ge=100, le=1000000, description="Buffer size for queues")
    dlq_path: str = Field("./deadletter/", description="Dead letter queue path")
    checkpoint_interval: int = Field(100, ge=1, description="Checkpoint every N batches")


class StreamStageConfig(BaseModel):
    """Configuration for individual pipeline stages."""
    
    model_config = ConfigDict(extra='forbid')
    
    enabled: bool = Field(True, description="Whether stage is enabled")
    timeout_seconds: int = Field(300, ge=1, le=3600, description="Stage timeout in seconds")
    retry_attempts: int = Field(3, ge=0, le=10, description="Number of retry attempts")
    metrics_enabled: bool = Field(True, description="Whether to collect metrics for this stage")


class PipelineStagesConfig(BaseModel):
    """Configuration for all pipeline stages."""
    
    model_config = ConfigDict(extra='allow')
    
    extract: StreamStageConfig = Field(default_factory=StreamStageConfig)
    transform: StreamStageConfig = Field(default_factory=StreamStageConfig)
    load: StreamStageConfig = Field(default_factory=StreamStageConfig)
    checkpoint: StreamStageConfig = Field(default_factory=StreamStageConfig)


class StreamProcessingConfig(BaseModel):
    """Complete configuration for stream processing."""

    model_config = ConfigDict(extra='allow', populate_by_name=True)
    
    stream_id: str = Field(..., description="Unique stream identifier")
    stream_name: str = Field(..., description="Human-readable stream name")
    pipeline_id: str = Field(..., description="Pipeline identifier")
    
    # Processing configuration
    batch_size: int = Field(1000, ge=1, description="Batch size for this stream")
    stages: PipelineStagesConfig = Field(default_factory=PipelineStagesConfig)
    
    # Source and destination configurations
    source: Dict[str, Any] = Field(
        ...,
        description="Source endpoint configuration with merged host credentials",
    )
    destination: Dict[str, Any] = Field(
        ...,
        description="Destination endpoint configuration with merged host credentials",
    )
    
    # Data transformation
    mapping: Dict[str, Any] = Field(default_factory=dict, description="Field mappings and transformations")
    
    # Replication configuration
    replication_method: str = Field("incremental", description="Replication method")
    cursor_field: Optional[str] = Field(None, description="Cursor field for incremental sync")
    safety_window_seconds: int = Field(120, ge=0, description="Safety window for late-arriving data")
    tie_breaker_fields: Optional[List[str]] = Field(None, description="Tie-breaker fields")
    primary_key: Optional[List[str]] = Field(None, description="Primary key fields")

    # Destination configuration
    refresh_mode: str = Field("upsert", description="Write strategy")
    batch_support: bool = Field(False, description="Whether destination supports batching")

    @field_validator("replication_method")
    @classmethod
    def validate_replication_method(cls, v):
        if v not in ["full", "incremental"]:
            raise ValueError("replication_method must be 'full' or 'incremental'")
        return v

    @field_validator("refresh_mode")
    @classmethod
    def validate_refresh_mode(cls, v):
        if v not in ["insert", "upsert", "truncate_insert"]:
            raise ValueError("refresh_mode must be 'insert', 'upsert', or 'truncate_insert'")
        return v

    @field_validator("source", "destination")
    @classmethod
    def validate_endpoint_configs(cls, value: Dict[str, Any], info):
        """Ensure endpoint configs include merged credential metadata."""
        if not isinstance(value, dict):
            raise TypeError("must be a dictionary of endpoint configuration values")

        required_keys = ["endpoint_id", "connection_id"]
        missing = [key for key in required_keys if key not in value]
        if missing:
            display = info.field_name
            missing_keys = ", ".join(missing)
            raise ValueError(
                f"{display} configuration missing required keys: {missing_keys}"
            )

        return value


class PipelineMetricsSnapshot(BaseModel):
    """Pipeline-level metrics snapshot."""
    
    model_config = ConfigDict(extra='allow')
    
    pipeline_id: str = Field(..., description="Pipeline identifier")
    run_id: str = Field(..., description="Current run identifier")
    started_at: datetime = Field(..., description="Pipeline start time")
    
    # Stream metrics
    total_streams: int = Field(0, ge=0, description="Total number of streams")
    completed_streams: int = Field(0, ge=0, description="Successfully completed streams")
    failed_streams: int = Field(0, ge=0, description="Failed streams")
    
    # Record metrics
    total_records_processed: int = Field(0, ge=0, description="Total records processed")
    total_records_failed: int = Field(0, ge=0, description="Total records failed")
    total_batches_processed: int = Field(0, ge=0, description="Total batches processed")
    total_batches_failed: int = Field(0, ge=0, description="Total batches failed")
    
    # Performance metrics
    average_batch_size: float = Field(0.0, ge=0.0, description="Average batch size")
    records_per_second: float = Field(0.0, ge=0.0, description="Records processed per second")
    
    # Error metrics
    dlq_records: int = Field(0, ge=0, description="Records sent to DLQ")
    last_error: Optional[str] = Field(None, description="Last error message")
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate for streams."""
        if self.total_streams == 0:
            return 100.0
        return (self.completed_streams / self.total_streams) * 100.0
    
    @property
    def record_success_rate(self) -> float:
        """Calculate success rate for records."""
        total = self.total_records_processed + self.total_records_failed
        if total == 0:
            return 100.0
        return (self.total_records_processed / total) * 100.0


class TaskExecutionInfo(BaseModel):
    """Information about executing tasks."""
    
    model_config = ConfigDict(extra='forbid')
    
    stream_id: str = Field(..., description="Stream identifier")
    stream_name: str = Field(..., description="Stream name")
    task_name: str = Field(..., description="Task name (stage)")
    started_at: datetime = Field(..., description="Task start time")
    status: str = Field(..., description="Task status")
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v):
        if v not in ["pending", "running", "completed", "failed", "cancelled"]:
            raise ValueError("status must be one of: pending, running, completed, failed, cancelled")
        return v