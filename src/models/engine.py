"""Pydantic models for engine metrics and execution tracking."""

from datetime import datetime
from typing import Optional
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    ConfigDict,
)


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