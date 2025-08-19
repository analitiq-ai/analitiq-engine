"""Pipeline metrics model with Pydantic validation."""

from datetime import datetime, timezone
from pydantic import BaseModel, Field, computed_field, ConfigDict


class PipelineMetrics(BaseModel):
    """
    Pipeline execution metrics with validation and computed fields.
    
    Provides comprehensive metrics tracking for pipeline execution including
    success rates, processing statistics, and temporal information.
    """
    
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_default=True,
        frozen=False,  # Allow updates during pipeline execution
        extra='forbid'  # Prevent accidental metric fields
    )
    
    # Core processing metrics
    records_processed: int = Field(
        default=0, 
        ge=0, 
        description="Total number of records successfully processed"
    )
    records_failed: int = Field(
        default=0, 
        ge=0, 
        description="Total number of records that failed processing"
    )
    batches_processed: int = Field(
        default=0, 
        ge=0, 
        description="Total number of batches successfully processed"
    )
    batches_failed: int = Field(
        default=0, 
        ge=0, 
        description="Total number of batches that failed processing"
    )
    streams_processed: int = Field(
        default=0, 
        ge=0, 
        description="Total number of streams successfully completed"
    )
    streams_failed: int = Field(
        default=0, 
        ge=0, 
        description="Total number of streams that failed"
    )
    
    # Temporal tracking
    pipeline_start_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the pipeline started execution"
    )
    last_update_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Last time metrics were updated"
    )
    
    # Computed success rates
    @computed_field  # type: ignore[prop-decorator]
    @property
    def records_success_rate(self) -> float:
        """Calculate the percentage of records processed successfully."""
        total_records = self.records_processed + self.records_failed
        if total_records == 0:
            return 0.0
        return round((self.records_processed / total_records) * 100, 2)
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def batches_success_rate(self) -> float:
        """Calculate the percentage of batches processed successfully."""
        total_batches = self.batches_processed + self.batches_failed
        if total_batches == 0:
            return 0.0
        return round((self.batches_processed / total_batches) * 100, 2)
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def streams_success_rate(self) -> float:
        """Calculate the percentage of streams processed successfully."""
        total_streams = self.streams_processed + self.streams_failed
        if total_streams == 0:
            return 0.0
        return round((self.streams_processed / total_streams) * 100, 2)
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_records(self) -> int:
        """Total number of records processed (successful + failed)."""
        return self.records_processed + self.records_failed
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_batches(self) -> int:
        """Total number of batches processed (successful + failed)."""
        return self.batches_processed + self.batches_failed
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_streams(self) -> int:
        """Total number of streams processed (successful + failed)."""
        return self.streams_processed + self.streams_failed
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def execution_duration_seconds(self) -> float:
        """Duration of pipeline execution in seconds."""
        return (self.last_update_time - self.pipeline_start_time).total_seconds()
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def records_per_second(self) -> float:
        """Average records processed per second."""
        duration = self.execution_duration_seconds
        if duration <= 0:
            return 0.0
        return round(self.records_processed / duration, 2)
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def has_failures(self) -> bool:
        """Whether any failures occurred during processing."""
        return (
            self.records_failed > 0 or 
            self.batches_failed > 0 or 
            self.streams_failed > 0
        )
    
    def increment_records_processed(self, count: int = 1) -> None:
        """Increment the count of successfully processed records."""
        if count < 0:
            raise ValueError("Count must be non-negative")
        object.__setattr__(self, 'records_processed', self.records_processed + count)
        self._update_timestamp()
    
    def increment_records_failed(self, count: int = 1) -> None:
        """Increment the count of failed records."""
        if count < 0:
            raise ValueError("Count must be non-negative")
        object.__setattr__(self, 'records_failed', self.records_failed + count)
        self._update_timestamp()
    
    def increment_batches_processed(self, count: int = 1) -> None:
        """Increment the count of successfully processed batches."""
        if count < 0:
            raise ValueError("Count must be non-negative")
        object.__setattr__(self, 'batches_processed', self.batches_processed + count)
        self._update_timestamp()
    
    def increment_batches_failed(self, count: int = 1) -> None:
        """Increment the count of failed batches."""
        if count < 0:
            raise ValueError("Count must be non-negative")
        object.__setattr__(self, 'batches_failed', self.batches_failed + count)
        self._update_timestamp()
    
    def increment_streams_processed(self, count: int = 1) -> None:
        """Increment the count of successfully processed streams."""
        if count < 0:
            raise ValueError("Count must be non-negative")
        object.__setattr__(self, 'streams_processed', self.streams_processed + count)
        self._update_timestamp()
    
    def increment_streams_failed(self, count: int = 1) -> None:
        """Increment the count of failed streams."""
        if count < 0:
            raise ValueError("Count must be non-negative")
        object.__setattr__(self, 'streams_failed', self.streams_failed + count)
        self._update_timestamp()
    
    def _update_timestamp(self) -> None:
        """Update the last update timestamp."""
        object.__setattr__(self, 'last_update_time', datetime.now(timezone.utc))
    
    def reset(self) -> None:
        """Reset all metrics to initial state."""
        current_time = datetime.now(timezone.utc)
        object.__setattr__(self, 'records_processed', 0)
        object.__setattr__(self, 'records_failed', 0)
        object.__setattr__(self, 'batches_processed', 0)
        object.__setattr__(self, 'batches_failed', 0)
        object.__setattr__(self, 'streams_processed', 0)
        object.__setattr__(self, 'streams_failed', 0)
        object.__setattr__(self, 'pipeline_start_time', current_time)
        object.__setattr__(self, 'last_update_time', current_time)