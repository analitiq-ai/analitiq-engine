"""Pipeline configuration models.

These models mirror k2m's canonical PipelineConfig (k2m/models/pipeline.py).
At runtime in Docker, k2m is available and these definitions must stay in sync.

The models here are the single source of truth for the engine runtime.
k2m's PipelineConfig is the single source of truth for the API layer.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------

class ScheduleConfig(BaseModel):
    """Schedule configuration."""
    model_config = ConfigDict(extra="forbid")

    type: str = Field(default="manual", description="Schedule type")
    timezone: str = Field(default="UTC", description="Timezone for scheduling")
    interval_minutes: Optional[int] = Field(default=None, ge=1, description="Interval in minutes")
    cron_expression: Optional[str] = Field(default=None, description="Cron expression")

    @model_validator(mode="after")
    def validate_schedule_params(self) -> "ScheduleConfig":
        if self.type == "interval" and self.interval_minutes is None:
            raise ValueError("interval_minutes required for interval schedule type")
        if self.type == "cron" and self.cron_expression is None:
            raise ValueError("cron_expression required for cron schedule type")
        return self


# ---------------------------------------------------------------------------
# Batching / Engine Resources
# ---------------------------------------------------------------------------

class BatchingConfig(BaseModel):
    """Batching configuration."""
    model_config = ConfigDict(extra="forbid")

    batch_size: int = Field(default=100, ge=1, le=100000, description="Batch size for processing")
    max_concurrent_batches: int = Field(default=3, ge=1, le=100, description="Maximum concurrent batches")


class EngineResourceConfig(BaseModel):
    """Engine resource configuration."""
    model_config = ConfigDict(extra="forbid")

    buffer_size: int = Field(default=5000, ge=100, le=1000000, description="Buffer size")
    vcpu: float = Field(default=1.0, ge=0.25, description="vCPU allocation")
    memory: int = Field(default=8192, ge=512, description="Memory allocation in MB")


# ---------------------------------------------------------------------------
# Logging / Error Handling / Retry
# ---------------------------------------------------------------------------

class LoggingConfig(BaseModel):
    """Logging configuration for pipeline runtime."""
    model_config = ConfigDict(extra="forbid")

    log_level: str = Field(default="INFO", description="Logging level")
    progress_monitoring: str = Field(default="enabled", description="Progress monitoring status")
    health_check_interval: int = Field(default=300, ge=1, description="Health check interval in seconds")
    checkpoint_interval: int = Field(default=50, ge=1, description="Checkpoint interval in records")
    metrics_enabled: bool = Field(default=True, description="Whether metrics collection is enabled")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of: {valid_levels}")
        return v.upper()


class ErrorCategoriesConfig(BaseModel):
    """Error category to action mapping."""
    model_config = ConfigDict(extra="allow")

    api_error: str = Field(default="retry", description="Action for API errors")
    validation_error: str = Field(default="dlq", description="Action for validation errors")
    transformation_error: str = Field(default="dlq", description="Action for transformation errors")
    rate_limit_error: str = Field(default="retry_with_backoff", description="Action for rate limit errors")
    default_action: str = Field(default="dlq", description="Default action for unclassified errors")


class ErrorHandlingConfig(BaseModel):
    """Error handling configuration."""
    model_config = ConfigDict(extra="forbid")

    retry_failed_records: bool = Field(default=True, description="Whether to retry failed records")
    strategy: str = Field(default="dlq", description="Error handling strategy")
    max_retries: int = Field(default=3, ge=0, le=100, description="Maximum retry attempts")
    retry_delay: int = Field(default=5, ge=0, description="Retry delay in seconds")
    error_categories: ErrorCategoriesConfig = Field(
        default_factory=ErrorCategoriesConfig,
        description="Error category to action mapping",
    )


class RetryConfig(BaseModel):
    """Retry configuration."""
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=5, ge=1, le=100, description="Maximum retry attempts")
    backoff: str = Field(default="exponential", description="Backoff strategy")
    base_delay_ms: int = Field(default=500, ge=0, description="Base delay in milliseconds")


# ---------------------------------------------------------------------------
# EngineConfig  (replaces the old RuntimeConfig)
# ---------------------------------------------------------------------------

class EngineConfig(BaseModel):
    """Pipeline-level engine configuration.

    Contains schedule, batching, resource settings, plus logging,
    error handling, and retry configuration.

    This mirrors k2m's EngineConfig.
    """
    model_config = ConfigDict(extra="allow")

    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig, description="Schedule configuration")
    batching: BatchingConfig = Field(default_factory=BatchingConfig, description="Batching configuration")
    engine: EngineResourceConfig = Field(default_factory=EngineResourceConfig, description="Engine resource configuration")
    logging: LoggingConfig = Field(default_factory=LoggingConfig, description="Logging configuration")
    error_handling: ErrorHandlingConfig = Field(default_factory=ErrorHandlingConfig, description="Error handling")
    retry: RetryConfig = Field(default_factory=RetryConfig, description="Retry configuration")


# ---------------------------------------------------------------------------
# Pipeline Connections
# ---------------------------------------------------------------------------

class PipelineConnectionsConfig(BaseModel):
    """Pipeline connections configuration with source and destinations.

    Engine-side utility wrapper that adds helper methods on top of the
    connection structure defined in k2m's PipelineConnections.
    """
    model_config = ConfigDict(extra="allow")

    source: Dict[str, str] = Field(
        default_factory=dict,
        description="Source connection alias to UUID mapping (e.g., conn_1: uuid)",
    )
    destinations: List[Dict[str, str]] = Field(
        default_factory=list,
        description="List of destination connection alias to UUID mappings",
    )

    def get_source_connection(self) -> tuple[str, str]:
        """Get source connection (alias, uuid) tuple."""
        if not self.source:
            raise ValueError("No source connection defined")
        alias, uuid = next(iter(self.source.items()))
        return alias, uuid

    def get_destination_connections(self) -> List[tuple[str, str]]:
        """Get list of destination connection (alias, uuid) tuples."""
        result = []
        for dest in self.destinations:
            if dest:
                alias, uuid = next(iter(dest.items()))
                result.append((alias, uuid))
        return result

    def get_connection_uuid(self, alias: str) -> str:
        """Get connection UUID by alias from either source or destinations."""
        if alias in self.source:
            return self.source[alias]
        for dest in self.destinations:
            if alias in dest:
                return dest[alias]
        raise KeyError(f"Connection alias '{alias}' not found in pipeline connections")


# ---------------------------------------------------------------------------
# PipelineConfig  (top-level model)
# ---------------------------------------------------------------------------

class PipelineConfig(BaseModel):
    """Complete Pipeline configuration model.

    Mirrors k2m's PipelineConfig (canonical model). Contains:
    - Pipeline metadata (id, name, version, tags, status)
    - Connection references (aliases to connection UUIDs)
    - Engine configuration (schedule, batching, resources, logging, error handling, retry)
    - Optional timestamps
    """
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    version: int = Field(1, description="Pipeline configuration version")
    org_id: str = Field("", description="Org identifier")
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    name: str = Field(..., description="Human-readable pipeline name")
    description: Optional[str] = Field(default=None, description="Pipeline description")
    status: str = Field(default="draft", description="Pipeline status (draft, active, paused, etc.)")
    tags: Optional[List[str]] = Field(default=None, description="Pipeline tags for categorization")

    connections: PipelineConnectionsConfig = Field(
        default_factory=PipelineConnectionsConfig,
        description="Connection configuration with source and destinations",
    )

    engine_config: EngineConfig = Field(
        default_factory=EngineConfig,
        description="Engine configuration",
    )

    created_at: Optional[datetime] = Field(default=None, description="Pipeline creation timestamp")
    updated_at: Optional[datetime] = Field(default=None, description="Pipeline last update timestamp")

    @field_validator("connections", mode="before")
    @classmethod
    def validate_connections(cls, v: Any) -> Any:
        """Validate and normalize connections format."""
        if isinstance(v, dict) and not isinstance(v, PipelineConnectionsConfig):  # type: ignore[unreachable]
            if "source" in v or "destinations" in v:
                return v
            raise ValueError(
                "Connections must use nested format with 'source' and 'destinations' keys"
            )
        return v

    def get_connection_uuid(self, alias: str) -> str:
        """Get connection UUID by alias."""
        return self.connections.get_connection_uuid(alias)
