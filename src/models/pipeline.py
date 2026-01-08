"""Pydantic models for Pipeline configuration (PIPELINE.yaml specification)."""

import hashlib
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

import orjson
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ScheduleType(str, Enum):
    """Pipeline schedule types."""
    MANUAL = "manual"
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"


class BackoffStrategy(str, Enum):
    """Retry backoff strategies."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"


class ErrorAction(str, Enum):
    """Error handling actions."""
    DLQ = "dlq"
    SKIP_RECORD = "skip_record"
    STOP_STREAM = "stop_stream"
    DEFAULT_VALUE = "default_value"
    QUARANTINE = "quarantine"
    RETRY = "retry"
    RETRY_WITH_BACKOFF = "retry_with_backoff"


class ExpressionPolicy(BaseModel):
    """Policy constraints for expression evaluation."""
    model_config = ConfigDict(extra="forbid")

    max_ops: int = Field(200, ge=1, le=10000, description="Maximum operations per expression")
    max_depth: int = Field(20, ge=1, le=100, description="Maximum nesting depth")
    max_string_len: int = Field(10000, ge=1, le=1000000, description="Maximum string length")


class ExpressionConfig(BaseModel):
    """Expression language configuration."""
    model_config = ConfigDict(extra="forbid")

    lang: str = Field("jsonata", description="Expression language (jsonata, jmespath, etc.)")
    policy: ExpressionPolicy = Field(default_factory=ExpressionPolicy, description="Expression evaluation policy")


class LoggingConfig(BaseModel):
    """Logging configuration for pipeline runtime."""
    model_config = ConfigDict(extra="forbid")

    log_level: str = Field("INFO", description="Logging level")
    progress_monitoring: str = Field("enabled", description="Progress monitoring status")
    health_check_interval: int = Field(300, ge=1, description="Health check interval in seconds")
    checkpoint_interval: int = Field(50, ge=1, description="Checkpoint interval in records")
    metrics_enabled: bool = Field(True, description="Whether metrics collection is enabled")

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

    api_error: ErrorAction = Field(ErrorAction.RETRY, description="Action for API errors")
    validation_error: ErrorAction = Field(ErrorAction.DLQ, description="Action for validation errors")
    transformation_error: ErrorAction = Field(ErrorAction.DLQ, description="Action for transformation errors")
    rate_limit_error: ErrorAction = Field(ErrorAction.RETRY_WITH_BACKOFF, description="Action for rate limit errors")
    default_action: ErrorAction = Field(ErrorAction.DLQ, description="Default action for unclassified errors")


class ErrorHandlingConfig(BaseModel):
    """Error handling configuration."""
    model_config = ConfigDict(extra="forbid")

    retry_failed_records: bool = Field(True, description="Whether to retry failed records")
    strategy: str = Field("dlq", description="Error handling strategy")
    max_retries: int = Field(3, ge=0, le=100, description="Maximum retry attempts")
    retry_delay: int = Field(5, ge=0, description="Retry delay in seconds")
    error_categories: ErrorCategoriesConfig = Field(
        default_factory=ErrorCategoriesConfig,
        description="Error category to action mapping"
    )
    supported_actions: List[ErrorAction] = Field(
        default_factory=lambda: [ErrorAction.DLQ, ErrorAction.SKIP_RECORD, ErrorAction.STOP_STREAM,
                                  ErrorAction.DEFAULT_VALUE, ErrorAction.QUARANTINE],
        description="Supported error actions"
    )


class RetryConfig(BaseModel):
    """Retry configuration."""
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(5, ge=1, le=100, description="Maximum retry attempts")
    backoff: BackoffStrategy = Field(BackoffStrategy.EXPONENTIAL, description="Backoff strategy")
    base_delay_ms: int = Field(500, ge=0, description="Base delay in milliseconds")


class BatchingConfig(BaseModel):
    """Batching configuration."""
    model_config = ConfigDict(extra="forbid")

    batch_size: int = Field(100, ge=1, le=100000, description="Batch size for processing")
    max_concurrent_batches: int = Field(3, ge=1, le=100, description="Maximum concurrent batches")


class EngineRuntimeConfig(BaseModel):
    """Engine runtime configuration."""
    model_config = ConfigDict(extra="forbid")

    requests_per_second: Optional[int] = Field(None, ge=1, description="Rate limit requests per second")
    buffer_size: int = Field(5000, ge=100, le=1000000, description="Buffer size")
    vcpu: float = Field(1.0, ge=0.25, description="vCPU allocation")
    memory: int = Field(8192, ge=512, description="Memory allocation in MB")


class ScheduleConfig(BaseModel):
    """Schedule configuration."""
    model_config = ConfigDict(extra="forbid")

    type: ScheduleType = Field(ScheduleType.MANUAL, description="Schedule type")
    timezone: str = Field("UTC", description="Timezone for scheduling")
    interval_minutes: Optional[int] = Field(None, ge=1, description="Interval in minutes (for interval type)")
    cron_expression: Optional[str] = Field(None, description="Cron expression (for cron type)")

    @model_validator(mode="after")
    def validate_schedule_params(self) -> "ScheduleConfig":
        if self.type == ScheduleType.INTERVAL and self.interval_minutes is None:
            raise ValueError("interval_minutes required for interval schedule type")
        if self.type == ScheduleType.CRON and self.cron_expression is None:
            raise ValueError("cron_expression required for cron schedule type")
        return self


class RuntimeConfig(BaseModel):
    """Complete runtime configuration for pipeline."""
    model_config = ConfigDict(extra="forbid")

    expression: ExpressionConfig = Field(default_factory=ExpressionConfig, description="Expression configuration")
    logging: LoggingConfig = Field(default_factory=LoggingConfig, description="Logging configuration")
    error_handling: ErrorHandlingConfig = Field(default_factory=ErrorHandlingConfig, description="Error handling")
    retry: RetryConfig = Field(default_factory=RetryConfig, description="Retry configuration")
    batching: BatchingConfig = Field(default_factory=BatchingConfig, description="Batching configuration")
    engine: EngineRuntimeConfig = Field(default_factory=EngineRuntimeConfig, description="Engine configuration")
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig, description="Schedule configuration")


class FunctionCatalogConfig(BaseModel):
    """Function catalog configuration."""
    model_config = ConfigDict(extra="forbid")

    catalog_id: str = Field("core", description="Catalog identifier")
    version: int = Field(1, ge=1, description="Catalog version")
    allow: Optional[List[str]] = Field(None, description="Allowed function names (allowlist)")


class PipelineConnectionsConfig(BaseModel):
    """Pipeline connections configuration with source and destinations."""
    model_config = ConfigDict(extra="forbid")

    source: Dict[str, str] = Field(
        default_factory=dict,
        description="Source connection alias to UUID mapping (e.g., conn_1: uuid)"
    )
    destinations: List[Dict[str, str]] = Field(
        default_factory=list,
        description="List of destination connection alias to UUID mappings"
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
        # Check source
        if alias in self.source:
            return self.source[alias]
        # Check destinations
        for dest in self.destinations:
            if alias in dest:
                return dest[alias]
        raise KeyError(f"Connection alias '{alias}' not found in pipeline connections")


class PipelineConfig(BaseModel):
    """
    Complete Pipeline configuration model based on PIPELINE.yaml specification.

    This represents the top-level pipeline configuration that contains:
    - Pipeline metadata (id, name, version, tags)
    - Connection references (aliases to connection UUIDs)
    - Runtime configuration (expression, logging, error handling, etc.)
    - Function catalog reference
    """
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    version: int = Field(1, description="Pipeline configuration version")
    client_id: str = Field("", description="Client identifier")
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    name: str = Field(..., description="Human-readable pipeline name")
    description: Optional[str] = Field(None, description="Pipeline description")
    status: str = Field("draft", description="Pipeline status (draft, active, paused, etc.)")
    is_active: bool = Field(False, description="Whether the pipeline is active")
    tags: List[str] = Field(default_factory=list, description="Pipeline tags for categorization")

    # Connection aliases map to connection UUIDs (nested structure)
    connections: PipelineConnectionsConfig = Field(
        default_factory=PipelineConnectionsConfig,
        description="Connection configuration with source and destinations"
    )

    # Runtime configuration
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig, description="Runtime configuration")

    # Function catalog (optional)
    function_catalog: Optional[FunctionCatalogConfig] = Field(
        None,
        description="Function catalog configuration"
    )

    # Timestamps
    created_at: Optional[datetime] = Field(None, description="Pipeline creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Pipeline last update timestamp")

    @field_validator("connections", mode="before")
    @classmethod
    def validate_connections(cls, v: Any) -> Any:
        """Validate and normalize connections format."""
        if isinstance(v, dict) and not isinstance(v, PipelineConnectionsConfig):
            # Already in the new format with source/destinations
            if "source" in v or "destinations" in v:
                return v
            # Flat format - not supported anymore
            raise ValueError(
                "Connections must use nested format with 'source' and 'destinations' keys"
            )
        return v

    def get_connection_uuid(self, alias: str) -> str:
        """Get connection UUID by alias."""
        return self.connections.get_connection_uuid(alias)

    def fingerprint(self) -> str:
        """
        Return a deterministic SHA256 fingerprint of the pipeline configuration.
        Used for config-state compatibility validation.
        """
        # Include only fields that affect processing behavior
        data = {
            "version": self.version,
            "pipeline_id": self.pipeline_id,
            "connections": self.connections.model_dump(mode="json"),
            "runtime": self.runtime.model_dump(mode="json"),
        }
        if self.function_catalog:
            data["function_catalog"] = self.function_catalog.model_dump(mode="json")
        raw = orjson.dumps(data, option=orjson.OPT_SORT_KEYS)
        return f"sha256:{hashlib.sha256(raw).hexdigest()}"


class PipelineConfigLegacy(BaseModel):
    """
    Legacy pipeline configuration for backwards compatibility during migration.
    Maps old structure to new PipelineConfig model.
    """
    model_config = ConfigDict(extra="allow")

    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    client_id: str = Field("", description="Client identifier")
    name: str = Field(..., description="Human-readable pipeline name")
    version: str = Field("1.0", description="Pipeline version string")

    # Old structure fields
    source: Dict[str, Any] = Field(default_factory=dict, description="Legacy source configuration")
    destination: Dict[str, Any] = Field(default_factory=dict, description="Legacy destination configuration")
    streams: Dict[str, Any] = Field(default_factory=dict, description="Legacy streams configuration")
    engine_config: Optional[Dict[str, Any]] = Field(None, description="Legacy engine configuration")
    error_handling: Optional[Dict[str, Any]] = Field(None, description="Legacy error handling")
    monitoring: Optional[Dict[str, Any]] = Field(None, description="Legacy monitoring configuration")

    def to_new_format(self) -> tuple["PipelineConfig", List[Dict[str, Any]]]:
        """
        Convert legacy pipeline config to new format.
        Returns (PipelineConfig, list of stream configs).
        """
        # Build connections mapping from source/destination
        connections = {}
        if self.source.get("connection_id"):
            connections["conn_src"] = self.source["connection_id"]
        if self.destination.get("connection_id"):
            connections["conn_dst"] = self.destination["connection_id"]

        # Build runtime config from engine_config and other settings
        runtime_dict: Dict[str, Any] = {}

        if self.engine_config:
            runtime_dict["batching"] = {
                "batch_size": self.engine_config.get("batch_size", 100),
                "max_concurrent_batches": self.engine_config.get("max_concurrent_batches", 3),
            }
            runtime_dict["engine"] = {
                "buffer_size": self.engine_config.get("buffer_size", 5000),
            }
            if self.engine_config.get("schedule"):
                runtime_dict["schedule"] = self.engine_config["schedule"]

        if self.error_handling:
            runtime_dict["error_handling"] = self.error_handling

        if self.monitoring:
            runtime_dict["logging"] = {
                "log_level": self.monitoring.get("log_level", "INFO"),
                "metrics_enabled": self.monitoring.get("metrics_enabled", True),
                "checkpoint_interval": self.monitoring.get("checkpoint_interval", 50),
                "health_check_interval": self.monitoring.get("health_check_interval", 300),
                "progress_monitoring": self.monitoring.get("progress_monitoring", "enabled"),
            }

        pipeline = PipelineConfig(
            version=self.version,
            client_id=self.client_id,
            pipeline_id=self.pipeline_id,
            name=self.name,
            is_enabled=True,
            connections=connections,
            runtime=RuntimeConfig(**runtime_dict) if runtime_dict else RuntimeConfig(),
        )

        # Convert streams to new format
        stream_configs = []
        for stream_id, stream_data in self.streams.items():
            stream_configs.append({
                "stream_id": stream_id,
                "pipeline_id": self.pipeline_id,
                "client_id": self.client_id,
                **stream_data,
            })

        return pipeline, stream_configs
