"""Data models for configuration and state."""

from .stream import (
    Assignment,
    AssignmentTarget,
    AssignmentValue,
    ConstValue,
    DestinationBatchingConfig,
    DestinationConfig,
    IdempotencyKeyConfig,
    MappingConfig,
    ReplicationConfig,
    SourceConfig,
    StreamConfig,
    StreamEngineConfig,
    TargetType,
    ValidationConfig,
    ValidationRule,
    ValueKind,
    WriteMode,
    WriteModeConfig,
)

# Engine models
from .engine import (
    PipelineMetricsSnapshot,
    PipelineStagesConfig,
    StreamProcessingConfig,
    StreamStageConfig,
    TaskExecutionInfo,
)

# API models
from .api import (
    APIConfig,
    APIConnectionConfig,
    APIReadConfig,
    APIRequestParams,
    APIWriteConfig,
    EndpointConfig,
    FilterConfig,
    HostConfig,
    HTTPResponse,
    PaginationConfig,
    PaginationParams,
    RateLimitConfig,
    RecordBatch,
)

# Transformation models
from .transformations import (
    TransformationConfig,
    TransformationType,
)

# Metrics models
from .metrics import PipelineMetrics

# Enriched config models (merged connection + endpoint config)
from .enriched import (
    APIConnectionParameters,
    DatabaseConnectionParameters,
    EnrichedAPIConfig,
    EnrichedDatabaseConfig,
    EnrichedDestinationConfig,
    EnrichedSourceConfig,
)

from .state import (
    CursorField,
    StreamCursor,
    StreamStats,
    StreamState as SimpleStreamState,
    DestinationConfig as StateDestinationConfig,
    ReplicationConfig as StateReplicationConfig,
    SourceConfig as StateSourceConfig,
)

__all__ = [
    # Stream models
    "Assignment",
    "AssignmentTarget",
    "AssignmentValue",
    "ConstValue",
    "DestinationBatchingConfig",
    "DestinationConfig",
    "IdempotencyKeyConfig",
    "MappingConfig",
    "ReplicationConfig",
    "SourceConfig",
    "StreamConfig",
    "StreamEngineConfig",
    "TargetType",
    "ValidationConfig",
    "ValidationRule",
    "ValueKind",
    "WriteMode",
    "WriteModeConfig",
    # Engine models
    "PipelineMetricsSnapshot",
    "PipelineStagesConfig",
    "StreamProcessingConfig",
    "StreamStageConfig",
    "TaskExecutionInfo",
    # API models
    "APIConfig",
    "APIConnectionConfig",
    "APIReadConfig",
    "APIRequestParams",
    "APIWriteConfig",
    "EndpointConfig",
    "FilterConfig",
    "HostConfig",
    "HTTPResponse",
    "PaginationConfig",
    "PaginationParams",
    "RateLimitConfig",
    "RecordBatch",
    # Transformation models
    "TransformationConfig",
    "TransformationType",
    # Metrics models
    "PipelineMetrics",
    # Enriched config models
    "APIConnectionParameters",
    "DatabaseConnectionParameters",
    "EnrichedAPIConfig",
    "EnrichedDatabaseConfig",
    "EnrichedDestinationConfig",
    "EnrichedSourceConfig",
    # State models
    "CursorField",
    "StreamCursor",
    "StreamStats",
    "SimpleStreamState",
    "StateDestinationConfig",
    "StateReplicationConfig",
    "StateSourceConfig",
]
