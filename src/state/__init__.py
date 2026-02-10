"""Fault tolerance components for reliable data streaming."""

from .batch_commit_tracker import BatchCommitTracker, BatchCommitRecord
from .circuit_breaker import CircuitBreaker
from .dead_letter_queue import (
    DeadLetterQueue,
    DLQStorageSettings,
    DLQStorageBackend,
    LocalDLQStorage,
    S3DLQStorage,
    create_dlq_storage,
)
from .retry_handler import RetryHandler
from .state_emission import STATE_MARKER, emit_state_log
from .state_manager import StateManager
from .state_storage import (
    LocalStateStorage,
    S3StateStorage,
    StateStorageBackend,
    StateStorageSettings,
    create_storage_backend,
)
from .log_storage import (
    LogStorageSettings,
    S3LogHandler,
    BufferedS3LogHandler,
    create_log_handler,
)
from .metrics_storage import (
    METRICS_MARKER,
    PipelineMetricsRecord,
    emit_metrics_log,
    create_metrics_record,
    save_pipeline_metrics,
)

__all__ = [
    "BatchCommitTracker",
    "BatchCommitRecord",
    "RetryHandler",
    "CircuitBreaker",
    "DeadLetterQueue",
    "DLQStorageSettings",
    "DLQStorageBackend",
    "LocalDLQStorage",
    "S3DLQStorage",
    "create_dlq_storage",
    "StateManager",
    "StateStorageBackend",
    "StateStorageSettings",
    "LocalStateStorage",
    "S3StateStorage",
    "create_storage_backend",
    "LogStorageSettings",
    "S3LogHandler",
    "BufferedS3LogHandler",
    "create_log_handler",
    "METRICS_MARKER",
    "PipelineMetricsRecord",
    "emit_metrics_log",
    "create_metrics_record",
    "save_pipeline_metrics",
    "STATE_MARKER",
    "emit_state_log",
]
