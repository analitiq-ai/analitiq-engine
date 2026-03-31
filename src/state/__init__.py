"""Fault tolerance components for reliable data streaming."""

from .batch_commit_tracker import BatchCommitTracker, BatchCommitRecord
from .circuit_breaker import CircuitBreaker
from .dead_letter_queue import (
    DeadLetterQueue,
    LocalDLQStorage,
    DLQ_MARKER,
    emit_dlq_log,
)
from .retry_handler import RetryHandler
from .state_emission import STATE_MARKER, emit_state_log
from .state_manager import StateManager
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
    "LocalDLQStorage",
    "DLQ_MARKER",
    "emit_dlq_log",
    "StateManager",
    "METRICS_MARKER",
    "PipelineMetricsRecord",
    "emit_metrics_log",
    "create_metrics_record",
    "save_pipeline_metrics",
    "STATE_MARKER",
    "emit_state_log",
]
