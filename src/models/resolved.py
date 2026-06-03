"""Resolved runtime types: typed output of PipelineConfigPrep.

These dataclasses represent the engine-side view of a pipeline after all
on-disk artifacts (connections, connectors, endpoints) have been loaded,
validated, and resolved. Everything downstream of PipelineConfigPrep uses
these types; no code outside pipeline_config_prep.py should access raw
JSON dicts for pipeline/stream/source/destination configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .stream import DestinationConfig, MappingConfig, SourceConfig
from cdk.connection_runtime import ConnectionRuntime


@dataclass(frozen=True)
class ResolvedSource:
    """Source connector fully resolved to a runtime and endpoint document."""

    config: SourceConfig
    runtime: ConnectionRuntime
    endpoint: Dict[str, Any]  # raw endpoint contract document (external JSON)


@dataclass(frozen=True)
class ResolvedDestination:
    """Destination connector fully resolved to a runtime and endpoint document."""

    config: DestinationConfig
    runtime: ConnectionRuntime
    endpoint: Dict[str, Any]


@dataclass(frozen=True)
class ResolvedStream:
    """Stream with all endpoint references resolved to runtimes and documents."""

    stream_id: str
    pipeline_id: str
    display_name: Optional[str]
    description: Optional[str]
    status: str
    tags: List[str]
    source: ResolvedSource
    destinations: List[ResolvedDestination]
    mapping: MappingConfig

    def get_primary_destination(self) -> ResolvedDestination:
        if not self.destinations:
            raise ValueError(
                f"Stream {self.stream_id!r} has no destinations configured"
            )
        return self.destinations[0]


@dataclass(frozen=True)
class BatchingConfig:
    """Batch sizing for the engine's producer/consumer loop."""

    batch_size: int = 1000
    max_concurrent_batches: int = 3

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {self.batch_size}")
        if self.max_concurrent_batches <= 0:
            raise ValueError(
                f"max_concurrent_batches must be positive, got {self.max_concurrent_batches}"
            )


_VALID_ERROR_STRATEGIES = frozenset({"fail", "dlq"})


@dataclass(frozen=True)
class ErrorHandlingConfig:
    """Fault-handling policy for a pipeline run."""

    strategy: str = "fail"
    max_retries: int = 3
    retry_delay_seconds: int = 5

    def __post_init__(self) -> None:
        if self.strategy not in _VALID_ERROR_STRATEGIES:
            raise ValueError(
                f"Unknown error strategy {self.strategy!r}; "
                f"expected one of {sorted(_VALID_ERROR_STRATEGIES)}"
            )
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be non-negative, got {self.max_retries}")


@dataclass(frozen=True)
class RuntimeConfig:
    """Pipeline runtime tuning parameters.

    ``batching`` / ``error_handling`` are typed sub-configs (closed, known
    key sets) so consumers read attributes instead of doing ``dict.get(...)``
    with per-call-site defaults -- the defaults live once, in the parser.
    """

    batching: BatchingConfig = field(default_factory=BatchingConfig)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)
    buffer_size: int = 5000


@dataclass(frozen=True)
class ResolvedPipeline:
    """Complete resolved pipeline, ready for execution by StreamingEngine."""

    pipeline_id: str
    display_name: Optional[str]
    description: Optional[str]
    status: str
    tags: List[str]
    source_connection_id: str
    destination_connection_ids: List[str]
    streams: List[ResolvedStream]
    runtime: RuntimeConfig
    schedule: Dict[str, Any]
    engine_config: Dict[str, Any]
    connections: Dict[str, ConnectionRuntime]

    def __post_init__(self) -> None:
        if not self.pipeline_id:
            raise ValueError("ResolvedPipeline.pipeline_id cannot be empty")
