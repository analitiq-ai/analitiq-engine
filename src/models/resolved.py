"""Typed resolved-runtime boundary between PipelineConfigPrep and the engine.

These dataclasses own all raw-JSON access.  ``PipelineConfigPrep`` builds
them once; ``StreamingEngine`` consumes typed attributes throughout (via the
translation helpers in ``src.runner``), and only at the ``WorkerReadable``
boundary are the contract documents serialised back to JSON-safe dicts.

``ConnectionRuntime`` and the resolved endpoint document live as explicit
typed fields rather than ``_runtime`` / ``_endpoint`` magic dict keys.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from cdk.connection_runtime import ConnectionRuntime
from src.models.stream import EndpointRef


# Mirrors the published stream contract's replication-method enum.
_VALID_REPLICATION_METHODS = frozenset({"full_refresh", "incremental"})


@dataclass(frozen=True)
class ReplicationConfig:
    """Source replication policy, mirroring the published stream contract.

    ``safety_window_seconds`` is intentionally not carried: the engine never
    reads it (it travels to the connector inside the ``stream_source`` wire
    document), so there is nothing to type here.
    """

    method: str
    cursor_field: Optional[str] = None
    tie_breaker_fields: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if self.method not in _VALID_REPLICATION_METHODS:
            raise ValueError(
                f"Unknown replication method {self.method!r}; "
                f"expected one of {sorted(_VALID_REPLICATION_METHODS)}"
            )


@dataclass
class ResolvedSource:
    """Source side of a resolved stream — runtime object and contract docs.

    ``replication`` and ``primary_keys`` are the engine-internal typed view of
    the source-read policy (parsed from ``stream_source``); the raw
    ``stream_source`` document still travels to the connector unchanged.
    """

    endpoint_ref: EndpointRef
    connection_ref: str
    runtime: ConnectionRuntime
    endpoint_document: Dict[str, Any]
    stream_source: Dict[str, Any]
    replication: Optional[ReplicationConfig] = None
    primary_keys: List[str] = field(default_factory=list)

    def to_source_config(self) -> Dict[str, Any]:
        """JSON-safe source config dict for the worker bootstrap.

        Returns only the contract documents; the ``ConnectionRuntime``
        object travels as a separate argument to ``build_bootstrap`` and is
        never embedded in the JSON payload.
        """
        return {
            "endpoint_ref": self.endpoint_ref.to_dict(),
            "connection_ref": self.connection_ref,
            "endpoint_document": self.endpoint_document,
            "stream_source": self.stream_source,
        }


@dataclass
class ResolvedDestination:
    """Destination side of a resolved stream — runtime object and contract docs."""

    endpoint_ref: EndpointRef
    connection_ref: str
    runtime: ConnectionRuntime
    endpoint_document: Dict[str, Any]
    write: Dict[str, Any]


@dataclass
class ResolvedStream:
    """Fully resolved stream — typed source/destinations and metadata."""

    stream_id: str
    stream_version: int
    pipeline_id: Optional[str]
    display_name: Optional[str]
    description: Optional[str]
    status: str
    is_enabled: bool
    tags: List[str]
    source: ResolvedSource
    destinations: List[ResolvedDestination]
    mapping: Dict[str, Any]

    def __post_init__(self) -> None:
        if not self.stream_id:
            raise ValueError("ResolvedStream.stream_id cannot be empty")

    def primary_destination(self) -> ResolvedDestination:
        if not self.destinations:
            raise ValueError(
                f"Stream {self.stream_id!r} has no destinations"
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
                "max_concurrent_batches must be positive, got "
                f"{self.max_concurrent_batches}"
            )


# The published pipeline contract's error-handling strategy enum. Kept in sync
# with the contract rather than narrowed to what the engine branches on today,
# so a contract-valid pipeline is never rejected at this boundary.
_VALID_ERROR_STRATEGIES = frozenset({"fail", "dlq", "skip"})


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
            raise ValueError(
                f"max_retries must be non-negative, got {self.max_retries}"
            )
        if self.retry_delay_seconds < 0:
            raise ValueError(
                "retry_delay_seconds must be non-negative, got "
                f"{self.retry_delay_seconds}"
            )


@dataclass(frozen=True)
class RuntimeConfig:
    """Pipeline runtime tuning.

    ``batching`` / ``error_handling`` are typed sub-configs (closed, known
    key sets) so consumers read attributes instead of ``dict.get(...)`` with
    per-call-site defaults -- the defaults live once, in the parser.
    """

    batching: BatchingConfig = field(default_factory=BatchingConfig)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)
    buffer_size: int = 5000

    def __post_init__(self) -> None:
        if self.buffer_size <= 0:
            raise ValueError(f"buffer_size must be positive, got {self.buffer_size}")

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to the JSON-safe shape carried in the worker-boundary config.

        The engine's stages still read ``runtime`` as a dict (threading the
        typed model through them is tracked separately); this is the single
        conversion point back to that contract shape.
        """
        return {
            "batching": {
                "batch_size": self.batching.batch_size,
                "max_concurrent_batches": self.batching.max_concurrent_batches,
            },
            "error_handling": {
                "strategy": self.error_handling.strategy,
                "max_retries": self.error_handling.max_retries,
                "retry_delay_seconds": self.error_handling.retry_delay_seconds,
            },
            "buffer_size": self.buffer_size,
        }


@dataclass(frozen=True)
class PipelineConnections:
    """Connection-id wiring: source connection and destination connections."""

    source: str
    destinations: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("PipelineConnections.source cannot be empty")


@dataclass
class ResolvedPipeline:
    """Resolved pipeline-level configuration.

    ``schedule`` and ``engine_config`` stay raw dicts on purpose: they are
    opaque control-plane passthroughs (scheduler hint, vCPU/memory sizing)
    that the engine never reads, so there is no structure to type.
    """

    pipeline_id: str
    name: str
    display_name: Optional[str]
    description: Optional[str]
    status: str
    connections: PipelineConnections
    tags: List[str] = field(default_factory=list)
    schedule: Dict[str, Any] = field(default_factory=dict)
    engine_config: Dict[str, Any] = field(default_factory=dict)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)

    def __post_init__(self) -> None:
        if not self.pipeline_id:
            raise ValueError("ResolvedPipeline.pipeline_id cannot be empty")
