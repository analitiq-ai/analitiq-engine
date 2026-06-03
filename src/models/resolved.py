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
class RuntimeConfig:
    """Pipeline runtime tuning parameters."""

    batching: Dict[str, Any] = field(default_factory=dict)
    error_handling: Dict[str, Any] = field(default_factory=dict)
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
