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


@dataclass
class ResolvedSource:
    """Source side of a resolved stream — runtime object and contract docs."""

    endpoint_ref: EndpointRef
    connection_ref: str
    runtime: ConnectionRuntime
    endpoint_document: Dict[str, Any]
    stream_source: Dict[str, Any]

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

    def primary_destination(self) -> ResolvedDestination:
        if not self.destinations:
            raise ValueError(
                f"Stream {self.stream_id!r} has no destinations"
            )
        return self.destinations[0]


@dataclass
class ResolvedPipeline:
    """Resolved pipeline-level configuration."""

    pipeline_id: str
    name: str
    display_name: Optional[str]
    description: Optional[str]
    status: str
    tags: List[str] = field(default_factory=list)
    connections: Dict[str, Any] = field(default_factory=dict)
    schedule: Dict[str, Any] = field(default_factory=dict)
    engine_config: Dict[str, Any] = field(default_factory=dict)
    runtime: Dict[str, Any] = field(default_factory=dict)
