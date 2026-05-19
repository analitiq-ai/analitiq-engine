"""Pipeline configuration preparation.

Loads a `ResolvedPipeline` from disk via the resolved-runtime layer and
builds the matching `ConnectionRuntime` objects (the runtime managers
that own secrets, transport materialization, and lifecycle).

This is the only place that bridges raw JSON on disk to live engine
state. Returns:

    (resolved: ResolvedPipeline,
     runtimes: Dict[connection_id, ConnectionRuntime],
     raw_endpoints: Dict[(scope, connection_id, endpoint_id), dict])

`raw_endpoints` is exposed because the destination schema message and
some connector code still consume the raw endpoint JSON; Step 6 of the
schema-alignment plan removes that surface.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Tuple

from src.engine.resolved import (
    ResolvedPipeline,
    discover_pipeline_ids,
    load_resolved_pipeline,
)
from src.engine.type_map import (
    TypeMapper,
    load_connection_type_map,
    load_type_map,
)
from src.secrets import LocalFileSecretsResolver
from src.shared.connection_runtime import ConnectionRuntime


logger = logging.getLogger(__name__)


EndpointKey = Tuple[str, str, str]


class PipelineConfigPrep:
    """Prepare a pipeline for execution.

    Reads PIPELINE_ID from the environment, discovers the project root
    by locating `pipelines/manifest.json`, and produces the resolved
    pipeline plus its runtime managers.
    """

    def __init__(self) -> None:
        self.pipeline_id = os.environ.get("PIPELINE_ID", "")
        if not self.pipeline_id:
            raise RuntimeError("PIPELINE_ID environment variable is required")

        self.root = self._discover_root()
        logger.info(
            "PipelineConfigPrep root=%s pipeline_id=%s",
            self.root,
            self.pipeline_id,
        )

    @staticmethod
    def _discover_root() -> Path:
        current = Path.cwd()
        for _ in range(10):
            if (current / "pipelines" / "manifest.json").is_file():
                return current
            if current.parent == current:
                break
            current = current.parent
        raise RuntimeError(
            "Could not find pipelines/manifest.json in current or parent directories. "
            "Ensure you are running from the project root."
        )

    def create_config(
        self,
    ) -> Tuple[ResolvedPipeline, Dict[str, ConnectionRuntime], Dict[EndpointKey, Dict[str, Any]]]:
        """Load the pipeline and build its connection runtimes."""

        resolved = load_resolved_pipeline(self.root, self.pipeline_id)

        runtimes = self._build_runtimes(resolved)
        raw_endpoints = self._load_raw_endpoints(resolved)

        logger.info(
            "Loaded pipeline %s (%d streams, %d connections, %d endpoints)",
            resolved.pipeline_id,
            len(resolved.streams),
            len(runtimes),
            len(raw_endpoints),
        )
        return resolved, runtimes, raw_endpoints

    # ------------------------------------------------------------------
    # Runtime construction
    # ------------------------------------------------------------------

    def _build_runtimes(self, resolved: ResolvedPipeline) -> Dict[str, ConnectionRuntime]:
        runtimes: Dict[str, ConnectionRuntime] = {}

        connection_ids = {resolved.source_connection_id, *resolved.destination_connection_ids}

        for cid in connection_ids:
            runtimes[cid] = self._build_runtime(cid)
        return runtimes

    def _build_runtime(self, connection_id: str) -> ConnectionRuntime:
        conn_path = self.root / "connections" / connection_id / "connection.json"
        connector_path = self._connector_path_for_connection(conn_path)

        raw_connection = _read_json(conn_path)
        raw_connector = _read_json(connector_path)

        connector_slug = raw_connection["connector_id"]
        connector_type = raw_connector["kind"]

        resolver = LocalFileSecretsResolver(
            self.root / "connections" / connection_id / ".secrets"
        )

        connector_type_mapper: TypeMapper = load_type_map(
            self.root / "connectors", connector_slug
        )
        connection_type_mapper = load_connection_type_map(
            self.root / "connections", connection_id
        )

        runtime = ConnectionRuntime(
            raw_config=raw_connection,
            connection_id=connection_id,
            connector_type=connector_type,
            resolver=resolver,
            connector_definition=raw_connector,
            connector_type_mapper=connector_type_mapper,
            connection_type_mapper=connection_type_mapper,
        )
        logger.info("Built ConnectionRuntime for %s (kind=%s)", connection_id, connector_type)
        return runtime

    def _connector_path_for_connection(self, conn_path: Path) -> Path:
        raw = _read_json(conn_path)
        slug = raw.get("connector_id")
        if not slug:
            raise ValueError(f"{conn_path}: missing connector_id")
        return self.root / "connectors" / slug / "definition" / "connector.json"

    # ------------------------------------------------------------------
    # Raw endpoint passthrough
    # ------------------------------------------------------------------

    def _load_raw_endpoints(
        self, resolved: ResolvedPipeline
    ) -> Dict[EndpointKey, Dict[str, Any]]:
        """Load raw endpoint JSONs for every endpoint referenced by any stream."""

        out: Dict[EndpointKey, Dict[str, Any]] = {}
        seen: set[EndpointKey] = set()

        for stream in resolved.streams:
            self._collect_endpoint(stream.source.endpoint_ref, out, seen, stream.source.connection.connector.connector_id)
            for dest in stream.destinations:
                self._collect_endpoint(dest.endpoint_ref, out, seen, dest.connection.connector.connector_id)
        return out

    def _collect_endpoint(
        self,
        ref,
        out: Dict[EndpointKey, Dict[str, Any]],
        seen: set,
        connector_slug: str,
    ) -> None:
        key: EndpointKey = (ref.scope, ref.connection_id, ref.endpoint_id)
        if key in seen:
            return
        seen.add(key)
        if ref.scope == "connector":
            path = (
                self.root
                / "connectors"
                / connector_slug
                / "definition"
                / "endpoints"
                / f"{ref.endpoint_id}.json"
            )
        elif ref.scope == "connection":
            path = (
                self.root
                / "connections"
                / ref.connection_id
                / "definition"
                / "endpoints"
                / f"{ref.endpoint_id}.json"
            )
        else:
            raise ValueError(f"Unknown endpoint scope: {ref.scope!r}")
        if not path.is_file():
            raise FileNotFoundError(f"Endpoint not found: {path}")
        out[key] = _read_json(path)


def _read_json(path: Path) -> Dict[str, Any]:
    with open(path) as f:
        return json.load(f)


__all__ = ["PipelineConfigPrep", "discover_pipeline_ids"]
