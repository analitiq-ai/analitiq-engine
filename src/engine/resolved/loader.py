"""Disk loader for on-disk pipeline / connection / connector artifacts.

This is the one place that walks `pipelines/{id}/`, `connections/{id}/`,
`connectors/{slug}/`. It reads raw JSON, builds the resolved-runtime
object graph via the factories in `build.py`, and returns a
`ResolvedPipeline`.
"""

from __future__ import annotations

import json
from pathlib import Path

from .build import (
    build_connection,
    build_connector,
    build_resolved_pipeline,
)
from .errors import ResolveError
from .types import ResolvedConnection, ResolvedConnector, ResolvedPipeline


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise ResolveError(str(path), f"$:line {e.lineno}", f"invalid JSON: {e.msg}") from e


def _load_manifest(root: Path) -> list[dict]:
    manifest = _read_json(root / "pipelines" / "manifest.json")
    pipelines = manifest.get("pipelines") or []
    if not isinstance(pipelines, list):
        raise ResolveError(str(root / "pipelines" / "manifest.json"), "$.pipelines", "must be an array")
    return pipelines


def load_connector(root: Path, connector_slug: str) -> tuple[ResolvedConnector, Path]:
    path = root / "connectors" / connector_slug / "definition" / "connector.json"
    if not path.is_file():
        raise ResolveError(str(path), "$", f"connector definition not found at {path}")
    spec = _read_json(path)
    return build_connector(spec, source=str(path)), path


def load_connection(
    root: Path, connection_id: str, connectors: dict[str, ResolvedConnector]
) -> ResolvedConnection:
    path = root / "connections" / connection_id / "connection.json"
    if not path.is_file():
        raise ResolveError(str(path), "$", f"connection not found at {path}")
    spec = _read_json(path)
    connector_id = spec.get("connector_id")
    if not connector_id:
        raise ResolveError(str(path), "$.connector_id", "missing connector_id")
    connector = connectors.get(connector_id)
    if connector is None:
        raise ResolveError(str(path), "$.connector_id", f"connector {connector_id!r} not loaded")
    return build_connection(spec, source=str(path), connector=connector)


def _load_endpoints_for_connector(
    root: Path, connector_slug: str
) -> dict[tuple[str, str, str], dict]:
    """Return endpoints keyed by (scope='connector', connector_slug, endpoint_id).

    Note: when streams reference connector endpoints, the `connection_id` on
    the ref equals the source connection id (NOT the connector slug). We
    re-key on connection level in the caller after loading.
    """

    out: dict[tuple[str, str, str], dict] = {}
    endpoints_dir = root / "connectors" / connector_slug / "definition" / "endpoints"
    if not endpoints_dir.is_dir():
        return out
    for f in sorted(endpoints_dir.glob("*.json")):
        spec = _read_json(f)
        spec["__source__"] = str(f)
        endpoint_id = spec.get("endpoint_id") or f.stem
        out[("connector", connector_slug, endpoint_id)] = spec
    return out


def _load_endpoints_for_connection(
    root: Path, connection_id: str
) -> dict[tuple[str, str, str], dict]:
    out: dict[tuple[str, str, str], dict] = {}
    endpoints_dir = root / "connections" / connection_id / "definition" / "endpoints"
    if not endpoints_dir.is_dir():
        return out
    for f in sorted(endpoints_dir.glob("*.json")):
        spec = _read_json(f)
        spec["__source__"] = str(f)
        endpoint_id = spec.get("endpoint_id") or f.stem
        out[("connection", connection_id, endpoint_id)] = spec
    return out


def load_resolved_pipeline(root: Path, pipeline_id: str) -> ResolvedPipeline:
    """Load and resolve one pipeline from disk.

    Walks the manifest, loads the pipeline + its streams, then loads
    every connection referenced by the pipeline and every endpoint
    referenced by any stream.
    """

    manifest = _load_manifest(root)
    entry = next((e for e in manifest if e.get("pipeline_id") == pipeline_id), None)
    if entry is None:
        raise ResolveError(
            str(root / "pipelines" / "manifest.json"),
            "$.pipelines",
            f"pipeline {pipeline_id!r} not in manifest",
        )
    pipeline_path = root / "pipelines" / (entry.get("path") or f"{pipeline_id}/pipeline.json")
    pipeline_spec = _read_json(pipeline_path)

    # Streams
    streams_dir = pipeline_path.parent / "streams"
    stream_ids = pipeline_spec.get("streams") or []
    stream_specs: list[tuple[dict, str]] = []
    for sid in stream_ids:
        s_path = streams_dir / f"{sid}.json"
        if not s_path.is_file():
            raise ResolveError(str(s_path), "$", f"stream file missing for {sid!r}")
        stream_specs.append((_read_json(s_path), str(s_path)))

    # Connections (source + all destinations)
    conns_section = pipeline_spec.get("connections") or {}
    needed_connection_ids: set[str] = set()
    src = conns_section.get("source")
    if src:
        needed_connection_ids.add(src)
    for d in conns_section.get("destinations") or []:
        needed_connection_ids.add(d)

    # Cross-check: any stream endpoint_ref must point at one of the
    # pipeline's connections (source for read, any destination for write).
    # We don't enforce here — the build step will fail clearly if not.

    # Load each connection's connector first.
    connectors: dict[str, ResolvedConnector] = {}
    raw_connection_specs: dict[str, dict] = {}
    for cid in needed_connection_ids:
        c_path = root / "connections" / cid / "connection.json"
        if not c_path.is_file():
            raise ResolveError(str(c_path), "$", f"connection {cid!r} not found")
        c_spec = _read_json(c_path)
        raw_connection_specs[cid] = c_spec
        connector_slug = c_spec.get("connector_id")
        if not connector_slug:
            raise ResolveError(str(c_path), "$.connector_id", "missing")
        if connector_slug not in connectors:
            connector, _ = load_connector(root, connector_slug)
            connectors[connector_slug] = connector

    connections: dict[str, ResolvedConnection] = {}
    for cid, c_spec in raw_connection_specs.items():
        connector = connectors[c_spec["connector_id"]]
        connections[cid] = build_connection(c_spec, source=str(root / "connections" / cid / "connection.json"), connector=connector)

    # Endpoints — keyed by (scope, connection_id, endpoint_id) where
    # connection_id is the *connection* not the connector slug.
    endpoints: dict[tuple[str, str, str], dict] = {}
    for cid, c_spec in raw_connection_specs.items():
        connector_slug = c_spec["connector_id"]
        # connector-scoped: every endpoint in the connector dir maps to
        # this connection_id under scope='connector'.
        for (_, _, eid), e_spec in _load_endpoints_for_connector(root, connector_slug).items():
            endpoints[("connector", cid, eid)] = e_spec
        for key, e_spec in _load_endpoints_for_connection(root, cid).items():
            endpoints[key] = e_spec

    return build_resolved_pipeline(
        pipeline_spec,
        source=str(pipeline_path),
        stream_specs=stream_specs,
        connections=connections,
        endpoints=endpoints,
    )


def discover_pipeline_ids(root: Path) -> list[str]:
    """Return all `active` pipeline ids from the manifest."""
    return [
        e["pipeline_id"]
        for e in _load_manifest(root)
        if e.get("status") == "active" and e.get("pipeline_id")
    ]
