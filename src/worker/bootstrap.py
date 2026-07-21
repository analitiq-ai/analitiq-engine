"""Worker bootstrap: the one-shot launch payload read from stdin.

The bootstrap is the ONLY input a worker receives besides its UDS channel.
It is self-contained — resolved connection payload (secrets already in
values), type-map rules, endpoint documents — so the worker needs no config
volume and no secret store. It crosses the local parent->child pipe once,
at launch, and must never be logged.

Shape::

    {
      "role": "source" | "destination",
      "kind": "<connector kind: database, api, ... >",
      "connector_id": "postgres",
      "uds_path": "/.../worker.sock",
      "connection": {<ConnectionRuntime.resolve_spec() payload>},
      "type_maps": {
        "connector":  {"rules": [...], "write_rules": [...] | null} | null,
        "connection": {"rules": [...], "write_rules": [...] | null} | null
      },
      # destination role:
      "endpoint_refs":   {stream_id: {...}},
      "stream_endpoints": {stream_id: {...}},
      "stream_conflict_keys": {stream_id: ["col", ...]},
      # source role:
      "source_config": {...}      # endpoint_document, stream_source, ...
    }
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Any

from cdk.connection_runtime import ConnectionRuntime
from cdk.type_map import TypeMapper
from cdk.type_map.loader import build_type_mapper

_ROLES = ("source", "destination")


@dataclass(frozen=True)
class WorkerBootstrap:
    role: str
    kind: str
    connector_id: str
    uds_path: str
    connection_payload: dict[str, Any]
    connector_type_mapper: TypeMapper | None
    connection_type_mapper: TypeMapper | None
    endpoint_refs: dict[str, Any]
    stream_endpoints: dict[str, Any]
    stream_conflict_keys: dict[str, list[str]]
    source_config: dict[str, Any]

    def build_runtime(self) -> ConnectionRuntime:
        """Rebuild the connection runtime from the resolved payload."""
        return ConnectionRuntime.from_resolved_payload(
            self.connection_payload,
            connector_type_mapper=self.connector_type_mapper,
            connection_type_mapper=self.connection_type_mapper,
        )


def _mapper_from(block: dict[str, Any] | None, label: str) -> TypeMapper | None:
    if not block:
        return None
    return build_type_mapper(label, block.get("rules") or [], block.get("write_rules"))


def parse_bootstrap(raw: dict[str, Any]) -> WorkerBootstrap:
    """Validate and parse the bootstrap dict into a typed object."""
    role = raw.get("role")
    if role not in _ROLES:
        raise ValueError(f"bootstrap.role must be one of {_ROLES}, got {role!r}")
    for key in ("kind", "connector_id", "uds_path", "connection"):
        if not raw.get(key):
            raise ValueError(f"bootstrap.{key} is required")

    type_maps = raw.get("type_maps") or {}
    connector_id = raw["connector_id"]
    return WorkerBootstrap(
        role=role,
        kind=raw["kind"],
        connector_id=connector_id,
        uds_path=raw["uds_path"],
        connection_payload=dict(raw["connection"]),
        connector_type_mapper=_mapper_from(type_maps.get("connector"), connector_id),
        connection_type_mapper=_mapper_from(
            type_maps.get("connection"),
            f"connection:{raw['connection'].get('connection_id', '?')}",
        ),
        endpoint_refs=dict(raw.get("endpoint_refs") or {}),
        stream_endpoints=dict(raw.get("stream_endpoints") or {}),
        stream_conflict_keys=dict(raw.get("stream_conflict_keys") or {}),
        source_config=dict(raw.get("source_config") or {}),
    )


def read_bootstrap_from_stdin() -> WorkerBootstrap:
    """Read the one-shot JSON bootstrap from stdin (closed by the parent).

    The payload contains resolved credentials: it is parsed and never
    echoed. Errors raised here report shape problems only, not values.
    """
    raw_text = sys.stdin.read()
    if not raw_text.strip():
        raise ValueError("worker started without a bootstrap on stdin")
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as err:
        # Deliberately do not include the document in the error.
        raise ValueError(f"worker bootstrap is not valid JSON: {err.msg}") from err
    if not isinstance(raw, dict):
        raise ValueError("worker bootstrap must be a JSON object")
    return parse_bootstrap(raw)
