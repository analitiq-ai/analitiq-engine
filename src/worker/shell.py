"""Trusted-shell helpers: assemble a worker's launch bootstrap.

The shell (engine source side or destination service) owns the secret
store and the config volume. It resolves the connection into a JSON-safe
payload (``ConnectionRuntime.resolve_spec``), reads the raw type-map rule
arrays from the connector/connection definitions, and packs everything a
worker may use into one self-contained bootstrap dict — the worker itself
never touches the filesystem config or the secret store.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from cdk.connection_runtime import ConnectionRuntime
from cdk.type_map.loader import TYPE_MAP_FILENAME, WRITE_TYPE_MAP_FILENAME


def _read_rules(definition_dir: Path) -> Optional[Dict[str, Any]]:
    """Read raw type-map (+ optional write map) arrays from a definition dir."""
    path = definition_dir / TYPE_MAP_FILENAME
    if not path.is_file():
        return None
    rules = json.loads(path.read_text())
    write_path = definition_dir / WRITE_TYPE_MAP_FILENAME
    write_rules = (
        json.loads(write_path.read_text()) if write_path.is_file() else None
    )
    return {"rules": rules, "write_rules": write_rules}


def read_type_map_payloads(
    connectors_dir: Path,
    connector_id: str,
    connections_dir: Path,
    connection_id: str,
) -> Dict[str, Any]:
    """Raw type-map payloads for the bootstrap (connector + connection scope).

    Mirrors the lookup the file loaders perform; the worker rebuilds the
    mappers from these arrays with the same validation.
    """
    connector_block = _read_rules(connectors_dir / connector_id / "definition")
    if connector_block is None:
        # The loaders also honor the alternate ``connector-{id}`` layout.
        connector_block = _read_rules(
            connectors_dir / f"connector-{connector_id}" / "definition"
        )
    connection_block = _read_rules(connections_dir / connection_id / "definition")
    return {"connector": connector_block, "connection": connection_block}


async def build_bootstrap(
    runtime: ConnectionRuntime,
    *,
    role: str,
    connectors_dir: Path,
    connections_dir: Path,
    endpoint_refs: Optional[Dict[str, Any]] = None,
    stream_endpoints: Optional[Dict[str, Any]] = None,
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Resolve *runtime* and assemble the worker bootstrap for *role*.

    The returned dict carries resolved credentials in its values: hand it
    to ``spawn_worker`` (which writes it once to the child's stdin) and
    never log it.
    """
    connection_payload = await runtime.resolve_spec()
    return {
        "role": role,
        "kind": runtime.connector_type,
        "connector_id": runtime.connector_id,
        "connection": connection_payload,
        "type_maps": read_type_map_payloads(
            connectors_dir,
            runtime.connector_id,
            connections_dir,
            runtime.connection_id,
        ),
        "endpoint_refs": endpoint_refs or {},
        "stream_endpoints": stream_endpoints or {},
        "source_config": source_config or {},
    }
