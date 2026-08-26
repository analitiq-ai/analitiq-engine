"""Trusted-shell helpers: assemble a worker's launch bootstrap.

The shell (engine source side or destination service) owns the secret
store and the config volume. It resolves the connection into a JSON-safe
payload (``ConnectionRuntime.resolve_spec``), reads the raw type-map rule
arrays from the connector/connection definitions, and packs everything a
worker may use into one self-contained bootstrap dict — the worker itself
never touches the filesystem config or the secret store.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from cdk.api.request import endpoint_transport_refs
from cdk.connection_runtime import ConnectionRuntime
from cdk.type_map.loader import connector_definition_dir, read_raw_type_maps


def read_type_map_payloads(
    connectors_dir: Path,
    connector_id: str,
    connections_dir: Path,
    connection_id: str,
) -> dict[str, Any]:
    """Raw type-map payloads for the bootstrap (connector + connection scope).

    Same directory lookup and JSON validation as the file loaders (one
    parser, one error surface); the worker rebuilds the mappers from these
    arrays with the same rule validation.
    """
    connector_block = read_raw_type_maps(
        connector_definition_dir(connectors_dir, connector_id),
        f"connector {connector_id!r}",
    )
    connection_block = read_raw_type_maps(
        connections_dir / connection_id / "definition",
        f"connection {connection_id!r}",
    )
    return {"connector": connector_block, "connection": connection_block}


async def build_bootstrap(
    runtime: ConnectionRuntime,
    *,
    role: str,
    connectors_dir: Path,
    connections_dir: Path,
    endpoint_refs: dict[str, Any] | None = None,
    stream_endpoints: dict[str, Any] | None = None,
    stream_conflict_keys: dict[str, list[str]] | None = None,
    stream_write_modes: dict[str, str] | None = None,
    source_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve *runtime* and assemble the worker bootstrap for *role*.

    The returned dict carries resolved credentials in its values: hand it
    to ``spawn_worker`` (which writes it once to the child's stdin) and
    never log it.

    The endpoint documents are read for one thing before they are packed:
    which transports this run's operations dispatch through. A worker
    holds no secret store, so a transport whose spec does not travel in
    this payload can never be opened there -- and the shell is the last
    side that can resolve one.

    Scoped to *role* and, for a destination, to the write mode each
    stream selected: a resolved spec carries the credentials resolved into
    it and its origin widens what the worker may reach. A worker handed an
    operation it never executes is given secrets it never sends, and fails
    here if that operation's transport needs a credential this run does
    not carry.
    """
    transport_refs: set[str] = set()
    for stream_id, document in (stream_endpoints or {}).items():
        mode = (stream_write_modes or {}).get(stream_id)
        transport_refs |= endpoint_transport_refs(
            document, role=role, write_modes=[mode] if mode else None
        )
    if source_config:
        transport_refs |= endpoint_transport_refs(
            source_config.get("endpoint_document"), role=role
        )
    connection_payload = await runtime.resolve_spec(transport_refs=transport_refs)
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
        "stream_conflict_keys": stream_conflict_keys or {},
        "source_config": source_config or {},
    }
