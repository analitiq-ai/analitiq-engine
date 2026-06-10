"""Trusted-shell helpers: assemble a worker's launch bootstrap.

The shell (engine source side or destination service) owns the secret
store and the config volume. It resolves the connection into a JSON-safe
payload (``ConnectionRuntime.resolve_spec``), reads the raw type-map rule
arrays from the connector/connection definitions, and packs everything a
worker may use into one self-contained bootstrap dict — the worker itself
never touches the filesystem config or the secret store.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

from cdk.connection_runtime import ConnectionRuntime
from cdk.type_map.loader import connector_definition_dir, read_raw_type_maps

# A destination SQL statement is cancelled this many seconds before the
# engine's gRPC ack timeout, so the database returns the cancelled statement
# instead of the engine abandoning the handshake with a bare "ACK timeout"
# (issue #231). The shell reads GRPC_TIMEOUT_SECONDS - the engine's ack budget
# whenever that env var is set (the common case; both run from the same image).
# The engine otherwise falls back to the pipeline's grpc.timeout_seconds, which
# the destination shell does not see; set the env var to keep the two in step.
_STATEMENT_TIMEOUT_ACK_MARGIN_SECONDS = 5
_MIN_DESTINATION_STATEMENT_TIMEOUT_SECONDS = 5


def _destination_statement_timeout_seconds() -> float:
    """Per-statement budget for a destination worker, kept below the engine's
    gRPC ack timeout so a blocked DDL/write is cancelled before the engine
    gives up waiting for the ack."""
    ack_timeout = int(os.getenv("GRPC_TIMEOUT_SECONDS", "30"))
    return float(
        max(
            ack_timeout - _STATEMENT_TIMEOUT_ACK_MARGIN_SECONDS,
            _MIN_DESTINATION_STATEMENT_TIMEOUT_SECONDS,
        )
    )


def read_type_map_payloads(
    connectors_dir: Path,
    connector_id: str,
    connections_dir: Path,
    connection_id: str,
) -> Dict[str, Any]:
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
        # Only the destination worker bounds statements; a source read is not
        # gated by the destination's ack budget.
        "statement_timeout_seconds": (
            _destination_statement_timeout_seconds()
            if role == "destination"
            else None
        ),
    }
