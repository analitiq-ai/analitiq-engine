"""Connection and connector file loading utilities.

Loads connection configs from ``connections/{connection_id}/connection.json`` and
connector definitions from ``connectors/{connector_id}/definition/connector.json``.

Connection JSON references its connector by ``connector_id``. The canonical
connection identity is the directory name (= ``connection_id``) under
``connections/``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

from src.config.exceptions import ConnectionConfigError, ConnectorNotFoundError

logger = logging.getLogger(__name__)


def load_connection_file(path: Path) -> Dict[str, Any]:
    """Read and JSON-parse a connection.json. Raises with file context on bad JSON."""
    if not path.is_file():
        raise FileNotFoundError(f"connection.json not found: {path}")
    try:
        with path.open() as fh:
            return json.load(fh)
    except json.JSONDecodeError as err:
        raise ValueError(f"Invalid JSON in {path}: {err}") from err


def load_connection(connection_id: str, connections_dir: Path) -> Dict[str, Any]:
    """Load a connection configuration by ``connection_id`` (= directory name).

    Args:
        connection_id: Connection directory name under ``connections/``.
        connections_dir: Path to the ``connections/`` root.

    Returns:
        Parsed connection configuration dict.

    Raises:
        ConnectionConfigError: If the directory or file is missing or malformed.
    """
    conn_dir = connections_dir / connection_id
    if not conn_dir.is_dir():
        raise ConnectionConfigError(
            connection_id, detail=f"Connection directory not found: {conn_dir}"
        )

    conn_file = conn_dir / "connection.json"
    if not conn_file.is_file():
        raise ConnectionConfigError(
            connection_id, detail=f"connection.json not found in {conn_dir}"
        )

    try:
        config = load_connection_file(conn_file)
    except (ValueError, json.JSONDecodeError) as err:
        raise ConnectionConfigError(connection_id, detail=str(err)) from err

    logger.info("Loaded connection %r from %s", connection_id, conn_file)
    return config


def load_connector_definition(
    connector_id: str, connectors_dir: Path
) -> Dict[str, Any]:
    """Load a connector definition by ``connector_id``.

    Connector directories may be named either ``{connector_id}`` or
    ``connector-{connector_id}``; the lookup tries both.
    """
    candidates = [
        connectors_dir / connector_id / "definition" / "connector.json",
        connectors_dir / f"connector-{connector_id}" / "definition" / "connector.json",
    ]
    for candidate in candidates:
        if candidate.is_file():
            try:
                with candidate.open() as fh:
                    config = json.load(fh)
            except json.JSONDecodeError as err:
                raise ConnectorNotFoundError(
                    connector_id,
                    detail=f"Invalid JSON in {candidate}: {err}",
                ) from err
            logger.info("Loaded connector %r from %s", connector_id, candidate)
            return config

    raise ConnectorNotFoundError(
        connector_id,
        detail=(
            f"Connector definition not found at {candidates[0]} "
            f"or {candidates[1]}"
        ),
    )
