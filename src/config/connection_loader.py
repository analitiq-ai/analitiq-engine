"""Connection and connector file loading utilities.

Loads connection configs from ``connections/{alias}/connection.json`` and
connector definitions from ``connectors/{connector_alias}/definition/connector.json``.

Connection JSON references its connector by ``connector_alias``. The
canonical connection identity is the directory name (= alias) under
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


def load_connection(alias: str, connections_dir: Path) -> Dict[str, Any]:
    """Load a connection configuration by directory alias.

    Args:
        alias: Connection directory name under ``connections/``.
        connections_dir: Path to the ``connections/`` root.

    Returns:
        Parsed connection configuration dict.

    Raises:
        ConnectionConfigError: If the directory or file is missing or malformed.
    """
    conn_dir = connections_dir / alias
    if not conn_dir.is_dir():
        raise ConnectionConfigError(
            alias, detail=f"Connection directory not found: {conn_dir}"
        )

    conn_file = conn_dir / "connection.json"
    if not conn_file.is_file():
        raise ConnectionConfigError(
            alias, detail=f"connection.json not found in {conn_dir}"
        )

    try:
        config = load_connection_file(conn_file)
    except (ValueError, json.JSONDecodeError) as err:
        raise ConnectionConfigError(alias, detail=str(err)) from err

    logger.info("Loaded connection %r from %s", alias, conn_file)
    return config


def load_connector_definition(
    connector_alias: str, connectors_dir: Path
) -> Dict[str, Any]:
    """Load a connector definition by ``connector_alias``.

    Connector directories may be named either ``{alias}`` or
    ``connector-{alias}`` (legacy-tolerant lookup).
    """
    candidates = [
        connectors_dir / connector_alias / "definition" / "connector.json",
        connectors_dir / f"connector-{connector_alias}" / "definition" / "connector.json",
    ]
    for candidate in candidates:
        if candidate.is_file():
            try:
                with candidate.open() as fh:
                    config = json.load(fh)
            except json.JSONDecodeError as err:
                raise ConnectorNotFoundError(
                    connector_alias,
                    detail=f"Invalid JSON in {candidate}: {err}",
                ) from err
            logger.info("Loaded connector %r from %s", connector_alias, candidate)
            return config

    raise ConnectorNotFoundError(
        connector_alias,
        detail=(
            f"Connector definition not found at {candidates[0]} "
            f"or {candidates[1]}"
        ),
    )


# Backward-compatible alias for older imports. Prefer
# ``load_connector_definition`` going forward.
load_connector_for_connection = load_connector_definition
