"""
Connection and connector file loading utilities.

Loads connection configs from connections/{alias}/connection.json
and connector definitions from connectors/{slug}/definition/connector.json.
"""

import json
import logging
from pathlib import Path
from typing import Any

from src.config.exceptions import ConnectionConfigError, ConnectorNotFoundError

logger = logging.getLogger(__name__)


def load_connection(alias: str, connections_dir: Path) -> dict[str, Any]:
    """Load a connection configuration by alias.

    Args:
        alias: Human-readable connection alias (directory name)
        connections_dir: Path to the connections/ directory

    Returns:
        Parsed connection configuration dict

    Raises:
        ConnectionConfigError: If the connection directory or file doesn't exist
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
        with open(conn_file) as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ConnectionConfigError(
            alias, detail=f"Invalid JSON in {conn_file}: {e}"
        )

    logger.info(f"Loaded connection '{alias}' from {conn_file}")
    return config


def load_connector_for_connection(
    connector_slug: str, connectors_dir: Path
) -> dict[str, Any]:
    """Load a connector definition by slug.

    Args:
        connector_slug: Connector slug (directory name under connectors/)
        connectors_dir: Path to the connectors/ directory

    Returns:
        Parsed connector definition dict

    Raises:
        ConnectorNotFoundError: If the connector directory or definition doesn't exist
    """
    # Connector directories may be named either {slug} or connector-{slug}
    connector_file = (
        connectors_dir / connector_slug / "definition" / "connector.json"
    )
    if not connector_file.is_file():
        connector_file = (
            connectors_dir / f"connector-{connector_slug}" / "definition" / "connector.json"
        )
    if not connector_file.is_file():
        raise ConnectorNotFoundError(
            connector_slug,
            detail=f"Connector definition not found: {connector_file}",
        )

    try:
        with open(connector_file) as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ConnectorNotFoundError(
            connector_slug, detail=f"Invalid JSON in {connector_file}: {e}"
        )

    logger.info(f"Loaded connector '{connector_slug}' from {connector_file}")
    return config
