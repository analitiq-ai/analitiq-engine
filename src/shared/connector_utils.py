"""Connector lookup utilities.

Shared utilities for looking up connector information from the connectors array.
Used by both engine (source) and destination components.
"""

from typing import Any, Dict, List, Optional


def find_connector(
    connectors: List[Dict[str, Any]],
    connector_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Find a connector definition by its connector_id.

    Args:
        connectors: List of connector definitions
        connector_id: The connector_id to search for

    Returns:
        The connector definition dict, or None if not found
    """
    for connector in connectors:
        if connector.get("connector_id") == connector_id:
            return connector
    return None


def get_connector_type_from_list(
    connectors: List[Dict[str, Any]],
    connector_id: str,
    connection_id: str = "",
) -> str:
    """
    Look up connector_type from the connectors array using connector_id.

    Args:
        connectors: List of connector definitions
        connector_id: The connector_id to look up
        connection_id: Connection identifier for error messages (optional)

    Returns:
        The connector_type string (e.g., "api", "database", "file", "s3", "stdout")

    Raises:
        ValueError: If connector not found or connector_type is missing
    """
    connector = find_connector(connectors, connector_id)

    if not connector:
        context = f" for connection '{connection_id}'" if connection_id else ""
        raise ValueError(
            f"Connector not found{context} with connector_id '{connector_id}'."
        )

    connector_type = connector.get("connector_type")
    if not connector_type:
        raise ValueError(
            f"Connector '{connector_id}' is missing 'connector_type' field."
        )

    return connector_type