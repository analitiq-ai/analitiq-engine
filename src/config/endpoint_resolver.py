"""
Endpoint reference resolver.

Resolves a structured ``EndpointRef`` (matching the published stream contract
``$defs/EndpointRef``: ``{scope, connection_id, endpoint_id}``) to the
endpoint configuration JSON on disk.

Path resolution:
- ``scope="connection"``: ``connections/{connection_id}/definition/endpoints/{endpoint_id}.json``
- ``scope="connector"``: look up the owning connection's ``connector_id``
  (i.e. ``connections/{connection_id}/connection.json``) then load
  ``connectors/{connector_id}/definition/endpoints/{endpoint_id}.json``.
  The fallback path ``connectors/{connection_id}/...`` is used when the
  connection isn't on disk (e.g. cloud bundles that pre-resolved the slug).
"""

import json
import logging
from pathlib import Path
from typing import Any, Union

from src.config.exceptions import EndpointNotFoundError
from src.models.stream import EndpointRef

logger = logging.getLogger(__name__)

EndpointRefInput = Union[EndpointRef, dict]


def _coerce(ref: EndpointRefInput) -> EndpointRef:
    """Validate and return an ``EndpointRef`` instance, accepting dicts too."""
    return EndpointRef.from_dict(ref)


def _connector_dir_name(
    connection_id: str, connections_dir: Path
) -> str:
    """For a connector-scoped ref, derive the on-disk connectors/ dir name.

    If ``connections/<connection_id>/connection.json`` exists, read its
    ``connector_id`` and use that. Otherwise fall back to ``connection_id``
    itself (cloud bundles already pre-resolve to the connector slug).
    """
    conn_file = connections_dir / connection_id / "connection.json"
    if conn_file.is_file():
        try:
            with open(conn_file) as f:
                cfg = json.load(f)
            connector_id = cfg.get("connector_id")
            if connector_id:
                return connector_id
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in connection file {conn_file}: {e}")
    return connection_id


def resolve_endpoint_path(ref: EndpointRefInput, paths: dict[str, Path]) -> Path:
    """Resolve an endpoint reference to its file path on disk.

    Args:
        ref: ``EndpointRef`` instance or equivalent dict
        paths: Dict with 'connectors' and 'connections' Path objects

    Returns:
        Path to the endpoint JSON file

    Raises:
        EndpointNotFoundError: If the endpoint file does not exist
    """
    parsed = _coerce(ref)

    if parsed.scope == "connector":
        connector_id = _connector_dir_name(
            parsed.connection_id, paths["connections"]
        )
        file_path = (
            paths["connectors"] / connector_id / "definition" / "endpoints"
            / f"{parsed.endpoint_id}.json"
        )
    else:
        file_path = (
            paths["connections"] / parsed.connection_id / "definition" / "endpoints"
            / f"{parsed.endpoint_id}.json"
        )

    if not file_path.is_file():
        raise EndpointNotFoundError(
            parsed, detail=f"File not found: {file_path}"
        )

    return file_path


def resolve_endpoint_ref(ref: EndpointRefInput, paths: dict[str, Path]) -> dict[str, Any]:
    """Resolve an endpoint reference and return the endpoint configuration."""
    parsed = _coerce(ref)
    file_path = resolve_endpoint_path(parsed, paths)

    try:
        with open(file_path) as f:
            endpoint = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in endpoint file {file_path}: {e}")

    logger.info(f"Resolved endpoint_ref {parsed} from {file_path}")
    return endpoint
