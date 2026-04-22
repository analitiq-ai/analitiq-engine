"""
Endpoint reference resolver.

Resolves scoped endpoint references to endpoint configuration dicts.

Reference format:
- "connector:{slug}/{endpoint_name}" -> connectors/{slug}/definition/endpoints/{endpoint_name}.json
- "connection:{alias}/{endpoint_name}" -> connections/{alias}/definition/endpoints/{endpoint_name}.json
"""

import json
import logging
from pathlib import Path
from typing import Any

from src.config.exceptions import EndpointNotFoundError

logger = logging.getLogger(__name__)

VALID_SCOPES = ("connector", "connection")


def parse_endpoint_ref(ref: str) -> tuple[str, str, str]:
    """Parse a scoped endpoint reference into (scope, identifier, endpoint_name).

    Args:
        ref: Endpoint reference string, e.g. "connector:pipedrive/deals"

    Returns:
        Tuple of (scope, identifier, endpoint_name)

    Raises:
        ValueError: If the reference format is invalid
    """
    if ":" not in ref:
        raise ValueError(
            f"Invalid endpoint_ref '{ref}': missing scope prefix. "
            f"Expected format: 'connector:slug/name' or 'connection:alias/name'"
        )

    scope, path = ref.split(":", 1)
    if scope not in VALID_SCOPES:
        raise ValueError(
            f"Invalid endpoint_ref scope '{scope}' in '{ref}'. "
            f"Expected one of: {VALID_SCOPES}"
        )

    if "/" not in path:
        raise ValueError(
            f"Invalid endpoint_ref path '{path}' in '{ref}'. "
            f"Expected format: 'identifier/endpoint_name'"
        )

    identifier, endpoint_name = path.split("/", 1)
    if not identifier or not endpoint_name:
        raise ValueError(
            f"Invalid endpoint_ref '{ref}': identifier and endpoint_name cannot be empty"
        )

    return scope, identifier, endpoint_name


def resolve_endpoint_path(ref: str, paths: dict[str, Path]) -> Path:
    """Resolve an endpoint reference to its file path on disk.

    Args:
        ref: Endpoint reference string
        paths: Dict with 'connectors' and 'connections' Path objects

    Returns:
        Path to the endpoint JSON file

    Raises:
        EndpointNotFoundError: If the endpoint file does not exist
    """
    scope, identifier, endpoint_name = parse_endpoint_ref(ref)

    if scope == "connector":
        file_path = (
            paths["connectors"] / identifier / "definition" / "endpoints"
            / f"{endpoint_name}.json"
        )
    else:  # connection
        file_path = (
            paths["connections"] / identifier / "definition" / "endpoints"
            / f"{endpoint_name}.json"
        )

    if not file_path.is_file():
        raise EndpointNotFoundError(
            ref, detail=f"File not found: {file_path}"
        )

    return file_path


def resolve_endpoint_ref(ref: str, paths: dict[str, Path]) -> dict[str, Any]:
    """Resolve an endpoint reference and return the endpoint configuration.

    Args:
        ref: Endpoint reference string
        paths: Dict with 'connectors' and 'connections' Path objects

    Returns:
        Parsed endpoint configuration dict
    """
    file_path = resolve_endpoint_path(ref, paths)

    try:
        with open(file_path) as f:
            endpoint = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in endpoint file {file_path}: {e}")

    logger.info(f"Resolved endpoint_ref '{ref}' from {file_path}")
    return endpoint
