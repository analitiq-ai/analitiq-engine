"""
Endpoint reference resolver.

Resolves a structured ``EndpointRef`` (or its plain-dict form) to the endpoint
configuration JSON on disk.

Reference shape (see ``src/models/stream.py:EndpointRef``):
    {"scope": "connector",  "identifier": "<slug>",  "endpoint": "<name>"}
        -> connectors/<slug>/definition/endpoints/<name>.json
    {"scope": "connection", "identifier": "<alias>", "endpoint": "<name>"}
        -> connections/<alias>/definition/endpoints/<name>.json
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

    root_key = "connectors" if parsed.scope == "connector" else "connections"
    file_path = (
        paths[root_key] / parsed.identifier / "definition" / "endpoints"
        / f"{parsed.endpoint}.json"
    )

    if not file_path.is_file():
        raise EndpointNotFoundError(
            parsed, detail=f"File not found: {file_path}"
        )

    return file_path


def resolve_endpoint_ref(ref: EndpointRefInput, paths: dict[str, Path]) -> dict[str, Any]:
    """Resolve an endpoint reference and return the endpoint configuration.

    Args:
        ref: ``EndpointRef`` instance or equivalent dict
        paths: Dict with 'connectors' and 'connections' Path objects

    Returns:
        Parsed endpoint configuration dict
    """
    parsed = _coerce(ref)
    file_path = resolve_endpoint_path(parsed, paths)

    try:
        with open(file_path) as f:
            endpoint = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in endpoint file {file_path}: {e}")

    logger.info(f"Resolved endpoint_ref {parsed} from {file_path}")
    return endpoint
