"""Endpoint reference resolver.

Translates a structured :class:`EndpointRef` into the on-disk endpoint
JSON document.

Reference shape (see ``src/models/stream.py:EndpointRef``):

    ``{"scope": "connector",  "connection_id": "<id>", "endpoint_id": "<name>"}``
        -> connectors/<connector_id>/definition/endpoints/<name>.json

    ``{"scope": "connection", "connection_id": "<id>", "endpoint_id": "<name>"}``
        -> connections/<directory>/definition/endpoints/<name>.json

:class:`PipelineConfigPrep` scans ``connections/`` once at config-load
time and supplies the ``connection_id → directory / connector_id`` maps
via :class:`ConnectionLookup`.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Union

from src.config.exceptions import EndpointNotFoundError
from src.config.utils import load_json_file
from src.models.stream import EndpointRef

logger = logging.getLogger(__name__)

EndpointRefInput = Union[EndpointRef, Mapping[str, Any]]


@dataclass(frozen=True)
class ConnectionLookup:
    """Indexed view of saved connections, keyed by ``connection_id``."""

    directory_by_id: Mapping[str, str]
    connector_id_by_id: Mapping[str, str]

    def directory_for(self, connection_id: str) -> str:
        if connection_id not in self.directory_by_id:
            raise KeyError(
                f"Unknown connection_id {connection_id!r}; "
                f"known: {sorted(self.directory_by_id)}"
            )
        return self.directory_by_id[connection_id]

    def connector_id_for(self, connection_id: str) -> str:
        if connection_id not in self.connector_id_by_id:
            raise KeyError(f"Connection {connection_id!r} has no connector_id mapping")
        return self.connector_id_by_id[connection_id]


def _coerce(ref: EndpointRefInput) -> EndpointRef:
    return EndpointRef.from_dict(ref)


def resolve_endpoint_path(
    ref: EndpointRefInput,
    paths: Mapping[str, Path],
    lookup: ConnectionLookup,
) -> Path:
    """Resolve an endpoint reference to its file path on disk."""
    parsed = _coerce(ref)

    if parsed.scope == "connector":
        connector_id = lookup.connector_id_for(parsed.connection_id)
        file_path = (
            paths["connectors"]
            / connector_id
            / "definition"
            / "endpoints"
            / f"{parsed.endpoint_id}.json"
        )
    elif parsed.scope == "connection":
        directory = lookup.directory_for(parsed.connection_id)
        file_path = (
            paths["connections"]
            / directory
            / "definition"
            / "endpoints"
            / f"{parsed.endpoint_id}.json"
        )
    else:  # pragma: no cover — scope is validated by the contract in from_dict
        raise ValueError(f"Unknown endpoint scope: {parsed.scope!r}")

    if not file_path.is_file():
        raise EndpointNotFoundError(parsed, detail=f"File not found: {file_path}")
    return file_path


def resolve_endpoint_ref(
    ref: EndpointRefInput,
    paths: Mapping[str, Path],
    lookup: ConnectionLookup,
) -> dict[str, Any]:
    """Resolve an endpoint reference and return the parsed endpoint document."""
    parsed = _coerce(ref)
    file_path = resolve_endpoint_path(parsed, paths, lookup)
    endpoint = load_json_file(file_path)
    logger.info("Resolved endpoint_ref %s from %s", parsed, file_path)
    return endpoint
