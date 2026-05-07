"""Endpoint reference resolver.

Translates a structured :class:`EndpointRef` into the on-disk endpoint
JSON document.

Reference shape (see ``src/models/stream.py:EndpointRef``):

    ``{"scope": "connector",  "connection_id": "<uuid_v#>", "alias": "<name>"}``
        -> connectors/<connector_alias>/definition/endpoints/<name>.json

    ``{"scope": "connection", "connection_id": "<uuid_v#>", "alias": "<name>"}``
        -> connections/<connection_dir>/definition/endpoints/<name>.json

Resolution requires two facts about the connection identified by
``connection_id``:

* its directory name on disk (used for connection-scoped endpoints);
* its ``connector_alias`` (used for connector-scoped endpoints).

The :class:`PipelineConfigPrep` layer scans ``connections/`` once at
config-load time and supplies these maps via :class:`ConnectionLookup`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Union

from src.config.exceptions import EndpointNotFoundError
from src.models.stream import EndpointRef

logger = logging.getLogger(__name__)

EndpointRefInput = Union[EndpointRef, Mapping[str, Any]]


@dataclass(frozen=True)
class ConnectionLookup:
    """Indexed view of saved connections, keyed by versioned ``connection_id``.

    ``directory`` is the connection directory name under
    ``connections/`` (typically the connection alias). ``connector_alias``
    is the connector slug declared by the connection's
    ``connector_alias`` field.
    """

    directory_by_id: Mapping[str, str]
    connector_alias_by_id: Mapping[str, str]

    def directory_for(self, connection_id: str) -> str:
        if connection_id not in self.directory_by_id:
            raise KeyError(
                f"Unknown connection_id {connection_id!r}; "
                f"known: {sorted(self.directory_by_id)}"
            )
        return self.directory_by_id[connection_id]

    def connector_alias_for(self, connection_id: str) -> str:
        if connection_id not in self.connector_alias_by_id:
            raise KeyError(
                f"Connection {connection_id!r} has no connector_alias mapping"
            )
        return self.connector_alias_by_id[connection_id]


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
        connector_alias = lookup.connector_alias_for(parsed.connection_id)
        file_path = (
            paths["connectors"]
            / connector_alias
            / "definition"
            / "endpoints"
            / f"{parsed.alias}.json"
        )
    elif parsed.scope == "connection":
        directory = lookup.directory_for(parsed.connection_id)
        file_path = (
            paths["connections"]
            / directory
            / "definition"
            / "endpoints"
            / f"{parsed.alias}.json"
        )
    else:  # pragma: no cover — defended in EndpointRef.__post_init__
        raise ValueError(f"Unknown endpoint scope: {parsed.scope!r}")

    if not file_path.is_file():
        raise EndpointNotFoundError(
            parsed, detail=f"File not found: {file_path}"
        )
    return file_path


def resolve_endpoint_ref(
    ref: EndpointRefInput,
    paths: Mapping[str, Path],
    lookup: ConnectionLookup,
) -> Dict[str, Any]:
    """Resolve an endpoint reference and return the parsed endpoint document."""
    parsed = _coerce(ref)
    file_path = resolve_endpoint_path(parsed, paths, lookup)
    try:
        with file_path.open() as fh:
            endpoint = json.load(fh)
    except json.JSONDecodeError as err:
        raise ValueError(f"Invalid JSON in endpoint file {file_path}: {err}") from err
    logger.info("Resolved endpoint_ref %s from %s", parsed, file_path)
    return endpoint
