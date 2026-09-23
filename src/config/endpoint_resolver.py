"""Endpoint reference resolver.

Translates the contract's ``endpoint_ref`` (``ConnectorEndpointRef |
ConnectionEndpointRef``) into the path of its endpoint document.

Reference shape (published contract, ``analitiq.contracts.stream``):

    ``{"scope": "connector",  "connection_id": "<id>", "endpoint_id": "<name>"}``
        -> connectors/<connector_id>/definition/endpoints/<name>.json

    ``{"scope": "connection", "connection_id": "<id>", "database_object": {...}}``
        -> connections/<directory>/definition/endpoints/<derived id>.json

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

from analitiq.contracts.stream import (
    ConnectorEndpointRef,
    EndpointRef,
    validate_endpoint_ref,
)
from pydantic import ValidationError

from src.config.exceptions import ConfigValidationError

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


def parse_endpoint_ref(ref: EndpointRefInput) -> EndpointRef:
    """Validate a raw ``endpoint_ref`` payload into its contract variant.

    Shape and cross-field rules -- including derivation of a
    ``connection``-scoped ``endpoint_id`` from ``database_object`` -- belong to
    the published contract; this is the one place the engine enters it, so a
    bad ref reads as a config defect naming the payload instead of a raw
    pydantic traceback. An already-validated ref passes straight through.
    """
    try:
        return validate_endpoint_ref(ref)
    except ValidationError as exc:
        raise ConfigValidationError(
            f"Invalid endpoint_ref {ref!r}: {exc}", field="endpoint_ref"
        ) from exc


def endpoint_ref_label(ref: EndpointRef) -> str:
    """Name a ref the way the on-disk layout does: ``<scope>:<connection>/<id>``.

    A contract model's own ``str`` is a dump of every field, which drags the
    whole ``database_object`` repr into each message that mentions a
    connection-scoped ref. Log lines and errors want the handle, not the model.
    """
    return f"{ref.scope}:{ref.connection_id}/{ref.endpoint_id}"


def resolve_endpoint_path(
    ref: EndpointRefInput,
    paths: Mapping[str, Path],
    lookup: ConnectionLookup,
) -> Path:
    """Return the path of the endpoint document ``ref`` names.

    The workspace verdict has already resolved every ref to its document, so
    the path is computed, not probed.
    """
    parsed = parse_endpoint_ref(ref)

    if isinstance(parsed, ConnectorEndpointRef):
        connector_id = lookup.connector_id_for(parsed.connection_id)
        root = paths["connectors"] / connector_id
    else:
        # The union has exactly two members and the discriminator is a
        # Literal on each, so a third scope cannot reach here -- there is no
        # unknown-scope branch to write.
        root = paths["connections"] / lookup.directory_for(parsed.connection_id)

    return root / "definition" / "endpoints" / f"{parsed.endpoint_id}.json"
