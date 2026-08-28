"""Endpoint reference resolver.

Translates the contract's ``endpoint_ref`` (``ConnectorEndpointRef |
ConnectionEndpointRef``) into the on-disk endpoint JSON document.

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

from src.config.exceptions import ConfigValidationError, EndpointNotFoundError
from src.config.utils import load_json_file

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


def endpoint_document_id(ref: EndpointRef) -> str:
    """Return the ``endpoint_id`` the endpoint document file is named by.

    A connection ref types ``endpoint_id`` as ``str | None`` because an author
    may omit it, but ``ConnectionEndpointRef._derive_or_verify_endpoint_id``
    fills it from ``database_object`` before validation returns -- so every ref
    that reaches the engine carries one. The check is what makes that
    dependency visible: if the contract ever stops deriving, resolution fails
    naming the ref instead of reading ``None.json``.
    """
    if ref.endpoint_id is None:
        raise ConfigValidationError(
            f"endpoint_ref {ref.scope}:{ref.connection_id} carries no endpoint_id; "
            f"the contract validator derives it from database_object",
            field="endpoint_ref.endpoint_id",
        )
    return ref.endpoint_id


def resolve_endpoint_path(
    ref: EndpointRefInput,
    paths: Mapping[str, Path],
    lookup: ConnectionLookup,
) -> Path:
    """Resolve an endpoint reference to its file path on disk."""
    parsed = parse_endpoint_ref(ref)

    if isinstance(parsed, ConnectorEndpointRef):
        connector_id = lookup.connector_id_for(parsed.connection_id)
        root = paths["connectors"] / connector_id
    else:
        # The union has exactly two members and the discriminator is a
        # Literal on each, so a third scope cannot reach here -- there is no
        # unknown-scope branch to write.
        root = paths["connections"] / lookup.directory_for(parsed.connection_id)

    file_path = (
        root / "definition" / "endpoints" / f"{endpoint_document_id(parsed)}.json"
    )
    if not file_path.is_file():
        raise EndpointNotFoundError(
            endpoint_ref_label(parsed), detail=f"File not found: {file_path}"
        )
    return file_path


def resolve_endpoint_ref(
    ref: EndpointRefInput,
    paths: Mapping[str, Path],
    lookup: ConnectionLookup,
) -> dict[str, Any]:
    """Resolve an endpoint reference and return the parsed endpoint document."""
    parsed = parse_endpoint_ref(ref)
    file_path = resolve_endpoint_path(parsed, paths, lookup)
    endpoint = load_json_file(file_path)
    logger.info(
        "Resolved endpoint_ref %s from %s", endpoint_ref_label(parsed), file_path
    )
    return endpoint
