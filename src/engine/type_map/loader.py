"""Filesystem loaders for ``type-map.json``.

Two parallel locations are supported:

- ``connectors/{connector_id}/definition/type-map.json`` — required. Covers
  the connector's public endpoints (API schemas shipped with the connector).
- ``connections/{connection_id}/definition/type-map.json`` — optional. Covers
  the connection's private endpoints (e.g. user-specific DB tables). Absent
  when a connection only uses public endpoints from its connector.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from .exceptions import InvalidTypeMapError
from .mapper import TypeMapper
from .rules import parse_rules

logger = logging.getLogger(__name__)


TYPE_MAP_FILENAME = "type-map.json"


def _definition_dir(connectors_dir: Path, slug: str) -> Path:
    """Return the connector's ``definition/`` directory, honoring both
    ``{slug}/`` and ``connector-{slug}/`` layouts used elsewhere in the repo."""
    primary = connectors_dir / slug / "definition"
    if primary.is_dir():
        return primary
    alternate = connectors_dir / f"connector-{slug}" / "definition"
    if alternate.is_dir():
        return alternate
    return primary  # let callers raise with the primary path in the message


def load_type_map(connectors_dir: Path, slug: str) -> TypeMapper:
    """Load and parse ``type-map.json`` for a connector.

    Raises ``InvalidTypeMapError`` if the file is missing or malformed — the
    engine cannot canonicalize types without it.
    """
    path = _definition_dir(connectors_dir, slug) / TYPE_MAP_FILENAME
    if not path.is_file():
        raise InvalidTypeMapError(
            f"connector {slug!r}: required type-map not found at {path}"
        )
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as err:
        raise InvalidTypeMapError(
            f"connector {slug!r}: {path} is not valid JSON: {err}"
        ) from err
    if not isinstance(payload, list):
        raise InvalidTypeMapError(
            f"connector {slug!r}: {path} must contain a JSON array of rules"
        )
    rules = parse_rules(payload, source=str(path))
    logger.info("Loaded type-map for connector '%s' (%d rules)", slug, len(rules))
    return TypeMapper(slug, rules)


def load_connection_type_map(
    connections_dir: Path, connection_id: str
) -> Optional[TypeMapper]:
    """Load a connection-scoped ``type-map.json`` if present.

    Lives at ``connections/{connection_id}/definition/type-map.json`` and
    governs type translation for private endpoints under the same
    ``connections/{connection_id}/definition/endpoints/`` tree. Absent file →
    ``None``; the caller decides whether that's an error (private
    endpoints referenced) or fine (pipeline only uses public endpoints).
    """
    path = connections_dir / connection_id / "definition" / TYPE_MAP_FILENAME
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as err:
        raise InvalidTypeMapError(
            f"connection {connection_id!r}: {path} is not valid JSON: {err}"
        ) from err
    if not isinstance(payload, list):
        raise InvalidTypeMapError(
            f"connection {connection_id!r}: {path} must contain a JSON array of rules"
        )
    rules = parse_rules(payload, source=str(path))
    logger.info(
        "Loaded connection type-map for '%s' (%d rules)", connection_id, len(rules)
    )
    return TypeMapper(f"connection:{connection_id}", rules)