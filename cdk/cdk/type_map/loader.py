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

from .exceptions import InvalidTypeMapError, TypeMapNotFoundError
from .mapper import TypeMapper
from .rules import WriteTypeMapRule, parse_rules, parse_write_rules

logger = logging.getLogger(__name__)


TYPE_MAP_FILENAME = "type-map.json"
WRITE_TYPE_MAP_FILENAME = "write-type-map.json"


def _load_write_rules(
    definition_dir: Path, label: str
) -> Optional[list[WriteTypeMapRule]]:
    """Load the optional sibling ``write-type-map.json`` from *definition_dir*.

    Absent file → ``None`` (source-only / API connectors have no write map). A
    present-but-malformed file is a hard ``InvalidTypeMapError`` — the write-map
    contract is "absent is fine, present must be valid", so a broken file fails
    at load (and is caught by connector/registry CI) rather than surfacing later
    as an opaque create_table error.
    """
    path = definition_dir / WRITE_TYPE_MAP_FILENAME
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as err:
        raise InvalidTypeMapError(
            f"{label}: {path} is not valid JSON: {err}"
        ) from err
    if not isinstance(payload, list):
        raise InvalidTypeMapError(
            f"{label}: {path} must contain a JSON array of rules"
        )
    rules = parse_write_rules(payload, source=str(path))
    logger.info("Loaded write-type-map for %s (%d rules)", label, len(rules))
    return rules


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
    definition = _definition_dir(connectors_dir, slug)
    path = definition / TYPE_MAP_FILENAME
    if not path.is_file():
        raise TypeMapNotFoundError(
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
    write_rules = _load_write_rules(definition, f"connector {slug!r}")
    logger.info("Loaded type-map for connector '%s' (%d rules)", slug, len(rules))
    return TypeMapper(slug, rules, write_rules)


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
    definition = connections_dir / connection_id / "definition"
    path = definition / TYPE_MAP_FILENAME
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
    write_rules = _load_write_rules(definition, f"connection {connection_id!r}")
    logger.info(
        "Loaded connection type-map for '%s' (%d rules)", connection_id, len(rules)
    )
    return TypeMapper(f"connection:{connection_id}", rules, write_rules)