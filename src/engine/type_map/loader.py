"""Filesystem loaders for ``type-map.json`` and ``ssl-mode-map.json``.

Two parallel locations are supported:

- ``connectors/{slug}/definition/type-map.json`` — required. Covers the
  connector's public endpoints (API schemas shipped with the connector).
- ``connections/{alias}/definition/type-map.json`` — optional. Covers the
  connection's private endpoints (e.g. user-specific DB tables). Absent
  when a connection only uses public endpoints from its connector.

``ssl-mode-map.json`` only lives at the connector level — SSL vocabularies
are driver-specific, not connection-specific.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from .exceptions import (
    InvalidSSLModeMapError,
    InvalidTypeMapError,
)
from .mapper import SSLModeMapper, TypeMapper
from .rules import parse_rules

logger = logging.getLogger(__name__)


TYPE_MAP_FILENAME = "type-map.json"
SSL_MODE_MAP_FILENAME = "ssl-mode-map.json"


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
    connections_dir: Path, alias: str
) -> Optional[TypeMapper]:
    """Load a connection-scoped ``type-map.json`` if present.

    Lives at ``connections/{alias}/definition/type-map.json`` and governs
    type translation for private endpoints under the same
    ``connections/{alias}/definition/endpoints/`` tree. Absent file →
    ``None``; the caller decides whether that's an error (private
    endpoints referenced) or fine (pipeline only uses public endpoints).
    """
    path = connections_dir / alias / "definition" / TYPE_MAP_FILENAME
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as err:
        raise InvalidTypeMapError(
            f"connection {alias!r}: {path} is not valid JSON: {err}"
        ) from err
    if not isinstance(payload, list):
        raise InvalidTypeMapError(
            f"connection {alias!r}: {path} must contain a JSON array of rules"
        )
    rules = parse_rules(payload, source=str(path))
    logger.info(
        "Loaded connection type-map for '%s' (%d rules)", alias, len(rules)
    )
    return TypeMapper(f"connection:{alias}", rules)


def load_ssl_mode_map(connectors_dir: Path, slug: str) -> Optional[SSLModeMapper]:
    """Load ``ssl-mode-map.json`` if present. Returns ``None`` when absent."""
    path = _definition_dir(connectors_dir, slug) / SSL_MODE_MAP_FILENAME
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as err:
        raise InvalidSSLModeMapError(
            f"connector {slug!r}: {path} is not valid JSON: {err}"
        ) from err
    if not isinstance(payload, dict):
        raise InvalidSSLModeMapError(
            f"connector {slug!r}: {path} must contain a JSON object"
        )
    # Drop JSON Schema metadata keys — ``$schema`` / ``$id`` are allowed in
    # the file but are not mapping entries.
    entries = {k: v for k, v in payload.items() if not k.startswith("$")}
    mapper = SSLModeMapper(slug, entries)
    logger.info(
        "Loaded ssl-mode-map for connector '%s' (%d entries)",
        slug,
        len(mapper.entries),
    )
    return mapper