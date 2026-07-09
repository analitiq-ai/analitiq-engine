"""Filesystem loaders for ``type-map-read.json``.

Two parallel locations are supported:

- ``connectors/{connector_id}/definition/type-map-read.json`` — required. Covers
  the connector's public endpoints (API schemas shipped with the connector).
- ``connections/{connection_id}/definition/type-map-read.json`` — optional. Covers
  the connection's private endpoints (e.g. user-specific DB tables). Absent
  when a connection only uses public endpoints from its connector.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from .exceptions import InvalidTypeMapError, TypeMapNotFoundError
from .mapper import TypeMapper
from .rules import WriteTypeMapRule, parse_rules, parse_write_rules

logger = logging.getLogger(__name__)


TYPE_MAP_FILENAME = "type-map-read.json"
WRITE_TYPE_MAP_FILENAME = "type-map-write.json"


def _read_json_array(path: Path, label: str) -> list | None:
    """Read *path* as a JSON array; ``None`` when absent.

    Malformed JSON or a non-array document is a hard
    ``InvalidTypeMapError`` — every type-map consumer (file loaders and the
    worker-bootstrap path) shares this validation, so the same broken file
    fails with the same error everywhere.
    """
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as err:
        raise InvalidTypeMapError(f"{label}: {path} is not valid JSON: {err}") from err
    if not isinstance(payload, list):
        raise InvalidTypeMapError(f"{label}: {path} must contain a JSON array of rules")
    return payload


def read_raw_type_maps(
    definition_dir: Path, label: str
) -> dict[str, list | None] | None:
    """Raw read/write rule arrays from *definition_dir*, unparsed.

    The worker-bootstrap path: the trusted shell ships these arrays in the
    launch bootstrap and the worker rebuilds the mappers via
    :func:`build_type_mapper`. ``None`` when the directory has no
    ``type-map-read.json``.
    """
    rules = _read_json_array(definition_dir / TYPE_MAP_FILENAME, label)
    if rules is None:
        return None
    write_rules = _read_json_array(definition_dir / WRITE_TYPE_MAP_FILENAME, label)
    return {"rules": rules, "write_rules": write_rules}


def _load_write_rules(
    definition_dir: Path, label: str
) -> list[WriteTypeMapRule] | None:
    """Load the optional sibling ``type-map-write.json`` from *definition_dir*.

    Absent file → ``None`` (source-only / API connectors have no write map). A
    present-but-malformed file is a hard ``InvalidTypeMapError`` — the write-map
    contract is "absent is fine, present must be valid", so a broken file fails
    at load (and is caught by connector/registry CI) rather than surfacing later
    as an opaque create_table error.
    """
    payload = _read_json_array(definition_dir / WRITE_TYPE_MAP_FILENAME, label)
    if payload is None:
        return None
    rules = parse_write_rules(
        payload, source=str(definition_dir / WRITE_TYPE_MAP_FILENAME)
    )
    logger.info("Loaded write-type-map for %s (%d rules)", label, len(rules))
    return rules


def build_type_mapper(
    label: str,
    rules_payload: list,
    write_rules_payload: list | None = None,
) -> TypeMapper:
    """Build a :class:`TypeMapper` from raw rule payloads (no filesystem).

    The worker-bootstrap path: the trusted shell reads the connector's /
    connection's ``type-map-read.json`` (+ optional ``type-map-write.json``) and
    ships the raw arrays in the launch bootstrap; the worker rebuilds the
    mapper here with the same validation the file loaders apply.
    """
    if not isinstance(rules_payload, list):
        raise InvalidTypeMapError(
            f"{label}: type-map payload must be a JSON array of rules"
        )
    rules = parse_rules(rules_payload, source=f"{label} (bootstrap)")
    write_rules = None
    if write_rules_payload is not None:
        if not isinstance(write_rules_payload, list):
            raise InvalidTypeMapError(
                f"{label}: write-type-map payload must be a JSON array of rules"
            )
        write_rules = parse_write_rules(
            write_rules_payload, source=f"{label} (bootstrap)"
        )
    return TypeMapper(label, rules, write_rules)


def connector_definition_dir(connectors_dir: Path, slug: str) -> Path:
    """Return the connector's ``definition/`` directory (``{slug}/definition``)."""
    return connectors_dir / slug / "definition"


def load_type_map(connectors_dir: Path, slug: str) -> TypeMapper:
    """Load and parse ``type-map-read.json`` for a connector.

    Raises ``InvalidTypeMapError`` if the file is missing or malformed — the
    engine cannot canonicalize types without it.
    """
    definition = connector_definition_dir(connectors_dir, slug)
    path = definition / TYPE_MAP_FILENAME
    payload = _read_json_array(path, f"connector {slug!r}")
    if payload is None:
        raise TypeMapNotFoundError(
            f"connector {slug!r}: required type-map not found at {path}"
        )
    rules = parse_rules(payload, source=str(path))
    write_rules = _load_write_rules(definition, f"connector {slug!r}")
    logger.info("Loaded type-map for connector '%s' (%d rules)", slug, len(rules))
    return TypeMapper(slug, rules, write_rules)


def load_connection_type_map(
    connections_dir: Path, connection_id: str
) -> TypeMapper | None:
    """Load a connection-scoped ``type-map-read.json`` if present.

    Lives at ``connections/{connection_id}/definition/type-map-read.json`` and
    governs type translation for private endpoints under the same
    ``connections/{connection_id}/definition/endpoints/`` tree. Absent file →
    ``None``; the caller decides whether that's an error (private
    endpoints referenced) or fine (pipeline only uses public endpoints).
    """
    definition = connections_dir / connection_id / "definition"
    path = definition / TYPE_MAP_FILENAME
    payload = _read_json_array(path, f"connection {connection_id!r}")
    if payload is None:
        return None
    rules = parse_rules(payload, source=str(path))
    write_rules = _load_write_rules(definition, f"connection {connection_id!r}")
    logger.info(
        "Loaded connection type-map for '%s' (%d rules)", connection_id, len(rules)
    )
    return TypeMapper(f"connection:{connection_id}", rules, write_rules)
