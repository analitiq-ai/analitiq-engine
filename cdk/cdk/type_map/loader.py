"""Filesystem loaders for a definition directory's type-map documents.

Two parallel locations are supported:

- ``connectors/{connector_id}/definition/`` — required. Covers the connector's
  public endpoints (API schemas shipped with the connector).
- ``connections/{connection_id}/definition/`` — optional. Covers the
  connection's private endpoints (e.g. user-specific DB tables). Absent when a
  connection only uses public endpoints from its connector.

Each ``type-map-*.json`` document is a top-level JSON object
``{$schema, direction, rules}``. ``direction`` (``"read"`` / ``"write"``) says
which map the document is; the filename does not, so a directory holds at
most one document per direction. Rule well-formedness is analitiq-validator's
contract, gated once in DIP CI before a connector is published (one gate per
document; see schema-contracts.md), so this loader only unwraps the envelope.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from .exceptions import InvalidTypeMapError, TypeMapNotFoundError
from .mapper import TypeMapper
from .rules import parse_rules, parse_write_rules

logger = logging.getLogger(__name__)


_TYPE_MAP_GLOB = "type-map-*.json"


@dataclass(frozen=True)
class _TypeMapDocument:
    source: str
    rules: list


def _read_type_map_documents(
    definition_dir: Path, label: str
) -> dict[str, _TypeMapDocument]:
    """*definition_dir*'s type-map documents, keyed by their ``direction``.

    A directory with no documents yields ``{}``. Malformed JSON, an envelope
    without ``direction``/``rules``, or two documents declaring the same
    direction is a hard ``InvalidTypeMapError`` -- a file
    this broken did not pass the DIP publish gate, so the shell or the
    checkout is broken, not the document's authored content.
    """
    documents: dict[str, _TypeMapDocument] = {}
    for path in sorted(definition_dir.glob(_TYPE_MAP_GLOB)):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError as err:
            raise InvalidTypeMapError(
                f"{label}: {path} is not valid JSON: {err}"
            ) from err
        try:
            direction = payload["direction"]
            # The rule array this returns, not the envelope it came from, is
            # what parse_rules/parse_write_rules validate against the
            # contract's TypeMapReadDoc/TypeMapWriteDoc RootModel (rules.py's
            # _parse). The pinned contract still models that RootModel over
            # the bare array; when claude-code-plugins#316 makes it the
            # envelope instead, this unwrap and that validation level have to
            # move together.
            rules: list = payload["rules"]
        except (TypeError, KeyError) as err:
            raise InvalidTypeMapError(
                f"{label}: {path} is not a type-map document with 'direction' "
                f"and 'rules'"
            ) from err
        if direction in documents:
            raise InvalidTypeMapError(
                f"{label}: {documents[direction].source} and {path} both declare "
                f"direction {direction!r}"
            )
        documents[direction] = _TypeMapDocument(str(path), rules)
    return documents


def read_raw_type_maps(
    definition_dir: Path, label: str
) -> dict[str, list | None] | None:
    """Raw read/write rule arrays from *definition_dir*, unparsed.

    The worker-bootstrap path: the trusted shell ships these arrays in the
    launch bootstrap and the worker rebuilds the mappers via
    :func:`build_type_mapper`. ``None`` when the directory has no read
    document.
    """
    documents = _read_type_map_documents(definition_dir, label)
    if "read" not in documents:
        return None
    write = documents.get("write")
    return {
        "rules": documents["read"].rules,
        "write_rules": None if write is None else write.rules,
    }


def _load_type_mapper(
    definition_dir: Path, label: str, mapper_label: str
) -> TypeMapper | None:
    """Parse *definition_dir*'s documents into a mapper; ``None`` with no read map.

    The write map is optional (source-only / API connectors have none), but a
    present one is parsed here, so a broken write map fails at load rather
    than later as an opaque create_table error.
    """
    documents = _read_type_map_documents(definition_dir, label)
    if "read" not in documents:
        return None
    mapper = _parse_type_mapper(mapper_label, documents["read"], documents.get("write"))
    logger.info("Loaded type-map for %s from %s", label, definition_dir)
    return mapper


def _parse_type_mapper(
    mapper_label: str, read: _TypeMapDocument, write: _TypeMapDocument | None
) -> TypeMapper:
    rules = parse_rules(read.rules, source=read.source)
    write_rules = None
    if write is not None:
        write_rules = parse_write_rules(write.rules, source=write.source)
    return TypeMapper(mapper_label, rules, write_rules)


def build_type_mapper(
    label: str,
    rules_payload: list,
    write_rules_payload: list | None = None,
) -> TypeMapper:
    """Build a :class:`TypeMapper` from raw rule payloads (no filesystem).

    The worker-bootstrap path: the trusted shell ships the arrays
    :func:`read_raw_type_maps` returns in the launch bootstrap, and the worker
    rebuilds the mapper here with the same rule validation the file loaders
    apply.
    """
    source = f"{label} (bootstrap)"
    write = None
    if write_rules_payload is not None:
        write = _TypeMapDocument(source, write_rules_payload)
    return _parse_type_mapper(label, _TypeMapDocument(source, rules_payload), write)


def connector_definition_dir(connectors_dir: Path, slug: str) -> Path:
    """Return the connector's ``definition/`` directory (``{slug}/definition``)."""
    return connectors_dir / slug / "definition"


def load_type_map(connectors_dir: Path, slug: str) -> TypeMapper:
    """Load and parse a connector's type-map documents.

    Raises ``TypeMapNotFoundError`` when no document declares the read
    direction, ``InvalidTypeMapError`` when one is malformed — the engine
    cannot canonicalize types without a read map.
    """
    definition = connector_definition_dir(connectors_dir, slug)
    mapper = _load_type_mapper(definition, f"connector {slug!r}", slug)
    if mapper is None:
        raise TypeMapNotFoundError(
            f"connector {slug!r}: required type-map not found: no "
            f"{_TYPE_MAP_GLOB} document in {definition} declares direction 'read'"
        )
    return mapper


def load_connection_type_map(
    connections_dir: Path, connection_id: str
) -> TypeMapper | None:
    """Load a connection-scoped type map if present.

    Lives under ``connections/{connection_id}/definition/`` and governs type
    translation for private endpoints under the same
    ``connections/{connection_id}/definition/endpoints/`` tree. No read
    document → ``None``; the caller decides whether that's an error (private
    endpoints referenced) or fine (pipeline only uses public endpoints).
    """
    return _load_type_mapper(
        connections_dir / connection_id / "definition",
        f"connection {connection_id!r}",
        f"connection:{connection_id}",
    )
