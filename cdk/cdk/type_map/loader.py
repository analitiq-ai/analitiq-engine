"""Filesystem loaders for a definition directory's ``type-map.json``.

Two parallel locations are supported:

- ``connectors/{connector_id}/definition/`` — required. Covers the connector's
  public endpoints (API schemas shipped with the connector).
- ``connections/{connection_id}/definition/`` — optional. Covers the
  connection's private endpoints (e.g. user-specific DB tables). Absent when a
  connection only uses public endpoints from its connector.

``type-map.json`` is one document, ``{$schema, read?, write?}``. Each
direction is optional on its own: a source uses the read map, a destination
the write map. Document well-formedness is analitiq-validator's contract,
gated once in DIP CI before a connector is published (one gate per document;
see schema-contracts.md); this loader parses the document into typed rules
and refuses what this process will not execute.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from .exceptions import InvalidTypeMapError, TypeMapNotFoundError
from .mapper import TypeMapper
from .rules import parse_type_map

logger = logging.getLogger(__name__)


TYPE_MAP_FILENAME = "type-map.json"


def read_raw_type_map(definition_dir: Path, label: str) -> object | None:
    """*definition_dir*'s ``type-map.json`` as parsed JSON, unvalidated.

    The worker-bootstrap path: the trusted shell ships this document in the
    launch bootstrap and the worker rebuilds the mapper via
    :func:`build_type_mapper`. ``None`` when the file is absent; malformed
    JSON is a hard ``InvalidTypeMapError`` -- a file this broken did not pass
    the DIP publish gate, so the shell or the checkout is broken.
    """
    path = definition_dir / TYPE_MAP_FILENAME
    if not path.is_file():
        return None
    try:
        document: object = json.loads(path.read_text())
    except json.JSONDecodeError as err:
        raise InvalidTypeMapError(f"{label}: {path} is not valid JSON: {err}") from err
    return document


def _build_type_mapper(
    mapper_label: str, document: object, *, source: str
) -> TypeMapper:
    parsed = parse_type_map(document, source=source)
    return TypeMapper(mapper_label, parsed.read, parsed.write)


def _load_type_mapper(
    definition_dir: Path, label: str, mapper_label: str
) -> TypeMapper | None:
    """Parse *definition_dir*'s ``type-map.json`` into a mapper; ``None`` if absent.

    The document is parsed here, so a broken map fails at load rather than
    later as an opaque read or create_table error.
    """
    document = read_raw_type_map(definition_dir, label)
    if document is None:
        return None
    mapper = _build_type_mapper(
        mapper_label, document, source=str(definition_dir / TYPE_MAP_FILENAME)
    )
    logger.info("Loaded type-map for %s from %s", label, definition_dir)
    return mapper


def build_type_mapper(label: str, document: object) -> TypeMapper:
    """Build a :class:`TypeMapper` from a :func:`read_raw_type_map` document.

    The worker-bootstrap path: the worker rebuilds the mapper the trusted
    shell read, with the same parsing the file loaders apply.
    """
    return _build_type_mapper(label, document, source=f"{label} (bootstrap)")


def connector_definition_dir(connectors_dir: Path, slug: str) -> Path:
    """Return the connector's ``definition/`` directory (``{slug}/definition``)."""
    return connectors_dir / slug / "definition"


def load_type_map(connectors_dir: Path, slug: str) -> TypeMapper:
    """Load and parse a connector's ``type-map.json``.

    Raises ``TypeMapNotFoundError`` when the file is absent,
    ``InvalidTypeMapError`` when it is malformed.
    """
    definition = connector_definition_dir(connectors_dir, slug)
    mapper = _load_type_mapper(definition, f"connector {slug!r}", slug)
    if mapper is None:
        raise TypeMapNotFoundError(
            f"connector {slug!r}: required type-map not found: "
            f"{definition / TYPE_MAP_FILENAME} does not exist"
        )
    return mapper


def load_connection_type_map(
    connections_dir: Path, connection_id: str
) -> TypeMapper | None:
    """Load a connection-scoped type map if present.

    Lives under ``connections/{connection_id}/definition/`` and governs type
    translation for private endpoints under the same
    ``connections/{connection_id}/definition/endpoints/`` tree. No
    ``type-map.json`` → ``None``; the caller decides whether that's an error
    (private endpoints referenced) or fine (pipeline only uses public
    endpoints from its connector).
    """
    return _load_type_mapper(
        connections_dir / connection_id / "definition",
        f"connection {connection_id!r}",
        f"connection:{connection_id}",
    )
