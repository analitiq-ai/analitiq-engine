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
most one document per direction. Each direction is optional on its own: a
source uses the read map, a destination the write map. Document
well-formedness, including the allowed ``direction`` values, is
analitiq-validator's contract, gated once in DIP CI before a connector is
published (one gate per document; see schema-contracts.md), so this loader
only unwraps each envelope and keys it by ``direction``. A ``direction``
other than ``read``/``write`` is keyed and never used.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .exceptions import InvalidTypeMapError, TypeMapNotFoundError
from .mapper import TypeMapper
from .rules import parse_rules, parse_write_rules

logger = logging.getLogger(__name__)


_TYPE_MAP_GLOB = "type-map-*.json"
# Worker-bootstrap block key for each direction's rule array.
_BLOCK_KEYS = {"read": "rules", "write": "write_rules"}


@dataclass(frozen=True)
class _TypeMapDocument:
    source: str
    rules: object


def _read_type_map_documents(
    definition_dir: Path, label: str
) -> dict[str, _TypeMapDocument]:
    """*definition_dir*'s type-map documents, keyed by their ``direction``.

    A directory with no documents yields ``{}``. Malformed JSON or an
    envelope without ``direction``/``rules`` is a hard ``InvalidTypeMapError``
    -- a file this broken did not pass the DIP publish gate, so the shell or
    the checkout is broken. Two documents declaring the same direction are
    also an ``InvalidTypeMapError``: keying by direction would otherwise keep
    one and silently drop the other.
    """
    documents: dict[str, _TypeMapDocument] = {}
    for path in sorted(definition_dir.glob(_TYPE_MAP_GLOB)):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError as err:
            raise InvalidTypeMapError(
                f"{label}: {path} is not valid JSON: {err}"
            ) from err
        # parse_rules/parse_write_rules validate the bare rules array against
        # the pinned contract's RootModel (rules.py's _parse). When
        # claude-code-plugins#316 makes those models the envelope, this
        # unwrap, the direction keying and that validation level move together.
        try:
            direction = payload["direction"]
            rules = payload["rules"]
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


def read_raw_type_maps(definition_dir: Path, label: str) -> dict[str, object] | None:
    """*definition_dir*'s rule arrays as a raw block, unparsed.

    The worker-bootstrap path: the trusted shell ships this block in the
    launch bootstrap and the worker rebuilds the mapper via
    :func:`build_type_mapper`. ``rules`` / ``write_rules`` are present only
    when their document is, so a document whose rules are ``null`` is never
    read as absent. ``None`` when the directory has neither document.
    """
    documents = _read_type_map_documents(definition_dir, label)
    block: dict[str, object] = {
        key: documents[direction].rules
        for direction, key in _BLOCK_KEYS.items()
        if direction in documents
    }
    return block or None


def _load_type_mapper(
    definition_dir: Path, label: str, mapper_label: str
) -> TypeMapper | None:
    """Parse *definition_dir*'s documents into a mapper; ``None`` with neither.

    Every present document is parsed here, so a broken map fails at load
    rather than later as an opaque read or create_table error.
    """
    documents = _read_type_map_documents(definition_dir, label)
    if "read" not in documents and "write" not in documents:
        return None
    mapper = _parse_type_mapper(
        mapper_label, documents.get("read"), documents.get("write")
    )
    logger.info("Loaded type-map for %s from %s", label, definition_dir)
    return mapper


def _parse_type_mapper(
    mapper_label: str,
    read: _TypeMapDocument | None,
    write: _TypeMapDocument | None,
) -> TypeMapper:
    rules = None
    if read is not None:
        rules = parse_rules(read.rules, source=read.source)
    write_rules = None
    if write is not None:
        write_rules = parse_write_rules(write.rules, source=write.source)
    return TypeMapper(mapper_label, rules, write_rules)


def build_type_mapper(label: str, block: Mapping[str, object]) -> TypeMapper:
    """Build a :class:`TypeMapper` from a :func:`read_raw_type_maps` block.

    The worker-bootstrap path: the worker rebuilds the mapper the trusted
    shell read, with the same rule parsing the file loaders apply.
    """
    source = f"{label} (bootstrap)"
    documents = {
        direction: _TypeMapDocument(source, block[key])
        for direction, key in _BLOCK_KEYS.items()
        if key in block
    }
    return _parse_type_mapper(label, documents.get("read"), documents.get("write"))


def connector_definition_dir(connectors_dir: Path, slug: str) -> Path:
    """Return the connector's ``definition/`` directory (``{slug}/definition``)."""
    return connectors_dir / slug / "definition"


def load_type_map(connectors_dir: Path, slug: str) -> TypeMapper:
    """Load and parse a connector's type-map documents.

    Raises ``TypeMapNotFoundError`` when the directory has no read or write
    document, ``InvalidTypeMapError`` when any document is malformed or two
    declare the same direction.
    """
    definition = connector_definition_dir(connectors_dir, slug)
    mapper = _load_type_mapper(definition, f"connector {slug!r}", slug)
    if mapper is None:
        raise TypeMapNotFoundError(
            f"connector {slug!r}: required type-map not found: no "
            f"{_TYPE_MAP_GLOB} document in {definition} declares direction "
            f"'read' or 'write'"
        )
    return mapper


def load_connection_type_map(
    connections_dir: Path, connection_id: str
) -> TypeMapper | None:
    """Load a connection-scoped type map if present.

    Lives under ``connections/{connection_id}/definition/`` and governs type
    translation for private endpoints under the same
    ``connections/{connection_id}/definition/endpoints/`` tree. No read or
    write document → ``None``; the caller decides whether that's an error (private
    endpoints referenced) or fine (pipeline only uses public endpoints).
    """
    return _load_type_mapper(
        connections_dir / connection_id / "definition",
        f"connection {connection_id!r}",
        f"connection:{connection_id}",
    )
