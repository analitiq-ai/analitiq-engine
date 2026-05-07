"""JSON Schema validation against the published Analitiq contract schemas.

Schemas are cached under ``src/schemas/cache/`` and are refreshed by
``scripts/sync_schemas.py``. The engine validates every artifact at load
time so contract drift surfaces with a clean error path instead of
appearing as a ``KeyError`` deep inside the runtime.

Each artifact kind maps to one cached file:

    ``connector``          -> ``connector.json``
    ``connection``         -> ``connection.json``
    ``pipeline``           -> ``pipeline.json``
    ``stream``             -> ``stream.json``
    ``endpoint``           -> ``endpoint.json``           (umbrella, oneOf)
    ``api-endpoint``       -> ``api-endpoint.json``
    ``database-endpoint``  -> ``database-endpoint.json``
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

logger = logging.getLogger(__name__)


_CACHE_DIR = Path(__file__).resolve().parent.parent / "schemas" / "cache"

ARTIFACT_KINDS = (
    "connector",
    "connection",
    "pipeline",
    "stream",
    "endpoint",
    "api-endpoint",
    "database-endpoint",
)


class ContractValidationError(ValueError):
    """Raised when an artifact fails JSON Schema validation."""

    def __init__(self, kind: str, source: str, errors: Iterable[ValidationError]):
        self.kind = kind
        self.source = source
        self.errors = list(errors)
        message_lines = [
            f"{kind!r} schema validation failed for {source}:",
        ]
        for err in self.errors[:10]:
            location = "/".join(str(p) for p in err.absolute_path) or "<root>"
            message_lines.append(f"  - {location}: {err.message}")
        if len(self.errors) > 10:
            message_lines.append(f"  ... and {len(self.errors) - 10} more")
        super().__init__("\n".join(message_lines))


@lru_cache(maxsize=None)
def _load_schema(kind: str) -> Dict[str, Any]:
    if kind not in ARTIFACT_KINDS:
        raise ValueError(
            f"Unknown artifact kind {kind!r}; expected one of {ARTIFACT_KINDS}"
        )
    schema_path = _CACHE_DIR / f"{kind}.json"
    if not schema_path.is_file():
        raise FileNotFoundError(
            f"Cached schema not found at {schema_path}; "
            f"run scripts/sync_schemas.py to populate the cache."
        )
    with schema_path.open() as fh:
        return json.load(fh)


def validate(kind: str, document: Dict[str, Any], *, source: str = "<inline>") -> None:
    """Validate ``document`` against the schema for ``kind``.

    Raises :class:`ContractValidationError` on failure. The ``source``
    parameter is woven into the error message so callers can include the
    file path or other context.
    """
    schema = _load_schema(kind)
    errors = sorted(
        Draft202012Validator(schema).iter_errors(document),
        key=lambda e: list(e.path),
    )
    if errors:
        raise ContractValidationError(kind, source, errors)
    logger.debug("Schema %r validated %s", kind, source)


def validate_file(kind: str, path: Path) -> Dict[str, Any]:
    """Convenience: read a JSON file, validate it, and return the parsed dict."""
    if not path.is_file():
        raise FileNotFoundError(f"Artifact not found: {path}")
    try:
        with path.open() as fh:
            document = json.load(fh)
    except json.JSONDecodeError as err:
        raise ValueError(f"Invalid JSON in {path}: {err}") from err
    validate(kind, document, source=str(path))
    return document
