"""JSON Schema validation against the published Analitiq contract schemas.

The engine fetches each schema from ``schemas.analitiq.ai`` the first
time an artifact of that kind is loaded, caches it in-process for the
remainder of the run, and validates every artifact against it.

Each artifact kind maps to one URL:

    ``connector``          -> ``/connector/latest.json``
    ``connection``         -> ``/connection/latest.json``
    ``pipeline``           -> ``/pipeline/latest.json``
    ``stream``             -> ``/stream/latest.json``
    ``endpoint``           -> ``/endpoint/latest.json``    (umbrella, oneOf)
    ``api-endpoint``       -> ``/api-endpoint/latest.json``
    ``database-endpoint``  -> ``/database-endpoint/latest.json``

Set ``ANALITIQ_SCHEMA_BASE_URL`` to point at a different host (testing,
staging, mirror).
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

logger = logging.getLogger(__name__)


_DEFAULT_SCHEMAS_DOMAIN = "schemas.analitiq.ai"
_FETCH_TIMEOUT_SECONDS = 15

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


def _schema_base_url() -> str:
    return (os.getenv("ANALITIQ_SCHEMA_BASE_URL") or _DEFAULT_SCHEMA_BASE_URL).rstrip("/")


@lru_cache(maxsize=None)
def _load_schema(kind: str) -> Dict[str, Any]:
    if kind not in ARTIFACT_KINDS:
        raise ValueError(
            f"Unknown artifact kind {kind!r}; expected one of {ARTIFACT_KINDS}"
        )
    url = f"{_schema_base_url()}/{kind}/latest.json"
    try:
        with urllib.request.urlopen(url, timeout=_FETCH_TIMEOUT_SECONDS) as response:
            payload = response.read()
    except urllib.error.URLError as err:
        raise RuntimeError(
            f"Could not fetch {kind!r} schema from {url}: {err}"
        ) from err
    try:
        schema = json.loads(payload)
    except json.JSONDecodeError as err:
        raise RuntimeError(
            f"Schema fetched from {url} is not valid JSON: {err}"
        ) from err
    logger.debug("Loaded %r schema from %s", kind, url)
    return schema


def validate(kind: str, document: Dict[str, Any], *, source: str = "<inline>") -> None:
    """Validate ``document`` against the schema for ``kind``.

    Raises :class:`ContractValidationError` on failure. The ``source``
    parameter is woven into the error message so callers can include the
    file path or other context.

    Errors anchored at the top-level ``$schema`` field are dropped before
    raising. The ``$schema`` value is an informational pointer to the
    contract URL; whether the engine fetched ``schemas.analitiq.ai`` or
    ``schemas.analitiq.work`` for validation is independent of which host
    the document advertises, and the body of the contract is what
    actually governs correctness.
    """
    schema = _load_schema(kind)
    errors = [
        err
        for err in Draft202012Validator(schema).iter_errors(document)
        if tuple(err.absolute_path) != ("$schema",)
    ]
    errors.sort(key=lambda e: list(e.path))
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
