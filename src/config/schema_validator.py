"""Offline contract validation against the published Analitiq models.

Each artifact kind is validated against a Pydantic model from
``analitiq-contract-models`` -- the same models the published JSON Schemas
at ``schemas.analitiq.ai`` are generated from. Validation is fully offline:
no network fetch, and no chance of the engine drifting from a "latest"
schema it downloaded separately from the models that produced it.

Each artifact kind maps to one model:

    ``connector``          -> ``ConnectorConfig``
    ``connection``         -> ``ConnectionInput``
    ``pipeline``           -> ``PipelineInput``
    ``stream``             -> ``StreamInput``
    ``api-endpoint``       -> ``ApiEndpointDoc``
    ``database-endpoint``  -> ``DatabaseEndpointDoc``

The pinned ``analitiq-contract-models`` version is the single source of
contract shape; bump it deliberately to adopt a new contract revision.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from analitiq.contracts.connection import ConnectionInput
from analitiq.contracts.connector import Connector
from analitiq.contracts.endpoints import ApiEndpointDoc, DatabaseEndpointDoc
from analitiq.contracts.pipelines.config import PipelineInput
from analitiq.contracts.stream import StreamInput
from analitiq.validator import validate_pipeline_bundle
from pydantic import TypeAdapter, ValidationError

from src.config.utils import load_json_file

logger = logging.getLogger(__name__)


# One model per artifact kind. The model is the authored-document ("Input")
# variant, matching what the engine loads from disk and what the published
# ``{kind}/latest.json`` schema is rendered from.
_MODELS: dict[str, type] = {
    "connector": Connector,
    "connection": ConnectionInput,
    "pipeline": PipelineInput,
    "stream": StreamInput,
    "api-endpoint": ApiEndpointDoc,
    "database-endpoint": DatabaseEndpointDoc,
}

ARTIFACT_KINDS = tuple(_MODELS)

_ADAPTERS: dict[str, TypeAdapter[Any]] = {
    kind: TypeAdapter(model) for kind, model in _MODELS.items()
}


class ContractValidationError(ValueError):
    """Raised when an artifact fails contract-model validation.

    ``errors`` holds the Pydantic error records (``ValidationError.errors()``)
    for the failing fields, already stripped of the informational ``$schema``
    field (see :func:`validate`).
    """

    def __init__(self, kind: str, source: str, errors: Sequence[Any]):
        self.kind = kind
        self.source = source
        self.errors = list(errors)
        message_lines = [f"{kind!r} contract validation failed for {source}:"]
        for err in self.errors[:10]:
            location = "/".join(str(p) for p in err["loc"]) or "<root>"
            message_lines.append(f"  - {location}: {err['msg']}")
        if len(self.errors) > 10:
            message_lines.append(f"  ... and {len(self.errors) - 10} more")
        super().__init__("\n".join(message_lines))


def _adapter_for(kind: str) -> TypeAdapter[Any]:
    try:
        return _ADAPTERS[kind]
    except KeyError:
        raise ValueError(
            f"Unknown artifact kind {kind!r}; expected one of {ARTIFACT_KINDS}"
        ) from None


def validate(kind: str, document: dict[str, Any], *, source: str = "<inline>") -> None:
    """Validate ``document`` against the contract model for ``kind``.

    Raises :class:`ContractValidationError` on failure. The ``source``
    parameter is woven into the error message so callers can include the
    file path or other context.

    Errors anchored at the top-level ``$schema`` field are dropped before
    raising. The ``$schema`` value is an informational pointer to the
    contract URL; whether a document advertises ``schemas.analitiq.ai`` or
    another host is independent of the contract body, which is what actually
    governs correctness.
    """
    adapter = _adapter_for(kind)
    try:
        adapter.validate_python(document)
    except ValidationError as exc:
        errors = [e for e in exc.errors() if tuple(e["loc"]) != ("$schema",)]
        if errors:
            raise ContractValidationError(kind, source, errors) from exc
    logger.debug("Contract %r validated %s", kind, source)


def validate_file(kind: str, path: Path) -> dict[str, Any]:
    """Read a JSON file, validate it, and return the parsed dict."""
    if not path.is_file():
        raise FileNotFoundError(f"Artifact not found: {path}")
    document = load_json_file(path)
    validate(kind, document, source=str(path))
    return document


class BundleValidationError(ValueError):
    """Raised when a pipeline run bundle fails referential validation.

    ``findings`` holds the error-severity records the published validator
    returned (``analitiq.validator.validate_pipeline_bundle``).
    """

    def __init__(self, source: str, findings: Sequence[dict[str, Any]]):
        self.source = source
        self.findings = list(findings)
        message_lines = [f"pipeline bundle referential validation failed for {source}:"]
        for f in self.findings[:10]:
            message_lines.append(f"  - {f.get('path', '')}: {f.get('message', '')}")
        if len(self.findings) > 10:
            message_lines.append(f"  ... and {len(self.findings) - 10} more")
        super().__init__("\n".join(message_lines))


def validate_bundle(bundle: dict[str, Any], *, source: str = "<pipeline>") -> None:
    """Validate cross-document referential integrity of an assembled run bundle.

    Delegates to the published validator's ``validate_pipeline_bundle`` (the
    single source of the referential rules -- stream/connection/connector/
    endpoint references resolve, source/destination role wiring, active-gate).
    Per-document shape is validated separately by :func:`validate`. Any finding
    that is not an advisory ``warning`` blocks (so a future higher-than-error
    severity cannot slip through the gate); advisory warnings are logged, never
    silently dropped. Raises :class:`BundleValidationError` on a blocking
    finding.
    """
    findings = validate_pipeline_bundle(bundle)
    blocking = [f for f in findings if f.get("severity") != "warning"]
    for f in findings:
        if f not in blocking:
            logger.warning(
                "Bundle advisory for %s [%s] %s: %s",
                source,
                f.get("severity"),
                f.get("path", ""),
                f.get("message", ""),
            )
    if blocking:
        raise BundleValidationError(source, blocking)
    logger.debug("Pipeline bundle referentially validated %s", source)
