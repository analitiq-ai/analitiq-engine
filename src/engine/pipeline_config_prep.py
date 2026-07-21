"""Pipeline configuration preparation.

Loads pipeline, stream, connection, connector, and endpoint artifacts
from the on-disk modular layout and assembles the in-memory config the
runtime consumes.

Layout (rooted at the project containing ``pipelines/manifest.json``):

    pipelines/manifest.json
    pipelines/<pipeline_id>/pipeline.json
    pipelines/<pipeline_id>/streams/<stream_id>.json
    connections/<connection_id>/connection.json
    connections/<connection_id>/.secrets/credentials.json
    connections/<connection_id>/definition/endpoints/<endpoint_id>.json
        (private endpoints)
    connections/<connection_id>/definition/type-map-read.json (optional)
    connectors/<connector_id>/definition/connector.json
    connectors/<connector_id>/definition/type-map-read.json
    connectors/<connector_id>/definition/endpoints/<endpoint_id>.json (public endpoints)

Identity is ``*_id`` throughout. Cross-document references carry the id
that matches the on-disk directory name:

    pipeline.connections.source       -> "<connection_id>"
    pipeline.connections.destinations -> ["<connection_id>", ...]
    pipeline.streams                  -> ["<stream_id>", ...]
    stream.pipeline_id                -> "<pipeline_id>"
    stream.source.endpoint_ref        -> {scope, connection_id, endpoint_id}
    stream.destinations[].endpoint_ref-> {scope, connection_id, endpoint_id}
        (connection-scoped refs carry database_object; endpoint_id is
         server-derived from it)

Every artifact is JSON-Schema validated against the published Analitiq
contract before it is consumed.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from analitiq.contracts.endpoints import ApiEndpointDoc, DatabaseEndpointDoc
from analitiq.contracts.pipelines.config import RuntimeConfig as ContractRuntimeConfig
from analitiq.contracts.stream import ReplicationConfig as ContractReplicationConfig
from pydantic import BaseModel

from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets import SchemeSecretsResolver, SecretsResolver
from cdk.type_map import (
    TypeMapNotFoundError,
    TypeMapper,
    load_connection_type_map,
    load_type_map,
)
from src.config import settings
from src.config.connection_loader import load_connection_file, load_connector_definition
from src.config.endpoint_resolver import ConnectionLookup, resolve_endpoint_ref
from src.config.schema_validator import ContractValidationError, EndpointDocument
from src.config.schema_validator import validate as validate_artifact
from src.config.schema_validator import validate_bundle
from src.config.utils import load_json_file
from src.models.resolved import (
    BatchingConfig,
    ErrorHandlingConfig,
    PipelineConnections,
    ReplicationConfig,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
    RuntimeConfig,
)
from src.models.stream import EndpointRef

logger = logging.getLogger(__name__)


def _author_set(model: BaseModel, keys: tuple[str, ...]) -> dict[str, Any]:
    """Return ``{key: value}`` for keys the author explicitly set to a non-null value.

    The contract models fill omitted fields with their own defaults, so consult
    ``model_fields_set`` (a reliable author-intent signal as of
    analitiq-contract-models 1.0.0rc2, infra #938) to forward only
    author-provided values. A present-but-null value is treated as unset. The
    engine keeps its own defaults for the rest, so its precedence is preserved.
    """
    return {
        key: getattr(model, key)
        for key in keys
        if key in model.model_fields_set and getattr(model, key) is not None
    }


def _parse_runtime_config(raw: Mapping[str, Any]) -> RuntimeConfig:
    """Build the engine's :class:`RuntimeConfig` from a pipeline's runtime block.

    Reads the validated contract model and forwards only the fields the author
    explicitly set; every omitted key falls through to the engine's own default
    (sourced from :mod:`src.config.settings`, env-overridable). The engine
    deliberately keeps its own defaults -- e.g. error strategy ``fail`` -- rather
    than the contract's (``dlq``), so it must forward author-set values only, not
    the contract's defaults. Precedence: pipeline config > env var > engine default.
    """
    contract = ContractRuntimeConfig.model_validate(dict(raw))
    return RuntimeConfig(
        batching=BatchingConfig(
            **_author_set(contract.batching, ("batch_size", "max_concurrent_batches"))
        ),
        error_handling=ErrorHandlingConfig(
            **_author_set(
                contract.error_handling,
                ("strategy", "max_retries", "retry_delay_seconds"),
            )
        ),
        **_author_set(contract, ("buffer_size",)),
    )


def _parse_replication(raw_source: Mapping[str, Any]) -> ReplicationConfig | None:
    """Build the engine's :class:`ReplicationConfig` from a stream's source block.

    Returns ``None`` when no replication policy is present (full-refresh sources
    may omit it). Reads the validated contract model: ``method`` is
    contract-required; the optional cursor/tie-breaker fields carry through as
    ``None`` when absent (the engine has no settings default for these, so no
    author-intent filtering is needed).
    """
    raw = raw_source.get("replication")
    if not raw:
        return None
    contract = ContractReplicationConfig.model_validate(dict(raw))
    return ReplicationConfig(
        method=contract.method,
        cursor_field=contract.cursor_field,
        tie_breaker_fields=contract.tie_breaker_fields,
    )


# Matches the endpoint variant name in a $schema URL.
# The trailing boundary is either "/" or end-of-string so that both
# "https://schemas.analitiq.ai/api-endpoint/latest.json" and
# "https://schemas.analitiq.ai/api-endpoint" extract "api-endpoint".
_ENDPOINT_KIND_RE = re.compile(r"/([A-Za-z][\w-]*-endpoint)(?:/|$)")

# A stream reference in the pipeline's ``streams`` list may carry a trailing
# version suffix (e.g. ``{uuid}_v2``). The stream document's own ``stream_id``
# is always bare, so the suffix is split off for the index lookup and the
# integer rides onto every emitted checkpoint line. The engine never acts on
# the version; it is metadata the deployment uses to scope the durable cursor.
_STREAM_VERSION_RE = re.compile(r"_v(\d+)$")


def _split_stream_ref(ref: str) -> tuple[str, int]:
    """Split a stream reference into ``(bare_stream_id, version)``.

    A bare reference (no ``_v{n}`` suffix) is version 1: a stream that was
    never edited, or any locally hand-authored config with no versioning
    concept. Resolution and cursor keying use the bare id regardless.
    """
    match = _STREAM_VERSION_RE.search(ref)
    if not match:
        return ref, 1
    return ref[: match.start()], int(match.group(1))


# ---------------------------------------------------------------------------
# Dataclasses for internal state
# ---------------------------------------------------------------------------


@dataclass
class _ConnectionRecord:
    """One entry in the on-disk connection index, keyed by ``connection_id``."""

    connection_id: str  # directory name under connections/
    connector_id: str
    raw_config: dict[str, Any]


@dataclass
class _StreamRecord:
    """One entry in the on-disk stream index, keyed by ``stream_id``."""

    stream_id: str
    file_path: Path
    raw_document: dict[str, Any]


# ---------------------------------------------------------------------------
# PipelineConfigPrep
# ---------------------------------------------------------------------------


class PipelineConfigPrep:
    """Loads, validates, and assembles a pipeline's runtime configuration."""

    def __init__(self) -> None:
        """Resolve project paths and the PIPELINE_ID this run executes."""
        self._paths = self._discover_paths()

        self.pipeline_id_input = os.getenv("PIPELINE_ID", "")
        if not self.pipeline_id_input:
            raise RuntimeError("PIPELINE_ID environment variable is required")

        # Populated during create_config()
        self._manifest_entry: dict[str, Any] | None = None
        self._pipeline_dir: Path | None = None
        self._pipeline_document: dict[str, Any] | None = None

        # Indexes built once per create_config() call, keyed by id.
        self._connection_records: dict[str, _ConnectionRecord] = {}  # by connection_id
        self._stream_records: dict[str, _StreamRecord] = {}  # by stream_id

        # Resolved artifacts
        self._resolved_connections: dict[
            str, ConnectionRuntime
        ] = {}  # by connection_id
        self._resolved_endpoints: dict[EndpointRef, EndpointDocument] = {}
        self._loaded_connectors: dict[str, dict[str, Any]] = {}  # by connector_id
        self._connector_type_mappers: dict[str, TypeMapper] = {}
        self._connection_type_mappers: dict[str, TypeMapper | None] = {}

        logger.info(
            "PipelineConfigPrep initialized: PIPELINE_ID=%s, paths=%s",
            self.pipeline_id_input,
            {k: str(v) for k, v in self._paths.items()},
        )

    # ------------------------------------------------------------------
    # Project layout
    # ------------------------------------------------------------------

    @staticmethod
    def _discover_paths() -> dict[str, Path]:
        """Walk up from CWD until ``pipelines/manifest.json`` is found."""
        current = Path.cwd()
        for _ in range(10):
            candidate = current / "pipelines" / "manifest.json"
            if candidate.exists():
                return {
                    "connectors": current / "connectors",
                    "connections": current / "connections",
                    "pipelines": current / "pipelines",
                }
            if current.parent == current:
                break
            current = current.parent
        raise RuntimeError(
            "Could not find pipelines/manifest.json in current or parent "
            "directories. Run from the project root."
        )

    # ------------------------------------------------------------------
    # Manifest + pipeline document
    # ------------------------------------------------------------------

    def _load_manifest(self) -> dict[str, Any]:
        manifest_path = self._paths["pipelines"] / "manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(f"Pipeline manifest not found: {manifest_path}")
        manifest = load_json_file(manifest_path)
        if not isinstance(manifest, Mapping) or "pipelines" not in manifest:
            raise ValueError("manifest.json missing required key: 'pipelines'")
        return manifest

    def _find_manifest_entry(self, manifest: dict[str, Any]) -> dict[str, Any]:
        """Match manifest entry by ``pipeline_id``."""
        target = self.pipeline_id_input
        entry: dict[str, Any]
        for entry in manifest["pipelines"]:
            if entry.get("pipeline_id") == target:
                return entry
        choices = sorted(e.get("pipeline_id") or "?" for e in manifest["pipelines"])
        raise ValueError(
            f"Pipeline id {target!r} not found in manifest. Available: {choices}"
        )

    def _load_pipeline_document(self) -> dict[str, Any]:
        manifest = self._load_manifest()
        entry = self._find_manifest_entry(manifest)
        status = entry.get("status", "")
        if status != "active":
            raise ValueError(
                f"Pipeline {self.pipeline_id_input!r} has status {status!r}; "
                f"only 'active' pipelines can be executed."
            )
        path = self._paths["pipelines"] / entry["path"]
        if not path.is_file():
            raise FileNotFoundError(f"Pipeline document not found: {path}")
        document = load_json_file(path)
        validate_artifact("pipeline", document, source=str(path))
        self._manifest_entry = dict(entry)
        self._pipeline_dir = path.parent
        self._pipeline_document = document
        logger.info("Loaded pipeline document: %s", path)
        return document

    # ------------------------------------------------------------------
    # On-disk indexes (id -> directory / file)
    # ------------------------------------------------------------------

    def _build_connection_index(self, referenced_ids: list[str]) -> None:
        """Index only the connections referenced by the active pipeline.

        Earlier versions scanned every directory under ``connections/`` and
        validated each, so a single malformed connection from an unrelated
        pipeline broke startup. We now index just the ids the pipeline names
        in its ``connections.{source,destinations}``.
        """
        self._connection_records.clear()
        connections_dir = self._paths["connections"]
        if not connections_dir.is_dir():
            raise FileNotFoundError(
                f"Connections directory not found: {connections_dir}"
            )

        for connection_id in referenced_ids:
            if connection_id in self._connection_records:
                continue
            child = connections_dir / connection_id
            if not child.is_dir():
                raise FileNotFoundError(f"Connection directory not found: {child}")
            connection_file = child / "connection.json"
            if not connection_file.is_file():
                raise FileNotFoundError(f"Connection file not found: {connection_file}")

            raw_config = load_connection_file(connection_file)
            validate_artifact("connection", raw_config, source=str(connection_file))

            connector_id = raw_config.get("connector_id")
            if not connector_id:
                raise ValueError(
                    f"Connection {connection_file} missing required field "
                    f"'connector_id'"
                )
            doc_connection_id = raw_config.get("connection_id") or child.name
            if doc_connection_id != child.name:
                raise ValueError(
                    f"Connection id mismatch in {connection_file}: "
                    f"directory={child.name!r} but document "
                    f"connection_id={doc_connection_id!r}"
                )
            self._connection_records[child.name] = _ConnectionRecord(
                connection_id=child.name,
                connector_id=connector_id,
                raw_config=raw_config,
            )

        logger.info(
            "Indexed %d connection(s) under %s",
            len(self._connection_records),
            connections_dir,
        )

    def _build_stream_index(self) -> None:
        """Scan the active pipeline's ``streams/`` and index every stream.

        Streams are indexed by ``stream_id``.
        """
        self._stream_records.clear()
        if self._pipeline_dir is None:
            raise RuntimeError(
                "Pipeline document must be loaded before stream indexing"
            )
        streams_dir = self._pipeline_dir / "streams"
        if not streams_dir.is_dir():
            raise FileNotFoundError(f"Streams directory not found: {streams_dir}")

        for stream_file in sorted(streams_dir.glob("*.json")):
            document = load_json_file(stream_file)
            validate_artifact("stream", document, source=str(stream_file))
            stream_id = document.get("stream_id")
            if not stream_id:
                raise ValueError(f"Stream document {stream_file} missing 'stream_id'")
            # Key by the version-stripped base id so the index shares one key
            # space with pipeline.streams lookup (which strips ``_v{n}``) and the
            # bundle validator (which matches on base form).
            base_id = _split_stream_ref(stream_id)[0]
            if base_id in self._stream_records:
                raise ValueError(
                    f"Duplicate stream_id {base_id!r} in {streams_dir} "
                    f"({self._stream_records[base_id].file_path}, {stream_file})"
                )
            self._stream_records[base_id] = _StreamRecord(
                stream_id=stream_id,
                file_path=stream_file,
                raw_document=document,
            )

        logger.info(
            "Indexed %d stream(s) under %s",
            len(self._stream_records),
            streams_dir,
        )

    def _connection_lookup(self) -> ConnectionLookup:
        return ConnectionLookup(
            directory_by_id={
                cid: rec.connection_id for cid, rec in self._connection_records.items()
            },
            connector_id_by_id={
                cid: rec.connector_id for cid, rec in self._connection_records.items()
            },
        )

    # ------------------------------------------------------------------
    # Connector + connection materialization (in-memory only)
    # ------------------------------------------------------------------

    def _load_connector(self, connector_id: str) -> dict[str, Any]:
        if connector_id in self._loaded_connectors:
            return self._loaded_connectors[connector_id]
        connector_file = (
            self._paths["connectors"] / connector_id / "definition" / "connector.json"
        )
        if not connector_file.is_file():
            raise FileNotFoundError(
                f"Connector definition not found for {connector_id!r}"
            )
        document = load_connector_definition(connector_id, self._paths["connectors"])
        validate_artifact("connector", document, source=str(connector_file))
        self._loaded_connectors[connector_id] = document

        # Connector type-map is optional from this layer's perspective:
        # API-only connectors that never expose SQL native types do not ship
        # one. Only a genuinely ABSENT map (TypeMapNotFoundError) is benign and
        # downgraded to None; a present-but-malformed read or write map is a
        # real config error and propagates so CI catches it at load instead of
        # silently dropping the connector's type resolution.
        try:
            self._connector_type_mappers[connector_id] = load_type_map(
                self._paths["connectors"], connector_id
            )
        except TypeMapNotFoundError as err:
            logger.info(
                "No connector type-map for %r (%s); native SQL types will not "
                "be resolvable for this connector",
                connector_id,
                err,
            )
            self._connector_type_mappers[
                connector_id
            ] = None  # type: ignore[assignment]

        return document

    def _connection_type_mapper(self, directory: str) -> TypeMapper | None:
        if directory not in self._connection_type_mappers:
            self._connection_type_mappers[directory] = load_connection_type_map(
                self._paths["connections"], directory
            )
        return self._connection_type_mappers[directory]

    def _create_secrets_resolver(self, directory: str) -> SecretsResolver:
        connection_dir = self._paths["connections"] / directory
        return SchemeSecretsResolver(
            connection_dir,
            s3_endpoint_url=settings.s3_secrets_endpoint_url(),
            s3_region=settings.s3_secrets_region(),
        )

    def _resolve_connection_by_id(self, connection_id: str) -> ConnectionRuntime:
        """Materialize (or return cached) ConnectionRuntime for a ``connection_id``."""
        record = self._connection_records.get(connection_id)
        if record is None:
            raise ValueError(
                f"Connection id {connection_id!r} is not present under "
                f"{self._paths['connections']}; "
                f"known: {sorted(self._connection_records)}"
            )
        if connection_id in self._resolved_connections:
            return self._resolved_connections[connection_id]

        connector = self._load_connector(record.connector_id)
        # kind is a closed-enum discriminator validated by the connector
        # contract in _load_connector; whether that kind is runnable is the
        # worker registry's job (ConnectorNotRegisteredError). Config prep
        # neither re-checks the shape nor hard-codes a kind set.
        kind = connector["kind"]

        runtime = ConnectionRuntime(
            raw_config=record.raw_config,
            connection_id=connection_id,
            connector_id=record.connector_id,
            connector_type=kind,
            resolver=self._create_secrets_resolver(connection_id),
            connector_definition=connector,
            connector_type_mapper=self._connector_type_mappers.get(record.connector_id),
            connection_type_mapper=self._connection_type_mapper(connection_id),
        )
        self._resolved_connections[connection_id] = runtime
        logger.info(
            "Resolved connection: connection_id=%s connector=%s",
            connection_id,
            record.connector_id,
        )
        return runtime

    # ------------------------------------------------------------------
    # Endpoint resolution
    # ------------------------------------------------------------------

    def _resolve_endpoint(self, endpoint_ref: Any) -> EndpointDocument:
        """Resolve one endpoint reference to its typed contract document."""
        ref = EndpointRef.from_dict(endpoint_ref)
        if ref in self._resolved_endpoints:
            return self._resolved_endpoints[ref]
        document = resolve_endpoint_ref(ref, self._paths, self._connection_lookup())
        # Extract the endpoint variant name from the document's declared
        # ``$schema`` URL. The variant name is the path segment ending in
        # ``-endpoint`` (e.g. ``api-endpoint``, ``database-endpoint``).
        # ``validate_artifact`` then validates against that variant's
        # contract model; an unrecognised variant name fails loudly there
        # rather than here.
        schema_url = document.get("$schema") or ""
        match = _ENDPOINT_KIND_RE.search(schema_url)
        if not match:
            raise ValueError(
                f"Endpoint {ref!s} has no recognizable endpoint $schema URL "
                f"({schema_url!r}); $schema must contain an *-endpoint path segment "
                f"(e.g. api-endpoint, database-endpoint)"
            )
        endpoint_kind = match.group(1)
        try:
            model = validate_artifact(endpoint_kind, document, source=str(ref))
        except ContractValidationError:
            # ContractValidationError already embeds str(ref) as its source and
            # carries structured per-field errors; re-raise as-is to preserve type.
            raise
        except ValueError as exc:
            # validate_artifact raises plain ValueError for unknown artifact
            # kinds; add ref and $schema URL context which that error omits.
            raise ValueError(
                f"Endpoint {ref!s} ($schema={schema_url!r}, "
                f"kind={endpoint_kind!r}): {exc}"
            ) from exc
        if not isinstance(model, ApiEndpointDoc | DatabaseEndpointDoc):
            # A *-endpoint kind that maps to a non-endpoint contract model is
            # a wiring defect in the artifact-kind registry, not a document
            # problem; fail loud rather than carry an unexpected type.
            raise ValueError(
                f"Endpoint {ref!s}: artifact kind {endpoint_kind!r} validated "
                f"to {type(model).__name__}, not an endpoint document model"
            )
        # For a connection-scoped ref the endpoint_id is the server-derived
        # handle over database_object (the table identity the SQL source/
        # destination consumes). The bundle validator only proves a file named
        # {endpoint_id}.json exists; guard against a stale/mismatched file whose
        # contents point at a different table by requiring the loaded document's
        # own endpoint_id to equal the ref's.
        if ref.scope == "connection" and model.endpoint_id != ref.endpoint_id:
            raise ValueError(
                f"Endpoint {ref!s}: on-disk document declares endpoint_id "
                f"{model.endpoint_id!r}, which does not match the "
                f"reference's server-derived {ref.endpoint_id!r}; the endpoint "
                f"file does not describe the referenced table."
            )
        self._resolved_endpoints[ref] = model
        logger.info("Resolved endpoint: %s", ref)
        return model

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def create_config(
        self,
    ) -> tuple[
        ResolvedPipeline,
        list[ResolvedStream],
        dict[str, ConnectionRuntime],
        dict[EndpointRef, EndpointDocument],
        list[dict[str, Any]],
    ]:
        """Load and return the validated, resolved pipeline configuration.

        Returns a tuple of:

        * ``pipeline``: :class:`ResolvedPipeline` with pipeline-level config.
        * ``streams``: list of :class:`ResolvedStream` with typed
          source/destinations — ``ConnectionRuntime`` and the resolved
          endpoint document (a typed contract model) live as explicit
          fields, not dict keys.
        * ``resolved_connections``: dict keyed by ``connection_id`` of
          :class:`ConnectionRuntime` (one per saved connection used by
          the pipeline).
        * ``resolved_endpoints``: dict keyed by :class:`EndpointRef` of
          typed endpoint documents.
        * ``connectors``: list of connector documents loaded.
        """
        pipeline_doc = self._load_pipeline_document()

        connections = pipeline_doc["connections"]
        source_id = connections["source"]
        # The pipeline contract requires >= 1 destination (validated when the
        # pipeline document was loaded), so dest_ids is non-empty here.
        dest_ids = list(connections.get("destinations") or [])

        self._build_connection_index([source_id, *dest_ids])
        self._build_stream_index()

        self._resolve_connection_by_id(source_id)
        for dest_id in dest_ids:
            self._resolve_connection_by_id(dest_id)

        # pipeline_id is nullable in the contract: an authored pipeline.json may
        # omit it and rely on the manifest for executable identity. Fall back to
        # the manifest/env id (always present) so a schema-valid omitted id is
        # honored rather than rejected by ResolvedPipeline's guard. This is the
        # run-bundle identity, so it is what the bundle validator sees.
        pipeline_id = pipeline_doc.get("pipeline_id") or self.pipeline_id_input

        # Cross-document referential validation (published validator): every
        # stream/connection/connector/endpoint reference resolves, source and
        # destination roles are wired correctly, and the pipeline is runnable.
        # Per-document shape was validated as each artifact was loaded above.
        validate_bundle(
            self._assemble_bundle(pipeline_doc, pipeline_id),
            source=str(self._pipeline_dir),
        )

        # Stream configs. A reference may carry a ``_v{n}`` version suffix; the
        # bare id resolves the stream record (referential soundness is already
        # guaranteed by validate_bundle) and the version rides onto the emitted
        # checkpoint line.
        stream_configs: list[ResolvedStream] = [
            self._build_stream_config(self._stream_records[bare_id], stream_version)
            for bare_id, stream_version in (
                _split_stream_ref(ref) for ref in pipeline_doc.get("streams") or []
            )
        ]
        display_name = pipeline_doc.get("display_name")
        pipeline = ResolvedPipeline(
            pipeline_id=pipeline_id,
            name=display_name or pipeline_id,
            display_name=display_name,
            description=pipeline_doc.get("description"),
            status=pipeline_doc.get("status", "draft"),
            tags=pipeline_doc.get("tags") or [],
            connections=PipelineConnections(
                source=source_id,
                destinations=dest_ids,
            ),
            schedule=pipeline_doc.get("schedule") or {"type": "manual"},
            engine_config=pipeline_doc.get("engine") or {"vcpu": 1, "memory": 8192},
            runtime=_parse_runtime_config(pipeline_doc.get("runtime") or {}),
        )

        connectors = list(self._loaded_connectors.values())
        logger.info(
            "Configuration assembled: pipeline=%s, streams=%d, connections=%d, "
            "endpoints=%d, connectors=%d",
            pipeline_id,
            len(stream_configs),
            len(self._resolved_connections),
            len(self._resolved_endpoints),
            len(connectors),
        )
        return (
            pipeline,
            stream_configs,
            dict(self._resolved_connections),
            dict(self._resolved_endpoints),
            connectors,
        )

    # ------------------------------------------------------------------
    # Bundle assembly (for referential validation)
    # ------------------------------------------------------------------

    def _assemble_bundle(
        self, pipeline_doc: dict[str, Any], pipeline_id: str
    ) -> dict[str, Any]:
        """Assemble the identity-only run bundle the published validator checks.

        ``connectors`` and ``endpoints`` carry identity only (connector ids
        present; connection-scoped endpoint ``(connection_id, endpoint_id)``) --
        the validator resolves references between the parsed documents, not their
        contents. ``pipeline_id`` and each connection's ``connection_id`` are the
        resolved identities (see :meth:`create_config` and
        :meth:`_build_connection_index`) so an authored doc that omits its id --
        connection_id is server-assigned and optional in the authored contract,
        falling back to the directory name -- still resolves against the
        pipeline's references instead of being reported as a missing connection.

        ``status`` is forced to ``active``: the engine's execution gate is the
        manifest entry status (checked in :meth:`_load_pipeline_document`), so
        by construction this pipeline is being run because the manifest marks it
        active. The pipeline document's own ``status`` is optional/informational,
        so feeding the manifest-derived status keeps the validator's
        runnable-pipeline check aligned with the engine's actual gate rather than
        rejecting a document that omits or under-states its status.
        """
        return {
            "pipeline": {
                **pipeline_doc,
                "pipeline_id": pipeline_id,
                "status": "active",
            },
            "streams": [rec.raw_document for rec in self._stream_records.values()],
            "connections": [
                {**rec.raw_config, "connection_id": rec.connection_id}
                for rec in self._connection_records.values()
            ],
            "connectors": sorted(self._loaded_connectors),
            "endpoints": self._connection_scoped_endpoint_identities(),
        }

    def _connection_scoped_endpoint_identities(self) -> list[dict[str, str]]:
        """Identify each private endpoint document present on disk.

        Returns ``{scope, connection_id, endpoint_id}`` for every endpoint file
        under an indexed connection's ``definition/endpoints/``.
        """
        identities: list[dict[str, str]] = []
        for record in self._connection_records.values():
            endpoints_dir = (
                self._paths["connections"]
                / record.connection_id
                / "definition"
                / "endpoints"
            )
            if not endpoints_dir.is_dir():
                continue
            for endpoint_file in sorted(endpoints_dir.glob("*.json")):
                # Identity is the filename stem: resolution locates the doc as
                # ``endpoints/{endpoint_id}.json`` (resolve_endpoint_path), so the
                # stem IS the on-disk endpoint_id. Reading the file is
                # unnecessary and would let a malformed sibling abort the run.
                identities.append(
                    {
                        "scope": "connection",
                        "connection_id": record.connection_id,
                        "endpoint_id": endpoint_file.stem,
                    }
                )
        return identities

    # ------------------------------------------------------------------
    # Stream config construction
    # ------------------------------------------------------------------

    def _resolve_endpoint_block(
        self, block: Mapping[str, Any]
    ) -> tuple[EndpointRef, ConnectionRuntime, EndpointDocument]:
        """Resolve one stream side's ``endpoint_ref`` into its parts.

        ``endpoint_ref`` presence is guaranteed by per-document stream
        validation and the bundle validator; this resolves it to the
        connection runtime and the endpoint document it points at.
        """
        endpoint_ref = EndpointRef.from_dict(block["endpoint_ref"])
        runtime = self._resolve_connection_by_id(endpoint_ref.connection_id)
        endpoint = self._resolve_endpoint(endpoint_ref)
        return endpoint_ref, runtime, endpoint

    def _build_stream_config(
        self, record: _StreamRecord, stream_version: int
    ) -> ResolvedStream:
        """Translate a saved stream document into a typed :class:`ResolvedStream`."""
        document = record.raw_document
        stream_id = record.stream_id

        # ---- source ----
        raw_source = document["source"]
        (
            source_endpoint_ref,
            source_runtime,
            source_endpoint,
        ) = self._resolve_endpoint_block(raw_source)

        resolved_source = ResolvedSource(
            endpoint_ref=source_endpoint_ref,
            connection_ref=source_runtime.connection_id,
            runtime=source_runtime,
            endpoint_document=source_endpoint,
            stream_source=dict(raw_source),
            replication=_parse_replication(raw_source),
            primary_keys=list(raw_source.get("primary_keys") or []),
        )

        # ---- destinations ----
        resolved_destinations: list[ResolvedDestination] = []
        for raw_dest in document.get("destinations") or []:
            (
                dest_endpoint_ref,
                dest_runtime,
                dest_endpoint,
            ) = self._resolve_endpoint_block(raw_dest)

            resolved_destinations.append(
                ResolvedDestination(
                    endpoint_ref=dest_endpoint_ref,
                    connection_ref=dest_runtime.connection_id,
                    runtime=dest_runtime,
                    endpoint_document=dest_endpoint,
                    write=dict(raw_dest.get("write") or {}),
                )
            )

        return ResolvedStream(
            stream_id=stream_id,
            stream_version=stream_version,
            display_name=document.get("display_name"),
            description=document.get("description"),
            pipeline_id=document.get("pipeline_id"),
            status=document.get("status", "draft"),
            is_enabled=document.get("status") == "active",
            tags=document.get("tags") or [],
            source=resolved_source,
            destinations=resolved_destinations,
            mapping=document.get("mapping") or {"assignments": []},
        )

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def get_resolved_connection(self, connection_id: str) -> ConnectionRuntime:
        if connection_id not in self._resolved_connections:
            raise KeyError(
                f"Connection {connection_id!r} not resolved; "
                f"known: {sorted(self._resolved_connections)}"
            )
        return self._resolved_connections[connection_id]

    def get_connectors(self) -> list[dict[str, Any]]:
        return list(self._loaded_connectors.values())

    def get_connector_for_connection(self, connection_id: str) -> dict[str, Any]:
        record = self._connection_records.get(connection_id)
        if record is None:
            raise KeyError(
                f"Connection id {connection_id!r} not indexed; "
                f"known: {sorted(self._connection_records)}"
            )
        return self._loaded_connectors[record.connector_id]
