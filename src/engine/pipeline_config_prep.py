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
    connections/<connection_id>/definition/endpoints/<endpoint_id>.json   (private endpoints)
    connections/<connection_id>/definition/type-map.json                  (optional)
    connectors/<connector_id>/definition/connector.json
    connectors/<connector_id>/definition/type-map.json
    connectors/<connector_id>/definition/endpoints/<endpoint_id>.json   (public endpoints)

Identity is ``*_id`` throughout. Cross-document references carry the id
that matches the on-disk directory name:

    pipeline.connections.source       -> "<connection_id>"
    pipeline.connections.destinations -> ["<connection_id>", ...]
    pipeline.streams                  -> ["<stream_id>", ...]
    stream.pipeline_id                -> "<pipeline_id>"
    stream.source.endpoint_ref        -> {scope, connection_id, endpoint_id[, x-*]}
    stream.destinations[].endpoint_ref-> {scope, connection_id, endpoint_id[, x-*]}

Every artifact is JSON-Schema validated against the published Analitiq
contract before it is consumed.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from src.config.schema_validator import validate as validate_artifact
from src.config.endpoint_resolver import (
    ConnectionLookup,
    resolve_endpoint_ref,
)
from src.config.connection_loader import (
    load_connection,
    load_connection_file,
    load_connector_definition,
)
from src.config.utils import load_json_file
from src.engine.type_map import (
    InvalidTypeMapError,
    TypeMapper,
    load_connection_type_map,
    load_type_map,
)
from src.models.stream import (
    Assignment,
    AssignmentTarget,
    AssignmentValue,
    ConstantValue,
    DatabasePagination,
    DestinationConfig,
    EndpointRef,
    ExecutionConfig,
    MappingConfig,
    ReplicationConfig,
    ReplicationMethod,
    SourceConfig,
    StreamFilter,
    WriteConfig,
    WriteMode,
)
from src.models.resolved import (
    BatchingConfig,
    ErrorHandlingConfig,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
    RuntimeConfig,
)
from src.secrets import LocalFileSecretsResolver, SecretsResolver
from src.shared.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)


VALID_CONNECTOR_KINDS = {"api", "database", "file", "s3", "stdout"}


# ---------------------------------------------------------------------------
# Dataclasses for internal state
# ---------------------------------------------------------------------------


@dataclass
class _ConnectionRecord:
    """One entry in the on-disk connection index, keyed by ``connection_id``."""

    connection_id: str       # directory name under connections/
    connector_id: str
    raw_config: Dict[str, Any]


@dataclass
class _StreamRecord:
    """One entry in the on-disk stream index, keyed by ``stream_id``."""

    stream_id: str
    file_path: Path
    raw_document: Dict[str, Any]


# ---------------------------------------------------------------------------
# PipelineConfigPrep
# ---------------------------------------------------------------------------


class PipelineConfigPrep:
    """Loads, validates, and assembles a pipeline's runtime configuration."""

    def __init__(self) -> None:
        self._paths = self._discover_paths()

        self.pipeline_id_input = os.getenv("PIPELINE_ID", "")
        if not self.pipeline_id_input:
            raise RuntimeError("PIPELINE_ID environment variable is required")

        # Populated during create_config()
        self._manifest_entry: Optional[Dict[str, Any]] = None
        self._pipeline_dir: Optional[Path] = None
        self._pipeline_document: Optional[Dict[str, Any]] = None

        # Indexes built once per create_config() call, keyed by id.
        self._connection_records: Dict[str, _ConnectionRecord] = {}      # by connection_id
        self._stream_records: Dict[str, _StreamRecord] = {}              # by stream_id

        # Resolved artifacts
        self._resolved_connections: Dict[str, ConnectionRuntime] = {}    # by connection_id
        self._resolved_endpoints: Dict[EndpointRef, Dict[str, Any]] = {}
        self._loaded_connectors: Dict[str, Dict[str, Any]] = {}          # by connector_id
        self._connector_type_mappers: Dict[str, TypeMapper] = {}
        self._connection_type_mappers: Dict[str, Optional[TypeMapper]] = {}

        logger.info(
            "PipelineConfigPrep initialized: PIPELINE_ID=%s, paths=%s",
            self.pipeline_id_input,
            {k: str(v) for k, v in self._paths.items()},
        )

    # ------------------------------------------------------------------
    # Project layout
    # ------------------------------------------------------------------

    @staticmethod
    def _discover_paths() -> Dict[str, Path]:
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

    def _load_manifest(self) -> Dict[str, Any]:
        manifest_path = self._paths["pipelines"] / "manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(f"Pipeline manifest not found: {manifest_path}")
        manifest = load_json_file(manifest_path)
        if not isinstance(manifest, Mapping) or "pipelines" not in manifest:
            raise ValueError("manifest.json missing required key: 'pipelines'")
        return manifest

    def _find_manifest_entry(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Match manifest entry by ``pipeline_id``."""
        target = self.pipeline_id_input
        for entry in manifest["pipelines"]:
            if entry.get("pipeline_id") == target:
                return entry
        choices = sorted(e.get("pipeline_id") or "?" for e in manifest["pipelines"])
        raise ValueError(
            f"Pipeline id {target!r} not found in manifest. Available: {choices}"
        )

    def _load_pipeline_document(self) -> Dict[str, Any]:
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

    def _build_connection_index(self, referenced_ids: List[str]) -> None:
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
                raise FileNotFoundError(
                    f"Connection directory not found: {child}"
                )
            connection_file = child / "connection.json"
            if not connection_file.is_file():
                raise FileNotFoundError(
                    f"Connection file not found: {connection_file}"
                )

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
                    f"directory={child.name!r} but document connection_id={doc_connection_id!r}"
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
        """Scan the active pipeline's ``streams/`` and index every stream by ``stream_id``."""
        self._stream_records.clear()
        if self._pipeline_dir is None:
            raise RuntimeError("Pipeline document must be loaded before stream indexing")
        streams_dir = self._pipeline_dir / "streams"
        if not streams_dir.is_dir():
            raise FileNotFoundError(f"Streams directory not found: {streams_dir}")

        for stream_file in sorted(streams_dir.glob("*.json")):
            document = load_json_file(stream_file)
            validate_artifact("stream", document, source=str(stream_file))
            stream_id = document.get("stream_id")
            if not stream_id:
                raise ValueError(
                    f"Stream document {stream_file} missing 'stream_id'"
                )
            if stream_id in self._stream_records:
                raise ValueError(
                    f"Duplicate stream_id {stream_id!r} in {streams_dir} "
                    f"({self._stream_records[stream_id].file_path}, {stream_file})"
                )
            self._stream_records[stream_id] = _StreamRecord(
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
                cid: rec.connector_id
                for cid, rec in self._connection_records.items()
            },
        )

    # ------------------------------------------------------------------
    # Connector + connection materialization (in-memory only)
    # ------------------------------------------------------------------

    def _load_connector(self, connector_id: str) -> Dict[str, Any]:
        if connector_id in self._loaded_connectors:
            return self._loaded_connectors[connector_id]
        connector_dir = self._paths["connectors"] / connector_id
        connector_file = connector_dir / "definition" / "connector.json"
        if not connector_file.is_file():
            connector_file = (
                self._paths["connectors"]
                / f"connector-{connector_id}"
                / "definition"
                / "connector.json"
            )
        if not connector_file.is_file():
            raise FileNotFoundError(
                f"Connector definition not found for {connector_id!r}"
            )
        document = load_connector_definition(
            connector_id, self._paths["connectors"]
        )
        validate_artifact("connector", document, source=str(connector_file))
        self._loaded_connectors[connector_id] = document

        # Connector type-map is optional from this layer's perspective:
        # API-only connectors that never expose SQL native types do not
        # ship one. The loader raises InvalidTypeMapError when the file
        # is absent; downstream code that actually needs a mapper (e.g.
        # the database destination) will surface the missing-mapper
        # condition with a precise error.
        try:
            self._connector_type_mappers[connector_id] = load_type_map(
                self._paths["connectors"], connector_id
            )
        except InvalidTypeMapError as err:
            logger.info(
                "No connector type-map for %r (%s); native SQL types will not "
                "be resolvable for this connector",
                connector_id,
                err,
            )
            self._connector_type_mappers[connector_id] = None  # type: ignore[assignment]

        return document

    def _connection_type_mapper(self, directory: str) -> Optional[TypeMapper]:
        if directory not in self._connection_type_mappers:
            self._connection_type_mappers[directory] = load_connection_type_map(
                self._paths["connections"], directory
            )
        return self._connection_type_mappers[directory]

    def _create_secrets_resolver(self, directory: str) -> SecretsResolver:
        secrets_dir = self._paths["connections"] / directory / ".secrets"
        return LocalFileSecretsResolver(secrets_dir)

    def _resolve_connection_by_id(self, connection_id: str) -> ConnectionRuntime:
        """Materialize (or return cached) ConnectionRuntime for a ``connection_id``."""
        record = self._connection_records.get(connection_id)
        if record is None:
            raise ValueError(
                f"Connection id {connection_id!r} is not present under "
                f"{self._paths['connections']}; known: {sorted(self._connection_records)}"
            )
        if connection_id in self._resolved_connections:
            return self._resolved_connections[connection_id]

        connector = self._load_connector(record.connector_id)
        kind = connector.get("kind")
        if kind not in VALID_CONNECTOR_KINDS:
            raise ValueError(
                f"Connector {record.connector_id!r} has invalid kind {kind!r}; "
                f"expected one of {sorted(VALID_CONNECTOR_KINDS)}"
            )

        runtime = ConnectionRuntime(
            raw_config=record.raw_config,
            connection_id=connection_id,
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

    def _resolve_endpoint(self, endpoint_ref: Any) -> Dict[str, Any]:
        ref = EndpointRef.from_dict(endpoint_ref)
        if ref in self._resolved_endpoints:
            return self._resolved_endpoints[ref]
        document = resolve_endpoint_ref(ref, self._paths, self._connection_lookup())
        # Dispatch to the variant endpoint schema (``api-endpoint`` or
        # ``database-endpoint``) by the document's declared ``$schema``
        # URL. The umbrella ``endpoint`` schema is not currently
        # published, and the variant schemas pin the expected URL anyway,
        # so this also catches mislabelled documents at validation time.
        schema_url = document.get("$schema") or ""
        if "api-endpoint" in schema_url:
            endpoint_kind = "api-endpoint"
        elif "database-endpoint" in schema_url:
            endpoint_kind = "database-endpoint"
        else:
            raise ValueError(
                f"Endpoint {ref!s} has no recognized $schema URL "
                f"({schema_url!r}); expected api-endpoint or database-endpoint"
            )
        validate_artifact(endpoint_kind, document, source=str(ref))
        self._resolved_endpoints[ref] = document
        logger.info("Resolved endpoint: %s", ref)
        return document

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def create_config(self) -> ResolvedPipeline:
        """Load and return the validated, resolved pipeline configuration.

        Returns a :class:`ResolvedPipeline` with all connections, endpoints,
        and stream configs resolved to typed runtime objects. This is the
        single point at which raw JSON is parsed; everything downstream uses
        typed attributes instead of dict key access.
        """
        pipeline_doc = self._load_pipeline_document()

        connections = pipeline_doc["connections"]
        source_id = connections["source"]
        dest_ids = list(connections.get("destinations") or [])
        if not dest_ids:
            raise ValueError("Pipeline must declare at least one destination")

        self._build_connection_index([source_id, *dest_ids])
        self._build_stream_index()

        self._resolve_connection_by_id(source_id)
        for dest_id in dest_ids:
            self._resolve_connection_by_id(dest_id)

        pipeline_stream_ids = list(pipeline_doc.get("streams") or [])
        resolved_streams: List[ResolvedStream] = []
        for stream_id in pipeline_stream_ids:
            record = self._stream_records.get(stream_id)
            if record is None:
                raise ValueError(
                    f"pipeline.streams references {stream_id!r} but no stream "
                    f"file in {self._pipeline_dir}/streams declares that id; "
                    f"known: {sorted(self._stream_records)}"
                )
            resolved_streams.append(self._build_resolved_stream(record))

        pipeline_id = pipeline_doc.get("pipeline_id")
        if not pipeline_id:
            raise ValueError(
                f"Pipeline document in {self._pipeline_dir} is missing required "
                f"field 'pipeline_id'"
            )
        raw_runtime = pipeline_doc.get("runtime") or {}
        runtime_cfg = RuntimeConfig(
            batching=_parse_batching_config(raw_runtime.get("batching") or {}),
            error_handling=_parse_error_handling_config(
                raw_runtime.get("error_handling") or {}
            ),
            buffer_size=int(raw_runtime.get("buffer_size", 5000)),
        )

        pipeline = ResolvedPipeline(
            pipeline_id=pipeline_id,
            display_name=pipeline_doc.get("display_name"),
            description=pipeline_doc.get("description"),
            status=pipeline_doc.get("status", "draft"),
            tags=list(pipeline_doc.get("tags") or []),
            source_connection_id=source_id,
            destination_connection_ids=dest_ids,
            streams=resolved_streams,
            runtime=runtime_cfg,
            schedule=pipeline_doc.get("schedule") or {"type": "manual"},
            engine_config=pipeline_doc.get("engine") or {"vcpu": 1, "memory": 8192},
            connections=dict(self._resolved_connections),
        )

        logger.info(
            "Configuration assembled: pipeline=%s, streams=%d, connections=%d, "
            "endpoints=%d",
            pipeline_id,
            len(resolved_streams),
            len(self._resolved_connections),
            len(self._resolved_endpoints),
        )
        return pipeline

    # ------------------------------------------------------------------
    # Stream resolution
    # ------------------------------------------------------------------

    def _build_resolved_stream(self, record: _StreamRecord) -> ResolvedStream:
        """Parse a saved stream document into a fully typed :class:`ResolvedStream`."""
        document = record.raw_document
        stream_id = record.stream_id

        raw_source = document["source"]
        source_endpoint_ref_dict = raw_source.get("endpoint_ref")
        if not source_endpoint_ref_dict:
            raise ValueError(f"Stream {stream_id} source missing 'endpoint_ref'")

        source_endpoint_ref = EndpointRef.from_dict(source_endpoint_ref_dict)
        source_runtime = self._resolve_connection_by_id(
            source_endpoint_ref.connection_id
        )
        source_endpoint = self._resolve_endpoint(source_endpoint_ref)

        resolved_source = ResolvedSource(
            config=_parse_source_config(raw_source),
            runtime=source_runtime,
            endpoint=source_endpoint,
        )

        resolved_destinations: List[ResolvedDestination] = []
        for raw_dest in document.get("destinations") or []:
            dest_endpoint_ref_dict = raw_dest.get("endpoint_ref")
            if not dest_endpoint_ref_dict:
                raise ValueError(
                    f"Stream {stream_id} destination missing 'endpoint_ref'"
                )
            dest_endpoint_ref = EndpointRef.from_dict(dest_endpoint_ref_dict)
            dest_runtime = self._resolve_connection_by_id(
                dest_endpoint_ref.connection_id
            )
            dest_endpoint = self._resolve_endpoint(dest_endpoint_ref)

            resolved_destinations.append(ResolvedDestination(
                config=_parse_destination_config(raw_dest),
                runtime=dest_runtime,
                endpoint=dest_endpoint,
            ))

        if not resolved_destinations:
            raise ValueError(
                f"Stream {stream_id} has no destinations; an executable stream "
                f"requires at least one"
            )

        stream_pipeline_id = document.get("pipeline_id") or ""
        if not stream_pipeline_id:
            logger.warning(
                "Stream %s has no 'pipeline_id' field; state key will fall back to 'unknown-stream'",
                stream_id,
            )

        return ResolvedStream(
            stream_id=stream_id,
            pipeline_id=stream_pipeline_id,
            display_name=document.get("display_name"),
            description=document.get("description"),
            status=document.get("status", "draft"),
            tags=list(document.get("tags") or []),
            source=resolved_source,
            destinations=resolved_destinations,
            mapping=_parse_mapping_config(document.get("mapping") or {}),
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

    def get_connectors(self) -> List[Dict[str, Any]]:
        return list(self._loaded_connectors.values())

    def get_connector_for_connection(self, connection_id: str) -> Dict[str, Any]:
        record = self._connection_records.get(connection_id)
        if record is None:
            raise KeyError(
                f"Connection id {connection_id!r} not indexed; "
                f"known: {sorted(self._connection_records)}"
            )
        return self._loaded_connectors[record.connector_id]


# ---------------------------------------------------------------------------
# Typed parsers: raw JSON dict -> dataclass (called once, at load time)
# ---------------------------------------------------------------------------


def _parse_batching_config(raw: Dict[str, Any]) -> BatchingConfig:
    return BatchingConfig(
        batch_size=int(raw.get("batch_size", 1000)),
        max_concurrent_batches=int(raw.get("max_concurrent_batches", 3)),
    )


def _parse_error_handling_config(raw: Dict[str, Any]) -> ErrorHandlingConfig:
    return ErrorHandlingConfig(
        strategy=raw.get("strategy", "fail"),
        max_retries=int(raw.get("max_retries", 3)),
        retry_delay_seconds=int(raw.get("retry_delay_seconds", 5)),
    )


def _parse_replication_config(raw: Dict[str, Any]) -> ReplicationConfig:
    method_str = raw.get("method", ReplicationMethod.FULL_REFRESH.value)
    try:
        method = ReplicationMethod(method_str)
    except ValueError as e:
        raise ValueError(
            f"Unknown replication method {method_str!r}; "
            f"expected one of {[m.value for m in ReplicationMethod]}"
        ) from e
    return ReplicationConfig(
        method=method,
        cursor_field=raw.get("cursor_field"),
        safety_window_seconds=raw.get("safety_window_seconds"),
        tie_breaker_fields=list(raw["tie_breaker_fields"])
        if raw.get("tie_breaker_fields")
        else None,
    )


def _parse_stream_filter(raw: Dict[str, Any]) -> StreamFilter:
    field = raw.get("field")
    if not field:
        raise ValueError(
            f"Stream filter entry is missing required 'field' key: {raw!r}"
        )
    return StreamFilter(
        field=field,
        operator=raw.get("operator", "eq"),
        value=raw.get("value"),
    )


def _parse_database_pagination(raw: Optional[Dict[str, Any]]) -> Optional[DatabasePagination]:
    if not raw:
        return None
    return DatabasePagination(
        type=raw.get("type", "offset"),
        page_size=raw.get("page_size"),
        order_by_field=raw.get("order_by_field"),
    )


def _parse_source_config(raw: Dict[str, Any]) -> SourceConfig:
    """Parse the raw ``source`` block from a stream document into :class:`SourceConfig`."""
    return SourceConfig(
        endpoint_ref=EndpointRef.from_dict(raw["endpoint_ref"]),
        selected_columns=list(raw["selected_columns"]) if raw.get("selected_columns") else None,
        filters=[_parse_stream_filter(f) for f in (raw.get("filters") or [])],
        replication=_parse_replication_config(raw.get("replication") or {}),
        database_pagination=_parse_database_pagination(raw.get("database_pagination")),
        primary_keys=list(raw["primary_keys"]) if raw.get("primary_keys") else None,
    )


def _parse_write_config(raw: Dict[str, Any]) -> WriteConfig:
    mode_str = raw.get("mode", WriteMode.UPSERT.value)
    try:
        mode = WriteMode(mode_str)
    except ValueError as e:
        raise ValueError(
            f"Unknown write mode {mode_str!r}; "
            f"expected one of {[m.value for m in WriteMode]}"
        ) from e
    conflict_keys = raw.get("conflict_keys")
    return WriteConfig(
        mode=mode,
        conflict_keys=conflict_keys if conflict_keys else None,
    )


def _parse_execution_config(raw: Optional[Dict[str, Any]]) -> Optional[ExecutionConfig]:
    if not raw:
        return None
    return ExecutionConfig(
        batch_size=raw.get("batch_size"),
        max_concurrent_batches=raw.get("max_concurrent_batches"),
    )


def _parse_destination_config(raw: Dict[str, Any]) -> DestinationConfig:
    """Parse the raw ``destination`` block from a stream document into :class:`DestinationConfig`."""
    return DestinationConfig(
        endpoint_ref=EndpointRef.from_dict(raw["endpoint_ref"]),
        write=_parse_write_config(raw.get("write") or {}),
        execution=_parse_execution_config(raw.get("execution")),
    )


def _parse_mapping_config(raw: Dict[str, Any]) -> MappingConfig:
    """Parse the raw ``mapping`` block from a stream document into :class:`MappingConfig`."""
    assignments = []
    for raw_a in raw.get("assignments") or []:
        raw_target = raw_a.get("target") or {}
        raw_value = raw_a.get("value") or {}

        target = AssignmentTarget(
            path=raw_target.get("path", ""),
            arrow_type=raw_target.get("arrow_type", "Utf8"),
            native_type=raw_target.get("native_type"),
            nullable=raw_target.get("nullable", True),
            properties=raw_target.get("properties"),
            items=raw_target.get("items"),
        )

        # Match _translate_assignment's semantics: a present-but-null
        # expression/constant is not a valid value block. Checking key
        # presence alone would let {"expression": null} through as a silent
        # null-fill (the transformer then defaults it to None on every row).
        value = AssignmentValue()
        expr = raw_value.get("expression")
        const = raw_value.get("constant")
        if expr is not None:
            value.expression = expr
        elif const is not None:
            value.constant = ConstantValue(
                arrow_type=const.get("arrow_type", "Utf8"),
                value=const.get("value"),
            )
        else:
            raise ValueError(
                f"Assignment value block must have a non-null 'expression' or "
                f"'constant': {raw_value!r}"
            )

        assignments.append(
            Assignment(target=target, value=value, validate=raw_a.get("validate"))
        )

    return MappingConfig(assignments=assignments)
