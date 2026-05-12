"""Pipeline configuration preparation.

Loads pipeline, stream, connection, connector, and endpoint artifacts
from the on-disk modular layout and assembles the in-memory config the
runtime consumes.

Layout (rooted at the project containing ``pipelines/manifest.json``):

    pipelines/manifest.json
    pipelines/<pipeline-alias>/pipeline.json
    pipelines/<pipeline-alias>/streams/<stream-alias>.json
    connections/<connection-alias>/connection.json
    connections/<connection-alias>/.secrets/credentials.json
    connections/<connection-alias>/definition/endpoints/<alias>.json   (private endpoints)
    connections/<connection-alias>/definition/type-map.json            (optional)
    connectors/<connector-alias>/definition/connector.json
    connectors/<connector-alias>/definition/type-map.json
    connectors/<connector-alias>/definition/endpoints/<alias>.json   (public endpoints)

Identity is alias-based throughout. Cross-document references carry
aliases (matching the directory name on disk):

    pipeline.connections.source       -> "<connection-alias>"
    pipeline.connections.destinations -> ["<connection-alias>", ...]
    pipeline.streams                  -> ["<stream-alias>", ...]
    stream.pipeline_id                -> "<pipeline-alias>"
    stream.source.endpoint_ref        -> {scope, connection_alias, alias}
    stream.destinations[].endpoint_ref-> {scope, connection_alias, alias}

Every artifact is JSON-Schema validated against the published Analitiq
contract before it is consumed.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

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
from src.engine.type_map import (
    InvalidTypeMapError,
    TypeMapper,
    load_connection_type_map,
    load_type_map,
)
from src.models.stream import EndpointRef
from src.secrets import LocalFileSecretsResolver, SecretsResolver
from src.shared.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)


VALID_CONNECTOR_KINDS = {"api", "database", "file", "s3", "stdout"}


# ---------------------------------------------------------------------------
# Dataclasses for internal state
# ---------------------------------------------------------------------------


@dataclass
class _ConnectionRecord:
    """One entry in the on-disk connection index, keyed by alias."""

    alias: str               # directory name under connections/
    connector_alias: str
    raw_config: Dict[str, Any]


@dataclass
class _StreamRecord:
    """One entry in the on-disk stream index, keyed by alias."""

    alias: str
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

        # Indexes built once per create_config() call, both keyed by alias.
        self._connection_records: Dict[str, _ConnectionRecord] = {}
        self._stream_records: Dict[str, _StreamRecord] = {}

        # Resolved artifacts
        self._resolved_connections: Dict[str, ConnectionRuntime] = {}    # by connection alias
        self._resolved_endpoints: Dict[EndpointRef, Dict[str, Any]] = {}
        self._loaded_connectors: Dict[str, Dict[str, Any]] = {}          # by connector_alias
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
        with manifest_path.open() as fh:
            try:
                manifest = json.load(fh)
            except json.JSONDecodeError as err:
                raise ValueError(f"Invalid JSON in {manifest_path}: {err}") from err
        if not isinstance(manifest, Mapping) or "pipelines" not in manifest:
            raise ValueError("manifest.json missing required key: 'pipelines'")
        return manifest

    def _find_manifest_entry(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Match manifest entry by alias (``PIPELINE_ID`` is the alias)."""
        target = self.pipeline_id_input
        for entry in manifest["pipelines"]:
            if entry.get("alias") == target:
                return entry
        choices = sorted(e.get("alias") or "?" for e in manifest["pipelines"])
        raise ValueError(
            f"Pipeline alias {target!r} not found in manifest. Available: {choices}"
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
        with path.open() as fh:
            try:
                document = json.load(fh)
            except json.JSONDecodeError as err:
                raise ValueError(f"Invalid JSON in {path}: {err}") from err
        validate_artifact("pipeline", document, source=str(path))
        self._manifest_entry = dict(entry)
        self._pipeline_dir = path.parent
        self._pipeline_document = document
        logger.info("Loaded pipeline document: %s", path)
        return document

    # ------------------------------------------------------------------
    # On-disk indexes (UUID -> directory / file)
    # ------------------------------------------------------------------

    def _build_connection_index(self) -> None:
        """Scan ``connections/`` and index every connection by its directory alias."""
        self._connection_records.clear()
        connections_dir = self._paths["connections"]
        if not connections_dir.is_dir():
            raise FileNotFoundError(
                f"Connections directory not found: {connections_dir}"
            )

        for child in sorted(connections_dir.iterdir()):
            if not child.is_dir():
                continue
            connection_file = child / "connection.json"
            if not connection_file.is_file():
                continue

            raw_config = load_connection_file(connection_file)
            validate_artifact("connection", raw_config, source=str(connection_file))

            connector_alias = raw_config.get("connector_alias")
            if not connector_alias:
                raise ValueError(
                    f"Connection {connection_file} missing required field "
                    f"'connector_alias'"
                )
            alias = raw_config.get("alias") or child.name
            if alias != child.name:
                raise ValueError(
                    f"Connection alias mismatch in {connection_file}: "
                    f"directory={child.name!r} but document alias={alias!r}"
                )
            if alias in self._connection_records:
                raise ValueError(
                    f"Duplicate connection alias {alias!r} under {connections_dir}"
                )
            self._connection_records[alias] = _ConnectionRecord(
                alias=alias,
                connector_alias=connector_alias,
                raw_config=raw_config,
            )

        logger.info(
            "Indexed %d connection(s) under %s",
            len(self._connection_records),
            connections_dir,
        )

    def _build_stream_index(self) -> None:
        """Scan the active pipeline's ``streams/`` and index every stream by alias."""
        self._stream_records.clear()
        if self._pipeline_dir is None:
            raise RuntimeError("Pipeline document must be loaded before stream indexing")
        streams_dir = self._pipeline_dir / "streams"
        if not streams_dir.is_dir():
            raise FileNotFoundError(f"Streams directory not found: {streams_dir}")

        for stream_file in sorted(streams_dir.glob("*.json")):
            with stream_file.open() as fh:
                try:
                    document = json.load(fh)
                except json.JSONDecodeError as err:
                    raise ValueError(f"Invalid JSON in {stream_file}: {err}") from err
            validate_artifact("stream", document, source=str(stream_file))
            alias = document.get("alias")
            if not alias:
                raise ValueError(
                    f"Stream document {stream_file} missing 'alias'"
                )
            if alias in self._stream_records:
                raise ValueError(
                    f"Duplicate stream alias {alias!r} in {streams_dir} "
                    f"({self._stream_records[alias].file_path}, {stream_file})"
                )
            self._stream_records[alias] = _StreamRecord(
                alias=alias,
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
            directory_by_alias={
                alias: rec.alias for alias, rec in self._connection_records.items()
            },
            connector_alias_by_alias={
                alias: rec.connector_alias
                for alias, rec in self._connection_records.items()
            },
        )

    # ------------------------------------------------------------------
    # Connector + connection materialization (in-memory only)
    # ------------------------------------------------------------------

    def _load_connector(self, connector_alias: str) -> Dict[str, Any]:
        if connector_alias in self._loaded_connectors:
            return self._loaded_connectors[connector_alias]
        connector_dir = self._paths["connectors"] / connector_alias
        connector_file = connector_dir / "definition" / "connector.json"
        if not connector_file.is_file():
            connector_file = (
                self._paths["connectors"]
                / f"connector-{connector_alias}"
                / "definition"
                / "connector.json"
            )
        if not connector_file.is_file():
            raise FileNotFoundError(
                f"Connector definition not found for {connector_alias!r}"
            )
        document = load_connector_definition(
            connector_alias, self._paths["connectors"]
        )
        validate_artifact("connector", document, source=str(connector_file))
        self._loaded_connectors[connector_alias] = document

        # Connector type-map is optional from this layer's perspective:
        # API-only connectors that never expose SQL native types do not
        # ship one. The loader raises InvalidTypeMapError when the file
        # is absent; downstream code that actually needs a mapper (e.g.
        # the database destination) will surface the missing-mapper
        # condition with a precise error.
        try:
            self._connector_type_mappers[connector_alias] = load_type_map(
                self._paths["connectors"], connector_alias
            )
        except InvalidTypeMapError as err:
            logger.info(
                "No connector type-map for %r (%s); native SQL types will not "
                "be resolvable for this connector",
                connector_alias,
                err,
            )
            self._connector_type_mappers[connector_alias] = None  # type: ignore[assignment]

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

    def _resolve_connection_by_alias(self, alias: str) -> ConnectionRuntime:
        """Materialize (or return cached) ConnectionRuntime for a connection alias."""
        record = self._connection_records.get(alias)
        if record is None:
            raise ValueError(
                f"Connection alias {alias!r} is not present under "
                f"{self._paths['connections']}; known: {sorted(self._connection_records)}"
            )
        if alias in self._resolved_connections:
            return self._resolved_connections[alias]

        connector = self._load_connector(record.connector_alias)
        kind = connector.get("kind")
        if kind not in VALID_CONNECTOR_KINDS:
            raise ValueError(
                f"Connector {record.connector_alias!r} has invalid kind {kind!r}; "
                f"expected one of {sorted(VALID_CONNECTOR_KINDS)}"
            )

        runtime = ConnectionRuntime(
            raw_config=record.raw_config,
            connection_id=alias,
            connector_type=kind,
            resolver=self._create_secrets_resolver(alias),
            connector_definition=connector,
            connector_type_mapper=self._connector_type_mappers.get(record.connector_alias),
            connection_type_mapper=self._connection_type_mapper(alias),
        )
        self._resolved_connections[alias] = runtime
        logger.info(
            "Resolved connection: alias=%s connector=%s",
            alias,
            record.connector_alias,
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

    def create_config(
        self,
    ) -> Tuple[
        Dict[str, Any],
        List[Dict[str, Any]],
        Dict[str, ConnectionRuntime],
        Dict[EndpointRef, Dict[str, Any]],
        List[Dict[str, Any]],
    ]:
        """Load and return the validated, resolved pipeline configuration.

        Returns a tuple of:

        * ``pipeline_config``: dict with ``alias``, ``status``, ``connections``,
          ``streams``, ``runtime``, ``engine``, ``schedule``, plus
          server-managed identity fields.
        * ``stream_configs``: list of fully-resolved stream dicts (the new
          contract shape, with ``_runtime`` and ``_endpoint`` injected on
          each source/destination for downstream convenience).
        * ``resolved_connections``: directory-alias-keyed dict of
          :class:`ConnectionRuntime` (one per saved connection used by
          the pipeline).
        * ``resolved_endpoints``: dict keyed by :class:`EndpointRef`.
        * ``connectors``: list of connector documents loaded.
        """
        pipeline_doc = self._load_pipeline_document()
        self._build_connection_index()
        self._build_stream_index()

        connections = pipeline_doc["connections"]
        source_alias = connections["source"]
        dest_aliases = list(connections.get("destinations") or [])
        if not dest_aliases:
            raise ValueError("Pipeline must declare at least one destination")

        self._resolve_connection_by_alias(source_alias)
        for dest_alias in dest_aliases:
            self._resolve_connection_by_alias(dest_alias)

        # Stream configs
        pipeline_stream_aliases = list(pipeline_doc.get("streams") or [])
        stream_configs: List[Dict[str, Any]] = []
        for stream_alias in pipeline_stream_aliases:
            record = self._stream_records.get(stream_alias)
            if record is None:
                raise ValueError(
                    f"pipeline.streams references {stream_alias!r} but no stream "
                    f"file in {self._pipeline_dir}/streams declares that alias; "
                    f"known: {sorted(self._stream_records)}"
                )
            stream_configs.append(self._build_stream_config(record))

        pipeline_alias = pipeline_doc.get("alias")
        pipeline_config = {
            "pipeline_id": pipeline_alias,
            "alias": pipeline_alias,
            "name": pipeline_doc.get("display_name") or pipeline_alias,
            "display_name": pipeline_doc.get("display_name"),
            "description": pipeline_doc.get("description"),
            "status": pipeline_doc.get("status", "draft"),
            "tags": pipeline_doc.get("tags") or [],
            "connections": {
                "source": source_alias,
                "destinations": dest_aliases,
            },
            "schedule": pipeline_doc.get("schedule") or {"type": "manual"},
            "engine": pipeline_doc.get("engine") or {"vcpu": 1, "memory": 8192},
            "runtime": pipeline_doc.get("runtime") or {},
        }

        connectors = list(self._loaded_connectors.values())
        logger.info(
            "Configuration assembled: pipeline=%s, streams=%d, connections=%d, "
            "endpoints=%d, connectors=%d",
            pipeline_alias,
            len(stream_configs),
            len(self._resolved_connections),
            len(self._resolved_endpoints),
            len(connectors),
        )
        return (
            pipeline_config,
            stream_configs,
            dict(self._resolved_connections),
            dict(self._resolved_endpoints),
            connectors,
        )

    # ------------------------------------------------------------------
    # Stream config construction
    # ------------------------------------------------------------------

    def _build_stream_config(self, record: _StreamRecord) -> Dict[str, Any]:
        """Translate a saved stream document into the runtime-facing dict."""
        document = record.raw_document
        alias = record.alias

        # ---- source ----
        source = dict(document["source"])
        source_endpoint_ref_dict = source.get("endpoint_ref")
        if not source_endpoint_ref_dict:
            raise ValueError(
                f"Stream {alias} source missing 'endpoint_ref'"
            )
        source_endpoint_ref = EndpointRef.from_dict(source_endpoint_ref_dict)
        source_runtime = self._resolve_connection_by_alias(
            source_endpoint_ref.connection_alias
        )
        source_endpoint = self._resolve_endpoint(source_endpoint_ref)

        source["endpoint_ref"] = source_endpoint_ref.to_dict()
        source["connection_ref"] = source_runtime.connection_id
        source["_runtime"] = source_runtime
        source["_endpoint"] = source_endpoint

        # ---- destinations ----
        destinations: List[Dict[str, Any]] = []
        for raw_dest in document.get("destinations") or []:
            dest = dict(raw_dest)
            dest_endpoint_ref_dict = dest.get("endpoint_ref")
            if not dest_endpoint_ref_dict:
                raise ValueError(
                    f"Stream {alias} destination missing 'endpoint_ref'"
                )
            dest_endpoint_ref = EndpointRef.from_dict(dest_endpoint_ref_dict)
            dest_runtime = self._resolve_connection_by_alias(
                dest_endpoint_ref.connection_alias
            )
            dest_endpoint = self._resolve_endpoint(dest_endpoint_ref)

            dest["endpoint_ref"] = dest_endpoint_ref.to_dict()
            dest["connection_ref"] = dest_runtime.connection_id
            dest["_runtime"] = dest_runtime
            dest["_endpoint"] = dest_endpoint
            destinations.append(dest)

        return {
            "stream_id": alias,
            "alias": alias,
            "display_name": document.get("display_name"),
            "description": document.get("description"),
            "pipeline_id": document.get("pipeline_id"),
            "status": document.get("status", "draft"),
            "is_enabled": document.get("status") == "active",
            "tags": document.get("tags") or [],
            "source": source,
            "destinations": destinations,
            "mapping": document.get("mapping") or {"assignments": []},
        }

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def get_resolved_connection(self, alias: str) -> ConnectionRuntime:
        if alias not in self._resolved_connections:
            raise KeyError(
                f"Connection {alias!r} not resolved; "
                f"known: {sorted(self._resolved_connections)}"
            )
        return self._resolved_connections[alias]

    def get_connectors(self) -> List[Dict[str, Any]]:
        return list(self._loaded_connectors.values())

    def get_connector_for_alias(self, alias: str) -> Dict[str, Any]:
        record = self._connection_records.get(alias)
        if record is None:
            raise KeyError(
                f"Connection alias {alias!r} not indexed; "
                f"known: {sorted(self._connection_records)}"
            )
        return self._loaded_connectors[record.connector_alias]
