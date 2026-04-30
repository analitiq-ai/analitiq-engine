"""
Pipeline Configuration Preparation

This module provides the PipelineConfigPrep class that handles loading and merging
of pipeline and stream configurations from a modular filesystem layout.

Architecture:

Configuration is assembled at runtime from separate files:
- Manifest: {paths.pipelines}/manifest.json (central index of all pipelines)
- Pipeline: {paths.pipelines}/{id}/pipeline.json (lightweight references)
- Streams: {paths.pipelines}/{id}/streams/{stream_id}.json (individual stream files)
- Connectors: {paths.connectors}/{slug}/definition/connector.json (downloaded from GitHub)
- Connections: {paths.connections}/{alias}/connection.json (user-created)
- Public endpoints: {paths.connectors}/{slug}/definition/endpoints/{name}.json
- Private endpoints: {paths.connections}/{alias}/definition/endpoints/{name}.json
- Secrets: {paths.connections}/{alias}/.secrets/credentials.json

Endpoint references use scoped path format:
- "connector:{slug}/{endpoint_name}" for public endpoints
- "connection:{alias}/{endpoint_name}" for private endpoints

Loading Flow:
1. Discover project root by locating pipelines/manifest.json
2. Load manifest.json, find entry matching PIPELINE_ID, enforce status=active
3. Load pipeline.json from the path specified in the manifest entry
4. Load individual stream files from {pipeline_dir}/streams/{stream_id}.json
5. For each connection alias in pipeline.connections:
   - Load connection config from connections/{alias}/connection.json
   - Load connector definition from connectors/{slug}/definition/connector.json
   - Create ConnectionRuntime with per-connection secrets resolver
6. For each stream, resolve endpoint_ref to endpoint JSON
7. Build stream configs with resolved connections and endpoints
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.config import (
    validate_pipeline_config,
    validate_connection_config,
)
from src.config.endpoint_resolver import resolve_endpoint_ref
from src.config.connection_loader import load_connection, load_connector_for_connection
from src.models.stream import EndpointRef
from src.engine.type_map import (
    TypeMapper,
    load_connection_type_map,
    load_type_map,
)
from src.secrets import (
    SecretsResolver,
    LocalFileSecretsResolver,
    InMemorySecretsResolver,
    SecretNotFoundError,
)
from src.shared.connection_runtime import ConnectionRuntime


logger = logging.getLogger(__name__)


VALID_CONNECTOR_TYPES = {"api", "database", "file", "s3", "stdout"}


class PipelineConfigPrep:
    """
    Orchestrates loading and preparation of pipeline and stream configurations.

    Configuration is discovered via manifest.json and assembled from modular files:
    - Manifest: {paths.pipelines}/manifest.json
    - Pipeline: {paths.pipelines}/{id}/pipeline.json
    - Streams: {paths.pipelines}/{id}/streams/{stream_id}.json
    - Connections: {paths.connections}/{alias}/connection.json
    - Connectors: {paths.connectors}/{slug}/definition/connector.json
    - Endpoints: resolved via scoped endpoint_ref strings

    Connection aliases (directory names) serve as connection identifiers.
    Secrets are colocated at connections/{alias}/.secrets/credentials.json.
    Only pipelines with status 'active' in the manifest can be executed.
    """

    def __init__(self):
        """Initialize PipelineConfigPrep."""
        self._paths = self._discover_paths()

        # Cache for pipeline file (loaded lazily)
        self._pipeline_file: Optional[Dict[str, Any]] = None

        # Manifest entry and pipeline directory (set during pipeline loading)
        self._manifest_entry: Optional[Dict[str, Any]] = None
        self._pipeline_dir: Optional[Path] = None

        # Cache for resolved connections and endpoints
        self._resolved_connections: Dict[str, ConnectionRuntime] = {}
        self._resolved_endpoints: Dict[EndpointRef, Dict[str, Any]] = {}

        # Cache for loaded connectors (keyed by slug)
        self._loaded_connectors: Dict[str, Dict[str, Any]] = {}

        # Per-connector type mappers (keyed by slug). Populated eagerly by
        # _load_connector so downstream code can look them up without
        # re-reading the filesystem. SSL handling is now driven by the
        # connector's `transports.<ref>.connect_args.ssl` lookup; no separate
        # mapper is loaded.
        self._type_mappers: Dict[str, TypeMapper] = {}

        # Per-connection type mappers (keyed by alias). Optional — absent
        # file means the connection only references public endpoints.
        self._connection_type_mappers: Dict[str, Optional[TypeMapper]] = {}

        self.pipeline_id = os.getenv("PIPELINE_ID", "")
        if not self.pipeline_id:
            raise RuntimeError("PIPELINE_ID environment variable is required")

        self.validate_environment()

        logger.info(f"Initialized PipelineConfigPrep for pipeline: {self.pipeline_id}")
        logger.info(
            f"Using paths: connectors={self._paths['connectors']}, "
            f"connections={self._paths['connections']}, "
            f"pipelines={self._paths['pipelines']}"
        )

    def _discover_paths(self) -> Dict[str, Path]:
        """Discover project root and derive convention-based directory paths.

        Searches for pipelines/manifest.json starting from CWD and walking
        up parent directories. The project root is the parent of the
        pipelines/ directory. connectors/ and connections/ are expected
        as siblings of pipelines/.

        Returns:
            Dict with Path objects for connectors, connections, and pipelines
        """
        current = Path.cwd()
        for _ in range(10):
            candidate = current / "pipelines" / "manifest.json"
            if candidate.exists():
                project_root = current
                return {
                    "connectors": project_root / "connectors",
                    "connections": project_root / "connections",
                    "pipelines": project_root / "pipelines",
                }
            if current.parent == current:
                break
            current = current.parent

        raise RuntimeError(
            "Could not find pipelines/manifest.json in current or parent directories. "
            "Ensure you are running from the project root."
        )

    def _create_secrets_resolver(self, alias: str) -> SecretsResolver:
        """Create a secrets resolver for a specific connection.

        Each connection has its own .secrets/ directory at
        connections/{alias}/.secrets/

        Args:
            alias: Connection alias (directory name)

        Returns:
            LocalFileSecretsResolver pointing to the connection's secrets dir
        """
        secrets_dir = self._paths["connections"] / alias / ".secrets"
        return LocalFileSecretsResolver(secrets_dir)

    # =========================================================================
    # Manifest and Pipeline File Loading
    # =========================================================================

    def _load_manifest(self) -> Dict[str, Any]:
        """Load the pipelines manifest.json.

        Returns:
            Parsed manifest dict with 'pipelines' key
        """
        manifest_path = self._paths["pipelines"] / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Pipeline manifest not found: {manifest_path}"
            )

        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in manifest {manifest_path}: {e}")

        if "pipelines" not in manifest:
            raise ValueError("manifest.json missing required key: 'pipelines'")

        return manifest

    def _find_manifest_entry(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Find the manifest entry matching self.pipeline_id.

        Args:
            manifest: Parsed manifest dict

        Returns:
            Manifest entry dict for the matched pipeline

        Raises:
            ValueError: If pipeline not found or not active
        """
        for entry in manifest["pipelines"]:
            if entry.get("pipeline_id") == self.pipeline_id:
                return entry

        available = [
            e.get("pipeline_id", "unknown")
            for e in manifest["pipelines"]
        ]
        raise ValueError(
            f"Pipeline '{self.pipeline_id}' not found in manifest. "
            f"Available pipelines: {sorted(available)}"
        )

    def _load_pipeline_file(self) -> Dict[str, Any]:
        """Load the pipeline configuration file via manifest lookup.

        Loads manifest.json, finds the entry for self.pipeline_id,
        enforces active status, and loads the pipeline file from the
        path specified in the manifest entry.

        Returns:
            Dict with pipeline and streams keys
        """
        if self._pipeline_file is not None:
            return self._pipeline_file

        manifest = self._load_manifest()
        entry = self._find_manifest_entry(manifest)

        status = entry.get("status", "")
        if status != "active":
            raise ValueError(
                f"Pipeline '{self.pipeline_id}' has status '{status}'. "
                f"Only 'active' pipelines can be executed."
            )

        path = self._paths["pipelines"] / entry["path"]
        if not path.exists():
            raise FileNotFoundError(f"Pipeline config not found: {path}")

        try:
            with open(path) as f:
                self._pipeline_file = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in pipeline config {path}: {e}")

        logger.info(f"Loaded pipeline config from: {path}")
        validate_pipeline_config(self._pipeline_file)

        self._manifest_entry = entry
        self._pipeline_dir = path.parent

        return self._pipeline_file

    def _load_local_pipeline(self) -> Dict[str, Any]:
        """Load pipeline section from the pipeline file."""
        pipeline_file = self._load_pipeline_file()
        pipeline = pipeline_file.get("pipeline")
        if not pipeline:
            raise ValueError(
                f"Pipeline file missing 'pipeline' key for pipeline_id: "
                f"{self.pipeline_id}"
            )
        return pipeline

    def _load_local_streams(self) -> List[Dict[str, Any]]:
        """Load stream configurations from individual stream files.

        Stream IDs come from the pipeline.streams array. Each stream is
        loaded from {pipeline_dir}/streams/{stream_id}.json.

        Returns:
            List of stream configuration dicts
        """
        pipeline_file = self._load_pipeline_file()
        pipeline = pipeline_file.get("pipeline", {})
        stream_ids = pipeline.get("streams", [])

        if not stream_ids:
            logger.warning("Pipeline has no streams defined in 'streams' array")
            return []

        streams_dir = self._pipeline_dir / "streams"
        if not streams_dir.is_dir():
            raise FileNotFoundError(
                f"Streams directory not found: {streams_dir}"
            )

        streams = []
        for stream_id in stream_ids:
            stream_path = streams_dir / f"{stream_id}.json"
            if not stream_path.exists():
                raise FileNotFoundError(
                    f"Stream config not found: {stream_path} "
                    f"(referenced by pipeline '{self.pipeline_id}')"
                )
            try:
                with open(stream_path) as f:
                    stream_config = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid JSON in stream config {stream_path}: {e}"
                )
            streams.append(stream_config)

        logger.info(f"Loaded {len(streams)} streams from {streams_dir}")
        return streams

    # =========================================================================
    # Connection and Connector Loading
    # =========================================================================

    def _load_connection(self, alias: str) -> Dict[str, Any]:
        """Load a connection configuration by alias.

        Args:
            alias: Connection alias (directory name)

        Returns:
            Connection configuration dict
        """
        config = load_connection(alias, self._paths["connections"])
        validate_connection_config(config)
        return config

    def _load_connector(self, connector_slug: str) -> Dict[str, Any]:
        """Load a connector definition by slug, with caching.

        Also eagerly loads the connector's ``type-map.json`` (required for
        the schema contract). SSL behavior is encoded in the connector's
        ``transports.<ref>.connect_args.ssl`` lookup and resolved lazily by
        the transport factory; no separate ssl-mode-map is read here.
        """
        if connector_slug in self._loaded_connectors:
            return self._loaded_connectors[connector_slug]

        connector = load_connector_for_connection(
            connector_slug, self._paths["connectors"]
        )
        self._loaded_connectors[connector_slug] = connector
        self._type_mappers[connector_slug] = load_type_map(
            self._paths["connectors"], connector_slug
        )
        return connector

    def get_type_mapper(self, connector_slug: str) -> TypeMapper:
        """Return the cached ``TypeMapper`` for a connector.

        Raises ``KeyError`` if the connector has not been loaded yet — the
        caller should always trigger loading via ``_resolve_connection`` or
        ``_load_connector`` first.
        """
        if connector_slug not in self._type_mappers:
            self._load_connector(connector_slug)
        return self._type_mappers[connector_slug]

    def _load_connection_type_mapper(self, alias: str) -> Optional[TypeMapper]:
        """Load (and cache) the connection's own type-map for private endpoints."""
        if alias not in self._connection_type_mappers:
            self._connection_type_mappers[alias] = load_connection_type_map(
                self._paths["connections"], alias
            )
        return self._connection_type_mappers[alias]

    def get_connection_type_mapper(self, alias: str) -> Optional[TypeMapper]:
        """Return the cached connection-scoped ``TypeMapper`` or ``None``."""
        return self._load_connection_type_mapper(alias)

    def get_connector_for_connection(
        self, config: Dict[str, Any], alias: str
    ) -> Dict[str, Any]:
        """Get the connector definition for a connection config.

        Args:
            config: Connection configuration dict (must have connector_slug)
            alias: Connection alias (for error messages)

        Returns:
            Connector definition dict
        """
        slug = config.get("connector_slug")
        if not slug:
            raise ValueError(
                f"Connection '{alias}' is missing 'connector_slug' field."
            )
        return self._load_connector(slug)

    def get_connector_type(self, config: Dict[str, Any], alias: str) -> str:
        """Get the connector_type for a connection.

        Args:
            config: Connection configuration dict
            alias: Connection alias (for error messages)

        Returns:
            The connector_type string
        """
        connector = self.get_connector_for_connection(config, alias)
        connector_type = connector.get("connector_type")
        if not connector_type:
            raise ValueError(
                f"Connector for '{alias}' is missing 'connector_type' field."
            )
        return connector_type

    def _resolve_connection(
        self, alias: str,
    ) -> ConnectionRuntime:
        """Resolve a connection by alias.

        Creates a ConnectionRuntime with the connector definition attached.
        The connector's ``transports`` block drives transport materialization
        when ``materialize()`` is later called by source/destination handlers.
        """
        if alias in self._resolved_connections:
            return self._resolved_connections[alias]

        config = self._load_connection(alias)
        connector = self.get_connector_for_connection(config, alias)
        connection_type = connector.get("connector_type")

        if connection_type not in VALID_CONNECTOR_TYPES:
            raise ValueError(
                f"Connector for '{alias}' has invalid connector_type: {connection_type!r}. "
                f"Expected one of: {sorted(VALID_CONNECTOR_TYPES)}"
            )

        resolver = self._create_secrets_resolver(alias)
        connector_slug = config.get("connector_slug")

        runtime = ConnectionRuntime(
            raw_config=config,
            connection_id=alias,
            connector_type=connection_type,
            resolver=resolver,
            connector_definition=connector,
            connector_type_mapper=self._type_mappers.get(connector_slug),
            connection_type_mapper=self._load_connection_type_mapper(alias),
        )

        self._resolved_connections[alias] = runtime
        logger.info(f"Resolved connection: {alias}")
        return runtime

    # =========================================================================
    # Endpoint Resolution
    # =========================================================================

    def _resolve_endpoint(self, endpoint_ref: Any) -> Dict[str, Any]:
        """Resolve an endpoint by its structured reference.

        Args:
            endpoint_ref: ``EndpointRef`` instance or equivalent dict
                ``{"scope", "identifier", "endpoint"}``.

        Returns:
            Resolved endpoint configuration dict
        """
        ref = EndpointRef.from_dict(endpoint_ref)
        if ref in self._resolved_endpoints:
            return self._resolved_endpoints[ref]

        endpoint = resolve_endpoint_ref(ref, self._paths)
        self._resolved_endpoints[ref] = endpoint
        logger.info(f"Resolved endpoint: {ref}")
        return endpoint

    # =========================================================================
    # Configuration Loading and Assembly
    # =========================================================================

    def load_pipeline_config(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Load and assemble complete pipeline and stream configurations.

        Returns:
            Tuple of (pipeline_config dict, list of stream_config dicts)
        """
        raw_pipeline = self._load_local_pipeline()

        raw_connections = raw_pipeline.get("connections", {})
        source_alias = raw_connections.get("source")
        dest_aliases = raw_connections.get("destinations", [])

        if not source_alias:
            raise ValueError("Pipeline must define connections.source")
        if not isinstance(source_alias, str):
            raise ValueError(
                f"connections.source must be a string (alias), got {type(source_alias).__name__}"
            )

        # Resolve source connection
        self._resolve_connection(source_alias)

        # Resolve destination connections
        for dest_alias in dest_aliases:
            if not isinstance(dest_alias, str):
                raise ValueError(
                    f"Each destination must be a string (alias), got {type(dest_alias).__name__}"
                )
            self._resolve_connection(dest_alias)

        # Build connections_config in the format downstream code expects
        connections_config = {
            "source": source_alias,
            "destinations": list(dest_aliases),
        }

        # Load streams
        raw_streams = self._load_local_streams()

        # Handle version
        version = raw_pipeline.get("version", 1)
        if isinstance(version, str):
            version = int(float(version))

        pipeline_config = {
            "version": version,
            "pipeline_id": self.pipeline_id,
            "name": raw_pipeline.get("name", ""),
            "description": raw_pipeline.get("description"),
            "status": raw_pipeline.get("status", "draft"),
            "tags": raw_pipeline.get("tags", []),
            "connections": connections_config,
            "schedule": raw_pipeline.get("schedule"),
            "engine": raw_pipeline["engine"],
            "runtime": raw_pipeline["runtime"],
            "created_at": raw_pipeline.get("created_at"),
            "updated_at": raw_pipeline.get("updated_at"),
        }

        # Build stream configs — only include streams listed in pipeline.streams
        pipeline_stream_ids = set(raw_pipeline.get("streams", []))
        if not pipeline_stream_ids:
            logger.warning("Pipeline has no streams defined in 'streams' array")

        stream_configs = []
        for raw_stream in raw_streams:
            stream_id = raw_stream.get("stream_id", "unknown")

            if stream_id not in pipeline_stream_ids:
                raise ValueError(
                    f"Stream '{stream_id}' is not listed in pipeline.streams. "
                    f"Expected one of: {sorted(pipeline_stream_ids)}."
                )

            stream_config = self._build_stream_config(
                raw_stream, connections_config,
            )
            stream_configs.append(stream_config)

        pipeline_name = pipeline_config["name"]
        logger.info(f"Loaded pipeline '{pipeline_name}' with {len(stream_configs)} streams")
        return pipeline_config, stream_configs

    def _build_stream_config(
        self,
        raw_stream: Dict[str, Any],
        connections_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build a stream config dict with resolved connections and endpoints."""
        stream_id = raw_stream["stream_id"]

        # Resolve source connection by alias
        source_ref = raw_stream["source"]["connection_ref"]
        source_connection = self._resolved_connections.get(source_ref)
        if not source_connection:
            raise ValueError(
                f"Connection '{source_ref}' not resolved for stream {stream_id}"
            )

        # Resolve source endpoint
        source_endpoint_ref = raw_stream["source"].get("endpoint_ref")
        if not source_endpoint_ref:
            raise ValueError(
                f"Stream {stream_id} source is missing 'endpoint_ref'"
            )
        source_endpoint = self._resolve_endpoint(source_endpoint_ref)

        # Build enriched source data
        source_data = raw_stream["source"].copy()
        source_data["_runtime"] = source_connection
        source_data["_endpoint"] = source_endpoint

        # Resolve destinations
        destinations_data = []
        for dest in raw_stream["destinations"]:
            dest_ref = dest["connection_ref"]
            dest_connection = self._resolved_connections.get(dest_ref)
            if not dest_connection:
                raise ValueError(
                    f"Connection '{dest_ref}' not resolved for stream {stream_id}"
                )

            dest_endpoint_ref = dest.get("endpoint_ref")
            if not dest_endpoint_ref:
                raise ValueError(
                    f"Stream {stream_id} destination '{dest_ref}' is missing 'endpoint_ref'"
                )
            dest_endpoint = self._resolve_endpoint(dest_endpoint_ref)

            dest_data = dest.copy()
            dest_data["_runtime"] = dest_connection
            dest_data["_endpoint"] = dest_endpoint
            destinations_data.append(dest_data)

        # Handle version
        version = raw_stream.get("version", 1)
        if isinstance(version, str):
            version = int(float(version))

        # Mapping is passed through verbatim — type resolution is owned by
        # the connectors' type-map.json files applied at ingest and
        # materialization time, not by a stream-level translation layer.
        return {
            "version": version,
            "stream_id": stream_id,
            "pipeline_id": raw_stream.get("pipeline_id", self.pipeline_id),
            "status": raw_stream.get("status", "draft"),
            "is_enabled": raw_stream.get("is_enabled", True),
            "source": self._normalize_source_config(source_data),
            "destinations": [
                self._normalize_destination_config(d) for d in destinations_data
            ],
            "mapping": raw_stream.get("mapping", {}) or {},
            "tags": raw_stream.get("tags"),
            "runtime": raw_stream.get("runtime"),
            "created_at": raw_stream.get("created_at"),
            "updated_at": raw_stream.get("updated_at"),
        }

    def _normalize_source_config(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize source configuration to match SourceConfig model."""
        replication = source_data.get("replication", {})

        endpoint_ref = EndpointRef.from_dict(source_data["endpoint_ref"])

        result = {
            "connection_ref": source_data["connection_ref"],
            "endpoint_ref": endpoint_ref.to_dict(),
            "primary_key": source_data.get("primary_key", []),
            "replication": {
                "method": replication.get("method", "incremental"),
                "cursor_field": replication.get("cursor_field", []),
            },
        }

        if source_data.get("source_schema_fingerprint"):
            result["source_schema_fingerprint"] = source_data["source_schema_fingerprint"]
        if replication.get("safety_window_seconds") is not None:
            result["replication"]["safety_window_seconds"] = replication["safety_window_seconds"]
        if replication.get("tie_breaker_fields"):
            result["replication"]["tie_breaker_fields"] = replication["tie_breaker_fields"]
        if source_data.get("parameters"):
            result["parameters"] = source_data["parameters"]

        return result

    def _normalize_destination_config(self, dest_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize destination configuration to match DestinationConfig model."""
        write = dest_data.get("write", {})

        endpoint_ref = EndpointRef.from_dict(dest_data["endpoint_ref"])

        result = {
            "connection_ref": dest_data["connection_ref"],
            "endpoint_ref": endpoint_ref.to_dict(),
            "write": {
                "mode": write.get("mode", "upsert"),
            },
        }

        if dest_data.get("target_schema_fingerprint"):
            result["target_schema_fingerprint"] = dest_data["target_schema_fingerprint"]
        if write.get("conflict_keys"):
            result["write"]["conflict_keys"] = write["conflict_keys"]
        if write.get("idempotency_key"):
            result["write"]["idempotency_key"] = write["idempotency_key"]
        if dest_data.get("batching"):
            batching = dest_data["batching"]
            result["batching"] = {
                "supported": batching.get("supported", False),
                "size": batching.get("size", 1),
            }

        return result

    def get_connectors(self) -> List[Dict[str, Any]]:
        """Get the list of connector definitions that were loaded during resolution.

        Returns:
            List of connector definition dicts
        """
        return list(self._loaded_connectors.values())

    def create_config(self) -> Tuple[
        Dict[str, Any],
        List[Dict[str, Any]],
        Dict[str, "ConnectionRuntime"],
        Dict[str, Dict[str, Any]],
        List[Dict[str, Any]],
    ]:
        """Load and return validated pipeline and stream configurations.

        Returns:
            Tuple of (pipeline_config, stream_configs, resolved_connections,
                      resolved_endpoints, connectors)
        """
        pipeline_config, stream_configs = self.load_pipeline_config()

        resolved_connections = dict(self._resolved_connections)
        resolved_endpoints = dict(self._resolved_endpoints)
        connectors = self.get_connectors()

        logger.info("Configuration loaded (secrets resolved at connection time)")

        return pipeline_config, stream_configs, resolved_connections, resolved_endpoints, connectors

    def validate_environment(self) -> None:
        """Validate that the environment is properly configured."""
        pipelines_path = self._paths["pipelines"]
        if not pipelines_path.exists():
            raise RuntimeError(
                f"Pipelines directory not found: {pipelines_path}"
            )
        logger.debug(f"Pipelines directory validated: {pipelines_path}")

    def get_resolved_connection(self, connection_ref: str) -> ConnectionRuntime:
        """Get a resolved connection by its alias."""
        if connection_ref not in self._resolved_connections:
            raise ValueError(f"Connection not resolved: {connection_ref}")
        return self._resolved_connections[connection_ref]

    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two configurations (override takes precedence)."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        return result
