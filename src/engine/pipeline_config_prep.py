"""
Pipeline Configuration Preparation

This module provides the PipelineConfigPrep class that handles loading and merging
of pipeline and stream configurations from local filesystem.

Architecture:

Configuration is loaded from a SINGLE consolidated file per pipeline:
- Consolidated file: {paths.pipelines}/{pipeline_id}.json
- Secrets: {paths.secrets}/{connection_id}.json (kept separate for security)

The consolidated file format:
{
    "pipeline": { ... },        # Full pipeline object
    "connections": [ ... ],     # List of connection objects (no secrets)
    "connectors": [ ... ],      # List of connector metadata
    "endpoints": [ ... ],       # List of endpoint definitions
    "streams": [ ... ]          # List of stream configurations
}

In cloud environments (dev/prod):
1. config_fetcher.py fetches all data from Lambda and writes consolidated file + secrets
2. PipelineConfigPrep then loads from the consolidated file

The ENV variable controls storage backends (state, logs, DLQ, metrics), NOT config loading.

Loading Flow:
1. Load paths from analitiq.yaml (only pipelines and secrets paths needed)
2. Load consolidated config from {paths.pipelines}/{pipeline_id}.json
3. Extract pipeline, connections, connectors, endpoints, streams from consolidated file
4. For each connection:
   - Load secrets from {paths.secrets}/{connection_id}.json
   - Expand ${placeholder} values with secrets
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.config import load_analitiq_config, validate_consolidated_config


def _get_connection_uuid(connections: Dict[str, Any], alias: str) -> Optional[str]:
    """Look up connection UUID by alias from a connections dict."""
    if alias in connections.get("source", {}):
        return connections["source"][alias]
    for dest in connections.get("destinations", []):
        if alias in dest:
            return dest[alias]
    return None
from src.secrets import (
    SecretsResolver,
    LocalFileSecretsResolver,
    InMemorySecretsResolver,
    SecretNotFoundError,
)
from src.shared.connection_runtime import ConnectionRuntime


logger = logging.getLogger(__name__)



class PipelineConfigPrep:
    """
    Orchestrates loading and preparation of pipeline and stream configurations.

    Configuration is loaded from a SINGLE consolidated file per pipeline:
    - Consolidated file: {paths.pipelines}/{pipeline_id}.json
    - Secrets: {paths.secrets}/{connection_id}.json

    The consolidated file contains:
    - pipeline: Full pipeline object with connections structure
    - connections: List of connection objects (no secrets)
    - connectors: List of connector metadata
    - endpoints: List of endpoint definitions
    - streams: List of stream configurations

    Connection format in pipeline config:
    - connections.source.{alias}: "connection_id" (string)
    - connections.destinations[].{alias}: "connection_id" (string)

    In cloud environments (dev/prod), config_fetcher.py writes the consolidated
    file and secrets before this class runs.

    The ENV variable only affects storage backends (state, logs, DLQ, metrics),
    not config loading.
    """

    def __init__(self):
        """Initialize PipelineConfigPrep."""
        # Load paths from analitiq.yaml (only pipelines and secrets needed)
        self._paths = self._load_paths_from_analitiq_yaml()

        # Cache for consolidated config (loaded lazily)
        self._consolidated_config: Optional[Dict[str, Any]] = None

        # Cache for resolved connections and endpoints
        self._resolved_connections: Dict[str, ConnectionRuntime] = {}
        self._resolved_endpoints: Dict[str, Dict[str, Any]] = {}

        # Secrets resolver for late-binding (initialized lazily)
        self._secrets_resolver: Optional[SecretsResolver] = None
        self._secrets_dir: Optional[Path] = None

        self.pipeline_id = os.getenv("PIPELINE_ID", "")
        if not self.pipeline_id:
            raise RuntimeError("PIPELINE_ID environment variable is required")

        # Validate environment
        self.validate_environment()

        logger.info(f"Initialized PipelineConfigPrep for pipeline: {self.pipeline_id}")
        logger.info(f"Using paths from analitiq.yaml: pipelines={self._paths['pipelines']}, "
                    f"secrets={self._paths['secrets']}")

    def _load_paths_from_analitiq_yaml(self) -> Dict[str, Path]:
        """
        Load directory paths from analitiq.yaml (single source of truth).

        Only requires 'pipelines' and 'secrets' paths since configuration
        is now loaded from a single consolidated file.

        Returns:
            Dict with Path objects for pipelines and secrets

        Raises:
            FileNotFoundError: If analitiq.yaml is not found
            ValueError: If required paths are missing
        """
        config = load_analitiq_config()
        paths_config = config.get("paths", {})

        required_paths = ["pipelines", "secrets"]
        missing = [p for p in required_paths if p not in paths_config]
        if missing:
            raise ValueError(f"analitiq.yaml missing required paths: {missing}")

        # Return Path objects (relative paths resolved from current working directory)
        return {
            "pipelines": Path(paths_config["pipelines"]),
            "secrets": Path(paths_config["secrets"]),
        }

    @property
    def secrets_resolver(self) -> SecretsResolver:
        """
        Get the secrets resolver for late-binding secret resolution.

        Returns:
            SecretsResolver instance appropriate for the current environment
        """
        if self._secrets_resolver is None:
            if self._secrets_dir is None:
                raise RuntimeError(
                    "Secrets directory not configured. "
                    "Call create_config() first or set _secrets_dir manually."
                )
            self._secrets_resolver = LocalFileSecretsResolver(self._secrets_dir)
            logger.debug(f"Created LocalFileSecretsResolver for: {self._secrets_dir}")
        return self._secrets_resolver

    # =========================================================================
    # Secrets Methods
    # =========================================================================

    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two configurations (override takes precedence)."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        return result

    # =========================================================================
    # Consolidated Config Loading
    # =========================================================================

    def _load_consolidated_config(self) -> Dict[str, Any]:
        """
        Load the consolidated pipeline configuration file.

        The consolidated file contains all related configuration:
        - pipeline: Full pipeline object
        - connections: List of connection objects
        - connectors: List of connector metadata
        - endpoints: List of endpoint definitions
        - streams: List of stream configurations

        Returns:
            Dict with consolidated configuration

        Raises:
            FileNotFoundError: If consolidated file doesn't exist
        """
        if self._consolidated_config is not None:
            return self._consolidated_config

        path = self._paths["pipelines"] / f"{self.pipeline_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"Pipeline config not found: {path}")

        try:
            with open(path, "r") as f:
                self._consolidated_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in consolidated config {path}: {e}")

        logger.info(f"Loaded consolidated config from: {path}")

        # Validate minimum required configuration elements
        validate_consolidated_config(self._consolidated_config)
        logger.info("Consolidated config validation passed")

        return self._consolidated_config

    # =========================================================================
    # Local File Loading Methods
    # =========================================================================

    def _load_local_pipeline(self) -> Dict[str, Any]:
        """Load pipeline configuration from consolidated file."""
        consolidated = self._load_consolidated_config()
        pipeline = consolidated.get("pipeline")
        if not pipeline:
            raise ValueError(
                f"Consolidated config missing 'pipeline' key for pipeline_id: "
                f"{self.pipeline_id}"
            )
        return pipeline

    def _load_local_streams(self) -> List[Dict[str, Any]]:
        """Load stream configurations from consolidated file.

        Streams are loaded from the 'streams' array in the consolidated config.
        """
        consolidated = self._load_consolidated_config()
        streams = consolidated.get("streams", [])
        logger.info(f"Loaded {len(streams)} streams from consolidated config")
        return streams

    # =========================================================================
    # Connection Loading Methods (from consolidated config)
    # =========================================================================

    def _load_connection_config(self, connection_id: str) -> Dict[str, Any]:
        """
        Load connection configuration by ID from consolidated config.

        Args:
            connection_id: Connection identifier

        Returns:
            Connection configuration dictionary

        Raises:
            FileNotFoundError: If connection not found in consolidated config
        """
        consolidated = self._load_consolidated_config()
        connections = consolidated.get("connections", [])

        for conn in connections:
            if conn.get("connection_id") == connection_id:
                return conn

        raise FileNotFoundError(
            f"Connection not found in consolidated config: {connection_id}"
        )

    def get_connector_for_connection(
        self, config: Dict[str, Any], connection_id: str
    ) -> Dict[str, Any]:
        """
        Get the connector definition for a connection.

        Args:
            config: Connection configuration dictionary
            connection_id: Connection identifier (for error messages)

        Returns:
            Connector definition dict with connector_type, driver, etc.

        Raises:
            ValueError: If connector cannot be found
        """
        connector_id = config.get("connector_id")
        if not connector_id:
            raise ValueError(
                f"Connection '{connection_id}' is missing 'connector_id' field."
            )

        consolidated = self._load_consolidated_config()
        connectors = consolidated.get("connectors", [])

        for connector in connectors:
            if connector.get("connector_id") == connector_id:
                return connector

        raise ValueError(
            f"Connector not found for connection '{connection_id}' "
            f"with connector_id '{connector_id}'."
        )

    def get_connector_type(self, config: Dict[str, Any], connection_id: str) -> str:
        """
        Get the connector_type for a connection.

        This is a convenience method that looks up the connector and returns
        its connector_type. Unlike _determine_connection_type(), this method
        does not validate the type against VALID_CONNECTOR_TYPES - it returns
        whatever connector_type is defined.

        Args:
            config: Connection configuration dictionary containing connector_id
            connection_id: Connection identifier (for error messages)

        Returns:
            The connector_type string (e.g., "api", "database", "file", "s3", "stdout")

        Raises:
            ValueError: If connector_id is missing or connector not found
        """
        connector = self.get_connector_for_connection(config, connection_id)
        connector_type = connector.get("connector_type")
        if not connector_type:
            connector_id = config.get("connector_id")
            raise ValueError(
                f"Connector '{connector_id}' is missing 'connector_type' field."
            )
        return connector_type

    def _determine_connection_type(self, config: Dict[str, Any], connection_id: str) -> str:
        """
        Determine connection type by looking up connector_type from connectors array.

        Args:
            config: Connection configuration dictionary
            connection_id: Connection identifier (for error messages)

        Returns:
            Connection type: "api", "database", "file", "s3", or "stdout"

        Raises:
            ValueError: If connection type cannot be determined
        """
        connector = self.get_connector_for_connection(config, connection_id)
        conn_type = connector.get("connector_type")

        if conn_type in self.VALID_CONNECTOR_TYPES:
            return conn_type

        connector_id = config.get("connector_id")
        raise ValueError(
            f"Connector '{connector_id}' has invalid or missing 'connector_type'. "
            f"Expected one of {sorted(self.VALID_CONNECTOR_TYPES)}, got: {conn_type!r}"
        )

    def _normalize_database_connection(
        self, config: Dict[str, Any], connector: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normalize database connection fields to match expected schema.

        Args:
            config: Raw database connection configuration
            connector: Connector definition with driver field

        Returns:
            Normalized configuration dictionary
        """
        result = config.copy()
        params = result.get("parameters", {})

        # Add driver from connector definition (e.g. "postgresql", "mysql")
        driver = connector.get("driver")
        if driver:
            result["driver"] = driver

        # Convert port to integer inside parameters
        if isinstance(params.get("port"), str):
            params["port"] = int(params["port"])
            result["parameters"] = params

        return result

    VALID_CONNECTOR_TYPES = {"api", "database", "file", "s3", "stdout"}

    def _resolve_connection_by_id(
        self,
        connection_ref: str,
        connection_id: str,
        org_id: str = ""
    ) -> ConnectionRuntime:
        """
        Resolve a connection by its ID.

        Creates a ConnectionRuntime with enriched metadata (connector_type,
        driver baked in). Secrets are resolved lazily via materialize().

        Args:
            connection_ref: Reference alias from pipeline (e.g., "conn_1")
            connection_id: Connection identifier
            org_id: Org ID (for late-binding resolver)

        Returns:
            ConnectionRuntime object
        """
        cache_key = f"id:{connection_id}"
        if cache_key in self._resolved_connections:
            return self._resolved_connections[cache_key]

        config = self._load_connection_config(connection_id)

        connector = self.get_connector_for_connection(config, connection_id)
        connection_type = connector.get("connector_type")

        if connection_type not in self.VALID_CONNECTOR_TYPES:
            raise ValueError(
                f"Connector has invalid connector_type: {connection_type!r}. "
                f"Expected one of: {sorted(self.VALID_CONNECTOR_TYPES)}"
            )

        if connection_type == "database":
            config = self._normalize_database_connection(config, connector)

        # Store secrets directory for the resolver
        self._secrets_dir = self._paths["secrets"]

        runtime = ConnectionRuntime(
            raw_config=config,
            connection_id=connection_id,
            connector_type=connection_type,
            driver=connector.get("driver") if connection_type == "database" else None,
            resolver=self.secrets_resolver,
            org_id=org_id or None,
        )

        self._resolved_connections[cache_key] = runtime

        logger.info(f"Resolved connection: {connection_ref} -> {connection_id}")
        return runtime

    # =========================================================================
    # Endpoint Loading Methods (from consolidated config)
    # =========================================================================

    def _load_endpoint_config(self, endpoint_id: str) -> Dict[str, Any]:
        """
        Load endpoint configuration by ID from consolidated config.

        Args:
            endpoint_id: Endpoint identifier

        Returns:
            Endpoint configuration dictionary

        Raises:
            FileNotFoundError: If endpoint not found in consolidated config
        """
        consolidated = self._load_consolidated_config()
        endpoints = consolidated.get("endpoints", [])

        for ep in endpoints:
            if ep.get("endpoint_id") == endpoint_id:
                return ep

        raise FileNotFoundError(
            f"Endpoint not found in consolidated config: {endpoint_id}"
        )

    # =========================================================================
    # Endpoint Resolution
    # =========================================================================

    def _resolve_endpoint_flexible(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve endpoint from config by endpoint_id.

        Args:
            config: Dict containing endpoint_id

        Returns:
            Resolved endpoint configuration dict

        Raises:
            ValueError: If endpoint_id is not specified
        """
        endpoint_id = config.get("endpoint_id")

        if not endpoint_id:
            raise ValueError(
                "Stream config must specify 'endpoint_id'"
            )

        cache_key = f"id:{endpoint_id}"
        if cache_key in self._resolved_endpoints:
            return self._resolved_endpoints[cache_key]

        endpoint = self._load_endpoint_config(endpoint_id)
        self._resolved_endpoints[cache_key] = endpoint
        logger.info(f"Resolved endpoint: {endpoint_id}")
        return endpoint

    # =========================================================================
    # Configuration Loading and Assembly
    # =========================================================================

    def load_pipeline_config(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Load and assemble complete pipeline and stream configurations.

        Configuration is loaded from the consolidated pipeline file.
        Connections, connectors, endpoints, and streams are all in the same file.

        Connections use ID-based format where the value is a connection_id string
        that is looked up in the consolidated file's connections array.

        Returns:
            Tuple of (pipeline_config dict, list of stream_config dicts)
        """
        # Load raw pipeline config from consolidated file
        raw_pipeline = self._load_local_pipeline()

        org_id = raw_pipeline.get("org_id", "")

        # Get connections from pipeline
        raw_connections = raw_pipeline.get("connections", {})
        source_connections = raw_connections.get("source", {})
        dest_connections = raw_connections.get("destinations", [])

        # Validate all connections use ID-based format (string)
        for alias, value in source_connections.items():
            if not isinstance(value, str):
                raise ValueError(
                    f"Connection '{alias}' must be a string (connection_id) "
                    f"that maps to a file in the connections directory."
                )

        for dest_dict in dest_connections:
            for alias, value in dest_dict.items():
                if not isinstance(value, str):
                    raise ValueError(
                        f"Connection '{alias}' must be a string (connection_id)."
                    )

        # Resolve all connections (load from connections/ dir, secrets from .secrets/)
        for alias, connection_id in source_connections.items():
            self._resolve_connection_by_id(alias, connection_id, org_id)

        for dest_dict in dest_connections:
            for alias, connection_id in dest_dict.items():
                self._resolve_connection_by_id(alias, connection_id, org_id)

        # Build connections_config with plain connection IDs (no prefix)
        connections_config = {
            "source": dict(source_connections),
            "destinations": [dict(dest_dict) for dest_dict in dest_connections],
        }

        # Load stream configs from local filesystem
        raw_streams = self._load_local_streams()

        # Handle version - convert string to int if needed
        version = raw_pipeline.get("version", 1)
        if isinstance(version, str):
            version = int(float(version))

        # Build pipeline config dict
        pipeline_config = {
            "version": version,
            "org_id": org_id,
            "pipeline_id": raw_pipeline["pipeline_id"],
            "name": raw_pipeline.get("name", ""),
            "description": raw_pipeline.get("description"),
            "status": raw_pipeline.get("status", "draft"),
            "tags": raw_pipeline.get("tags", []),
            "connections": connections_config,
            "engine_config": raw_pipeline.get("engine_config", {}),
            "created_at": raw_pipeline.get("created_at"),
            "updated_at": raw_pipeline.get("updated_at"),
        }

        # Build StreamConfigs with resolved connections and endpoints
        # Only include streams that are listed in pipeline.streams
        pipeline_stream_ids = set(raw_pipeline.get("streams", []))
        if not pipeline_stream_ids:
            logger.warning("Pipeline has no streams defined in 'streams' array")

        stream_configs = []
        for raw_stream in raw_streams:
            stream_id = raw_stream.get("stream_id", "unknown")

            if stream_id not in pipeline_stream_ids:
                raise ValueError(
                    f"Stream '{stream_id}' is not listed in pipeline.streams. "
                    f"Expected one of: {sorted(pipeline_stream_ids)}. "
                    f"This indicates a data consistency issue - the stream must be "
                    f"referenced in the pipeline's 'streams' array."
                )

            stream_config = self._build_stream_config(
                raw_stream,
                connections_config,
                org_id
            )
            stream_configs.append(stream_config)

        pipeline_name = pipeline_config["name"]
        logger.info(f"Loaded pipeline '{pipeline_name}' with {len(stream_configs)} streams")
        return pipeline_config, stream_configs

    def _build_stream_config(
        self,
        raw_stream: Dict[str, Any],
        connections_config: Dict[str, Any],
        org_id: str
    ) -> Dict[str, Any]:
        """
        Build a stream config dict from raw stream data with resolved connections and endpoints.

        Connections are resolved from the consolidated config's connections array.
        Endpoints are resolved from the consolidated config's endpoints array.
        """
        stream_id = raw_stream["stream_id"]

        # Resolve source connection
        source_ref = raw_stream["source"]["connection_ref"]
        source_connection_id = _get_connection_uuid(connections_config, source_ref)
        if source_connection_id is None:
            raise ValueError(f"Unknown connection_ref '{source_ref}' in stream {stream_id}")

        # Get connection from cache - all connections must be pre-resolved
        # Internal cache uses "id:{uuid}" keys
        cache_key = f"id:{source_connection_id}"
        source_connection = self._resolved_connections.get(cache_key)
        if not source_connection:
            raise ValueError(
                f"Connection '{source_ref}' (id={source_connection_id}) not resolved. "
                f"Ensure pipeline uses path-based connection format with 'connector' key."
            )

        # Resolve source endpoint from local connectors directory
        source_endpoint = self._resolve_endpoint_flexible(raw_stream["source"])

        # Build enriched source config
        source_data = raw_stream["source"].copy()
        source_data["_runtime"] = source_connection
        source_data["_endpoint"] = source_endpoint

        # Resolve destinations
        destinations_data = []
        for dest in raw_stream["destinations"]:
            dest_ref = dest["connection_ref"]
            dest_connection_id = _get_connection_uuid(connections_config, dest_ref)
            if dest_connection_id is None:
                raise ValueError(f"Unknown connection_ref '{dest_ref}' in stream {stream_id}")

            # Get connection from cache - all connections must be pre-resolved
            # Internal cache uses "id:{uuid}" keys
            dest_cache_key = f"id:{dest_connection_id}"
            dest_connection = self._resolved_connections.get(dest_cache_key)
            if not dest_connection:
                raise ValueError(
                    f"Connection '{dest_ref}' (id={dest_connection_id}) not resolved. "
                    f"Ensure pipeline uses path-based connection format with 'connector' key."
                )

            # Resolve destination endpoint from local connectors directory
            dest_endpoint = self._resolve_endpoint_flexible(dest)

            dest_data = dest.copy()
            dest_data["_runtime"] = dest_connection
            dest_data["_endpoint"] = dest_endpoint
            destinations_data.append(dest_data)

        # Handle version - convert string to int if needed
        version = raw_stream.get("version", 1)
        if isinstance(version, str):
            version = int(float(version))

        # Build stream config dict
        raw_mapping = raw_stream.get("mapping", {})
        # Get first destination's connection_ref for type lookup
        dest_connection_ref = destinations_data[0].get("connection_ref") if destinations_data else None
        normalized_mapping = (
            self._normalize_mapping_config(raw_mapping, dest_connection_ref)
            if raw_mapping
            else {}
        )
        return {
            "version": version,
            "stream_id": stream_id,
            "pipeline_id": raw_stream.get("pipeline_id", self.pipeline_id),
            "org_id": org_id,
            "status": raw_stream.get("status", "draft"),
            "is_enabled": raw_stream.get("is_enabled", True),
            "source": self._normalize_source_config(source_data),
            "destinations": [
                self._normalize_destination_config(d)
                for d in destinations_data
            ],
            "mapping": normalized_mapping or {},
            "tags": raw_stream.get("tags"),
            "engine_config": raw_stream.get("engine_config"),
            "created_at": raw_stream.get("created_at"),
            "updated_at": raw_stream.get("updated_at"),
        }

    def _normalize_mapping_config(
        self, mapping: Dict[str, Any], dest_connection_ref: Optional[str] = None
    ) -> Dict[str, Any]:
        """Normalize mapping using provided source_to_generic and generic_to_destination.

        Sets target.type to generic type (for Pydantic validation) and
        target.dest_type to native destination type (for DDL/casting).

        Args:
            mapping: Raw mapping configuration with assignments
            dest_connection_ref: Destination connection reference for type lookup
                                (e.g., "conn_2"). Used to look up destination types
                                from generic_to_destination mapping.

        Returns:
            Normalized mapping with generic types set on target.type
        """
        normalized = dict(mapping)
        assignments = list(normalized.get("assignments", []))

        # Use provided mappings (trust they are correct)
        source_to_generic = mapping.get("source_to_generic", {}) or {}
        generic_to_dest = (
            mapping.get("generic_to_destination", {}).get(dest_connection_ref, {})
            if dest_connection_ref
            else {}
        )

        def _generic_from_source_path(path: List[str]) -> Optional[str]:
            if not path:
                return None
            key = ".".join(path)
            entry = source_to_generic.get(key, {})
            return entry.get("generic_type")

        def _resolve_generic_type(assignment: Dict[str, Any]) -> str:
            """Resolve generic type from source_to_generic mapping."""
            target = assignment.get("target", {})
            target_path = target.get("path") or []
            generic = _generic_from_source_path(target_path)
            if generic:
                return generic

            value = assignment.get("value", {})
            if value.get("kind") == "expr":
                expr = value.get("expr", {})
                if expr.get("op") == "get":
                    generic = _generic_from_source_path(expr.get("path") or [])
                    if generic:
                        return generic

            # Preserve existing target.type before falling back to "string"
            return assignment.get("target", {}).get("type", "string")

        normalized_assignments = []
        for assignment in assignments:
            updated = dict(assignment)
            target = dict(updated.get("target", {}))
            target_path = target.get("path") or []

            # Get generic type from source_to_generic
            generic_type = _resolve_generic_type(updated)
            target["type"] = generic_type

            # Get destination type and nullable from generic_to_destination
            if target_path:
                field_name = ".".join(target_path)
                dest_entry = generic_to_dest.get(field_name, {})
                dest_type = dest_entry.get("destination_type")
                if dest_type:
                    target["dest_type"] = dest_type
                # Only set nullable from dest mapping if present
                if "nullable" in dest_entry:
                    target["nullable"] = dest_entry["nullable"]

            updated["target"] = target
            normalized_assignments.append(updated)

        normalized["assignments"] = normalized_assignments
        return normalized

    def _normalize_source_config(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize source configuration to match SourceConfig model."""
        replication = source_data.get("replication", {})

        endpoint_ref = source_data.get("endpoint_id", "")

        result = {
            "connection_ref": source_data["connection_ref"],
            "endpoint_id": endpoint_ref,
            "primary_key": source_data.get("primary_key", []),
            "replication": {
                "method": replication.get("method", "incremental"),
                "cursor_field": replication.get("cursor_field", []),
            },
        }

        # Add optional fields only if present
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

        endpoint_ref = dest_data.get("endpoint_id", "")

        result = {
            "connection_ref": dest_data["connection_ref"],
            "endpoint_id": endpoint_ref,
            "write": {
                "mode": write.get("mode", "upsert"),
            },
        }

        # Add optional fields only if present
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
        """
        Get the connectors array from consolidated config.

        Connectors define the type and metadata for each integration.
        The connector_type field determines which handler processes connections.

        Returns:
            List of connector definitions with connector_id, connector_name,
            connector_type, and optional driver/slug fields.
        """
        consolidated = self._load_consolidated_config()
        return consolidated.get("connectors", [])

    def create_config(self) -> Tuple[
        Dict[str, Any],
        List[Dict[str, Any]],
        Dict[str, "ConnectionRuntime"],
        Dict[str, Dict[str, Any]],
        List[Dict[str, Any]],
    ]:
        """
        Load and return validated pipeline and stream configurations.

        Returns:
            Tuple of (pipeline_config, stream_configs, resolved_connections,
                      resolved_endpoints, connectors)
            - resolved_connections maps connection_id to ConnectionRuntime objects
            - resolved_endpoints maps endpoint_id to its resolved config dict
            - connectors is the list of connector definitions from consolidated config
        """
        pipeline_config, stream_configs = self.load_pipeline_config()

        # Strip internal key prefixes for external consumers
        # Internal keys use "id:{uuid}" format, but consumers expect plain UUIDs
        resolved_connections = {}
        for key, conn in self._resolved_connections.items():
            clean_key = key.split(":", 1)[1] if ":" in key else key
            resolved_connections[clean_key] = conn

        # Strip "path:" prefix from endpoint keys for external consumers
        resolved_endpoints = {}
        for key, endpoint in self._resolved_endpoints.items():
            clean_key = key.split(":", 1)[1] if ":" in key else key
            resolved_endpoints[clean_key] = endpoint

        logger.info("Configuration loaded (secrets resolved at connection time)")

        # Get connectors from consolidated config
        connectors = self.get_connectors()

        return pipeline_config, stream_configs, resolved_connections, resolved_endpoints, connectors

    def validate_environment(self) -> None:
        """Validate that the environment is properly configured.

        Configuration is always loaded from local filesystem. In cloud environments,
        config_fetcher.py populates directories defined in analitiq.yaml before this runs.
        """
        # Validate pipelines directory exists (most critical for loading pipeline config)
        pipelines_path = self._paths["pipelines"]
        if not pipelines_path.exists():
            raise RuntimeError(
                f"Pipelines directory not found: {pipelines_path}. "
                f"Check analitiq.yaml configuration or run config_fetcher.py first."
            )
        logger.debug(f"Pipelines directory validated: {pipelines_path}")

    def get_resolved_connection(self, connection_ref: str, connections_map: Dict[str, str]) -> ConnectionRuntime:
        """Get a resolved connection by its reference alias."""
        connection_id = connections_map.get(connection_ref)
        if not connection_id:
            raise ValueError(f"Unknown connection reference: {connection_ref}")
        cache_key = f"id:{connection_id}"
        if cache_key not in self._resolved_connections:
            raise ValueError(f"Connection not resolved: {connection_id}")
        return self._resolved_connections[cache_key]
