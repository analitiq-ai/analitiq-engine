"""Pipeline management and configuration."""

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from .engine import StreamingEngine

from ..models.stream import EndpointRef
from ..shared.connection_runtime import ConnectionRuntime
from ..shared.placeholder import expand_placeholders

_UNRESOLVED_PLACEHOLDER = re.compile(r"\$\{[^}]+\}")

logger = logging.getLogger(__name__)


class Pipeline:
    """
    High-level pipeline management class for loading modular configuration
    and orchestrating streaming operations.

    Accepts pipeline_config and stream_configs as plain dicts (from JSON).
    """

    def __init__(
        self,
        pipeline_config: Dict[str, Any],
        stream_configs: Optional[List[Dict[str, Any]]] = None,
        resolved_connections: Optional[Dict[str, Dict[str, Any]]] = None,
        resolved_endpoints: Optional[Dict[str, Dict[str, Any]]] = None,
        connectors: Optional[List[Dict[str, Any]]] = None,
        state_dir: Optional[str] = None,
    ):
        """
        Initialize pipeline with configuration.

        Args:
            pipeline_config: Pipeline configuration dict
            stream_configs: List of stream config dicts
            resolved_connections: Dict mapping connection_id to resolved config dict
            resolved_endpoints: Dict mapping endpoint_id to resolved endpoint config dict
            connectors: List of connector definitions (connector_id -> connector_type mapping)
            state_dir: Optional directory for state files (if None, uses default)
        """
        # Load .env file if it exists
        load_dotenv()

        self.pipeline_config = pipeline_config
        self.stream_configs = stream_configs or []
        self.resolved_connections = resolved_connections or {}
        self.resolved_endpoints = resolved_endpoints or {}
        self.connectors = connectors or []

        # Extract configuration values
        pipeline_id = pipeline_config["pipeline_id"]

        # Setup directory structure using centralized paths
        project_root = Path(__file__).parent.parent.parent
        self.state_dir = state_dir or str(project_root / "state")
        self.dlq_dir = str(project_root / "deadletter" / pipeline_id)

        # Create required directories
        self._ensure_directories()

        runtime = pipeline_config["runtime"]
        batching = runtime["batching"]
        error_handling = runtime["error_handling"]
        self.engine = StreamingEngine(
            pipeline_id=pipeline_id,
            batch_size=batching["batch_size"],
            max_concurrent_batches=batching["max_concurrent_batches"],
            buffer_size=runtime["buffer_size"],
            dlq_path=self.dlq_dir,
            max_retries=error_handling["max_retries"],
            retry_delay=error_handling["retry_delay"],
        )

    def _ensure_directories(self):
        """Create required directories for pipeline operation."""
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.dlq_dir).mkdir(parents=True, exist_ok=True)

    def _build_config_dict(self) -> Dict[str, Any]:
        """Build a config dict from pipeline and stream configs for engine."""
        connections = self.pipeline_config.get("connections", {})

        # Helper to get resolved connection config by alias
        def get_connection_config(connection_ref: str) -> Dict[str, Any]:
            if connection_ref not in self.resolved_connections:
                raise ValueError(
                    f"Connection '{connection_ref}' was not resolved"
                )
            runtime = self.resolved_connections[connection_ref]
            if isinstance(runtime, ConnectionRuntime):
                result = runtime.raw_config
                result["_runtime"] = runtime
                return result
            elif isinstance(runtime, dict):
                return runtime.copy()
            raise ValueError(
                f"Unexpected connection type for '{connection_ref}': {type(runtime)}"
            )

        # Helper to get resolved endpoint config by structured endpoint_ref.
        # ``self.resolved_endpoints`` is keyed by ``EndpointRef`` instances (see
        # ``PipelineConfigPrep._resolved_endpoints``); accept dict input from
        # serialized stream configs and validate.
        def get_endpoint_config(endpoint_ref: Any) -> Dict[str, Any]:
            ref = EndpointRef.from_dict(endpoint_ref)
            logger.debug(f"Looking up endpoint_ref: {ref}")
            if ref in self.resolved_endpoints:
                result = self.resolved_endpoints[ref].copy()
                logger.debug(f"Found endpoint with keys: {list(result.keys())}")
                return result
            logger.warning(f"Endpoint not found: {ref}")
            return {}

        streams = {}
        for stream in self.stream_configs:
            source = stream["source"]
            dest = stream["destinations"][0]

            # Get resolved connection configs and merge with stream configs
            source_conn = get_connection_config(source["connection_ref"])
            dest_conn = get_connection_config(dest["connection_ref"])

            # Get resolved endpoint configs
            source_endpoint = get_endpoint_config(source["endpoint_ref"])
            dest_endpoint = get_endpoint_config(dest["endpoint_ref"])

            # Read connector metadata from runtime
            source_runtime = source_conn.get("_runtime")
            dest_runtime = dest_conn.get("_runtime")
            source_connector_type = source_runtime.connector_type if source_runtime else None
            dest_connector_type = dest_runtime.connector_type if dest_runtime else None

            # Source replication config
            replication = source.get("replication", {})
            cursor_field_list = replication.get("cursor_field", [])

            # Build source config. ``host`` for database connectors comes
            # from ``parameters.host`` (the connection-owned address);
            # ``host`` for API connectors is whatever literal base URL the
            # connector declared, or None if the transport is templated
            # (the engine consumes the base URL via runtime.base_url).
            source_params = source_conn.get("parameters", {}) or {}
            source_host = _connection_host(source_conn, source_runtime, source_connector_type)
            source_config = {
                "host": source_host,
                "parameters": source_params,
                "connector_type": source_connector_type,
                "driver": source_runtime.driver if source_runtime else None,
                "_runtime": source_runtime,
                **source_endpoint,
                "endpoint_ref": source["endpoint_ref"],
                "connection_ref": source["connection_ref"],
                "replication_method": replication.get("method", "incremental"),
                "cursor_field": cursor_field_list[0] if cursor_field_list else None,
                "cursor_mode": "inclusive",
                "safety_window_seconds": replication.get("safety_window_seconds"),
                "primary_key": source.get("primary_key", []),
            }

            # Legacy ``${name}`` placeholders in endpoint files (e.g. wise's
            # transfers filter ``"default": "${profile_id}"``) resolve from
            # the union of connection.parameters, connection.selections and
            # connection.discovered. New endpoint files should use the
            # spec's ``{"ref": "connection.selections.profile_id"}`` form;
            # this flat-merge stays as a transition shim until endpoint
            # files migrate (analitiq-engine#37).
            #
            # ``ignore_missing=True`` keeps stale ``${name}`` text in the
            # resolved request rather than crashing; we log a WARNING for
            # every unresolved placeholder so the symptom is visible at
            # runtime instead of silently flowing into the provider call.
            placeholder_lookup = _flat_connection_lookup(source_conn)
            if placeholder_lookup:
                if "filters" in source_config:
                    source_config["filters"] = expand_placeholders(
                        source_config["filters"], placeholder_lookup, ignore_missing=True
                    )
                    _warn_unresolved_placeholders(
                        source_config["filters"], "filters", stream["stream_id"]
                    )
                if "endpoint" in source_config:
                    source_config["endpoint"] = expand_placeholders(
                        source_config["endpoint"], placeholder_lookup, ignore_missing=True
                    )
                    _warn_unresolved_placeholders(
                        source_config["endpoint"], "endpoint", stream["stream_id"]
                    )

            # Dest write config
            write = dest.get("write", {})
            batching = dest.get("batching", {})

            dest_params = dest_conn.get("parameters", {}) or {}
            dest_host = _connection_host(dest_conn, dest_runtime, dest_connector_type)
            dest_config = {
                "host": dest_host,
                "parameters": dest_params,
                "connector_type": dest_connector_type,
                "driver": dest_runtime.driver if dest_runtime else None,
                "_runtime": dest_runtime,
                **dest_endpoint,
                "endpoint_ref": dest["endpoint_ref"],
                "connection_ref": dest["connection_ref"],
                "refresh_mode": write.get("mode", "upsert"),
                "batch_support": batching.get("supported", False),
                "batch_size": batching.get("size", 1),
            }

            # Validation lives downstream now: ConnectionRuntime validates
            # connector definition + secret_refs at materialise time, and
            # the transport factory validates the resolved transport spec
            # before opening any session. Best-effort EnrichedConfig
            # validation here only produced WARNING logs that masked real
            # config bugs flowing into the transport.

            # Build mapping config for engine
            mapping = stream.get("mapping", {})
            mapping_config = {
                "assignments": mapping.get("assignments", [])
            }

            streams[stream["stream_id"]] = {
                "name": stream["stream_id"],
                "source": source_config,
                "destination": dest_config,
                "mapping": mapping_config,
            }

        # Build pipeline-level source/destination from first stream for engine compatibility
        first_stream = self.stream_configs[0] if self.stream_configs else None
        if first_stream:
            first_source = first_stream["source"]
            first_dest = first_stream["destinations"][0]
            first_source_conn = get_connection_config(first_source["connection_ref"])
            first_dest_conn = get_connection_config(first_dest["connection_ref"])

            source_host_id = first_source["connection_ref"]
            dest_host_id = first_dest["connection_ref"]

            first_source_runtime = first_source_conn.get("_runtime")
            first_dest_runtime = first_dest_conn.get("_runtime")
            first_source_type = (
                first_source_runtime.connector_type if first_source_runtime else None
            )
            first_dest_type = (
                first_dest_runtime.connector_type if first_dest_runtime else None
            )

            pipeline_source_config = {
                "host": _connection_host(
                    first_source_conn, first_source_runtime, first_source_type
                ),
                "parameters": first_source_conn.get("parameters", {}),
                "endpoint_ref": first_source["endpoint_ref"],
                "host_id": source_host_id,
            }
            pipeline_dest_config = {
                "host": _connection_host(
                    first_dest_conn, first_dest_runtime, first_dest_type
                ),
                "parameters": first_dest_conn.get("parameters", {}),
                "endpoint_ref": first_dest["endpoint_ref"],
                "host_id": dest_host_id,
            }
        else:
            pipeline_source_config = {}
            pipeline_dest_config = {}

        return {
            "pipeline_id": self.pipeline_config["pipeline_id"],
            "name": self.pipeline_config.get("name", ""),
            "version": self.pipeline_config.get("version", 1),
            "connections": connections,
            "resolved_connections": self.resolved_connections,
            "source": pipeline_source_config,
            "destination": pipeline_dest_config,
            "streams": streams,
            "runtime": self.pipeline_config["runtime"],
            "connectors": self.connectors,
        }

    async def run(self):
        """Execute the pipeline."""
        pipeline_id = self.pipeline_config["pipeline_id"]

        config_dict = self._build_config_dict()

        try:
            await self.engine.stream_data(config_dict)
            logger.info(f"Pipeline {pipeline_id} completed successfully")
        except Exception as e:
            logger.debug(f"Pipeline {pipeline_id} failed: {str(e)}")
            raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get pipeline execution metrics."""
        return self.engine.get_metrics()


def _connection_host(
    connection_config: Dict[str, Any],
    runtime: Optional[ConnectionRuntime],
    connector_type: Optional[str],  # accepted for legacy callers; unused
) -> Optional[str]:
    """Best-effort ``host`` for legacy consumers of the engine config dict.

    Resolution order (no connector-type branching):

    1. ``connection.parameters.host`` — addresses owned by the connection.
    2. The connector's default transport ``base_url`` when present as a
       literal string (resolved transports are not yet available here).
    3. The legacy top-level ``host`` field, if any.

    Returns ``None`` if none of the above produce a string. Callers that
    actually need the materialized address should consume
    :attr:`ConnectionRuntime.base_url` after ``materialize()``.
    """
    del connector_type  # legacy parameter kept for callers
    params = connection_config.get("parameters") or {}
    if isinstance(params, dict) and isinstance(params.get("host"), str):
        return params["host"]
    if runtime is not None:
        connector_def = runtime.connector_definition or {}
        transports = connector_def.get("transports") or {}
        default_ref = connector_def.get("default_transport")
        if default_ref and default_ref in transports:
            base_url = transports[default_ref].get("base_url")
            if isinstance(base_url, str):
                return base_url
    legacy = connection_config.get("host")
    return legacy if isinstance(legacy, str) else None


def _warn_unresolved_placeholders(value: Any, field: str, stream_id: str) -> None:
    """Emit a WARNING for any ``${name}`` placeholder still left in *value*.

    The flat-merge shim uses ``ignore_missing=True`` so the engine does not
    crash on legacy unresolved references; this helper makes the residue
    visible at runtime instead of letting it flow silently into the
    provider request.
    """
    leftover: List[str] = []

    def _scan(node: Any) -> None:
        if isinstance(node, str):
            leftover.extend(_UNRESOLVED_PLACEHOLDER.findall(node))
        elif isinstance(node, dict):
            for child in node.values():
                _scan(child)
        elif isinstance(node, list):
            for child in node:
                _scan(child)

    _scan(value)
    if leftover:
        logger.warning(
            "stream %r: %s contains unresolved placeholders %s; "
            "endpoint file uses legacy ${...} text not present in connection "
            "parameters/selections/discovered (analitiq-engine#37).",
            stream_id, field, sorted(set(leftover)),
        )


def _flat_connection_lookup(connection_config: Dict[str, Any]) -> Dict[str, str]:
    """Flat ``name -> str(value)`` map across ``parameters``, ``selections``,
    and ``discovered``. Used to expand legacy ``${name}`` placeholders in
    endpoint files until those files migrate to fully-qualified ``ref``
    expressions (tracked in analitiq-engine#37).

    Precedence on key collision (highest wins):

    1. ``connection.parameters`` — explicit, user-supplied connection values.
    2. ``connection.selections`` — durable post-auth user choices.
    3. ``connection.discovered`` — provider-returned values (e.g. api_domain).

    Conflicts between scopes are logged at WARNING so silent precedence
    surprises are visible in normal runs. The shim is intentionally
    transitional: when endpoint files migrate to typed ``ref`` /
    ``template`` expressions, this whole function disappears.
    """
    flat: Dict[str, str] = {}
    origin: Dict[str, str] = {}
    # Iterate lowest-precedence-first so the higher-precedence scope's
    # write overwrites it. Conflicts are logged when a key is overwritten.
    for scope_name in ("discovered", "selections", "parameters"):
        scope = connection_config.get(scope_name) or {}
        if not isinstance(scope, dict):
            continue
        for key, value in scope.items():
            if isinstance(value, (dict, list)):
                continue
            if value is None:
                continue
            stringified = str(value)
            if key in flat and flat[key] != stringified:
                logger.warning(
                    "connection placeholder %r differs across scopes "
                    "(%s=%r → %s=%r); using %s",
                    key, origin[key], flat[key], scope_name, stringified,
                    scope_name,
                )
            flat[key] = stringified
            origin[key] = scope_name
    return flat
