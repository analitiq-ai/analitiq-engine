"""Pipeline management and configuration."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from pydantic import ValidationError

from .engine import StreamingEngine

from ..models.enriched import (
    EnrichedAPIConfig,
    EnrichedDatabaseConfig,
)
from ..shared.connection_runtime import ConnectionRuntime

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

        # Helper to get resolved endpoint config by endpoint_id
        def get_endpoint_config(endpoint_id: str) -> Dict[str, Any]:
            logger.debug(f"Looking up endpoint_id: {endpoint_id}")
            logger.debug(f"Available endpoint keys: {list(self.resolved_endpoints.keys())}")
            if endpoint_id in self.resolved_endpoints:
                result = self.resolved_endpoints[endpoint_id].copy()
                logger.debug(f"Found endpoint with keys: {list(result.keys())}")
                return result
            logger.warning(f"Endpoint not found: {endpoint_id}")
            return {}

        # Helper to validate connection config against enriched model
        def validate_connection_config(config: Dict[str, Any], label: str) -> None:
            connector_type = config.get("connector_type")
            try:
                if connector_type == "api":
                    EnrichedAPIConfig(**config)
                elif connector_type == "database":
                    EnrichedDatabaseConfig(**config)
                else:
                    logger.debug(f"Unknown connector_type '{connector_type}', skipping validation")
            except ValidationError as e:
                logger.warning(f"{label} config validation failed: {e}")

        streams = {}
        for stream in self.stream_configs:
            source = stream["source"]
            dest = stream["destinations"][0]

            # Get resolved connection configs and merge with stream configs
            source_conn = get_connection_config(source["connection_ref"])
            dest_conn = get_connection_config(dest["connection_ref"])

            # Get resolved endpoint configs
            source_endpoint = get_endpoint_config(source["endpoint_id"])
            dest_endpoint = get_endpoint_config(dest["endpoint_id"])

            # Read connector metadata from runtime
            source_runtime = source_conn.get("_runtime")
            dest_runtime = dest_conn.get("_runtime")
            source_connector_type = source_runtime.connector_type if source_runtime else None
            dest_connector_type = dest_runtime.connector_type if dest_runtime else None

            # Source replication config
            replication = source.get("replication", {})
            cursor_field_list = replication.get("cursor_field", [])

            # Build source config: host at root, parameters nested, driver from connector
            source_config = {
                "host": source_conn.get("host"),
                "parameters": source_conn.get("parameters", {}),
                "connector_type": source_connector_type,
                "driver": source_runtime.driver if source_runtime else None,
                "_runtime": source_runtime,
                **source_endpoint,
                "endpoint_id": source["endpoint_id"],
                "connection_ref": source["connection_ref"],
                "replication_method": replication.get("method", "incremental"),
                "cursor_field": cursor_field_list[0] if cursor_field_list else None,
                "cursor_mode": "inclusive",
                "safety_window_seconds": replication.get("safety_window_seconds"),
                "primary_key": source.get("primary_key", []),
            }

            # Dest write config
            write = dest.get("write", {})
            batching = dest.get("batching", {})

            # Build destination config: host at root, parameters nested, driver from connector
            dest_config = {
                "host": dest_conn.get("host"),
                "parameters": dest_conn.get("parameters", {}),
                "connector_type": dest_connector_type,
                "driver": dest_runtime.driver if dest_runtime else None,
                "_runtime": dest_runtime,
                **dest_endpoint,
                "endpoint_id": dest["endpoint_id"],
                "connection_ref": dest["connection_ref"],
                "refresh_mode": write.get("mode", "upsert"),
                "batch_support": batching.get("supported", False),
                "batch_size": batching.get("size", 1),
            }

            # Validate configs against enriched models (logs warnings on failure)
            validate_connection_config(source_config, "Source")
            validate_connection_config(dest_config, "Destination")

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

            pipeline_source_config = {
                "host": first_source_conn.get("host"),
                "parameters": first_source_conn.get("parameters", {}),
                "endpoint_id": first_source["endpoint_id"],
                "host_id": source_host_id,
            }
            pipeline_dest_config = {
                "host": first_dest_conn.get("host"),
                "parameters": first_dest_conn.get("parameters", {}),
                "endpoint_id": first_dest["endpoint_id"],
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
