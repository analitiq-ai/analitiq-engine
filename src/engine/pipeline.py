"""Pipeline management and configuration."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv

from pydantic import ValidationError

from .engine import StreamingEngine
from .pipeline_config_prep import _get_connection_uuid

from ..shared.run_id import get_or_generate_run_id
from ..state.log_storage import LogStorageSettings, create_log_handler
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
        self.logs_dir = str(project_root / "logs" / pipeline_id)
        self.dlq_dir = str(project_root / "deadletter" / pipeline_id)

        # Create required directories
        self._ensure_directories()

        # Setup logging for this pipeline
        self._setup_pipeline_logging(pipeline_id)

        ec = pipeline_config.get("engine_config", {})
        batching = ec.get("batching", {})
        engine_res = ec.get("engine", {})
        self.engine = StreamingEngine(
            pipeline_id=pipeline_id,
            batch_size=batching.get("batch_size", 100),
            max_concurrent_batches=batching.get("max_concurrent_batches", 3),
            buffer_size=engine_res.get("buffer_size", 5000),
            dlq_path=self.dlq_dir,
        )

    def _ensure_directories(self):
        """Create required directories for pipeline operation."""
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.logs_dir).mkdir(parents=True, exist_ok=True)
        Path(self.dlq_dir).mkdir(parents=True, exist_ok=True)

    def _setup_pipeline_logging(self, pipeline_id: str):
        """
        Setup structured logging for the pipeline.

        Automatically uses S3 storage in cloud mode (ENV=dev or ENV=prod).
        Uses local filesystem in local mode.
        """
        self._log_handlers: List[logging.Handler] = []

        # Get log storage settings from environment
        self._log_settings = LogStorageSettings.from_env()
        self._log_settings.pipeline_id = pipeline_id

        # Get run_id for log file naming (already initialized at startup)
        self._run_id = get_or_generate_run_id()

        # Set log format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Set log level from environment (preferred) or config
        ec = self.pipeline_config.get("engine_config", {})
        logging_config = ec.get("logging", {})
        log_level = os.getenv("LOG_LEVEL", logging_config.get("log_level", "INFO"))

        # Attach handler to src package logger (parent of all src.* modules)
        root_package_logger = logging.getLogger("src")

        # Create handler based on environment
        pipeline_handler = create_log_handler(
            settings=self._log_settings,
            log_name="pipeline",
            run_id=self._run_id,
            buffered=True,
        )
        pipeline_handler.setFormatter(formatter)
        pipeline_handler.setLevel(getattr(logging, log_level.upper()))

        root_package_logger.addHandler(pipeline_handler)
        root_package_logger.setLevel(getattr(logging, log_level.upper()))
        self._log_handlers.append(pipeline_handler)

        if self._log_settings.is_cloud_mode:
            logger.info(
                f"Setup pipeline logging: s3://{self._log_settings.logs_bucket}/"
                f"{self._log_settings.org_id}/{pipeline_id}/..."
            )
        else:
            logger.info(f"Setup pipeline logging: {self.logs_dir}/pipeline.log")

        # Create DLQ directories for streams
        for stream_config in self.stream_configs:
            stream_dlq_path = Path(self.dlq_dir) / stream_config["stream_id"]
            stream_dlq_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Setup DLQ directory for stream: {stream_config['stream_id']}")

    def _close_log_handlers(self):
        """Close all log handlers to flush buffered logs to S3."""
        root_package_logger = logging.getLogger("src")
        for handler in getattr(self, '_log_handlers', []):
            try:
                root_package_logger.removeHandler(handler)
                handler.close()
            except Exception as e:
                import sys
                print(f"Failed to close log handler: {e}", file=sys.stderr)

    def _build_config_dict(self) -> Dict[str, Any]:
        """Build a config dict from pipeline and stream configs for engine."""
        connections = self.pipeline_config.get("connections", {})

        # Helper to get resolved connection config by reference
        def get_connection_config(connection_ref: str) -> Dict[str, Any]:
            connection_id = _get_connection_uuid(connections, connection_ref)
            if not connection_id:
                raise ValueError(
                    f"Connection reference '{connection_ref}' not found in pipeline connections"
                )
            if connection_id not in self.resolved_connections:
                raise ValueError(
                    f"Connection '{connection_id}' (ref='{connection_ref}') was not resolved"
                )
            runtime = self.resolved_connections[connection_id]
            if isinstance(runtime, ConnectionRuntime):
                result = runtime.raw_config
                result["_runtime"] = runtime
                return result
            elif isinstance(runtime, dict):
                return runtime.copy()
            raise ValueError(
                f"Unexpected connection type for '{connection_id}': {type(runtime)}"
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

            source_host_id = _get_connection_uuid(connections, first_source["connection_ref"]) or ""
            dest_host_id = _get_connection_uuid(connections, first_dest["connection_ref"]) or ""

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

        ec = self.pipeline_config.get("engine_config", {})
        logging_config = ec.get("logging", {})

        return {
            "pipeline_id": self.pipeline_config["pipeline_id"],
            "name": self.pipeline_config.get("name", ""),
            "version": self.pipeline_config.get("version", 1),
            "org_id": self.pipeline_config.get("org_id", ""),
            "connections": connections,
            "resolved_connections": self.resolved_connections,
            "source": pipeline_source_config,
            "destination": pipeline_dest_config,
            "streams": streams,
            "engine_config": ec,
            "monitoring": {
                "log_level": logging_config.get("log_level", "INFO"),
                "metrics_enabled": logging_config.get("metrics_enabled", True),
                "checkpoint_interval": logging_config.get("checkpoint_interval", 50),
                "progress_monitoring": logging_config.get("progress_monitoring", "enabled"),
            },
            "connectors": self.connectors,
        }

    async def run(self):
        """Execute the pipeline and optional progress monitoring."""
        pipeline_id = self.pipeline_config["pipeline_id"]

        # Check if progress monitoring is enabled
        ec = self.pipeline_config.get("engine_config", {})
        logging_config = ec.get("logging", {})
        progress_monitoring = logging_config.get("progress_monitoring", "enabled") == "enabled"

        # Build config dict for engine
        config_dict = self._build_config_dict()

        try:
            if progress_monitoring:
                await self._run_with_progress_monitoring(config_dict)
            else:
                await self.engine.stream_data(config_dict)
            logger.info(f"Pipeline {pipeline_id} completed successfully")
        except Exception as e:
            logger.debug(f"Pipeline {pipeline_id} failed: {str(e)}")
            raise
        finally:
            self._close_log_handlers()

    async def _run_with_progress_monitoring(self, config_dict: Dict[str, Any]):
        """Run pipeline with progress monitoring enabled."""
        import asyncio

        pipeline_id = self.pipeline_config["pipeline_id"]
        logger.info(f"Starting pipeline {pipeline_id} with progress monitoring")

        # Start pipeline in background
        pipeline_task = asyncio.create_task(self.engine.stream_data(config_dict))

        # Monitoring loop
        last_processed = 0
        start_time = datetime.now()

        logger.info("Monitoring pipeline progress (updates every 5 seconds)...")

        while not pipeline_task.done():
            await asyncio.sleep(5)

            try:
                metrics = self.get_metrics()
                current_processed = getattr(metrics, 'records_processed', 0)

                if current_processed > last_processed:
                    elapsed = datetime.now() - start_time
                    rate = current_processed / max(elapsed.total_seconds(), 1)
                    logger.info(f"  {current_processed} records processed ({rate:.1f}/sec)")
                    last_processed = current_processed

            except Exception as e:
                logger.debug(f"Progress monitoring error: {e}")

        await pipeline_task

        # Final summary
        final_metrics = self.get_metrics()
        total_time = datetime.now() - start_time

        logger.info(f"Pipeline {pipeline_id} monitoring completed")
        logger.info(f"Total time: {total_time}")
        logger.info(f"Final records processed: {getattr(final_metrics, 'records_processed', 0)}")

        if getattr(final_metrics, 'records_failed', 0) > 0:
            logger.warning(f"Failed records: {getattr(final_metrics, 'records_failed', 0)}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get pipeline execution metrics."""
        return self.engine.get_metrics()
