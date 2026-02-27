"""Pipeline management and configuration."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv

from pydantic import ValidationError

from .engine import StreamingEngine
from .pipeline_config_prep import ResolvedConnection
from ..shared.connector_utils import get_connector_type_from_list
from ..shared.run_id import get_or_generate_run_id
from ..state.log_storage import LogStorageSettings, create_log_handler
from ..models.enriched import (
    EnrichedAPIDestinationConfig,
    EnrichedAPISourceConfig,
    EnrichedDatabaseDestinationConfig,
    EnrichedDatabaseSourceConfig,
)
from ..models.pipeline import PipelineConfig
from ..models.stream import StreamConfig

logger = logging.getLogger(__name__)


class Pipeline:
    """
    High-level pipeline management class for loading modular configuration
    and orchestrating streaming operations.

    Supports both:
    - New format: PipelineConfig + List[StreamConfig]
    - Legacy format: Dict with embedded streams (for backwards compatibility)
    """

    def __init__(
        self,
        pipeline_config: Union[PipelineConfig, Dict[str, Any]],
        stream_configs: Optional[List[StreamConfig]] = None,
        resolved_connections: Optional[Dict[str, Dict[str, Any]]] = None,
        resolved_endpoints: Optional[Dict[str, Dict[str, Any]]] = None,
        connectors: Optional[List[Dict[str, Any]]] = None,
        state_dir: Optional[str] = None,
    ):
        """
        Initialize pipeline with configuration.

        Args:
            pipeline_config: Pipeline configuration (PipelineConfig model or dict)
            stream_configs: List of StreamConfig models (required if using new format)
            resolved_connections: Dict mapping connection_id to resolved config dict
            resolved_endpoints: Dict mapping endpoint_id to resolved endpoint config dict
            connectors: List of connector definitions (connector_id -> connector_type mapping)
            state_dir: Optional directory for state files (if None, uses default)
        """
        # Load .env file if it exists
        load_dotenv()

        # Handle both new and legacy config formats
        if isinstance(pipeline_config, PipelineConfig):
            self.pipeline_config = pipeline_config
            self.stream_configs = stream_configs or []
            self.resolved_connections = resolved_connections or {}
            self.resolved_endpoints = resolved_endpoints or {}
            self.connectors = connectors or []
            self._legacy_mode = False
        else:
            # Legacy dict format - convert for compatibility
            self._legacy_config = pipeline_config
            self.pipeline_config = self._convert_legacy_pipeline_config(pipeline_config)
            self.stream_configs = self._convert_legacy_stream_configs(pipeline_config)
            self.resolved_connections = resolved_connections or {}
            self.resolved_endpoints = resolved_endpoints or {}
            self.connectors = connectors or []
            self._legacy_mode = True

        # Extract configuration values
        pipeline_id = self.pipeline_config.pipeline_id

        # Setup directory structure using centralized paths
        project_root = Path(__file__).parent.parent.parent
        self.state_dir = state_dir or str(project_root / "state")
        self.logs_dir = str(project_root / "logs" / pipeline_id)
        self.dlq_dir = str(project_root / "deadletter" / pipeline_id)

        # Create required directories
        self._ensure_directories()

        # Setup logging for this pipeline
        self._setup_pipeline_logging(pipeline_id)

        ec = self.pipeline_config.engine_config
        self.engine = StreamingEngine(
            pipeline_id=pipeline_id,
            batch_size=ec.batching.batch_size,
            max_concurrent_batches=ec.batching.max_concurrent_batches,
            buffer_size=ec.engine.buffer_size,
            dlq_path=self.dlq_dir,
        )

    def _convert_legacy_pipeline_config(self, config: Dict[str, Any]) -> PipelineConfig:
        """Convert legacy dict config to PipelineConfig model."""
        from ..models.pipeline import (
            RuntimeConfig, BatchingConfig, EngineRuntimeConfig,
            LoggingConfig, ErrorHandlingConfig, ScheduleConfig,
            PipelineConnectionsConfig
        )

        engine_config = config.get("engine_config", {})
        monitoring = config.get("monitoring", {})
        error_handling = config.get("error_handling", {})

        # Build connections in new nested format
        source_conn = {}
        dest_conns = []
        if config.get("source", {}).get("connection_id"):
            source_conn["conn_src"] = config["source"]["connection_id"]
        if config.get("destination", {}).get("connection_id"):
            dest_conns.append({"conn_dst": config["destination"]["connection_id"]})

        connections = PipelineConnectionsConfig(
            source=source_conn,
            destinations=dest_conns
        )

        runtime_dict = {
            "batching": BatchingConfig(
                batch_size=engine_config.get("batch_size", 100),
                max_concurrent_batches=engine_config.get("max_concurrent_batches", 3),
            ),
            "engine": EngineRuntimeConfig(
                buffer_size=engine_config.get("buffer_size", 5000),
            ),
            "logging": LoggingConfig(
                log_level=monitoring.get("log_level", "INFO"),
                metrics_enabled=monitoring.get("metrics_enabled", True),
                checkpoint_interval=monitoring.get("checkpoint_interval", 50),
                health_check_interval=monitoring.get("health_check_interval", 300),
                progress_monitoring=monitoring.get("progress_monitoring", "enabled"),
            ),
        }

        if error_handling:
            runtime_dict["error_handling"] = error_handling

        if engine_config.get("schedule"):
            runtime_dict["schedule"] = engine_config["schedule"]

        # Handle version - convert string to int if needed
        version = config.get("version", 1)
        if isinstance(version, str):
            version = int(float(version))

        return PipelineConfig(
            version=version,
            client_id=config.get("client_id", ""),
            pipeline_id=config["pipeline_id"],
            name=config.get("name", ""),
            status="active",
            connections=connections,
            engine_config=RuntimeConfig(**runtime_dict),
        )

    def _convert_legacy_stream_configs(self, config: Dict[str, Any]) -> List[StreamConfig]:
        """Convert legacy embedded streams to StreamConfig models."""
        from ..models.stream import (
            StreamConfig, SourceConfig, DestinationConfig,
            ReplicationConfig, WriteModeConfig, DestinationBatchingConfig,
            MappingConfig, Assignment, AssignmentTarget, AssignmentValue,
            ConstValue, ValueKind, TargetType
        )

        stream_configs = []
        streams = config.get("streams", {})

        for stream_id, stream_data in streams.items():
            source_data = stream_data.get("source", {})
            dest_data = stream_data.get("destination", {})
            mapping_data = stream_data.get("mapping", {})

            # Build source config
            # Normalize replication method: "full_refresh" -> "full"
            raw_method = source_data.get("replication_method", "incremental")
            if raw_method == "full_refresh":
                raw_method = "full"

            source = SourceConfig(
                connection_ref="conn_src",
                endpoint_id=source_data.get("endpoint_id", ""),
                primary_key=source_data.get("primary_key", []),
                replication=ReplicationConfig(
                    method=raw_method,
                    cursor_field=[source_data["cursor_field"]] if source_data.get("cursor_field") else [],
                    safety_window_seconds=source_data.get("safety_window_seconds"),
                ),
            )

            # Build destination config
            destination = DestinationConfig(
                connection_ref="conn_dst",
                endpoint_id=dest_data.get("endpoint_id", ""),
                write=WriteModeConfig(
                    mode=dest_data.get("refresh_mode", "upsert"),
                ),
                batching=DestinationBatchingConfig(
                    supported=dest_data.get("batch_support", False),
                    size=dest_data.get("batch_size", 1),
                ),
            )

            # Convert mapping to assignments
            assignments = []
            for source_field, field_config in mapping_data.get("field_mappings", {}).items():
                if isinstance(field_config, str):
                    # String shorthand: "source_field": "target_field"
                    target_field = field_config
                    path = source_field.split(".")
                    assignments.append(Assignment(
                        target=AssignmentTarget(
                            path=[target_field],
                            type=TargetType.STRING,
                            nullable=True,
                        ),
                        value=AssignmentValue(
                            kind=ValueKind.EXPR,
                            expr={"op": "get", "path": path},
                        ),
                    ))
                elif isinstance(field_config, dict):
                    target_field = field_config.get("target", source_field)
                    path = source_field.split(".")

                    assignments.append(Assignment(
                        target=AssignmentTarget(
                            path=[target_field],
                            type=TargetType.STRING,
                            nullable=True,
                        ),
                        value=AssignmentValue(
                            kind=ValueKind.EXPR,
                            expr={"op": "get", "path": path},
                        ),
                    ))

            for field_name, computed in mapping_data.get("computed_fields", {}).items():
                if isinstance(computed, str):
                    # String shorthand: "field_name": "constant_value"
                    const_value = ConstValue(type="string", value=computed)
                    assignments.append(Assignment(
                        target=AssignmentTarget(
                            path=[field_name],
                            type=TargetType.STRING,
                            nullable=False,
                        ),
                        value=AssignmentValue(
                            kind=ValueKind.CONST,
                            const=const_value,
                        ),
                    ))
                elif isinstance(computed, dict):
                    expr_value = computed.get("expression", "")
                    try:
                        import json
                        parsed = json.loads(expr_value)
                        const_value = ConstValue(type="object", value=parsed)
                    except (json.JSONDecodeError, TypeError):
                        const_value = ConstValue(type="string", value=expr_value)

                    assignments.append(Assignment(
                        target=AssignmentTarget(
                            path=[field_name],
                            type=TargetType.STRING,
                            nullable=False,
                        ),
                        value=AssignmentValue(
                            kind=ValueKind.CONST,
                            const=const_value,
                        ),
                    ))

            stream_configs.append(StreamConfig(
                version=1,
                stream_id=stream_id,
                pipeline_id=config["pipeline_id"],
                client_id=config.get("client_id", ""),
                status="active",
                is_enabled=True,
                source=source,
                destinations=[destination],
                mapping=MappingConfig(assignments=assignments),
            ))

        return stream_configs

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
        log_level = os.getenv("LOG_LEVEL", self.pipeline_config.engine_config.logging.log_level)

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
                f"{self._log_settings.client_id}/{pipeline_id}/..."
            )
        else:
            logger.info(f"Setup pipeline logging: {self.logs_dir}/pipeline.log")

        # Create DLQ directories for streams
        for stream_config in self.stream_configs:
            stream_dlq_path = Path(self.dlq_dir) / stream_config.stream_id
            stream_dlq_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Setup DLQ directory for stream: {stream_config.stream_id}")

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
        # Helper to get resolved connection config by reference
        def get_connection_config(connection_ref: str) -> Dict[str, Any]:
            try:
                connection_id = self.pipeline_config.connections.get_connection_uuid(connection_ref)
                if connection_id in self.resolved_connections:
                    conn = self.resolved_connections[connection_id]
                    # Handle both ResolvedConnection and dict types
                    if isinstance(conn, ResolvedConnection):
                        result = conn.config.copy()
                        result["_connection_wrapper"] = conn.connection_config_wrapper
                        return result
                    elif isinstance(conn, dict):
                        return conn.copy()
            except KeyError:
                pass
            return {}

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

        # Helper to validate source config against enriched model
        def validate_source_config(config: Dict[str, Any]) -> None:
            connector_type = config.get("connector_type")
            try:
                if connector_type == "api":
                    EnrichedAPISourceConfig(**config)
                elif connector_type == "database":
                    EnrichedDatabaseSourceConfig(**config)
                else:
                    logger.debug(f"Unknown connector_type '{connector_type}', skipping validation")
            except ValidationError as e:
                logger.warning(f"Source config validation failed: {e}")

        # Helper to validate destination config against enriched model
        def validate_dest_config(config: Dict[str, Any]) -> None:
            connector_type = config.get("connector_type")
            try:
                if connector_type == "api":
                    EnrichedAPIDestinationConfig(**config)
                elif connector_type == "database":
                    EnrichedDatabaseDestinationConfig(**config)
                else:
                    logger.debug(f"Unknown connector_type '{connector_type}', skipping validation")
            except ValidationError as e:
                logger.warning(f"Destination config validation failed: {e}")

        streams = {}
        for stream in self.stream_configs:
            # Build stream dict compatible with engine
            dest = stream.get_primary_destination()

            # Get resolved connection configs and merge with stream configs
            source_conn = get_connection_config(stream.source.connection_ref)
            dest_conn = get_connection_config(dest.connection_ref)

            # Get resolved endpoint configs
            source_endpoint = get_endpoint_config(stream.source.endpoint_id)
            dest_endpoint = get_endpoint_config(dest.endpoint_id)

            # Look up connector_type for source and destination
            source_connector_id = source_conn.get("connector_id")
            dest_connector_id = dest_conn.get("connector_id")
            source_connector_type = None
            dest_connector_type = None
            if source_connector_id and self.connectors:
                try:
                    source_connector_type = get_connector_type_from_list(
                        self.connectors, source_connector_id
                    )
                except ValueError:
                    logger.warning(f"Could not find connector_type for source connector_id: {source_connector_id}")
            if dest_connector_id and self.connectors:
                try:
                    dest_connector_type = get_connector_type_from_list(
                        self.connectors, dest_connector_id
                    )
                except ValueError:
                    logger.warning(f"Could not find connector_type for dest connector_id: {dest_connector_id}")

            # Build source config with connection and endpoint details
            source_config = {
                **source_conn,  # Include host, headers, type, etc.
                **source_endpoint,  # Include endpoint path, method, pagination, filters, etc.
                "endpoint_id": stream.source.endpoint_id,
                "connection_ref": stream.source.connection_ref,
                "replication_method": stream.source.replication.method.value,
                "cursor_field": stream.source.replication.cursor_field[0] if stream.source.replication.cursor_field else None,
                "cursor_mode": "inclusive",  # Always use inclusive mode (>= for cursor comparison)
                "safety_window_seconds": stream.source.replication.safety_window_seconds,
                "primary_key": stream.source.primary_key,
                "connector_type": source_connector_type,
            }

            # Build destination config with connection and endpoint details
            dest_config = {
                **dest_conn,  # Include host, headers, type, etc.
                **dest_endpoint,  # Include endpoint path, method, etc.
                "endpoint_id": dest.endpoint_id,
                "connection_ref": dest.connection_ref,
                "refresh_mode": dest.write.mode.value,
                "batch_support": dest.batching.supported if dest.batching else False,
                "batch_size": dest.batching.size if dest.batching else 1,
                "connector_type": dest_connector_type,
            }

            # Validate configs against enriched models (logs warnings on failure)
            validate_source_config(source_config)
            validate_dest_config(dest_config)

            # Build mapping config for engine
            if self._legacy_mode:
                # In legacy mode, pass through the original field_mappings and
                # computed_fields so the DataTransformer can use the legacy path
                # which supports transformations like iso_to_date, uppercase, etc.
                legacy_stream_data = self._legacy_config.get("streams", {}).get(stream.stream_id, {})
                legacy_mapping = legacy_stream_data.get("mapping", {})
                mapping_config = {
                    "field_mappings": legacy_mapping.get("field_mappings", {}),
                    "computed_fields": legacy_mapping.get("computed_fields", {}),
                }
            else:
                mapping_config = {
                    "assignments": [a.model_dump() for a in stream.mapping.assignments]
                }

            streams[stream.stream_id] = {
                "name": stream.stream_id,
                "source": source_config,
                "destination": dest_config,
                "mapping": mapping_config,
            }

        # Build pipeline-level source/destination from first stream for engine compatibility
        # The engine's PipelineConfig model requires these fields
        first_stream = self.stream_configs[0] if self.stream_configs else None
        if first_stream:
            first_dest = first_stream.get_primary_destination()
            first_source_conn = get_connection_config(first_stream.source.connection_ref)
            first_dest_conn = get_connection_config(first_dest.connection_ref)

            # Get host_id (connection UUID) safely
            try:
                source_host_id = self.pipeline_config.connections.get_connection_uuid(
                    first_stream.source.connection_ref
                )
            except KeyError:
                source_host_id = ""

            try:
                dest_host_id = self.pipeline_config.connections.get_connection_uuid(
                    first_dest.connection_ref
                )
            except KeyError:
                dest_host_id = ""

            pipeline_source_config = {
                **first_source_conn,
                "endpoint_id": first_stream.source.endpoint_id,
                "host_id": source_host_id,
            }
            pipeline_dest_config = {
                **first_dest_conn,
                "endpoint_id": first_dest.endpoint_id,
                "host_id": dest_host_id,
            }
        else:
            pipeline_source_config = {}
            pipeline_dest_config = {}

        return {
            "pipeline_id": self.pipeline_config.pipeline_id,
            "name": self.pipeline_config.name,
            "version": self.pipeline_config.version,
            "client_id": self.pipeline_config.client_id,
            "connections": self.pipeline_config.connections.model_dump(),
            "resolved_connections": self.resolved_connections,  # Include for engine access
            "source": pipeline_source_config,
            "destination": pipeline_dest_config,
            "streams": streams,
            "engine_config": self.pipeline_config.engine_config.model_dump(),
            "monitoring": {
                "log_level": self.pipeline_config.engine_config.logging.log_level,
                "metrics_enabled": self.pipeline_config.engine_config.logging.metrics_enabled,
                "checkpoint_interval": self.pipeline_config.engine_config.logging.checkpoint_interval,
                "progress_monitoring": self.pipeline_config.engine_config.logging.progress_monitoring,
            },
            "connectors": self.connectors,
        }

    async def run(self):
        """Execute the pipeline and optional progress monitoring."""
        pipeline_id = self.pipeline_config.pipeline_id

        # Check if progress monitoring is enabled
        progress_monitoring = self.pipeline_config.engine_config.logging.progress_monitoring == "enabled"

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

        pipeline_id = self.pipeline_config.pipeline_id
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

