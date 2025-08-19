"""Pipeline management and configuration."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from .credentials import credentials_manager
from .engine import StreamingEngine
from ..fault_tolerance.config_compatibility import (
    ConfigStateValidator, ConfigStateRecoveryManager, ConfigCompatibilityError
)

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


class Pipeline:
    """
    High-level pipeline management class for loading modular configuration
    and orchestrating streaming operations.
    """

    def __init__(
        self,
        pipeline_config: Dict[str, Any],
        source_config: Dict[str, Any],
        destination_config: Dict[str, Any],
        state_dir: Optional[str] = None,
        validation_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize pipeline with pre-merged configurations.

        Args:
            pipeline_config: Pipeline configuration (id, name, streams, engine_config)
            source_config: Merged source configuration (host credentials + endpoint schema)
            destination_config: Merged destination configuration (host credentials + endpoint schema)
            state_dir: Optional directory for state files (if None, uses default)
            validation_config: Validation configuration (optional)
        """
        # Load .env file if it exists
        load_dotenv()
        
        # Validate required configurations
        if not pipeline_config:
            raise ValueError("pipeline_config is required")
        if not source_config:
            raise ValueError("source_config is required")
        if not destination_config:
            raise ValueError("destination_config is required")
        
        # Multi-stream configuration (only supported format)
        if "streams" not in pipeline_config:
            raise ValueError("pipeline_config must contain 'streams' section")
        if "pipeline_id" not in pipeline_config:
            raise ValueError("pipeline_config must contain 'pipeline_id'")
            
        # Build unified configuration
        self.config = self._build_unified_config(
            pipeline_config, source_config, destination_config, validation_config
        )

        # Extract configuration values
        pipeline_id = self.config["pipeline_id"]
        engine_config = self.config.get("engine_config", {})

        # Setup directory structure using centralized paths
        project_root = Path(__file__).parent.parent.parent
        self.state_dir = state_dir or str(project_root / "state")
        self.logs_dir = str(project_root / "logs" / pipeline_id)
        self.dlq_dir = str(project_root / "deadletter" / pipeline_id)
        
        # Create required directories
        self._ensure_directories()
        
        # Setup logging for this pipeline
        self._setup_pipeline_logging(pipeline_id)

        self.engine = StreamingEngine(
            pipeline_id=pipeline_id,
            batch_size=engine_config.get("batch_size", 1000),
            max_concurrent_batches=engine_config.get("max_concurrent_batches", 10),
            buffer_size=engine_config.get("buffer_size", 10000),
            dlq_path=self.dlq_dir,
        )


    def _build_unified_config(
        self, 
        pipeline_config: Dict[str, Any], 
        source_config: Dict[str, Any], 
        destination_config: Dict[str, Any],
        validation_config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Build unified configuration from pre-merged components."""
        try:
            # Build stream configurations
            streams_config = {}
            streams = pipeline_config.get("streams", {})
            
            for stream_id, stream_config in streams.items():
                # Build stream configuration using pre-merged source and destination configs
                src_config = stream_config.get("src", {})
                dst_config = stream_config.get("dst", {})
                
                # Extract replication settings from stream-level src config
                src_settings = {k: v for k, v in src_config.items() if k != "endpoint_id"}
                dst_settings = {k: v for k, v in dst_config.items() if k != "endpoint_id"}
                
                logger.debug(f"Stream {stream_id} src settings: {src_settings}")
                logger.debug(f"Stream {stream_id} dst settings: {dst_settings}")
                
                stream_unified_config = {
                    "stream_id": stream_id,
                    "name": stream_config.get("name", stream_id),
                    "description": stream_config.get("description", ""),
                    "source": source_config.copy(),  # Pre-merged source config
                    "destination": destination_config.copy(),  # Pre-merged destination config
                    "mapping": stream_config.get("mapping", {}),
                    # Include stream-level src config like replication settings
                    **src_settings,
                    # Include stream-level dst config
                    **dst_settings,
                }
                
                logger.debug(f"Final stream config keys: {list(stream_unified_config.keys())}")
                
                streams_config[stream_id] = stream_unified_config
            
            # Build final pipeline configuration
            unified_config = {
                "pipeline_id": pipeline_config["pipeline_id"],
                "name": pipeline_config.get("name", "Multi-Stream Pipeline"),
                "version": pipeline_config.get("version", "1.0"),
                "engine_config": pipeline_config.get("engine_config", {}),
                "streams": streams_config,
                "error_handling": pipeline_config.get("error_handling", {}),
                "monitoring": pipeline_config.get("monitoring", {}),
            }
            
            # Add validation config if provided
            if validation_config:
                unified_config["validation"] = validation_config
            
            logger.info(f"Built unified configuration with {len(streams_config)} streams")
            return unified_config
            
        except Exception as e:
            logger.error(f"Failed to build unified configuration: {str(e)}")
            raise
    
    def _ensure_directories(self):
        """Create required directories for pipeline operation."""
        from pathlib import Path
        
        # Create all required directories
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.logs_dir).mkdir(parents=True, exist_ok=True) 
        Path(self.dlq_dir).mkdir(parents=True, exist_ok=True)

    def _setup_pipeline_logging(self, pipeline_id: str):
        """Setup structured logging for the pipeline."""
        import logging
        import os
        from pathlib import Path
        
        # Create logs directory structure
        logs_path = Path(self.logs_dir)
        logs_path.mkdir(parents=True, exist_ok=True)
        
        # Setup pipeline-level logger
        pipeline_logger = logging.getLogger(f"analitiq_stream.pipeline.{pipeline_id}")
        
        # Remove existing handlers to avoid duplicates
        if pipeline_logger.handlers:
            pipeline_logger.handlers.clear()
        
        # Create file handler for pipeline logs
        log_file = logs_path / f"pipeline.log"
        file_handler = logging.FileHandler(log_file)
        
        # Set log format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Set log level from monitoring config
        log_level = self.config.get("monitoring", {}).get("log_level", "INFO")
        pipeline_logger.setLevel(getattr(logging, log_level.upper()))
        file_handler.setLevel(getattr(logging, log_level.upper()))
        
        pipeline_logger.addHandler(file_handler)
        
        logger.info(f"Setup pipeline logging: {log_file}")
        
        # Setup stream-specific loggers
        for stream_id, stream_config in self.config.get("streams", {}).items():
            stream_name = stream_config.get("name", stream_id)
            # Use stream_id for directory consistency with DLQ
            stream_logs_path = logs_path / stream_id
            stream_logs_path.mkdir(parents=True, exist_ok=True)
            
            # Use stream_id in logger name for consistency
            stream_logger = logging.getLogger(f"analitiq_stream.stream.{pipeline_id}.{stream_id}")
            
            # Remove existing handlers
            if stream_logger.handlers:
                stream_logger.handlers.clear()
            
            # Create stream log file
            stream_log_file = stream_logs_path / "stream.log"
            stream_handler = logging.FileHandler(stream_log_file)
            stream_handler.setFormatter(formatter)
            stream_handler.setLevel(getattr(logging, log_level.upper()))
            
            stream_logger.setLevel(getattr(logging, log_level.upper()))
            stream_logger.addHandler(stream_handler)
            
            logger.debug(f"Setup stream logging: {stream_log_file}")
            
            # Create DLQ directory for this stream (using stream_id for consistency)
            stream_dlq_path = Path(self.dlq_dir) / stream_id
            stream_dlq_path.mkdir(parents=True, exist_ok=True)

    def validate_config(self) -> bool:
        """Validate pipeline configuration."""
        try:
            # Validate pipeline-level configuration
            if not self._validate_pipeline_config():
                return False


            # Validate transformations if present
            if "transformations" in self.config:
                if not self._validate_transformations():
                    return False

            # Validate validation rules if present
            if "validation" in self.config:
                if not self._validate_validation_rules():
                    return False

            logger.info("Configuration validation passed")
            return True

        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            return False

    def _validate_pipeline_config(self) -> bool:
        """Validate pipeline-level configuration fields."""
        required_fields = ["pipeline_id"]

        for field in required_fields:
            if field not in self.config:
                logger.error(f"Missing required pipeline field: {field}")
                return False

        # Validate pipeline_id format
        pipeline_id = self.config["pipeline_id"]
        if not isinstance(pipeline_id, str) or not pipeline_id.strip():
            logger.error("pipeline_id must be a non-empty string")
            return False

        # Validate engine_config if present
        if "engine_config" in self.config:
            engine_config = self.config["engine_config"]
            if not isinstance(engine_config, dict):
                logger.error("engine_config must be a dictionary")
                return False

            # Validate numeric values in engine config
            numeric_fields = ["batch_size", "max_concurrent_batches", "buffer_size"]
            for field in numeric_fields:
                if field in engine_config and not isinstance(engine_config[field], int):
                    logger.error(f"engine_config.{field} must be an integer")
                    return False

        @staticmethod
        def _validate_src_dest(key:str) -> bool:
            config_value = self.config[key]
            if not isinstance(config_value, dict):
                logger.error(f"{key} must be a dictionary")
                return False

            # Validate required UUID fields
            required_uuid_fields = ["endpoint_id", "host_id"]
            for field in required_uuid_fields:
                if field not in config_value:
                    logger.error(f"{key}.{field} is required")
                    return False
                if not isinstance(config_value[field], str) or not config_value[field].strip():
                    logger.error(f"{key}.{field} must be a non-empty string")
                    return False

            # For data destinations, validate refresh_mode if present
            if key == "dst" and "refresh_mode" in config_value:
                valid_refresh_modes = ["insert", "upsert", "truncate_insert"]
                if config_value["refresh_mode"] not in valid_refresh_modes:
                    logger.error(f"Invalid dst.refresh_mode: {config_value['refresh_mode']}. Must be one of: {valid_refresh_modes}")
                    return False

            # Validate batch_size if present
            if "batch_size" in config_value:
                if not isinstance(config_value["batch_size"], int) or config_value["batch_size"] <= 0:
                    logger.error("dst.batch_size must be a positive integer")
                    return False

            return True

        # Validate src and dst sections if present
        for section in ("src", "dst"):
            if section in self.config:
                results = _validate_src_dest(section)
                if not results:
                    return False

        return True

    def _validate_transformations(self) -> bool:
        """Validate transformation configurations."""
        transformations = self.config["transformations"]

        if not isinstance(transformations, list):
            logger.error("transformations must be a list")
            return False

        valid_transformation_types = [
            "field_mapping",
            "value_transformation",
            "computed_field",
            "conditional_transformation",
        ]

        for i, transformation in enumerate(transformations):
            if not isinstance(transformation, dict):
                logger.error(f"Transformation {i} must be a dictionary")
                return False

            if "type" not in transformation:
                logger.error(f"Transformation {i} missing required field: type")
                return False

            if transformation["type"] not in valid_transformation_types:
                logger.error(f"Invalid transformation type: {transformation['type']}")
                return False

            # Validate specific transformation types
            if transformation["type"] == "field_mapping":
                if "mappings" not in transformation:
                    logger.error(f"Field mapping transformation {i} missing mappings")
                    return False

            elif transformation["type"] in ["value_transformation", "computed_field"]:
                if "field" not in transformation:
                    logger.error(f"Transformation {i} missing required field: field")
                    return False

        return True

    def _validate_validation_rules(self) -> bool:
        """Validate validation rule configurations."""
        validation_config = self.config["validation"]

        if not isinstance(validation_config, dict):
            logger.error("validation must be a dictionary")
            return False

        if "rules" not in validation_config:
            logger.error("validation missing required field: rules")
            return False

        rules = validation_config["rules"]
        if not isinstance(rules, list):
            logger.error("validation.rules must be a list")
            return False

        valid_rule_types = ["not_null", "enum", "range", "regex", "custom"]
        valid_error_actions = ["dlq", "skip", "fail"]

        for i, rule in enumerate(rules):
            if not isinstance(rule, dict):
                logger.error(f"Validation rule {i} must be a dictionary")
                return False

            required_fields = ["field", "type", "error_action"]
            for field in required_fields:
                if field not in rule:
                    logger.error(f"Validation rule {i} missing required field: {field}")
                    return False

            if rule["type"] not in valid_rule_types:
                logger.error(f"Invalid validation rule type: {rule['type']}")
                return False

            if rule["error_action"] not in valid_error_actions:
                logger.error(f"Invalid error action: {rule['error_action']}")
                return False

        return True

    def _validate_config_state_compatibility(self):
        """Validate config-state compatibility and handle breaking changes."""
        state_manager = self.engine.get_state_manager()
        run_info = state_manager.get_run_info()
        
        # Skip validation if no previous state exists
        if not run_info:
            logger.info("No previous state found - skipping compatibility validation")
            return
        
        validator = ConfigStateValidator()
        
        try:
            # Validate compatibility
            is_compatible, changes = validator.validate_startup_compatibility(self.config, run_info)
            
            if is_compatible:
                if changes:
                    logger.info(f"Non-breaking config changes detected: {len(changes)} changes")
                    for change in changes:
                        logger.info(f"  - {change.description}")
                return
            
            # Breaking changes detected - attempt automatic recovery
            breaking_changes = [c for c in changes if c.change_type.value == "breaking"]
            critical_changes = [c for c in changes if c.change_type.value == "critical"]
            
            # Log consolidated breaking changes message
            total_changes = len(breaking_changes) + len(critical_changes)
            if total_changes > 0:
                logger.warning(f"Breaking config changes detected: {total_changes} changes")
                
                if critical_changes:
                    critical_descriptions = [c.description for c in critical_changes]
                    logger.warning(f"  Critical changes: {', '.join(critical_descriptions)}")
                
                if breaking_changes:
                    breaking_descriptions = [c.description for c in breaking_changes]
                    logger.warning(f"  Breaking changes: {', '.join(breaking_descriptions)}")
            
            recovery_manager = ConfigStateRecoveryManager(state_manager)
            
            if recovery_manager.attempt_automatic_recovery(self.config, changes):
                logger.info("Automatic recovery successful - continuing with updated state")
            else:
                # Recovery failed - raise error with details
                breaking_changes = [c for c in changes if c.change_type.value in ["breaking", "critical"]]
                raise ConfigCompatibilityError(
                    f"Breaking configuration changes require manual intervention",
                    breaking_changes,
                    self.config.get("pipeline_id", "unknown")
                )
                
        except ConfigCompatibilityError:
            raise
        except Exception as e:
            logger.error(f"Config-state validation failed: {str(e)}")
            raise ConfigCompatibilityError(
                f"Config-state validation error: {str(e)}",
                [],
                self.config.get("pipeline_id", "unknown")
            )

    async def run(self):
        """Execute the pipeline with config-state validation and optional progress monitoring."""
        if not self.validate_config():
            raise ValueError("Invalid pipeline configuration")

        pipeline_id = self.config["pipeline_id"]
        
        # Perform config-state compatibility validation
        try:
            self._validate_config_state_compatibility()
        except ConfigCompatibilityError as e:
            logger.error(f"Config-state compatibility error: {str(e)}")
            logger.error(f"Detected {len(e.changes)} configuration changes")
            for change in e.changes:
                logger.error(f"  - {change.change_type.value}: {change.description}")
            raise

        # Check if progress monitoring is enabled
        monitoring_config = self.config.get("monitoring", {})
        progress_monitoring = monitoring_config.get("progress_monitoring") == "enabled"

        try:
            if progress_monitoring:
                await self._run_with_progress_monitoring()
            else:
                await self.engine.stream_data(self.config)
            logger.info(f"Pipeline {pipeline_id} completed successfully")
        except Exception as e:
            logger.debug(f"Pipeline {pipeline_id} failed: {str(e)}")
            raise

    async def _run_with_progress_monitoring(self):
        """Run pipeline with progress monitoring enabled."""
        import asyncio
        from datetime import datetime
        
        pipeline_id = self.config["pipeline_id"]
        logger.info(f"📊 Starting pipeline {pipeline_id} with progress monitoring")
        
        # Start pipeline in background
        pipeline_task = asyncio.create_task(self.engine.stream_data(self.config))
        
        # Monitoring loop
        last_processed = 0
        start_time = datetime.now()
        
        logger.info("📈 Monitoring pipeline progress (updates every 5 seconds)...")
        
        while not pipeline_task.done():
            await asyncio.sleep(5)  # Check every 5 seconds
            
            try:
                metrics = self.get_metrics()
                current_processed = getattr(metrics, 'records_processed', 0)
                
                if current_processed > last_processed:
                    elapsed = datetime.now() - start_time
                    rate = current_processed / max(elapsed.total_seconds(), 1)
                    
                    logger.info(f"  📈 {current_processed} records processed ({rate:.1f}/sec)")
                    last_processed = current_processed
                    
            except Exception as e:
                logger.debug(f"Progress monitoring error: {e}")
                # Continue monitoring even if metrics fail
                pass
        
        # Wait for completion
        await pipeline_task
        
        # Final summary
        final_metrics = self.get_metrics()
        total_time = datetime.now() - start_time
        
        logger.info(f"✅ Pipeline {pipeline_id} monitoring completed")
        logger.info(f"⏱️  Total time: {total_time}")
        logger.info(f"📊 Final records processed: {getattr(final_metrics, 'records_processed', 0)}")
        
        if getattr(final_metrics, 'records_failed', 0) > 0:
            logger.warning(f"⚠️  Failed records: {getattr(final_metrics, 'records_failed', 0)}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get pipeline execution metrics."""
        return self.engine.get_metrics()

    def get_status(self) -> Dict[str, Any]:
        """Get current pipeline status."""
        streams_info = {}
        for stream_id, stream_config in self.config["streams"].items():
            streams_info[stream_id] = {
                "name": stream_config.get("name", stream_id),
                "source": stream_config.get("source", {}).get("endpoint_id", "unknown"),
                "destination": stream_config.get("destination", {}).get("endpoint_id", "unknown"),
            }
        
        return {
            "pipeline_id": self.config["pipeline_id"],
            "streams": streams_info,
            "metrics": self.get_metrics(),
        }
