"""Modern multi-stream engine with state management and improved architecture."""

import asyncio
import logging
from asyncio import Queue, Semaphore
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, AsyncIterator, Tuple
from uuid import uuid4

from pydantic import ValidationError

from ..connectors.base import BaseConnector
from ..fault_tolerance.circuit_breaker import CircuitBreaker
from ..fault_tolerance.dead_letter_queue import DeadLetterQueue
from ..fault_tolerance.retry_handler import RetryHandler
from ..fault_tolerance.state_manager import StateManager
from ..config import DIRECTORIES
from ..models.metrics import PipelineMetrics
from ..models.state import PipelineConfig
from ..models.engine import (
    EngineConfig, StreamProcessingConfig, PipelineStagesConfig,
    StreamStageConfig, PipelineMetricsSnapshot
)
from .data_transformer import DataTransformer
from .exceptions import (
    ConfigurationError, StreamProcessingError, StreamExecutionError,
    StreamConfigurationError, StageConfigurationError
)
from .orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)


class StreamingEngine:
    """
    High-performance async multi-stream engine with state management.

    Features:
    - Multi-stream concurrent processing
    - State management for scalability
    - Pydantic validation for configuration
    - Async/await pattern for non-blocking I/O
    - Fault tolerance with retries, circuit breakers, and DLQ
    - Configurable batch processing with backpressure handling
    """

    def __init__(
        self,
        pipeline_id: str,
        batch_size: int = 1000,
        max_concurrent_batches: int = 10,
        buffer_size: int = 10000,
        dlq_path: str = "./deadletter/",
        engine_config: Optional[EngineConfig] = None,
    ):
        self.pipeline_id = pipeline_id
        
        # Validate and set engine configuration
        if engine_config:
            self.engine_config = engine_config
        else:
            self.engine_config = EngineConfig(
                batch_size=batch_size,
                max_concurrent_batches=max_concurrent_batches,
                buffer_size=buffer_size,
                dlq_path=dlq_path
            )
        
        # Initialize from validated config
        self.batch_size = self.engine_config.batch_size
        self.max_concurrent_batches = self.engine_config.max_concurrent_batches
        self.buffer_size = self.engine_config.buffer_size
        self.semaphore = Semaphore(self.engine_config.max_concurrent_batches)
        
        # Setup structured logging
        self.logger = logging.getLogger(f"{__name__}.{pipeline_id}")
        
        # Pipeline orchestration
        self.orchestrator = PipelineOrchestrator(pipeline_id)

        # Fault tolerance components with state management
        self.sharded_state_manager = StateManager(pipeline_id, str(DIRECTORIES["state"]))
        self.retry_handler = RetryHandler()
        self.circuit_breaker = CircuitBreaker()
        self.dlq = DeadLetterQueue(self.engine_config.dlq_path)
        
        # Data transformation component
        self.data_transformer = DataTransformer()

        # Metrics tracking with Pydantic validation
        self.metrics = PipelineMetrics()
        
        # Stage configurations
        self.stage_configs = PipelineStagesConfig()

    async def stream_data(self, pipeline_config: Dict[str, Any]) -> None:
        """Process all streams concurrently with state management."""
        
        # Validate configuration using Pydantic
        try:
            config = PipelineConfig(**pipeline_config)
            logger.info("Pipeline configuration validation passed")
        except Exception as e:
            logger.error(f"Pipeline configuration validation failed: {e}")
            raise ConfigurationError(f"Invalid pipeline configuration: {e}") from e

        pipeline_id = pipeline_config["pipeline_id"]
        streams = pipeline_config.get("streams", {})
        
        if not streams:
            raise ConfigurationError("No streams configured in pipeline")

        logger.info(f"Starting pipeline: {pipeline_id}")
        logger.info(f"Processing {len(streams)} streams concurrently")
            
        # Start new run
        run_id = self.sharded_state_manager.start_run(pipeline_config)
        logger.info(f"Started state run: {run_id}")

        stream_exceptions = []
        stream_tasks = []
        
        try:
            # Create a task for each stream with names for better debugging
            for stream_id, stream_config in streams.items():
                stream_name = stream_config.get('name', stream_id)
                logger.info(f"Starting stream: {stream_name}")
                
                task = asyncio.create_task(
                    self._process_stream(
                        stream_id=stream_id,
                        stream_config=stream_config,
                        pipeline_config=pipeline_config
                    ),
                    name=f"stream-{stream_name}"
                )
                stream_tasks.append((stream_id, stream_name, task))

            # Wait for all streams with exception collection
            results = await asyncio.gather(
                *[task for _, _, task in stream_tasks], 
                return_exceptions=True
            )
            
            # Collect exceptions using Python 3.11+ pattern
            for (stream_id, stream_name, _), result in zip(stream_tasks, results):
                if isinstance(result, Exception):
                    stream_error = StreamProcessingError(
                        f"Stream processing failed: {result}", 
                        stream_id=stream_id,
                        original_error=result
                    )
                    stream_exceptions.append(stream_error)
                    self.metrics.increment_streams_failed()
                    logger.error(f"Stream {stream_name} failed: {result}")
                else:
                    self.metrics.increment_streams_processed()
                    logger.info(f"Stream {stream_name} completed successfully")

            # Handle collected exceptions with ExceptionGroup
            if stream_exceptions:
                if len(stream_exceptions) == len(streams):
                    # All streams failed - critical failure
                    logger.error("All streams failed - pipeline failed completely")
                    raise ExceptionGroup("All streams failed", stream_exceptions)
                else:
                    # Partial failure - log but allow pipeline to complete
                    logger.warning(f"Pipeline completed with {len(stream_exceptions)} failed streams out of {len(streams)}")
                    # Could optionally raise ExceptionGroup based on failure threshold
            else:
                logger.info(f"Pipeline {pipeline_id} completed successfully - all {len(streams)} streams processed")

        except* StreamProcessingError as eg:
            # Handle stream processing errors specifically (Python 3.11+)
            logger.error(f"Stream processing errors occurred: {len(eg.exceptions)} streams failed")
            for exc in eg.exceptions:
                logger.error(f"  - {exc}")
            # Cancel remaining tasks
            for _, _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise
        except* Exception as eg:
            # Handle other unexpected errors (Python 3.11+)
            logger.error(f"Unexpected errors in pipeline: {len(eg.exceptions)} errors")
            for exc in eg.exceptions:
                logger.error(f"  - Unexpected error: {exc}")
            # Cancel remaining tasks
            for _, _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise

    async def _process_stream(
        self, 
        stream_id: str, 
        stream_config: Dict[str, Any], 
        pipeline_config: Dict[str, Any]
    ) -> None:
        """Process a single stream with extract-transform-load pipeline."""
        
        stream_name = stream_config.get('name', stream_id)
        logger.info(f"Processing stream: {stream_name}")
        
        source_connector = None
        dest_connector = None
        stream_dlq = None
        tasks = []
        
        try:
            # Get connectors for this stream using factory methods
            source_connector = self._create_source_connector(stream_config["source"])
            dest_connector = self._create_destination_connector(stream_config["destination"])
            
            # Create stream-specific DLQ
            stream_dlq_path = f"{self.dlq.dlq_path}/{stream_id}"
            stream_dlq = DeadLetterQueue(stream_dlq_path)
            
            # Connect to source and destination
            await source_connector.connect(stream_config["source"])
            await dest_connector.connect(stream_config["destination"])
            
            # Configure destination if it supports configuration (e.g., DatabaseConnector)
            if hasattr(dest_connector, 'configure'):
                await dest_connector.configure(stream_config["destination"])
            
            # Create async queues for pipeline stages
            extract_queue = Queue(maxsize=self.buffer_size)
            transform_queue = Queue(maxsize=self.buffer_size)
            load_queue = Queue(maxsize=self.buffer_size)

            # Build stream processing config
            stream_processing_config = {
                **pipeline_config,
                **stream_config,  # Stream config should override pipeline config
                "stream_id": stream_id,
                "stream_name": stream_name,
                "stream_config": stream_config,  # Keep original for reference
            }

            # Start pipeline stages for this stream using factory method
            tasks = self._create_pipeline_stages(
                source_connector=source_connector,
                dest_connector=dest_connector,
                extract_queue=extract_queue,
                transform_queue=transform_queue,
                load_queue=load_queue,
                stream_processing_config=stream_processing_config,
                stream_dlq=stream_dlq,
                stream_name=stream_name
            )

            # Wait for all stream stages to complete
            await asyncio.gather(*tasks)

            logger.info(f"Stream {stream_name} completed successfully")

        except Exception as e:
            logger.error(f"Stream {stream_name} processing failed: {str(e)}")
            # Cancel any running tasks for this stream
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise

        finally:
            # Clean up connections for this stream
            try:
                if source_connector:
                    await source_connector.disconnect()
                if dest_connector:
                    await dest_connector.disconnect()
                logger.debug(f"Stream {stream_name} connectors disconnected successfully")
            except Exception as e:
                logger.warning(f"Failed to disconnect connectors for stream {stream_name}: {str(e)}")

    async def _extract_stage(
        self, source_connector: BaseConnector, queue: Queue, config: Dict[str, Any]
    ):
        """Extract data from source in batches with state management."""
        stream_name = config["stream_name"]
        logger.debug(f"Starting extract stage for stream {stream_name}")

        try:
            source_config = config["source"].copy()
            # Add full config for state access
            source_config["pipeline_config"] = config
            
            # Add stream-level replication settings to source config
            from analitiq_stream.models.state import ReplicationConfig
            replication_fields = ReplicationConfig.get_replication_field_names()
            for field in replication_fields:
                if field in config:
                    source_config[field] = config[field]
            
            logger.debug(f"Stream {stream_name}: Built source config with keys: {list(source_config.keys())}")
            logger.debug(f"Stream {stream_name}: Source config cursor_field = {source_config.get('cursor_field')}")
            
            # Use stream_id as the stream name for state management
            state_stream_name = f"stream.{config['stream_id']}"
            partition = {}  # Single partition per stream for now

            batch_count = 0
            
            # Use state management with API connector
            async for batch in source_connector.read_batches(
                source_config,
                state_manager=self.sharded_state_manager,
                stream_name=state_stream_name,
                partition=partition,
                batch_size=self.batch_size
            ):
                await queue.put(batch)
                batch_count += 1
                logger.debug(f"Stream {stream_name}: Extracted batch {batch_count} with {len(batch)} records")

            # Signal end of stream
            await queue.put(None)
            logger.info(f"Stream {stream_name}: Extract stage completed with {batch_count} batches")

        except Exception as e:
            logger.error(f"Stream {stream_name}: Extract stage failed: {str(e)}")
            await queue.put(None)  # Signal end even on error
            raise

    async def _transform_stage(
        self, input_queue: Queue, output_queue: Queue, config: Dict[str, Any]
    ):
        """Transform data with field mappings and validation."""
        stream_name = config["stream_name"]
        logger.debug(f"Starting transform stage for stream {stream_name}")

        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                # Apply transformations using the modular transformer
                transformed_batch = await self.data_transformer.apply_transformations(batch, config)
                
                await output_queue.put(transformed_batch)
                batch_count += 1
                
                self.metrics.increment_batches_processed()
                self.metrics.increment_records_processed(len(transformed_batch))

            # Signal end of stream
            await output_queue.put(None)
            logger.info(f"Stream {stream_name}: Transform stage completed with {batch_count} batches")

        except Exception as e:
            logger.error(f"Stream {stream_name}: Transform stage failed: {str(e)}")
            await output_queue.put(None)  # Signal end even on error
            self.metrics.increment_batches_failed()
            raise

    async def _load_stage(
        self, input_queue: Queue, output_queue: Queue, dest_connector: BaseConnector, config: Dict[str, Any], stream_dlq: 'DeadLetterQueue'
    ):
        """Load transformed data to destination with fault tolerance."""
        stream_name = config["stream_name"]
        logger.debug(f"Starting load stage for stream {stream_name}")

        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                try:
                    logger.debug(f"Stream {stream_name}: Starting write_batch for batch {batch_count + 1} with {len(batch)} records")
                    # Apply semaphore for concurrency control
                    async with self.semaphore:
                        await dest_connector.write_batch(batch, config["destination"])
                    logger.debug(f"Stream {stream_name}: Completed write_batch for batch {batch_count + 1}")
                    
                    await output_queue.put(batch)  # Forward for checkpointing
                    batch_count += 1
                    logger.debug(f"Stream {stream_name}: Successfully processed batch {batch_count}")

                except Exception as e:
                    logger.error(f"Stream {stream_name}: Batch write failed: {str(e)}")
                    self.metrics.increment_records_failed(len(batch))
                    self.metrics.increment_batches_failed()
                    
                    # Send failed records to DLQ
                    await stream_dlq.send_batch(batch, str(e), self.pipeline_id)

            # Signal end of stream
            await output_queue.put(None)
            logger.info(f"Stream {stream_name}: Load stage completed with {batch_count} batches")

        except Exception as e:
            logger.error(f"Stream {stream_name}: Load stage failed: {str(e)}")
            await output_queue.put(None)  # Signal end even on error
            raise

    async def _checkpoint_stage(self, input_queue: Queue, config: Dict[str, Any]):
        """Checkpoint processing progress with state management."""
        stream_name = config["stream_name"]
        logger.debug(f"Starting checkpoint stage for stream {stream_name}")

        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                # Checkpoint is handled by the API connector during read_batches
                # This stage just tracks completion
                batch_count += 1

            logger.info(f"Stream {stream_name}: Checkpoint stage completed with {batch_count} batches")

        except Exception as e:
            logger.error(f"Stream {stream_name}: Checkpoint stage failed: {str(e)}")
            raise


    def _create_source_connector(self, config: Dict[str, Any]) -> BaseConnector:
        """Create source connector based on configuration."""
        return self._get_connector(config)
    
    def _create_destination_connector(self, config: Dict[str, Any]) -> BaseConnector:
        """Create destination connector based on configuration."""
        return self._get_connector(config)
    
    def _create_pipeline_stages(
        self,
        source_connector: BaseConnector,
        dest_connector: BaseConnector,
        extract_queue: Queue,
        transform_queue: Queue,
        load_queue: Queue,
        stream_processing_config: Dict[str, Any],
        stream_dlq,
        stream_name: str
    ) -> List[asyncio.Task]:
        """Create all pipeline stage tasks using factory pattern."""
        return [
            asyncio.create_task(
                self._extract_stage(
                    source_connector, extract_queue, stream_processing_config
                ),
                name=f"extract-{stream_name}"
            ),
            asyncio.create_task(
                self._transform_stage(
                    extract_queue, transform_queue, stream_processing_config
                ),
                name=f"transform-{stream_name}"
            ),
            asyncio.create_task(
                self._load_stage(
                    transform_queue, load_queue, dest_connector, stream_processing_config, stream_dlq
                ),
                name=f"load-{stream_name}"
            ),
            asyncio.create_task(
                self._checkpoint_stage(load_queue, stream_processing_config),
                name=f"checkpoint-{stream_name}"
            ),
        ]

    def _get_connector(self, config: Dict[str, Any]) -> BaseConnector:
        """Get appropriate connector based on configuration."""
        connector_type = config.get("type", "api")
        
        if connector_type == "api":
            from ..connectors.api import APIConnector
            return APIConnector()
        elif connector_type == "database":
            from ..connectors.database import DatabaseConnector
            return DatabaseConnector()
        else:
            raise ValueError(f"Unknown connector type: {connector_type}")

    def _get_stream_name(self, config: Dict[str, Any]) -> str:
        """Generate stream name for state management."""
        # Use endpoint ID as stream name
        if "source" in config and "endpoint_id" in config["source"]:
            return f"endpoint.{config['source']['endpoint_id']}"
        else:
            return config.get("pipeline_id", "unknown-stream")

    def get_metrics(self) -> PipelineMetrics:
        """Get pipeline execution metrics as a validated Pydantic model."""
        return self.metrics

    def get_state_manager(self) -> StateManager:
        """Get the state manager instance."""
        return self.sharded_state_manager