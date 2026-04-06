"""Modern multi-stream engine with state management and improved architecture."""

import asyncio
import json
import logging
import os
from asyncio import Queue, Semaphore
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, AsyncIterator, Tuple

from ..source.connectors.base import BaseConnector
from ..shared.connection_runtime import ConnectionRuntime
from ..shared.run_id import get_or_generate_run_id
from ..state.circuit_breaker import CircuitBreaker
from ..state.dead_letter_queue import DeadLetterQueue
from ..state.metrics_storage import emit_metrics_log, create_metrics_record
from ..state.retry_handler import RetryHandler
from ..state.state_manager import StateManager
from ..models.metrics import PipelineMetrics
from ..models.engine import (
    StreamProcessingConfig, PipelineStagesConfig,
    StreamStageConfig, PipelineMetricsSnapshot
)
from .data_transformer import DataTransformer
from .exceptions import (
    ConfigurationError, StreamProcessingError, StreamExecutionError,
    StreamConfigurationError, StageConfigurationError
)
from .orchestrator import PipelineOrchestrator

# gRPC imports for destination streaming
from ..grpc.client import DestinationGRPCClient, BatchResult, generate_record_id
from ..grpc.cursor import compute_max_cursor, cursor_to_state_dict
from ..grpc.generated.analitiq.v1 import AckStatus, PayloadFormat

logger = logging.getLogger(__name__)


def _deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge two dictionaries without mutating inputs."""

    merged = deepcopy(base)

    for key, value in override.items():
        if (
            isinstance(value, dict)
            and isinstance(merged.get(key), dict)
        ):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = deepcopy(value)

    return merged


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
        max_retries: int = 3,
        retry_delay: float = 5.0,
    ):
        self.pipeline_id = pipeline_id
        self.batch_size = batch_size
        self.max_concurrent_batches = max_concurrent_batches
        self.buffer_size = buffer_size
        self.dlq_path = dlq_path
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.semaphore = Semaphore(max_concurrent_batches)
        
        # Setup structured logging
        self.logger = logging.getLogger(f"{__name__}.{pipeline_id}")
        
        # Pipeline orchestration
        self.orchestrator = PipelineOrchestrator(pipeline_id)

        # Fault tolerance components with state management
        self.sharded_state_manager = StateManager(pipeline_id, "./state")
        self._state_manager = self.sharded_state_manager
        self.retry_handler = RetryHandler(max_retries=max_retries, base_delay=retry_delay)
        self.circuit_breaker = CircuitBreaker()
        self.dlq = DeadLetterQueue(self.dlq_path)
        
        # Data transformation component
        self.data_transformer = DataTransformer()

        # Metrics tracking with Pydantic validation
        self.metrics = PipelineMetrics()

        # Stage configurations
        self.stage_configs = PipelineStagesConfig()


    async def stream_data(self, pipeline_config: Dict[str, Any]) -> None:
        """Process all streams concurrently with state management."""

        pipeline_id = pipeline_config["pipeline_id"]
        streams = pipeline_config.get("streams", {})

        if not streams:
            raise ConfigurationError("No streams configured in pipeline")

        logger.info(f"Starting pipeline: {pipeline_id}")
        logger.info(f"Processing {len(streams)} streams concurrently")
            
        # Start new run
        run_id = self.state_manager.start_run(pipeline_config)
        logger.info(f"Started state run: {run_id}")

        # Initialize in-run commit tracker
        self.state_manager.init_commit_tracker(run_id)

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
        grpc_client = None
        stream_dlq = None
        tasks = []
        stream_start_time = datetime.now(timezone.utc)
        stream_metrics = {
            "records_processed": 0,
            "records_failed": 0,
            "batches_processed": 0,
            "batches_failed": 0,
        }
        status = "success"
        error_message: Optional[str] = None

        try:
            pipeline_src_config = pipeline_config.get("source", {})
            pipeline_dst_config = pipeline_config.get("destination", {})

            stream_src_config = stream_config.get("source", {})
            stream_dst_config = stream_config.get("destination", {})

            merged_src_config = _deep_merge_dicts(pipeline_src_config, stream_src_config)
            merged_dst_config = _deep_merge_dicts(pipeline_dst_config, stream_dst_config)

            # Get source connector
            source_connector = self._create_source_connector(merged_src_config)

            # Create stream-specific DLQ
            stream_dlq_path = f"{self.dlq.dlq_path}/{stream_id}"
            stream_dlq = DeadLetterQueue(stream_dlq_path)

            # Connect source connector using runtime
            runtime = merged_src_config.get("_runtime")
            if not runtime:
                raise StreamConfigurationError(
                    "Missing _runtime in source config",
                    stream_id=stream_id,
                )
            await source_connector.connect(runtime)

            # Get current run_id from centralized source
            run_id = self.state_manager.current_run_id or get_or_generate_run_id()

            # Connect to destination via gRPC
            grpc_client = self._create_grpc_client(merged_dst_config)
            connected = await grpc_client.connect()
            if not connected:
                raise StreamProcessingError(
                    f"Failed to connect to gRPC destination for stream {stream_name}",
                    stream_id=stream_id
                )

            # Start gRPC stream with schema
            schema_accepted = await grpc_client.start_stream(
                run_id=run_id,
                stream_id=stream_id,
                schema_config=merged_dst_config,
            )
            if not schema_accepted:
                raise StreamProcessingError(
                    f"Destination rejected schema for stream {stream_name}",
                    stream_id=stream_id
                )
            logger.info(f"Stream {stream_name}: gRPC stream started, schema accepted")

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
                "source": merged_src_config,
                "destination": merged_dst_config,
            }

            # Start pipeline stages for this stream
            tasks = self._create_pipeline_stages(
                source_connector=source_connector,
                grpc_client=grpc_client,
                extract_queue=extract_queue,
                transform_queue=transform_queue,
                load_queue=load_queue,
                stream_processing_config=stream_processing_config,
                stream_dlq=stream_dlq,
                stream_name=stream_name,
                run_id=run_id,
                stream_metrics=stream_metrics,
            )

            # Wait for all stream stages to complete
            await asyncio.gather(*tasks)

            logger.info(f"Stream {stream_name} completed successfully")

        except Exception as e:
            status = "failed"
            error_message = str(e)
            logger.error(f"Stream {stream_name} processing failed: {str(e)}")
            # Cancel any running tasks for this stream
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise

        finally:
            end_time = datetime.now(timezone.utc)
            # Clean up connections for this stream
            try:
                if source_connector:
                    await source_connector.disconnect()
                if grpc_client:
                    await grpc_client.disconnect()
                logger.debug(f"Stream {stream_name} connectors disconnected successfully")
            except Exception as e:
                logger.warning(f"Failed to disconnect connectors for stream {stream_name}: {str(e)}")

            try:
                pipeline_name = pipeline_config.get("name") or pipeline_config.get("pipeline", {}).get("name")
                record = create_metrics_record(
                    run_id=self.state_manager.current_run_id or get_or_generate_run_id(),
                    pipeline_id=self.pipeline_id,
                    start_time=stream_start_time,
                    end_time=end_time,
                    records_processed=stream_metrics["records_processed"],
                    records_failed=stream_metrics["records_failed"],
                    batches_processed=stream_metrics["batches_processed"],
                    status=status,
                    error_message=error_message,
                    pipeline_name=pipeline_name,
                )
                if os.getenv("METRICS_ENABLED", "false").lower() == "true":
                    emit_metrics_log({
                        "type": "stream",
                        "stream_id": stream_id,
                        **record.model_dump(),
                    })
                    logger.info(f"Emitted stream metrics for {stream_name}")
            except Exception as metrics_error:
                logger.error(f"Failed to emit stream metrics for {stream_name}: {metrics_error}")

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
            from src.models.state import ReplicationConfig
            replication_fields = ReplicationConfig.get_replication_field_names()
            for field in replication_fields:
                if field in config:
                    source_config[field] = config[field]
            
            logger.debug(f"Stream {stream_name}: Built source config with keys: {list(source_config.keys())}")
            logger.debug(f"Stream {stream_name}: Source config cursor_field = {source_config.get('cursor_field')}")
            
            # Use stream_id as the stream name for state management
            state_stream_name = config["stream_id"]
            partition = {}  # Single partition per stream for now

            batch_count = 0
            
            # Use state management with API connector
            async for batch in source_connector.read_batches(
                source_config,
                state_manager=self.state_manager,
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
        self,
        input_queue: Queue,
        output_queue: Queue,
        config: Dict[str, Any],
        stream_metrics: Dict[str, int],
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
                stream_metrics["batches_processed"] += 1

            # Signal end of stream
            await output_queue.put(None)
            logger.info(f"Stream {stream_name}: Transform stage completed with {batch_count} batches")

        except Exception as e:
            logger.error(f"Stream {stream_name}: Transform stage failed: {str(e)}")
            await output_queue.put(None)  # Signal end even on error
            self.metrics.increment_batches_failed()
            stream_metrics["batches_failed"] += 1
            raise

    async def _load_stage(
        self,
        input_queue: Queue,
        output_queue: Queue,
        grpc_client: DestinationGRPCClient,
        config: Dict[str, Any],
        stream_dlq: 'DeadLetterQueue',
        run_id: str,
        stream_metrics: Dict[str, int],
    ) -> None:
        """
        Load transformed data to destination via gRPC streaming.

        Implements strict in-order: send batch -> await ACK -> send next.

        Args:
            input_queue: Queue of transformed batches
            output_queue: Queue for checkpoint stage
            grpc_client: Connected gRPC client
            config: Stream processing configuration
            stream_dlq: Dead letter queue for failed batches
            run_id: Unique run identifier
        """
        stream_name = config["stream_name"]
        stream_id = config["stream_id"]
        dest_config = config.get("destination", {})

        # Extract cursor configuration
        cursor_field = config.get("cursor_field") or config.get("replication_key")
        tie_breaker_fields = config.get("tie_breaker_fields")
        primary_key_fields = dest_config.get("primary_key", [])

        logger.info(f"Stream {stream_name}: Starting gRPC load stage")

        batch_seq = 0
        max_retries = self.max_retries
        retry_base_delay = self.retry_delay
        error_strategy = config["runtime"]["error_handling"]["strategy"]

        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                batch_seq += 1
                logger.debug(
                    f"Stream {stream_name}: Processing batch {batch_seq} "
                    f"with {len(batch)} records"
                )

                # Generate stable record IDs for DLQ correlation
                record_ids = [
                    generate_record_id(
                        record=record,
                        run_id=run_id,
                        batch_seq=batch_seq,
                        index=i,
                        primary_key_fields=primary_key_fields if primary_key_fields else None,
                    )
                    for i, record in enumerate(batch)
                ]

                # Compute MAX cursor value in batch (batch may be unordered)
                cursor = None
                if cursor_field:
                    cursor = compute_max_cursor(
                        batch=batch,
                        cursor_field=cursor_field,
                        tie_breaker_fields=tie_breaker_fields,
                    )

                # In-run idempotency check (skip send, don't re-count)
                if self.state_manager.commit_tracker:
                    existing = self.state_manager.commit_tracker.check_committed(
                        stream_id=stream_id,
                        batch_seq=batch_seq,
                    )
                    if existing:
                        logger.info(
                            f"Stream {stream_name}: Batch {batch_seq} already committed "
                            f"(in-run), skipping send"
                        )
                        # Skip metrics increment and output_queue - batch was already counted
                        # when first committed. Checkpoint stage will show unique batches
                        # written this run, not total send attempts.
                        continue

                # Retry loop for RETRYABLE_FAILURE
                retry_count = 0
                while True:
                    result: BatchResult = await grpc_client.send_batch(
                        run_id=run_id,
                        stream_id=stream_id,
                        batch_seq=batch_seq,
                        records=batch,
                        record_ids=record_ids,
                        cursor=cursor,
                        payload_format=PayloadFormat.PAYLOAD_FORMAT_JSONL,
                    )

                    if result.status == AckStatus.ACK_STATUS_SUCCESS:
                        # Batch committed, persist cursor
                        cursor_data = {}
                        hwm = ""
                        if result.committed_cursor:
                            state_dict = cursor_to_state_dict(result.committed_cursor)
                            state_stream_name = stream_id
                            cursor_data = state_dict.get("cursor", {})
                            primary = cursor_data.get("primary", {})
                            hwm = primary.get("value", datetime.now(timezone.utc).isoformat())
                            self.state_manager.save_stream_checkpoint(
                                stream_name=state_stream_name,
                                partition={},
                                cursor=cursor_data,
                                hwm=hwm,
                            )
                        logger.debug(
                            f"Stream {stream_name}: Batch {batch_seq} committed, "
                            f"{result.records_written} records written"
                        )
                        self.metrics.increment_records_processed(result.records_written)
                        stream_metrics["records_processed"] += result.records_written

                        # Record batch commit for in-run idempotency
                        if self.state_manager.commit_tracker:
                            self.state_manager.commit_tracker.record_commit(
                                stream_id=stream_id,
                                batch_seq=batch_seq,
                                records_written=result.records_written,
                                cursor_bytes=result.committed_cursor.token if result.committed_cursor else b"",
                            )

                        # Emit per-batch metrics
                        if os.getenv("METRICS_ENABLED", "false").lower() == "true":
                            emit_metrics_log({
                                "type": "batch",
                                "run_id": run_id,
                                "pipeline_id": self.pipeline_id,
                                "stream_id": stream_id,
                                "batch_seq": batch_seq,
                                "records_written": result.records_written,
                                "cumulative_records_processed": self.metrics.records_processed,
                                "cumulative_records_failed": self.metrics.records_failed,
                                "cumulative_batches_processed": self.metrics.batches_processed,
                                "cursor": json.dumps(cursor_data).encode().hex() if cursor_data else "",
                                "cursor_value": hwm,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            })

                        await output_queue.put(batch)
                        break

                    elif result.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED:
                        # Idempotent replay - batch was already processed
                        cursor_data = {}
                        hwm = ""
                        if result.committed_cursor:
                            state_dict = cursor_to_state_dict(result.committed_cursor)
                            state_stream_name = stream_id
                            cursor_data = state_dict.get("cursor", {})
                            primary = cursor_data.get("primary", {})
                            hwm = primary.get("value", datetime.now(timezone.utc).isoformat())
                            self.state_manager.save_stream_checkpoint(
                                stream_name=state_stream_name,
                                partition={},
                                cursor=cursor_data,
                                hwm=hwm,
                            )

                        # Record batch commit for in-run idempotency (prevents re-send on retry)
                        if self.state_manager.commit_tracker:
                            self.state_manager.commit_tracker.record_commit(
                                stream_id=stream_id,
                                batch_seq=batch_seq,
                                records_written=result.records_written,
                                cursor_bytes=result.committed_cursor.token if result.committed_cursor else b"",
                            )

                        logger.info(
                            f"Stream {stream_name}: Batch {batch_seq} already committed "
                            "(idempotent replay)"
                        )
                        await output_queue.put(batch)
                        break

                    elif result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
                        retry_count += 1
                        if retry_count > max_retries:
                            logger.error(
                                f"Stream {stream_name}: Batch {batch_seq} failed after "
                                f"{max_retries} retries: {result.failure_summary}"
                            )
                            self.metrics.increment_records_failed(len(batch))
                            self.metrics.increment_batches_failed()
                            stream_metrics["records_failed"] += len(batch)
                            stream_metrics["batches_failed"] += 1
                            if error_strategy == "dlq":
                                await stream_dlq.send_batch(
                                    batch, result.failure_summary, self.pipeline_id
                                )
                            break

                        # Exponential backoff
                        delay = retry_base_delay * (2 ** (retry_count - 1))
                        logger.warning(
                            f"Stream {stream_name}: Batch {batch_seq} retryable failure, "
                            f"retry {retry_count}/{max_retries} after {delay:.2f}s: "
                            f"{result.failure_summary}"
                        )
                        await asyncio.sleep(delay)

                    elif result.status == AckStatus.ACK_STATUS_FATAL_FAILURE:
                        # Non-retryable failure - send entire batch to DLQ
                        logger.error(
                            f"Stream {stream_name}: Batch {batch_seq} fatal failure: "
                            f"{result.failure_summary}"
                        )
                        self.metrics.increment_records_failed(len(batch))
                        self.metrics.increment_batches_failed()
                        stream_metrics["records_failed"] += len(batch)
                        stream_metrics["batches_failed"] += 1

                        # Send entire batch to DLQ with record_ids for correlation
                        if error_strategy == "dlq":
                            await stream_dlq.send_batch(
                                batch, result.failure_summary, self.pipeline_id
                            )

                        # Raise exception to mark stream as failed
                        raise StreamProcessingError(
                            f"Batch {batch_seq} fatal failure: {result.failure_summary}"
                        )

                    else:
                        # Unknown status - treat as fatal
                        logger.error(
                            f"Stream {stream_name}: Batch {batch_seq} unknown status: "
                            f"{result.status}"
                        )
                        self.metrics.increment_records_failed(len(batch))
                        self.metrics.increment_batches_failed()
                        stream_metrics["records_failed"] += len(batch)
                        stream_metrics["batches_failed"] += 1
                        if error_strategy == "dlq":
                            await stream_dlq.send_batch(
                                batch, f"Unknown ACK status: {result.status}", self.pipeline_id
                            )
                        break

            # Signal end of stream
            await output_queue.put(None)
            logger.info(
                f"Stream {stream_name}: gRPC load stage completed with {batch_seq} batches"
            )

        except Exception as e:
            logger.error(f"Stream {stream_name}: gRPC load stage failed: {str(e)}")
            await output_queue.put(None)
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
        runtime = config.get("_runtime")
        if not runtime:
            raise ValueError("Missing _runtime in source config")

        connector_type = runtime.connector_type

        if connector_type == "api":
            from ..source.connectors.api import APIConnector
            return APIConnector()
        elif connector_type == "database":
            from ..source.connectors.database import DatabaseConnector
            return DatabaseConnector()
        else:
            raise ValueError(f"Unknown connector_type '{connector_type}'")

    def _create_pipeline_stages(
        self,
        source_connector: BaseConnector,
        grpc_client: DestinationGRPCClient,
        extract_queue: Queue,
        transform_queue: Queue,
        load_queue: Queue,
        stream_processing_config: Dict[str, Any],
        stream_dlq,
        stream_name: str,
        run_id: str,
        stream_metrics: Dict[str, int],
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
                    extract_queue,
                    transform_queue,
                    stream_processing_config,
                    stream_metrics,
                ),
                name=f"transform-{stream_name}"
            ),
            asyncio.create_task(
                self._load_stage(
                    input_queue=transform_queue,
                    output_queue=load_queue,
                    grpc_client=grpc_client,
                    config=stream_processing_config,
                    stream_dlq=stream_dlq,
                    run_id=run_id,
                    stream_metrics=stream_metrics,
                ),
                name=f"load-{stream_name}"
            ),
            asyncio.create_task(
                self._checkpoint_stage(load_queue, stream_processing_config),
                name=f"checkpoint-{stream_name}"
            ),
        ]

    def _create_grpc_client(
        self,
        dest_config: Dict[str, Any],
    ) -> DestinationGRPCClient:
        """
        Create a gRPC client for destination streaming.

        Configuration priority:
        1. Environment variables (DESTINATION_GRPC_HOST, DESTINATION_GRPC_PORT)
        2. Config dict (destination.grpc.host, destination.grpc.port)
        3. Defaults (localhost:50051)

        Args:
            dest_config: Destination configuration

        Returns:
            Configured DestinationGRPCClient instance
        """
        grpc_config = dest_config.get("grpc", {})

        host = os.getenv("DESTINATION_GRPC_HOST") or grpc_config.get("host", "localhost")
        port = int(os.getenv("DESTINATION_GRPC_PORT", "0")) or grpc_config.get("port", 50051)
        timeout = int(os.getenv("GRPC_TIMEOUT_SECONDS", "0")) or grpc_config.get("timeout_seconds", 300)
        max_retries = self.max_retries
        max_message_size = grpc_config.get("max_message_size", 16 * 1024 * 1024)

        logger.info(f"Creating gRPC client for {host}:{port}")

        return DestinationGRPCClient(
            host=host,
            port=port,
            timeout_seconds=timeout,
            max_retries=max_retries,
            max_message_size=max_message_size,
        )


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
        return self.state_manager

    @property
    def state_manager(self) -> StateManager:
        """State manager accessor for backward compatibility with legacy tests."""
        return self._state_manager

    @state_manager.setter
    def state_manager(self, value: StateManager) -> None:
        """Allow tests to override the state manager and keep aliases in sync."""
        self._state_manager = value
        self.sharded_state_manager = value

    @state_manager.deleter
    def state_manager(self) -> None:
        """Restore the default state manager when patched contexts exit."""
        self._state_manager = self.sharded_state_manager
