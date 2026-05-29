"""Multi-stream engine: extract, transform, load, checkpoint."""

import asyncio
import json
import logging
import os
from asyncio import Queue, Semaphore
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, AsyncIterator, Tuple

import pyarrow as pa

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
from ..models.resolved import ResolvedPipeline, ResolvedStream, ResolvedSource
from .data_transformer import DataTransformer, build_output_schema, _translate_assignment
from .exceptions import (
    ConfigurationError, StreamProcessingError, StreamExecutionError,
    StreamConfigurationError, StageConfigurationError
)
from .orchestrator import PipelineOrchestrator

# gRPC imports for destination streaming
from ..grpc.client import DestinationGRPCClient, BatchResult, generate_record_id
from ..grpc.cursor import compute_max_cursor, cursor_to_state_dict
from ..grpc.generated.analitiq.v1 import AckStatus

logger = logging.getLogger(__name__)


class StreamingEngine:
    """
    High-performance async multi-stream engine with state management.

    Features:
    - Multi-stream concurrent processing
    - State management for scalability
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

        self.logger = logging.getLogger(f"{__name__}.{pipeline_id}")

        self.orchestrator = PipelineOrchestrator(pipeline_id)

        self.sharded_state_manager = StateManager(pipeline_id, "./state")
        self._state_manager = self.sharded_state_manager
        self.retry_handler = RetryHandler(max_retries=max_retries, base_delay=retry_delay)
        self.circuit_breaker = CircuitBreaker()
        self.dlq = DeadLetterQueue(self.dlq_path)

        self.data_transformer = DataTransformer()

        self.metrics = PipelineMetrics()

        self.stage_configs = PipelineStagesConfig()

    async def stream_data(self, pipeline: ResolvedPipeline) -> None:
        """Process all streams concurrently."""

        streams = pipeline.streams
        if not streams:
            raise ConfigurationError("No streams configured in pipeline")

        logger.info(f"Starting pipeline: {pipeline.pipeline_id}")
        logger.info(f"Processing {len(streams)} streams concurrently")

        run_id = self.state_manager.start_run({"pipeline_id": pipeline.pipeline_id})
        logger.info(f"Started state run: {run_id}")

        self.state_manager.init_commit_tracker(run_id)

        stream_exceptions = []
        stream_tasks = []

        try:
            for stream in streams:
                logger.info(f"Starting stream: {stream.stream_id}")
                task = asyncio.create_task(
                    self._process_stream(stream=stream, pipeline=pipeline),
                    name=f"stream-{stream.stream_id}"
                )
                stream_tasks.append((stream.stream_id, task))

            results = await asyncio.gather(
                *[task for _, task in stream_tasks],
                return_exceptions=True
            )

            for (stream_id, _), result in zip(stream_tasks, results):
                if isinstance(result, Exception):
                    stream_error = StreamProcessingError(
                        f"Stream processing failed: {result}",
                        stream_id=stream_id,
                        original_error=result
                    )
                    stream_exceptions.append(stream_error)
                    self.metrics.increment_streams_failed()
                    logger.error(f"Stream {stream_id} failed: {result}")
                else:
                    self.metrics.increment_streams_processed()
                    logger.info(f"Stream {stream_id} completed successfully")

            if stream_exceptions:
                if len(stream_exceptions) == len(streams):
                    logger.error("All streams failed - pipeline failed completely")
                    raise ExceptionGroup("All streams failed", stream_exceptions)
                else:
                    logger.warning(
                        f"Pipeline completed with {len(stream_exceptions)} failed "
                        f"streams out of {len(streams)}"
                    )
            else:
                logger.info(
                    f"Pipeline {pipeline.pipeline_id} completed successfully - "
                    f"all {len(streams)} streams processed"
                )

        except* StreamProcessingError as eg:
            logger.error(f"Stream processing errors occurred: {len(eg.exceptions)} streams failed")
            for exc in eg.exceptions:
                logger.error(f"  - {exc}")
            for _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise
        except* Exception as eg:
            logger.error(f"Unexpected errors in pipeline: {len(eg.exceptions)} errors")
            for exc in eg.exceptions:
                logger.error(f"  - Unexpected error: {exc}")
            for _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise

    async def _process_stream(
        self,
        stream: ResolvedStream,
        pipeline: ResolvedPipeline,
    ) -> None:
        """Process a single stream with extract-transform-load pipeline."""

        stream_id = stream.stream_id
        logger.info(f"Processing stream: {stream_id}")

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
            source = stream.source
            dest = stream.get_primary_destination()

            source_connector = self._create_source_connector(source)

            stream_dlq_path = f"{self.dlq.dlq_path}/{stream_id}"
            stream_dlq = DeadLetterQueue(stream_dlq_path)

            await source_connector.connect(source.runtime)

            run_id = self.state_manager.current_run_id or get_or_generate_run_id()

            grpc_client = self._create_grpc_client()
            connected = await grpc_client.connect()
            if not connected:
                raise StreamProcessingError(
                    f"Failed to connect to gRPC destination for stream {stream_id}",
                    stream_id=stream_id,
                )

            schema_accepted = await grpc_client.start_stream(
                run_id=run_id,
                stream_id=stream_id,
                schema_config={"write_mode": dest.config.write.mode.value},
            )
            if not schema_accepted:
                raise StreamProcessingError(
                    f"Destination rejected schema for stream {stream_id}",
                    stream_id=stream_id,
                )
            logger.info(f"Stream {stream_id}: gRPC stream started, schema accepted")

            extract_queue = Queue(maxsize=self.buffer_size)
            transform_queue = Queue(maxsize=self.buffer_size)
            load_queue = Queue(maxsize=self.buffer_size)

            tasks = self._create_pipeline_stages(
                source_connector=source_connector,
                grpc_client=grpc_client,
                extract_queue=extract_queue,
                transform_queue=transform_queue,
                load_queue=load_queue,
                stream=stream,
                pipeline=pipeline,
                stream_dlq=stream_dlq,
                run_id=run_id,
                stream_metrics=stream_metrics,
            )

            await asyncio.gather(*tasks)

            logger.info(f"Stream {stream_id} completed successfully")

        except Exception as e:
            status = "failed"
            error_message = str(e)
            logger.exception("Stream %s processing failed: %s", stream_id, e)
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise

        finally:
            end_time = datetime.now(timezone.utc)
            try:
                if source_connector:
                    await source_connector.disconnect()
                if grpc_client:
                    await grpc_client.disconnect()
                logger.debug(f"Stream {stream_id} connectors disconnected successfully")
            except Exception as e:
                logger.warning(
                    "Failed to disconnect connectors for stream %s: %s",
                    stream_id, e, exc_info=True,
                )

            try:
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
                    pipeline_name=pipeline.display_name,
                )
                if os.getenv("METRICS_ENABLED", "false").lower() == "true":
                    emit_metrics_log({
                        "type": "stream",
                        "stream_id": stream_id,
                        **record.model_dump(),
                    })
                    logger.info(f"Emitted stream metrics for {stream_id}")
            except Exception as metrics_error:
                logger.error(
                    "Failed to emit stream metrics for %s: %s",
                    stream_id, metrics_error, exc_info=True,
                )

    async def _extract_stage(
        self,
        source_connector: BaseConnector,
        queue: Queue,
        stream: ResolvedStream,
    ):
        """Extract data from source in batches."""
        stream_id = stream.stream_id
        logger.debug(f"Starting extract stage for stream {stream_id}")

        try:
            batch_count = 0
            async for batch in source_connector.read_batches(
                source=stream.source,
                state_manager=self.state_manager,
                stream_name=stream_id,
                partition={},
                batch_size=self.batch_size,
            ):
                await queue.put(batch)
                batch_count += 1
                logger.debug(
                    f"Stream {stream_id}: Extracted batch {batch_count} "
                    f"with {len(batch)} records"
                )

            await queue.put(None)
            logger.info(
                f"Stream {stream_id}: Extract stage completed with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Extract stage failed: %s", stream_id, e,
            )
            await queue.put(None)
            raise

    async def _transform_stage(
        self,
        input_queue: Queue,
        output_queue: Queue,
        stream: ResolvedStream,
        stream_metrics: Dict[str, int],
    ):
        """Transform data with field mappings."""
        stream_id = stream.stream_id
        logger.debug(f"Starting transform stage for stream {stream_id}")

        assignments_raw = [
            _translate_assignment(a.model_dump()) for a in stream.mapping.assignments
        ]
        output_schema = build_output_schema(assignments_raw) if assignments_raw else None

        transform_config = {
            "stream_id": stream_id,
            "mapping": {"assignments": assignments_raw},
        }

        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                if not assignments_raw:
                    transformed_batch = batch
                else:
                    pylist = batch.to_pylist()
                    transformed_pylist = await self.data_transformer.apply_transformations(
                        pylist, transform_config
                    )
                    transformed_batch = pa.RecordBatch.from_pylist(
                        transformed_pylist, schema=output_schema,
                    )

                await output_queue.put(transformed_batch)
                batch_count += 1

                self.metrics.increment_batches_processed()
                stream_metrics["batches_processed"] += 1

            await output_queue.put(None)
            logger.info(
                f"Stream {stream_id}: Transform stage completed with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Transform stage failed: %s", stream_id, e,
            )
            await output_queue.put(None)
            self.metrics.increment_batches_failed()
            stream_metrics["batches_failed"] += 1
            raise

    async def _load_stage(
        self,
        input_queue: Queue,
        output_queue: Queue,
        grpc_client: DestinationGRPCClient,
        stream: ResolvedStream,
        pipeline: ResolvedPipeline,
        stream_dlq: "DeadLetterQueue",
        run_id: str,
        stream_metrics: Dict[str, int],
    ) -> None:
        """Load transformed data to destination via gRPC streaming."""
        stream_id = stream.stream_id

        # Typed access — no dict key lookups.
        replication = stream.source.config.replication
        cursor_field = replication.cursor_field
        if isinstance(cursor_field, list):
            cursor_field = cursor_field[0] if cursor_field else None
        tie_breaker_fields = replication.tie_breaker_fields
        primary_key_fields = list(stream.source.config.primary_keys or [])

        error_strategy = (
            pipeline.runtime.error_handling.get("strategy", "fail")
        )

        logger.info(f"Stream {stream_id}: Starting gRPC load stage")

        batch_seq = 0
        max_retries = self.max_retries
        retry_base_delay = self.retry_delay

        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                batch_seq += 1

                record_count = batch.num_rows
                record_dicts = batch.to_pylist()

                logger.debug(
                    f"Stream {stream_id}: Processing batch {batch_seq} "
                    f"with {record_count} records"
                )

                record_ids = [
                    generate_record_id(
                        record=record,
                        run_id=run_id,
                        batch_seq=batch_seq,
                        index=i,
                        primary_key_fields=primary_key_fields if primary_key_fields else None,
                    )
                    for i, record in enumerate(record_dicts)
                ]

                cursor = None
                if cursor_field:
                    cursor = compute_max_cursor(
                        batch=record_dicts,
                        cursor_field=cursor_field,
                        tie_breaker_fields=tie_breaker_fields,
                    )

                if self.state_manager.commit_tracker:
                    existing = self.state_manager.commit_tracker.check_committed(
                        stream_id=stream_id,
                        batch_seq=batch_seq,
                    )
                    if existing:
                        logger.info(
                            f"Stream {stream_id}: Batch {batch_seq} already committed "
                            f"(in-run), skipping send"
                        )
                        continue

                retry_count = 0
                while True:
                    result: BatchResult = await grpc_client.send_batch(
                        run_id=run_id,
                        stream_id=stream_id,
                        batch_seq=batch_seq,
                        record_batch=batch,
                        record_ids=record_ids,
                        cursor=cursor,
                    )

                    if result.status == AckStatus.ACK_STATUS_SUCCESS:
                        cursor_data = {}
                        hwm = ""
                        if result.committed_cursor:
                            state_dict = cursor_to_state_dict(result.committed_cursor)
                            cursor_data = state_dict.get("cursor", {})
                            primary = cursor_data.get("primary", {})
                            hwm = primary.get("value", datetime.now(timezone.utc).isoformat())
                            self.state_manager.save_stream_checkpoint(
                                stream_name=stream_id,
                                partition={},
                                cursor=cursor_data,
                                hwm=hwm,
                            )
                        logger.debug(
                            f"Stream {stream_id}: Batch {batch_seq} committed, "
                            f"{result.records_written} records written"
                        )
                        self.metrics.increment_records_processed(result.records_written)
                        stream_metrics["records_processed"] += result.records_written

                        if self.state_manager.commit_tracker:
                            self.state_manager.commit_tracker.record_commit(
                                stream_id=stream_id,
                                batch_seq=batch_seq,
                                records_written=result.records_written,
                                cursor_bytes=result.committed_cursor.token if result.committed_cursor else b"",
                            )

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
                        cursor_data = {}
                        hwm = ""
                        if result.committed_cursor:
                            state_dict = cursor_to_state_dict(result.committed_cursor)
                            cursor_data = state_dict.get("cursor", {})
                            primary = cursor_data.get("primary", {})
                            hwm = primary.get("value", datetime.now(timezone.utc).isoformat())
                            self.state_manager.save_stream_checkpoint(
                                stream_name=stream_id,
                                partition={},
                                cursor=cursor_data,
                                hwm=hwm,
                            )

                        if self.state_manager.commit_tracker:
                            self.state_manager.commit_tracker.record_commit(
                                stream_id=stream_id,
                                batch_seq=batch_seq,
                                records_written=result.records_written,
                                cursor_bytes=result.committed_cursor.token if result.committed_cursor else b"",
                            )

                        logger.info(
                            f"Stream {stream_id}: Batch {batch_seq} already committed "
                            "(idempotent replay)"
                        )
                        await output_queue.put(batch)
                        break

                    elif result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
                        retry_count += 1
                        if retry_count > max_retries:
                            logger.error(
                                f"Stream {stream_id}: Batch {batch_seq} failed after "
                                f"{max_retries} retries: {result.failure_summary}"
                            )
                            self.metrics.increment_records_failed(record_count)
                            self.metrics.increment_batches_failed()
                            stream_metrics["records_failed"] += record_count
                            stream_metrics["batches_failed"] += 1
                            if error_strategy == "dlq":
                                await stream_dlq.send_batch(
                                    record_dicts, result.failure_summary, self.pipeline_id
                                )
                            break

                        delay = retry_base_delay * (2 ** (retry_count - 1))
                        logger.warning(
                            f"Stream {stream_id}: Batch {batch_seq} retryable failure, "
                            f"retry {retry_count}/{max_retries} after {delay:.2f}s: "
                            f"{result.failure_summary}"
                        )
                        await asyncio.sleep(delay)

                    elif result.status == AckStatus.ACK_STATUS_FATAL_FAILURE:
                        logger.error(
                            f"Stream {stream_id}: Batch {batch_seq} fatal failure: "
                            f"{result.failure_summary}"
                        )
                        self.metrics.increment_records_failed(record_count)
                        self.metrics.increment_batches_failed()
                        stream_metrics["records_failed"] += record_count
                        stream_metrics["batches_failed"] += 1

                        if error_strategy == "dlq":
                            await stream_dlq.send_batch(
                                record_dicts, result.failure_summary, self.pipeline_id
                            )

                        raise StreamProcessingError(
                            f"Batch {batch_seq} fatal failure: {result.failure_summary}"
                        )

                    else:
                        logger.error(
                            f"Stream {stream_id}: Batch {batch_seq} unknown status: "
                            f"{result.status}"
                        )
                        self.metrics.increment_records_failed(record_count)
                        self.metrics.increment_batches_failed()
                        stream_metrics["records_failed"] += record_count
                        stream_metrics["batches_failed"] += 1
                        if error_strategy == "dlq":
                            await stream_dlq.send_batch(
                                record_dicts, f"Unknown ACK status: {result.status}", self.pipeline_id
                            )
                        break

            await output_queue.put(None)
            logger.info(
                f"Stream {stream_id}: gRPC load stage completed with {batch_seq} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: gRPC load stage failed: %s", stream_id, e,
            )
            await output_queue.put(None)
            raise

    async def _checkpoint_stage(self, input_queue: Queue, stream: ResolvedStream):
        """Track checkpoint progress."""
        stream_id = stream.stream_id
        logger.debug(f"Starting checkpoint stage for stream {stream_id}")

        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break
                batch_count += 1

            logger.info(
                f"Stream {stream_id}: Checkpoint stage completed with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Checkpoint stage failed: %s", stream_id, e,
            )
            raise

    def _create_source_connector(self, source: ResolvedSource) -> BaseConnector:
        """Create source connector based on the resolved source runtime."""
        connector_type = source.runtime.connector_type

        if connector_type == "api":
            from ..source.connectors.api import APIConnector
            return APIConnector()
        elif connector_type == "database":
            from ..source.connectors.database import DatabaseConnector
            return DatabaseConnector()
        else:
            raise ValueError(f"Unsupported source connector kind: {connector_type!r}")

    def _create_pipeline_stages(
        self,
        source_connector: BaseConnector,
        grpc_client: DestinationGRPCClient,
        extract_queue: Queue,
        transform_queue: Queue,
        load_queue: Queue,
        stream: ResolvedStream,
        pipeline: ResolvedPipeline,
        stream_dlq,
        run_id: str,
        stream_metrics: Dict[str, int],
    ) -> List[asyncio.Task]:
        """Create all pipeline stage tasks."""
        stream_id = stream.stream_id
        return [
            asyncio.create_task(
                self._extract_stage(source_connector, extract_queue, stream),
                name=f"extract-{stream_id}"
            ),
            asyncio.create_task(
                self._transform_stage(
                    extract_queue, transform_queue, stream, stream_metrics,
                ),
                name=f"transform-{stream_id}"
            ),
            asyncio.create_task(
                self._load_stage(
                    input_queue=transform_queue,
                    output_queue=load_queue,
                    grpc_client=grpc_client,
                    stream=stream,
                    pipeline=pipeline,
                    stream_dlq=stream_dlq,
                    run_id=run_id,
                    stream_metrics=stream_metrics,
                ),
                name=f"load-{stream_id}"
            ),
            asyncio.create_task(
                self._checkpoint_stage(load_queue, stream),
                name=f"checkpoint-{stream_id}"
            ),
        ]

    def _create_grpc_client(self) -> DestinationGRPCClient:
        """Create gRPC client. Env vars take precedence over defaults."""
        host = os.getenv("DESTINATION_GRPC_HOST", "localhost")
        port = int(os.getenv("DESTINATION_GRPC_PORT", "50051"))
        timeout = int(os.getenv("GRPC_TIMEOUT_SECONDS", "300"))
        max_message_size = 16 * 1024 * 1024

        logger.info(f"Creating gRPC client for {host}:{port}")

        return DestinationGRPCClient(
            host=host,
            port=port,
            timeout_seconds=timeout,
            max_retries=self.max_retries,
            max_message_size=max_message_size,
        )

    def _get_stream_name(self, stream: ResolvedStream) -> str:
        """Generate a stable stream name for state management."""
        ref = stream.source.config.endpoint_ref
        if ref:
            return f"endpoint.{ref.scope}:{ref.connection_id}/{ref.endpoint_id}"
        return stream.pipeline_id or "unknown-stream"

    def get_metrics(self) -> PipelineMetrics:
        """Get pipeline execution metrics."""
        return self.metrics

    def get_state_manager(self) -> StateManager:
        return self.state_manager

    @property
    def state_manager(self) -> StateManager:
        return self._state_manager

    @state_manager.setter
    def state_manager(self, value: StateManager) -> None:
        self._state_manager = value
        self.sharded_state_manager = value

    @state_manager.deleter
    def state_manager(self) -> None:
        self._state_manager = self.sharded_state_manager
