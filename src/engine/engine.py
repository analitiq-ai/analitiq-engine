"""Multi-stream streaming engine.

Consumes a typed :class:`ResolvedPipeline` plus its
:class:`ConnectionRuntime` map and runs every stream concurrently through
an extract / transform / load / checkpoint pipeline. There is no
dict-passing surface anywhere downstream of this module.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from asyncio import Queue, Semaphore
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..source.connectors.base import BaseConnector
from ..shared.connection_runtime import ConnectionRuntime
from ..shared.run_id import get_or_generate_run_id
from ..state.circuit_breaker import CircuitBreaker
from ..state.dead_letter_queue import DeadLetterQueue
from ..state.metrics_storage import create_metrics_record, emit_metrics_log
from ..state.retry_handler import RetryHandler
from ..state.state_manager import StateManager
from ..models.engine import PipelineStagesConfig
from ..models.metrics import PipelineMetrics

from .data_transformer import DataTransformer
from .exceptions import ConfigurationError, StreamProcessingError
from .resolved import (
    Assignment,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
)

from ..grpc.client import BatchResult, DestinationGRPCClient, generate_record_id
from ..grpc.cursor import compute_max_cursor, cursor_to_state_dict
from ..grpc.generated.analitiq.v1 import AckStatus, PayloadFormat


logger = logging.getLogger(__name__)


EndpointKey = tuple  # (scope, connection_id, endpoint_id)


@dataclass
class StreamContext:
    """Typed bundle passed between stages within a single stream."""

    stream: ResolvedStream
    pipeline: ResolvedPipeline
    src_runtime: ConnectionRuntime
    dest_runtime: ConnectionRuntime
    src_endpoint_raw: Dict[str, Any]
    dest_endpoint_raw: Dict[str, Any]

    @property
    def source(self) -> ResolvedSource:
        return self.stream.source

    @property
    def destination(self) -> ResolvedDestination:
        return self.stream.destinations[0]

    @property
    def stream_id(self) -> str:
        return self.stream.stream_id

    @property
    def stream_name(self) -> str:
        return self.stream.display_name or self.stream.stream_id


class StreamingEngine:
    """High-performance async multi-stream engine."""

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

        self.sharded_state_manager = StateManager(pipeline_id, "./state")
        self._state_manager = self.sharded_state_manager
        self.retry_handler = RetryHandler(max_retries=max_retries, base_delay=retry_delay)
        self.circuit_breaker = CircuitBreaker()
        self.dlq = DeadLetterQueue(self.dlq_path)
        self.data_transformer = DataTransformer()
        self.metrics = PipelineMetrics()
        self.stage_configs = PipelineStagesConfig()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    @classmethod
    def from_resolved(cls, resolved: ResolvedPipeline) -> "StreamingEngine":
        runtime = resolved.runtime
        return cls(
            pipeline_id=resolved.pipeline_id,
            batch_size=runtime.batching.batch_size,
            max_concurrent_batches=runtime.batching.max_concurrent_batches,
            buffer_size=runtime.buffer_size,
            dlq_path=f"./deadletter/{resolved.pipeline_id}",
            max_retries=runtime.error_handling.max_retries,
            retry_delay=runtime.error_handling.retry_delay_seconds,
        )

    async def run(
        self,
        resolved: ResolvedPipeline,
        runtimes: Dict[str, ConnectionRuntime],
        raw_endpoints: Dict[EndpointKey, Dict[str, Any]],
    ) -> None:
        """Run every stream in the pipeline concurrently."""
        if not resolved.streams:
            raise ConfigurationError("No streams configured in pipeline")

        logger.info(
            "Starting pipeline %s with %d stream(s)",
            resolved.pipeline_id, len(resolved.streams),
        )

        run_id = self.state_manager.start_run(
            {"pipeline_id": resolved.pipeline_id, "name": resolved.display_name}
        )
        logger.info("Started state run: %s", run_id)
        self.state_manager.init_commit_tracker(run_id)

        stream_exceptions: List[StreamProcessingError] = []
        stream_tasks: List = []

        try:
            for stream in resolved.streams:
                if stream.status and stream.status != "active":
                    logger.info(
                        "Skipping stream %s (status=%s)", stream.stream_id, stream.status
                    )
                    continue
                context = self._build_context(stream, resolved, runtimes, raw_endpoints)
                task = asyncio.create_task(
                    self._process_stream(context),
                    name=f"stream-{stream.stream_id}",
                )
                stream_tasks.append((stream.stream_id, task))

            results = await asyncio.gather(
                *[task for _, task in stream_tasks], return_exceptions=True
            )

            for (stream_id, _), result in zip(stream_tasks, results):
                if isinstance(result, Exception):
                    err = StreamProcessingError(
                        f"Stream processing failed: {result}",
                        stream_id=stream_id,
                        original_error=result,
                    )
                    stream_exceptions.append(err)
                    self.metrics.increment_streams_failed()
                    logger.error("Stream %s failed: %s", stream_id, result)
                else:
                    self.metrics.increment_streams_processed()
                    logger.info("Stream %s completed", stream_id)

            if stream_exceptions:
                if len(stream_exceptions) == len(stream_tasks):
                    raise ExceptionGroup("All streams failed", stream_exceptions)
                logger.warning(
                    "Pipeline completed with %d failed stream(s) of %d",
                    len(stream_exceptions), len(stream_tasks),
                )
            else:
                logger.info("Pipeline %s completed successfully", resolved.pipeline_id)

        except* StreamProcessingError as eg:
            logger.error("Stream errors: %d", len(eg.exceptions))
            for _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise
        except* Exception as eg:
            logger.error("Unexpected pipeline errors: %d", len(eg.exceptions))
            for _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise

    def _build_context(
        self,
        stream: ResolvedStream,
        resolved: ResolvedPipeline,
        runtimes: Dict[str, ConnectionRuntime],
        raw_endpoints: Dict[EndpointKey, Dict[str, Any]],
    ) -> StreamContext:
        src = stream.source
        dest = stream.destinations[0]
        src_key = (src.endpoint_ref.scope, src.endpoint_ref.connection_id, src.endpoint_ref.endpoint_id)
        dest_key = (dest.endpoint_ref.scope, dest.endpoint_ref.connection_id, dest.endpoint_ref.endpoint_id)
        return StreamContext(
            stream=stream,
            pipeline=resolved,
            src_runtime=runtimes[src.connection.connection_id],
            dest_runtime=runtimes[dest.connection.connection_id],
            src_endpoint_raw=raw_endpoints[src_key],
            dest_endpoint_raw=raw_endpoints[dest_key],
        )

    # ------------------------------------------------------------------
    # Stream execution
    # ------------------------------------------------------------------

    async def _process_stream(self, ctx: StreamContext) -> None:
        stream_id = ctx.stream_id
        stream_name = ctx.stream_name
        logger.info("Processing stream: %s", stream_name)

        source_connector: Optional[BaseConnector] = None
        grpc_client: Optional[DestinationGRPCClient] = None
        stream_dlq: Optional[DeadLetterQueue] = None
        tasks: List[asyncio.Task] = []
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
            source_connector = self._create_source_connector(ctx.source)

            stream_dlq_path = f"{self.dlq.dlq_path}/{stream_id}"
            stream_dlq = DeadLetterQueue(stream_dlq_path)

            await source_connector.connect(ctx.src_runtime)

            run_id = self.state_manager.current_run_id or get_or_generate_run_id()

            grpc_client = self._create_grpc_client()
            if not await grpc_client.connect():
                raise StreamProcessingError(
                    f"Failed to connect to gRPC destination for stream {stream_name}",
                    stream_id=stream_id,
                )

            if not await grpc_client.start_stream(
                run_id=run_id,
                stream_id=stream_id,
                destination=ctx.destination,
                endpoint_schema_json=ctx.dest_endpoint_raw,
            ):
                raise StreamProcessingError(
                    f"Destination rejected schema for stream {stream_name}",
                    stream_id=stream_id,
                )
            logger.info("Stream %s: gRPC stream started", stream_name)

            extract_queue: Queue = Queue(maxsize=self.buffer_size)
            transform_queue: Queue = Queue(maxsize=self.buffer_size)
            load_queue: Queue = Queue(maxsize=self.buffer_size)

            tasks = [
                asyncio.create_task(
                    self._extract_stage(source_connector, extract_queue, ctx),
                    name=f"extract-{stream_name}",
                ),
                asyncio.create_task(
                    self._transform_stage(extract_queue, transform_queue, ctx, stream_metrics),
                    name=f"transform-{stream_name}",
                ),
                asyncio.create_task(
                    self._load_stage(transform_queue, load_queue, grpc_client, ctx, stream_dlq, run_id, stream_metrics),
                    name=f"load-{stream_name}",
                ),
                asyncio.create_task(
                    self._checkpoint_stage(load_queue, ctx),
                    name=f"checkpoint-{stream_name}",
                ),
            ]

            await asyncio.gather(*tasks)
            logger.info("Stream %s completed", stream_name)

        except Exception as e:
            status = "failed"
            error_message = str(e)
            logger.error("Stream %s processing failed: %s", stream_name, e)
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
            except Exception as e:
                logger.warning("Stream %s: disconnect failed: %s", stream_name, e)

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
                    pipeline_name=None,
                )
                if os.getenv("METRICS_ENABLED", "false").lower() == "true":
                    emit_metrics_log({
                        "type": "stream",
                        "stream_id": stream_id,
                        **record.model_dump(),
                    })
            except Exception as metrics_error:
                logger.error("Failed to emit metrics for %s: %s", stream_name, metrics_error)

    async def _extract_stage(
        self,
        source_connector: BaseConnector,
        queue: Queue,
        ctx: StreamContext,
    ) -> None:
        stream_name = ctx.stream_name
        logger.debug("Extract stage start: %s", stream_name)
        try:
            partition: Dict[str, Any] = {}
            batch_count = 0

            async for batch in source_connector.read_batches(
                ctx.source,
                state_manager=self.state_manager,
                stream_id=ctx.stream_id,
                partition=partition,
                batch_size=self.batch_size,
            ):
                await queue.put(batch)
                batch_count += 1

            await queue.put(None)
            logger.info("Stream %s: extract done (%d batches)", stream_name, batch_count)

        except Exception as e:
            logger.error("Stream %s: extract failed: %s", stream_name, e)
            await queue.put(None)
            raise

    async def _transform_stage(
        self,
        input_queue: Queue,
        output_queue: Queue,
        ctx: StreamContext,
        stream_metrics: Dict[str, int],
    ) -> None:
        stream_name = ctx.stream_name
        assignments = _assignments_to_dicts(ctx.stream.mapping.assignments if ctx.stream.mapping else ())
        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break
                transformed = await self.data_transformer.apply_assignments(batch, assignments)
                await output_queue.put(transformed)
                batch_count += 1
                self.metrics.increment_batches_processed()
                stream_metrics["batches_processed"] += 1
            await output_queue.put(None)
            logger.info("Stream %s: transform done (%d batches)", stream_name, batch_count)
        except Exception as e:
            logger.error("Stream %s: transform failed: %s", stream_name, e)
            await output_queue.put(None)
            self.metrics.increment_batches_failed()
            stream_metrics["batches_failed"] += 1
            raise

    async def _load_stage(
        self,
        input_queue: Queue,
        output_queue: Queue,
        grpc_client: DestinationGRPCClient,
        ctx: StreamContext,
        stream_dlq: DeadLetterQueue,
        run_id: str,
        stream_metrics: Dict[str, int],
    ) -> None:
        stream_name = ctx.stream_name
        stream_id = ctx.stream_id

        cursor_field = ctx.source.replication.cursor_field
        tie_breaker_fields = [
            list(t) if isinstance(t, tuple) else [t]
            for t in (ctx.source.replication.tie_breaker_fields or ())
        ]

        dest_endpoint = ctx.destination.endpoint
        primary_key_fields = list(getattr(dest_endpoint, "primary_keys", ()) or ())

        logger.info("Stream %s: gRPC load stage start", stream_name)

        batch_seq = 0
        max_retries = self.max_retries
        retry_base_delay = self.retry_delay
        error_strategy = ctx.pipeline.runtime.error_handling.strategy

        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                batch_seq += 1
                record_ids = [
                    generate_record_id(
                        record=record,
                        run_id=run_id,
                        batch_seq=batch_seq,
                        index=i,
                        primary_key_fields=primary_key_fields or None,
                    )
                    for i, record in enumerate(batch)
                ]

                cursor = None
                if cursor_field:
                    cursor = compute_max_cursor(
                        batch=batch,
                        cursor_field=cursor_field,
                        tie_breaker_fields=tie_breaker_fields or None,
                    )

                if self.state_manager.commit_tracker:
                    existing = self.state_manager.commit_tracker.check_committed(
                        stream_id=stream_id, batch_seq=batch_seq
                    )
                    if existing:
                        logger.info(
                            "Stream %s: batch %d already committed (in-run), skipping send",
                            stream_name, batch_seq,
                        )
                        continue

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

                    if result.status in (
                        AckStatus.ACK_STATUS_SUCCESS,
                        AckStatus.ACK_STATUS_ALREADY_COMMITTED,
                    ):
                        cursor_data: Dict[str, Any] = {}
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

                        if result.status == AckStatus.ACK_STATUS_SUCCESS:
                            self.metrics.increment_records_processed(result.records_written)
                            stream_metrics["records_processed"] += result.records_written

                        if self.state_manager.commit_tracker:
                            self.state_manager.commit_tracker.record_commit(
                                stream_id=stream_id,
                                batch_seq=batch_seq,
                                records_written=result.records_written,
                                cursor_bytes=result.committed_cursor.token if result.committed_cursor else b"",
                            )

                        if (
                            result.status == AckStatus.ACK_STATUS_SUCCESS
                            and os.getenv("METRICS_ENABLED", "false").lower() == "true"
                        ):
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

                    if result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
                        retry_count += 1
                        if retry_count > max_retries:
                            logger.error(
                                "Stream %s: batch %d failed after %d retries: %s",
                                stream_name, batch_seq, max_retries, result.failure_summary,
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
                        delay = retry_base_delay * (2 ** (retry_count - 1))
                        logger.warning(
                            "Stream %s: batch %d retry %d/%d after %.2fs",
                            stream_name, batch_seq, retry_count, max_retries, delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    if result.status == AckStatus.ACK_STATUS_FATAL_FAILURE:
                        logger.error(
                            "Stream %s: batch %d fatal: %s",
                            stream_name, batch_seq, result.failure_summary,
                        )
                        self.metrics.increment_records_failed(len(batch))
                        self.metrics.increment_batches_failed()
                        stream_metrics["records_failed"] += len(batch)
                        stream_metrics["batches_failed"] += 1
                        if error_strategy == "dlq":
                            await stream_dlq.send_batch(
                                batch, result.failure_summary, self.pipeline_id
                            )
                        raise StreamProcessingError(
                            f"Batch {batch_seq} fatal failure: {result.failure_summary}"
                        )

                    logger.error(
                        "Stream %s: batch %d unknown status %s",
                        stream_name, batch_seq, result.status,
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

            await output_queue.put(None)
            logger.info("Stream %s: gRPC load done (%d batches)", stream_name, batch_seq)

        except Exception as e:
            logger.error("Stream %s: load failed: %s", stream_name, e)
            await output_queue.put(None)
            raise

    async def _checkpoint_stage(self, input_queue: Queue, ctx: StreamContext) -> None:
        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break
                batch_count += 1
            logger.info(
                "Stream %s: checkpoint done (%d batches)", ctx.stream_name, batch_count
            )
        except Exception as e:
            logger.error("Stream %s: checkpoint failed: %s", ctx.stream_name, e)
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_source_connector(self, source: ResolvedSource) -> BaseConnector:
        kind = source.connection.connector.kind
        if kind == "api":
            from ..source.connectors.api import APIConnector
            return APIConnector()
        if kind == "database":
            from ..source.connectors.database import DatabaseConnector
            return DatabaseConnector()
        raise ValueError(f"Unknown connector kind for source: {kind!r}")

    def _create_grpc_client(self) -> DestinationGRPCClient:
        host = os.getenv("DESTINATION_GRPC_HOST", "localhost")
        port = int(os.getenv("DESTINATION_GRPC_PORT", "50051"))
        timeout = int(os.getenv("GRPC_TIMEOUT_SECONDS", "300"))
        max_message_size = 16 * 1024 * 1024

        logger.info("Creating gRPC client for %s:%s", host, port)
        return DestinationGRPCClient(
            host=host,
            port=port,
            timeout_seconds=timeout,
            max_retries=self.max_retries,
            max_message_size=max_message_size,
        )

    def get_metrics(self) -> PipelineMetrics:
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


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------


def _assignments_to_dicts(assignments: tuple) -> List[Dict[str, Any]]:
    """Convert typed :class:`Assignment` tuple to the dict form the transformer expects."""
    out: List[Dict[str, Any]] = []
    for a in assignments:
        target = a.target
        out.append({
            "target": {
                "path": target.path.split(".") if isinstance(target.path, str) else list(target.path),
                "type": target.arrow_type,
                "nullable": target.nullable,
            },
            "value": a.value,
            "validate": a.validate,
        })
    return out
