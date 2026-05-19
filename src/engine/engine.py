"""Multi-stream streaming engine.

Consumes a `ResolvedPipeline` plus its `ConnectionRuntime` map and runs
every stream concurrently through an extract / transform / load /
checkpoint pipeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from asyncio import Queue, Semaphore
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..source.connectors.base import BaseConnector
from ..shared.connection_runtime import ConnectionRuntime
from ..shared.placeholder import expand_placeholders
from ..shared.run_id import get_or_generate_run_id
from ..state.circuit_breaker import CircuitBreaker
from ..state.dead_letter_queue import DeadLetterQueue
from ..state.metrics_storage import emit_metrics_log, create_metrics_record
from ..state.retry_handler import RetryHandler
from ..state.state_manager import StateManager
from ..models.metrics import PipelineMetrics
from ..models.engine import PipelineStagesConfig

from .data_transformer import DataTransformer
from .exceptions import (
    ConfigurationError,
    StreamProcessingError,
    StreamConfigurationError,
)
from .resolved import (
    ApiReadEndpoint,
    ApiWriteEndpoint,
    DatabaseReadEndpoint,
    DatabaseWriteEndpoint,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
)

from ..grpc.client import DestinationGRPCClient, BatchResult, generate_record_id
from ..grpc.cursor import compute_max_cursor, cursor_to_state_dict
from ..grpc.generated.analitiq.v1 import AckStatus, PayloadFormat


logger = logging.getLogger(__name__)

_UNRESOLVED_PLACEHOLDER = re.compile(r"\$\{[^}]+\}")

EndpointKey = tuple  # (scope, connection_id, endpoint_id)


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
        """Construct an engine from the runtime parameters of a `ResolvedPipeline`."""
        runtime = resolved.runtime
        batching = runtime.batching
        error_handling = runtime.error_handling

        dlq_path = f"./deadletter/{resolved.pipeline_id}"
        return cls(
            pipeline_id=resolved.pipeline_id,
            batch_size=batching.batch_size,
            max_concurrent_batches=batching.max_concurrent_batches,
            buffer_size=runtime.buffer_size,
            dlq_path=dlq_path,
            max_retries=error_handling.max_retries,
            retry_delay=error_handling.retry_delay,
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

        logger.info("Starting pipeline %s with %d stream(s)", resolved.pipeline_id, len(resolved.streams))

        run_id = self.state_manager.start_run(
            {"pipeline_id": resolved.pipeline_id, "name": resolved.display_name}
        )
        logger.info("Started state run: %s", run_id)
        self.state_manager.init_commit_tracker(run_id)

        stream_exceptions: List[StreamProcessingError] = []
        stream_tasks: List = []

        try:
            for stream in resolved.streams:
                if not stream.is_enabled:
                    logger.info("Skipping disabled stream %s", stream.stream_id)
                    continue
                stream_dict = self._build_stream_dict(
                    stream=stream,
                    resolved=resolved,
                    runtimes=runtimes,
                    raw_endpoints=raw_endpoints,
                )
                task = asyncio.create_task(
                    self._process_stream(stream_dict),
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
                    len(stream_exceptions),
                    len(stream_tasks),
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

    # Legacy entry kept for symmetry with previous code paths.
    async def stream_data(
        self,
        resolved: ResolvedPipeline,
        runtimes: Dict[str, ConnectionRuntime],
        raw_endpoints: Dict[EndpointKey, Dict[str, Any]],
    ) -> None:
        await self.run(resolved, runtimes, raw_endpoints)

    # ------------------------------------------------------------------
    # Stream dict construction (the Step-6 boundary)
    # ------------------------------------------------------------------
    # The connector and gRPC client layers still read merged dicts. We
    # build that dict here once per stream from the resolved-runtime
    # objects. Step 6 of the schema-alignment plan replaces this with
    # typed `ResolvedSource` / `ResolvedDestination` parameters on the
    # connector and client APIs.

    def _build_stream_dict(
        self,
        *,
        stream: ResolvedStream,
        resolved: ResolvedPipeline,
        runtimes: Dict[str, ConnectionRuntime],
        raw_endpoints: Dict[EndpointKey, Dict[str, Any]],
    ) -> Dict[str, Any]:
        src = stream.source
        dest = stream.destinations[0]

        src_runtime = runtimes[src.connection.connection_id]
        dest_runtime = runtimes[dest.connection.connection_id]

        src_endpoint_raw = raw_endpoints[(src.endpoint_ref.scope, src.endpoint_ref.connection_id, src.endpoint_ref.endpoint_id)]
        dest_endpoint_raw = raw_endpoints[(dest.endpoint_ref.scope, dest.endpoint_ref.connection_id, dest.endpoint_ref.endpoint_id)]

        source_dict = self._build_source_dict(src, src_runtime, src_endpoint_raw, stream.stream_id)
        dest_dict = self._build_dest_dict(dest, dest_runtime, dest_endpoint_raw)

        runtime = resolved.runtime
        return {
            "stream_id": stream.stream_id,
            "stream_name": stream.stream_id,
            "name": stream.stream_id,
            "pipeline_id": resolved.pipeline_id,
            "source": source_dict,
            "destination": dest_dict,
            "mapping": {"assignments": [self._assignment_to_dict(a) for a in stream.mapping.assignments]},
            "runtime": {
                "buffer_size": runtime.buffer_size,
                "batching": {
                    "batch_size": runtime.batching.batch_size,
                    "max_concurrent_batches": runtime.batching.max_concurrent_batches,
                },
                "logging": {
                    "log_level": runtime.logging.log_level,
                    "metrics_enabled": runtime.logging.metrics_enabled,
                },
                "error_handling": {
                    "strategy": runtime.error_handling.strategy,
                    "max_retries": runtime.error_handling.max_retries,
                    "retry_delay": runtime.error_handling.retry_delay,
                },
            },
            "cursor_field": stream.source.replication.cursor_field[0]
            if stream.source.replication.cursor_field
            else None,
            "tie_breaker_fields": list(stream.source.replication.tie_breaker_fields),
        }

    def _build_source_dict(
        self,
        src: ResolvedSource,
        runtime: ConnectionRuntime,
        endpoint_raw: Dict[str, Any],
        stream_id: str,
    ) -> Dict[str, Any]:
        replication = src.replication
        cursor_field = replication.cursor_field[0] if replication.cursor_field else None

        params = runtime.raw_config.get("parameters") or {}
        host = _connection_host(runtime)

        source_dict: Dict[str, Any] = {
            "host": host,
            "parameters": params,
            "connector_type": runtime.connector_type,
            "driver": runtime.driver,
            "_runtime": runtime,
            **endpoint_raw,
            "endpoint_ref": {
                "scope": src.endpoint_ref.scope,
                "identifier": src.endpoint_ref.connection_id,
                "endpoint": src.endpoint_ref.endpoint_id,
            },
            "connection_ref": src.connection.connection_id,
            "replication_method": replication.method,
            "cursor_field": cursor_field,
            "cursor_mode": "inclusive",
            "safety_window_seconds": replication.safety_window_seconds,
            "primary_key": list(src.primary_keys),
        }

        # Expand legacy ${name} placeholders against connection parameters/
        # selections/discovered. Endpoint files still use this form (analitiq-engine#37).
        flat = _flat_connection_lookup(runtime.raw_config)
        if flat:
            for key in ("filters", "endpoint"):
                if key in source_dict:
                    source_dict[key] = expand_placeholders(
                        source_dict[key], flat, ignore_missing=True
                    )
                    _warn_unresolved_placeholders(source_dict[key], key, stream_id)
        return source_dict

    def _build_dest_dict(
        self,
        dest: ResolvedDestination,
        runtime: ConnectionRuntime,
        endpoint_raw: Dict[str, Any],
    ) -> Dict[str, Any]:
        write = dest.write
        params = runtime.raw_config.get("parameters") or {}
        host = _connection_host(runtime)

        return {
            "host": host,
            "parameters": params,
            "connector_type": runtime.connector_type,
            "driver": runtime.driver,
            "_runtime": runtime,
            **endpoint_raw,
            "endpoint_ref": {
                "scope": dest.endpoint_ref.scope,
                "identifier": dest.endpoint_ref.connection_id,
                "endpoint": dest.endpoint_ref.endpoint_id,
            },
            "connection_ref": dest.connection.connection_id,
            "refresh_mode": write.mode,
            "write_mode": write.mode,
            "batch_support": write.batching.supported if write.batching else False,
            "batch_size": write.batching.size if write.batching else 1,
            "primary_key": list(write.conflict_keys),
        }

    @staticmethod
    def _assignment_to_dict(assignment) -> Dict[str, Any]:
        target = assignment.target
        return {
            "target": {
                "field": target.field,
                "type": target.type,
                "nullable": target.nullable,
            },
            "value": assignment.value,
        }

    # ------------------------------------------------------------------
    # Stream execution (unchanged below)
    # ------------------------------------------------------------------

    async def _process_stream(self, stream_config: Dict[str, Any]) -> None:
        stream_id = stream_config["stream_id"]
        stream_name = stream_config["stream_name"]
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
            src_config = stream_config["source"]
            dst_config = stream_config["destination"]

            source_connector = self._create_source_connector(src_config)

            stream_dlq_path = f"{self.dlq.dlq_path}/{stream_id}"
            stream_dlq = DeadLetterQueue(stream_dlq_path)

            src_runtime = src_config.get("_runtime")
            if not src_runtime:
                raise StreamConfigurationError(
                    "Missing _runtime in source config", stream_id=stream_id
                )
            await source_connector.connect(src_runtime)

            run_id = self.state_manager.current_run_id or get_or_generate_run_id()

            grpc_client = self._create_grpc_client(dst_config)
            if not await grpc_client.connect():
                raise StreamProcessingError(
                    f"Failed to connect to gRPC destination for stream {stream_name}",
                    stream_id=stream_id,
                )

            if not await grpc_client.start_stream(
                run_id=run_id, stream_id=stream_id, schema_config=dst_config
            ):
                raise StreamProcessingError(
                    f"Destination rejected schema for stream {stream_name}",
                    stream_id=stream_id,
                )
            logger.info("Stream %s: gRPC stream started", stream_name)

            extract_queue: Queue = Queue(maxsize=self.buffer_size)
            transform_queue: Queue = Queue(maxsize=self.buffer_size)
            load_queue: Queue = Queue(maxsize=self.buffer_size)

            tasks = self._create_pipeline_stages(
                source_connector=source_connector,
                grpc_client=grpc_client,
                extract_queue=extract_queue,
                transform_queue=transform_queue,
                load_queue=load_queue,
                stream_processing_config=stream_config,
                stream_dlq=stream_dlq,
                stream_name=stream_name,
                run_id=run_id,
                stream_metrics=stream_metrics,
            )

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
        self, source_connector: BaseConnector, queue: Queue, config: Dict[str, Any]
    ):
        stream_name = config["stream_name"]
        logger.debug("Extract stage start: %s", stream_name)
        try:
            source_config = config["source"].copy()
            source_config["pipeline_config"] = config

            from src.models.state import ReplicationConfig
            for field in ReplicationConfig.get_replication_field_names():
                if field in config:
                    source_config[field] = config[field]

            state_stream_name = config["stream_id"]
            partition: Dict[str, Any] = {}
            batch_count = 0

            async for batch in source_connector.read_batches(
                source_config,
                state_manager=self.state_manager,
                stream_name=state_stream_name,
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
        config: Dict[str, Any],
        stream_metrics: Dict[str, int],
    ):
        stream_name = config["stream_name"]
        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break
                transformed = await self.data_transformer.apply_transformations(batch, config)
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
        config: Dict[str, Any],
        stream_dlq: DeadLetterQueue,
        run_id: str,
        stream_metrics: Dict[str, int],
    ) -> None:
        stream_name = config["stream_name"]
        stream_id = config["stream_id"]
        dest_config = config.get("destination", {})

        cursor_field = config.get("cursor_field")
        tie_breaker_fields = config.get("tie_breaker_fields")
        primary_key_fields = dest_config.get("primary_key", [])

        logger.info("Stream %s: gRPC load stage start", stream_name)

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
                        tie_breaker_fields=tie_breaker_fields,
                    )

                if self.state_manager.commit_tracker:
                    existing = self.state_manager.commit_tracker.check_committed(
                        stream_id=stream_id, batch_seq=batch_seq
                    )
                    if existing:
                        logger.info(
                            "Stream %s: batch %d already committed (in-run), skipping send",
                            stream_name,
                            batch_seq,
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
                                stream_name,
                                batch_seq,
                                max_retries,
                                result.failure_summary,
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
                            stream_name,
                            batch_seq,
                            retry_count,
                            max_retries,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    if result.status == AckStatus.ACK_STATUS_FATAL_FAILURE:
                        logger.error(
                            "Stream %s: batch %d fatal: %s",
                            stream_name,
                            batch_seq,
                            result.failure_summary,
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

                    logger.error("Stream %s: batch %d unknown status %s", stream_name, batch_seq, result.status)
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

    async def _checkpoint_stage(self, input_queue: Queue, config: Dict[str, Any]):
        stream_name = config["stream_name"]
        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break
                batch_count += 1
            logger.info("Stream %s: checkpoint done (%d batches)", stream_name, batch_count)
        except Exception as e:
            logger.error("Stream %s: checkpoint failed: %s", stream_name, e)
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_source_connector(self, config: Dict[str, Any]) -> BaseConnector:
        runtime = config.get("_runtime")
        if not runtime:
            raise ValueError("Missing _runtime in source config")

        connector_type = runtime.connector_type
        if connector_type == "api":
            from ..source.connectors.api import APIConnector
            return APIConnector()
        if connector_type == "database":
            from ..source.connectors.database import DatabaseConnector
            return DatabaseConnector()
        raise ValueError(f"Unknown connector_type '{connector_type}'")

    def _create_pipeline_stages(
        self,
        source_connector: BaseConnector,
        grpc_client: DestinationGRPCClient,
        extract_queue: Queue,
        transform_queue: Queue,
        load_queue: Queue,
        stream_processing_config: Dict[str, Any],
        stream_dlq: DeadLetterQueue,
        stream_name: str,
        run_id: str,
        stream_metrics: Dict[str, int],
    ) -> List[asyncio.Task]:
        return [
            asyncio.create_task(
                self._extract_stage(source_connector, extract_queue, stream_processing_config),
                name=f"extract-{stream_name}",
            ),
            asyncio.create_task(
                self._transform_stage(
                    extract_queue, transform_queue, stream_processing_config, stream_metrics
                ),
                name=f"transform-{stream_name}",
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
                name=f"load-{stream_name}",
            ),
            asyncio.create_task(
                self._checkpoint_stage(load_queue, stream_processing_config),
                name=f"checkpoint-{stream_name}",
            ),
        ]

    def _create_grpc_client(self, dest_config: Dict[str, Any]) -> DestinationGRPCClient:
        grpc_config = dest_config.get("grpc", {})
        host = os.getenv("DESTINATION_GRPC_HOST") or grpc_config.get("host", "localhost")
        port = int(os.getenv("DESTINATION_GRPC_PORT", "0")) or grpc_config.get("port", 50051)
        timeout = int(os.getenv("GRPC_TIMEOUT_SECONDS", "0")) or grpc_config.get("timeout_seconds", 300)
        max_message_size = grpc_config.get("max_message_size", 16 * 1024 * 1024)

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


def _connection_host(runtime: ConnectionRuntime) -> Optional[str]:
    """Best-effort host string for legacy consumers of the dict shape."""
    params = runtime.raw_config.get("parameters") or {}
    if isinstance(params, dict) and isinstance(params.get("host"), str):
        return params["host"]
    connector_def = runtime.connector_definition or {}
    transports = connector_def.get("transports") or {}
    default_ref = connector_def.get("default_transport")
    if default_ref and default_ref in transports:
        base_url = transports[default_ref].get("base_url")
        if isinstance(base_url, str):
            return base_url
    legacy = runtime.raw_config.get("host")
    return legacy if isinstance(legacy, str) else None


def _flat_connection_lookup(connection_config: Dict[str, Any]) -> Dict[str, str]:
    flat: Dict[str, str] = {}
    for scope_name in ("discovered", "selections", "parameters"):
        scope = connection_config.get(scope_name) or {}
        if not isinstance(scope, dict):
            continue
        for key, value in scope.items():
            if isinstance(value, (dict, list)) or value is None:
                continue
            flat[key] = str(value)
    return flat


def _warn_unresolved_placeholders(value: Any, field: str, stream_id: str) -> None:
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
            "stream %r: %s contains unresolved placeholders %s",
            stream_id,
            field,
            sorted(set(leftover)),
        )
