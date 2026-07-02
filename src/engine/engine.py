"""Modern multi-stream engine with state management and improved architecture."""

import asyncio
import json
import logging
import os
from asyncio import Queue, Semaphore
from datetime import datetime, timezone
from typing import Any

from cdk.contract import Readable

# gRPC imports for destination streaming
from ..grpc.client import (
    BatchResult,
    DestinationGRPCClient,
    generate_record_id,
    resolve_grpc_ack_timeout_seconds,
)
from ..grpc.cursor import compute_max_cursor, cursor_to_state_dict
from ..grpc.generated.analitiq.v1 import AckStatus, RetrySemantics
from ..models.metrics import PipelineMetrics
from ..models.resolved import RuntimeConfig
from ..shared.run_id import get_or_generate_run_id
from ..state.circuit_breaker import CircuitBreaker
from ..state.dead_letter_queue import DeadLetterQueue
from ..state.error_classification import (
    ErrorCode,
    FailureStage,
    classify_destination_failure,
    classify_for_metrics,
    classify_handshake_failure,
    classify_source_extract,
    customer_message,
    detail_for_code,
    tag_failure,
)
from ..state.metrics_storage import create_metrics_record, emit_metrics_log
from ..state.retry_handler import RetryHandler
from ..state.state_manager import StateManager
from .data_transformer import compile_transform
from .exceptions import ConfigurationError, StreamProcessingError

logger = logging.getLogger(__name__)


class _FullRefreshCheckpoint:
    """Checkpoint view for truncate_insert streams: never resumes.

    A full refresh must re-read the source from scratch on every
    (re)start — the destination truncates once per run, so a resumed
    slice would be the only data left in the target (issue #307).
    ``get_cursor`` therefore always answers ``None`` (full re-scan);
    ``save_cursor`` delegates so in-run watermark tracking and state
    emission stay exactly as they are for every other stream.
    """

    def __init__(self, inner: StateManager) -> None:
        self._inner = inner

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        return None

    async def save_cursor(
        self,
        stream_name: str,
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
    ) -> None:
        await self._inner.save_cursor(stream_name, partition, cursor)


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
        runtime: RuntimeConfig,
        dlq_path: str = "./deadletter/",
    ):
        self.pipeline_id = pipeline_id
        self.runtime = runtime
        self.dlq_path = dlq_path
        # Working values derived from the single typed RuntimeConfig (built by
        # the runner with pipeline-config > env > default precedence). No loose
        # constructor defaults: the only source of truth is the RuntimeConfig.
        # error_strategy and retry_delay stay plain attributes so a test can
        # vary one knob without rebuilding the frozen config.
        self.batch_size = runtime.batching.batch_size
        self.max_concurrent_batches = runtime.batching.max_concurrent_batches
        self.buffer_size = runtime.buffer_size
        self.max_retries = runtime.error_handling.max_retries
        self.retry_delay = runtime.error_handling.retry_delay_seconds
        self.error_strategy = runtime.error_handling.strategy
        self.semaphore = Semaphore(self.max_concurrent_batches)

        # Setup structured logging
        self.logger = logging.getLogger(f"{__name__}.{pipeline_id}")

        # Fault tolerance components with state management
        self.sharded_state_manager = StateManager(pipeline_id, "./state")
        self._state_manager = self.sharded_state_manager
        self.retry_handler = RetryHandler(
            max_retries=self.max_retries, base_delay=self.retry_delay
        )
        self.circuit_breaker = CircuitBreaker()
        self.dlq = DeadLetterQueue(self.dlq_path)

        # Metrics tracking with Pydantic validation
        self.metrics = PipelineMetrics()

        # Representative exception from a stream that failed while OTHER streams
        # succeeded (a partial run that stream_data does not re-raise). The
        # runner classifies it so a partial run with a failed stream is not
        # reported as success.
        self._dominant_stream_error: BaseException | None = None

        # Connector code never runs in the engine process: every source read
        # goes through an isolated worker subprocess that owns the connector
        # class, the driver, and the external connection (registry resolution
        # happens IN the worker). The engine keeps only the worker client.
        from src.engine.pipeline_config_prep import PipelineConfigPrep
        from src.worker.readable import WorkerReadable

        paths = PipelineConfigPrep._discover_paths()
        self._worker_readable = WorkerReadable(
            connectors_dir=paths["connectors"],
            connections_dir=paths["connections"],
        )

    async def stream_data(self, pipeline_config: dict[str, Any]) -> None:
        """Process all streams concurrently with state management.

        ``pipeline_config`` is the runner-assembled config dict (pipeline id,
        name, and per-stream source/destination/mapping). Runtime tuning is not
        carried here -- it is the typed :class:`RuntimeConfig` passed to the
        constructor and read via ``self.runtime`` and the values derived from
        it.
        """
        pipeline_id = pipeline_config["pipeline_id"]
        streams = pipeline_config.get("streams", {})

        if not streams:
            raise ConfigurationError("No streams configured in pipeline")

        logger.info(f"Starting pipeline: {pipeline_id}")
        logger.info(f"Processing {len(streams)} streams concurrently")

        # Start new run
        run_id = self.state_manager.start_run(pipeline_config)
        logger.info(f"Started state run: {run_id}")

        stream_exceptions = []
        stream_tasks = []

        try:
            # Create a task for each stream with names for better debugging
            for stream_id, stream_config in streams.items():
                stream_name = stream_config.get("name", stream_id)
                logger.info(f"Starting stream: {stream_name}")

                task = asyncio.create_task(
                    self._process_stream(
                        stream_id=stream_id,
                        stream_config=stream_config,
                        pipeline_config=pipeline_config,
                    ),
                    name=f"stream-{stream_name}",
                )
                stream_tasks.append((stream_id, stream_name, task))

            # Wait for all streams with exception collection
            results = await asyncio.gather(
                *[task for _, _, task in stream_tasks], return_exceptions=True
            )

            # Collect exceptions using Python 3.11+ pattern
            for (stream_id, stream_name, _), result in zip(stream_tasks, results):
                if isinstance(result, Exception):
                    stream_error = StreamProcessingError(
                        f"Stream processing failed: {result}",
                        stream_id=stream_id,
                        original_error=result,
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
                    # Partial failure - log but allow pipeline to complete. Keep
                    # ALL failed-stream exceptions as a group (like the all-failed
                    # path) so the runner classifies the dominant cause across
                    # every failure, not just the first.
                    logger.warning(
                        f"Pipeline completed with {len(stream_exceptions)} "
                        f"failed streams out of {len(streams)}"
                    )
                    self._dominant_stream_error = ExceptionGroup(
                        "Partial stream failures", stream_exceptions
                    )
            else:
                logger.info(
                    f"Pipeline {pipeline_id} completed successfully - "
                    f"all {len(streams)} streams processed"
                )

        except* StreamProcessingError as eg:
            # Handle stream processing errors specifically (Python 3.11+)
            logger.error(
                f"Stream processing errors occurred: "
                f"{len(eg.exceptions)} streams failed"
            )
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
            for unexpected_exc in eg.exceptions:
                logger.error(f"  - Unexpected error: {unexpected_exc}")
            # Cancel remaining tasks
            for _, _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise

    async def _process_stream(
        self,
        stream_id: str,
        stream_config: dict[str, Any],
        pipeline_config: dict[str, Any],
    ) -> None:
        """Process a single stream with extract-transform-load pipeline."""
        stream_name = stream_config.get("name", stream_id)
        logger.info(f"Processing stream: {stream_name}")

        source_connector = None
        grpc_client = None
        stream_dlq = None
        tasks = []
        stream_start_time = datetime.now(timezone.utc)
        stream_metrics = {
            "records_processed": 0,
            "records_failed": 0,
            "records_skipped": 0,
            "batches_processed": 0,
            "batches_failed": 0,
        }
        status = "success"
        error_code: ErrorCode | None = None
        error_message: str | None = None
        error_detail: str | None = None

        try:
            source_cfg = stream_config["source"]
            destination_cfg = stream_config["destination"]

            # Resolves the worker-backed Readable; raises a CONFIG_INVALID-tagged
            # ValueError if the source config was never resolved (the real guard
            # for a missing _resolved_source).
            source_connector = self._create_source_connector(source_cfg)

            stream_dlq_path = f"{self.dlq.dlq_path}/{stream_id}"
            stream_dlq = DeadLetterQueue(stream_dlq_path)

            run_id = self.state_manager.current_run_id or get_or_generate_run_id()

            grpc_client = self._create_grpc_client(destination_cfg)
            connected = await grpc_client.connect()
            if not connected:
                raise tag_failure(
                    StreamProcessingError(
                        f"Failed to connect to gRPC destination for stream "
                        f"{stream_name}",
                        stream_id=stream_id,
                    ),
                    code=ErrorCode.DESTINATION_WRITE_FAILED,
                    stage=FailureStage.DESTINATION_LOAD,
                )

            schema_accepted = await grpc_client.start_stream(
                run_id=run_id,
                stream_id=stream_id,
                schema_config=destination_cfg,
            )
            if not schema_accepted:
                # The destination did not accept the stream. configure_schema
                # only prepares the destination's own table (no schema is
                # validated), so this is either a destination config defect or a
                # transport failure. Tag it at the raise site from the concrete
                # reason (engine/proxy-generated, including the inner reason
                # forwarded across the worker proxy) so classification is
                # structural: a transport reason -> DESTINATION_WRITE_FAILED, any
                # other reason -> CONFIG_INVALID.
                reason = grpc_client.schema_rejection_message
                detail = f": {reason}" if reason else ""
                raise tag_failure(
                    StreamProcessingError(
                        f"Destination did not accept the stream for "
                        f"{stream_name}{detail}",
                        stream_id=stream_id,
                    ),
                    code=classify_handshake_failure(reason),
                    stage=FailureStage.DESTINATION_LOAD,
                )
            logger.info(f"Stream {stream_name}: gRPC stream started, schema accepted")
            # Surface the destination's retry-safety verdict per stream
            # (issue #286): an at-least-once stream re-sends committed
            # records on a same-run restart, and the operator should learn
            # that at startup, not from duplicated side effects.
            retry_verdict = grpc_client.stream_retry_semantics
            if retry_verdict is not None:
                semantics_value, retry_reason = retry_verdict
                logger.info(
                    "Stream %s: retry semantics on a same-run restart: %s (%s)",
                    stream_name,
                    RetrySemantics.Name(semantics_value),
                    retry_reason,
                )

            extract_queue: Queue[Any] = Queue(maxsize=self.buffer_size)
            transform_queue: Queue[Any] = Queue(maxsize=self.buffer_size)
            load_queue: Queue[Any] = Queue(maxsize=self.buffer_size)

            # Per-stream context handed to each pipeline stage. Carries
            # only what the stages actually read; the contract source and
            # destination dicts are nested verbatim.
            stream_processing_config = {
                "pipeline_id": pipeline_config["pipeline_id"],
                "stream_id": stream_id,
                "stream_name": stream_name,
                # Absent version -> 1, mirroring _split_stream_ref's bare -> 1
                # rule so an unversioned or minimally-built config stays runnable.
                "stream_version": stream_config.get("stream_version", 1),
                "source": source_cfg,
                "destination": destination_cfg,
                "mapping": stream_config.get("mapping") or {},
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

            if stream_metrics["records_failed"] > 0:
                # The dlq/skip strategies complete the stream without raising
                # after exhausting retries; reflect that it only partly succeeded
                # and carry the destination cause rather than reporting success.
                status = "partial"
                error_code = ErrorCode.DESTINATION_WRITE_FAILED
                error_message = customer_message(error_code)
                # 'skip' drops exhausted batches without a DLQ entry, so those
                # records are NOT recoverable; do not imply dead-lettering. The
                # 'partial' path is only reached via the dlq/skip break, so a
                # non-zero skip count means the strategy was skip. error_detail
                # carries only allowlisted-safe fields; the destination
                # failure_summary stays in the DLQ (when used) and the logs.
                if stream_metrics["records_skipped"] > 0:
                    reason = "records skipped (dropped) after retries"
                    action = "skipped (dropped)"
                else:
                    reason = "records dead-lettered after retries"
                    action = "dead-lettered"
                error_detail = detail_for_code(
                    error_code,
                    stage=FailureStage.DESTINATION_LOAD,
                    reason=reason,
                )
                logger.warning(
                    f"Stream {stream_name} completed partially: "
                    f"{stream_metrics['records_failed']} records {action}"
                )
            else:
                logger.info(f"Stream {stream_name} completed successfully")

        except Exception as e:
            status = "failed"
            error_code, error_message, error_detail = classify_for_metrics(e)
            logger.exception("Stream %s processing failed: %s", stream_name, e)
            # Cancel any running tasks for this stream, then drive them to
            # completion. The source reader releases its runtime in its own
            # ``finally``, so the cancelled extract task must be awaited here —
            # otherwise the runtime/session could stay open if the run tears
            # down before the cancelled task runs its cleanup.
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            raise

        finally:
            end_time = datetime.now(timezone.utc)
            # Clean up connections for this stream. The source connector is a
            # pure reader: ``read_batches`` materializes and releases its own
            # runtime, so there is nothing to disconnect here.
            try:
                if grpc_client:
                    await grpc_client.disconnect()
                logger.debug(
                    f"Stream {stream_name} connectors disconnected successfully"
                )
            except Exception as e:
                logger.warning(
                    "Failed to disconnect connectors for stream %s: %s",
                    stream_name,
                    e,
                    exc_info=True,
                )

            try:
                pipeline_name = pipeline_config.get("name")
                record = create_metrics_record(
                    run_id=self.state_manager.current_run_id
                    or get_or_generate_run_id(),
                    pipeline_id=self.pipeline_id,
                    start_time=stream_start_time,
                    end_time=end_time,
                    records_processed=stream_metrics["records_processed"],
                    records_failed=stream_metrics["records_failed"],
                    batches_processed=stream_metrics["batches_processed"],
                    status=status,
                    error_code=error_code,
                    error_message=error_message,
                    error_detail=error_detail,
                    pipeline_name=pipeline_name,
                )
                if os.getenv("METRICS_ENABLED", "false").lower() == "true":
                    emit_metrics_log(
                        {
                            "type": "stream",
                            "stream_id": stream_id,
                            **record.model_dump(),
                        }
                    )
                    logger.info(f"Emitted stream metrics for {stream_name}")
            except Exception as metrics_error:
                logger.error(
                    "Failed to emit stream metrics for %s: %s",
                    stream_name,
                    metrics_error,
                    exc_info=True,
                )

    async def _extract_stage(
        self,
        source_connector: Readable,
        queue: Queue[Any],
        config: dict[str, Any],
    ) -> None:
        """Extract data from source in batches with state management."""
        stream_name = config["stream_name"]
        logger.debug(f"Starting extract stage for stream {stream_name}")

        try:
            # The connectors read the contract documents directly off the
            # source config (``endpoint_document``, ``stream_source``).
            # No flattening or replication-field injection is needed.
            source_config = config["source"]
            resolved_source = source_config["_resolved_source"]
            runtime = resolved_source.runtime
            json_safe_config = {
                k: v for k, v in source_config.items() if k != "_resolved_source"
            }
            state_stream_name = config["stream_id"]
            partition: dict[str, Any] = {}

            # A truncate_insert stream is a full refresh: the destination
            # truncates once per run, so the source must read from scratch
            # on every (re)start. Resuming from a persisted cursor would
            # load only the resumed slice into the freshly truncated table
            # (issue #307), so the resume read is disabled; cursor saves
            # still flow through unchanged for watermark emission.
            checkpoint: Any = self.state_manager
            write_mode = str(
                (config.get("destination") or {}).get("write_mode", "")
            ).lower()
            if write_mode == "truncate_insert":
                logger.info(
                    "Stream %s: truncate_insert is a full refresh; "
                    "ignoring any persisted resume cursor",
                    stream_name,
                )
                checkpoint = _FullRefreshCheckpoint(self.state_manager)

            batch_count = 0
            async for batch in source_connector.read_batches(
                runtime,
                json_safe_config,
                checkpoint=checkpoint,
                stream_name=state_stream_name,
                partition=partition,
                batch_size=self.batch_size,
            ):
                await queue.put(batch)
                batch_count += 1
                logger.debug(
                    f"Stream {stream_name}: Extracted batch {batch_count} "
                    f"with {len(batch)} records"
                )

            # Signal end of stream
            await queue.put(None)
            logger.info(
                f"Stream {stream_name}: Extract stage completed "
                f"with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Extract stage failed: %s",
                stream_name,
                e,
            )
            await queue.put(None)  # Signal end even on error
            # Tag the failure source-extract so classification is deterministic
            # and never confused with a destination/transform cause. The code
            # within source (auth/unreachable/rate/config) is the one split a tag
            # cannot make for an opaque driver error. tag_failure is no-overwrite,
            # so a deeper tag -- e.g. the worker's deterministic-config signal
            # from readable.py -- still wins over this coarser default.
            tag_failure(
                e,
                code=classify_source_extract(e),
                stage=FailureStage.SOURCE_EXTRACT,
            )
            raise

    async def _transform_stage(
        self,
        input_queue: Queue[Any],
        output_queue: Queue[Any],
        config: dict[str, Any],
        stream_metrics: dict[str, int],
    ) -> None:
        """Transform data with field mappings and validation."""
        stream_name = config["stream_name"]
        logger.debug(f"Starting transform stage for stream {stream_name}")

        assignments = (config.get("mapping") or {}).get("assignments") or []
        # The assignments are static, so the transform is compiled once here into
        # vectorized Arrow compute and applied to every batch -- the data never
        # leaves Arrow (no per-record Python, no to_pylist/from_pylist).
        compiled = compile_transform(assignments) if assignments else None
        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                if compiled is None:
                    transformed_batch = batch
                else:
                    transformed_batch = compiled.run(batch)

                await output_queue.put(transformed_batch)
                batch_count += 1

                self.metrics.increment_batches_processed()
                stream_metrics["batches_processed"] += 1

            # Signal end of stream
            await output_queue.put(None)
            logger.info(
                f"Stream {stream_name}: Transform stage completed "
                f"with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Transform stage failed: %s",
                stream_name,
                e,
            )
            await output_queue.put(None)  # Signal end even on error
            self.metrics.increment_batches_failed()
            stream_metrics["batches_failed"] += 1
            # A transform failure is a mapping/type problem. The engine validates
            # no data schema, so this is a configuration defect (CONFIG_INVALID),
            # not a data-vs-schema mismatch. tag_failure is no-overwrite, so a
            # deeper tag still wins.
            tag_failure(e, code=ErrorCode.CONFIG_INVALID, stage=FailureStage.TRANSFORM)
            raise

    def _persist_committed_cursor(
        self,
        committed_cursor: Any | None,
        stream_id: str,
        stream_version: int,
    ) -> tuple[dict[str, Any], Any]:
        """Checkpoint the destination-acked watermark for one batch.

        Shared by the SUCCESS and ALREADY_COMMITTED ack paths so the watermark
        is recorded identically regardless of which one runs.

        Returns ``(cursor_data, hwm)`` for metrics emission. An empty/absent
        cursor (a batch that advanced no watermark — e.g. every row's cursor
        field was NULL, or a non-incremental stream) returns ``({}, "")`` and
        writes no checkpoint, leaving any prior bookmark untouched.

        Fails loud when a cursor carries components but no value: persisting a
        fabricated ``datetime.now()`` there would silently checkpoint
        wall-clock now as the high-water mark, so the next run would filter
        from "now" and skip rows.
        """
        if committed_cursor is None:
            return {}, ""
        state_dict = cursor_to_state_dict(committed_cursor)
        cursor_data = state_dict.get("cursor", {})
        if not cursor_data:
            return {}, ""
        primary = cursor_data.get("primary", {})
        if primary.get("value") is None:
            raise StreamProcessingError(
                "committed cursor carries no watermark value; refusing to "
                "checkpoint a fabricated high-water mark",
                stream_id=stream_id,
            )
        hwm = primary["value"]
        self.state_manager.save_stream_checkpoint(
            stream_name=stream_id,
            partition={},
            cursor=cursor_data,
            hwm=hwm,
            stream_version=stream_version,
        )
        return cursor_data, hwm

    async def _load_stage(
        self,
        input_queue: Queue,
        output_queue: Queue,
        grpc_client: DestinationGRPCClient,
        config: dict[str, Any],
        stream_dlq: "DeadLetterQueue",
        run_id: str,
        stream_metrics: dict[str, int],
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
        stream_version = config.get("stream_version", 1)

        # Typed source-read policy (parsed once in PipelineConfigPrep). Record
        # IDs hash the source primary key (stable across the pipeline); the
        # cursor field comes from the same replication policy the connector
        # uses. cursor_field is a contract string|null, so no list handling.
        resolved_source = config["source"]["_resolved_source"]
        replication = resolved_source.replication
        cursor_field = replication.cursor_field if replication else None
        tie_breaker_fields = replication.tie_breaker_fields if replication else None
        primary_key_fields = list(resolved_source.primary_keys)

        logger.info(f"Stream {stream_name}: Starting gRPC load stage")

        batch_seq = 0
        max_retries = self.max_retries
        retry_base_delay = self.retry_delay
        error_strategy = self.error_strategy

        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                batch_seq += 1

                # Materialize once for record-id generation, cursor
                # extraction, and DLQ payloads. The Arrow batch travels
                # the wire untouched.
                record_count = batch.num_rows
                record_dicts = batch.to_pylist()

                logger.debug(
                    f"Stream {stream_name}: Processing batch {batch_seq} "
                    f"with {record_count} records"
                )

                # Content-derived record identity: DLQ correlation and the
                # destination's row-level idempotency key. Independent of
                # run_id/batch_seq/position, so the same row hashes identically
                # across attempts and an inclusive cursor re-read (issue #282).
                record_ids = [
                    generate_record_id(
                        record=record,
                        primary_key_fields=primary_key_fields or None,
                    )
                    for record in record_dicts
                ]

                # Compute MAX cursor value in batch (batch may be unordered)
                cursor = None
                if cursor_field:
                    cursor = compute_max_cursor(
                        batch=record_dicts,
                        cursor_field=cursor_field,
                        tie_breaker_fields=tie_breaker_fields,
                    )

                # Retry loop for RETRYABLE_FAILURE
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
                        # Batch committed, persist cursor
                        cursor_data, hwm = self._persist_committed_cursor(
                            result.committed_cursor,
                            stream_id,
                            stream_version,
                        )
                        logger.debug(
                            f"Stream {stream_name}: Batch {batch_seq} committed, "
                            f"{result.records_written} records written"
                        )
                        self.metrics.increment_records_processed(result.records_written)
                        stream_metrics["records_processed"] += result.records_written

                        # Emit per-batch metrics
                        if os.getenv("METRICS_ENABLED", "false").lower() == "true":
                            emit_metrics_log(
                                {
                                    "type": "batch",
                                    "run_id": run_id,
                                    "pipeline_id": self.pipeline_id,
                                    "stream_id": stream_id,
                                    "batch_seq": batch_seq,
                                    "records_written": result.records_written,
                                    "cumulative_records_processed": (
                                        self.metrics.records_processed
                                    ),
                                    "cumulative_records_failed": (
                                        self.metrics.records_failed
                                    ),
                                    "cumulative_batches_processed": (
                                        self.metrics.batches_processed
                                    ),
                                    "cursor": json.dumps(cursor_data).encode().hex()
                                    if cursor_data
                                    else "",
                                    "cursor_value": hwm,
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                }
                            )

                        await output_queue.put(batch)
                        break

                    elif result.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED:
                        # Idempotent replay - batch was already processed
                        self._persist_committed_cursor(
                            result.committed_cursor,
                            stream_id,
                            stream_version,
                        )
                        logger.info(
                            f"Stream {stream_name}: Batch {batch_seq} "
                            "already committed (idempotent replay)"
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
                            self.metrics.increment_records_failed(record_count)
                            self.metrics.increment_batches_failed()
                            stream_metrics["records_failed"] += record_count
                            stream_metrics["batches_failed"] += 1
                            if error_strategy == "dlq":
                                await stream_dlq.send_batch(
                                    record_dicts,
                                    result.failure_summary,
                                    self.pipeline_id,
                                    stream_id=stream_id,
                                )
                            elif error_strategy == "fail":
                                raise StreamProcessingError(
                                    f"Batch {batch_seq} failed after {max_retries} "
                                    f"retries: {result.failure_summary}"
                                )
                            elif error_strategy == "skip":
                                # Skipped batches are dropped, NOT dead-lettered,
                                # so track them separately from DLQ'd records (at
                                # both stream and pipeline level) to keep the
                                # partial-run reporting honest.
                                stream_metrics["records_skipped"] += record_count
                                self.metrics.increment_records_skipped(record_count)
                                logger.warning(
                                    f"Stream {stream_name}: Batch {batch_seq} skipped "
                                    f"after {max_retries} retries; {record_count} "
                                    f"records dropped: {result.failure_summary}"
                                )
                            else:
                                # Strategy is contract-validated upstream to
                                # {fail, dlq, skip}; an unhandled value must fail
                                # loud, never silently complete a failed batch.
                                raise StreamProcessingError(
                                    f"Unhandled error strategy {error_strategy!r}"
                                )
                            break

                        # Exponential backoff
                        delay = retry_base_delay * (2 ** (retry_count - 1))
                        logger.warning(
                            f"Stream {stream_name}: Batch {batch_seq} "
                            f"retryable failure, "
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
                        self.metrics.increment_records_failed(record_count)
                        self.metrics.increment_batches_failed()
                        stream_metrics["records_failed"] += record_count
                        stream_metrics["batches_failed"] += 1

                        # Send entire batch to DLQ with record_ids for correlation
                        if error_strategy == "dlq":
                            await stream_dlq.send_batch(
                                record_dicts,
                                result.failure_summary,
                                self.pipeline_id,
                                stream_id=stream_id,
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
                        self.metrics.increment_records_failed(record_count)
                        self.metrics.increment_batches_failed()
                        stream_metrics["records_failed"] += record_count
                        stream_metrics["batches_failed"] += 1
                        if error_strategy == "dlq":
                            await stream_dlq.send_batch(
                                record_dicts,
                                f"Unknown ACK status: {result.status}",
                                self.pipeline_id,
                                stream_id=stream_id,
                            )
                        raise StreamProcessingError(
                            f"Batch {batch_seq} unknown ACK status: {result.status}"
                        )

            # Signal end of stream
            await output_queue.put(None)
            logger.info(
                f"Stream {stream_name}: gRPC load stage completed "
                f"with {batch_seq} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: gRPC load stage failed: %s",
                stream_name,
                e,
            )
            await output_queue.put(None)
            # A load-stage failure is destination-side by construction; tag it so
            # a driver/HTTP code in the cause can never be misread as source auth.
            # A deterministic destination write-config defect (a type-map/dialect/
            # write-config/adbc fatal-ack summary) still routes to CONFIG_INVALID
            # so it stays user-fixable. tag_failure is no-overwrite, so a deeper
            # tag still wins.
            tag_failure(
                e,
                code=classify_destination_failure(e),
                stage=FailureStage.DESTINATION_LOAD,
            )
            raise

    async def _checkpoint_stage(
        self, input_queue: Queue[Any], config: dict[str, Any]
    ) -> None:
        """Checkpoint processing progress with state management."""
        stream_name = config["stream_name"]
        logger.debug(f"Starting checkpoint stage for stream {stream_name}")

        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                # Checkpoint is handled by the source connector during
                # read_batches; this stage just tracks completion
                batch_count += 1

            logger.info(
                f"Stream {stream_name}: Checkpoint stage completed "
                f"with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Checkpoint stage failed: %s",
                stream_name,
                e,
            )
            raise

    def _create_source_connector(self, config: dict[str, Any]) -> Readable:
        """Return the worker-backed Readable for this stream's source.

        Two-step registry resolution (connector_id -> package class, else
        the generic class for the kind) happens inside the spawned worker —
        the engine process never loads connector code.
        """
        resolved_source = config.get("_resolved_source")
        if not resolved_source:
            # The actual raise site for a missing _resolved_source (it runs before
            # the stream's own guard); tag it so the metrics record carries the
            # structured config signal instead of falling back to INTERNAL.
            raise tag_failure(
                ValueError("Missing _resolved_source in source config"),
                code=ErrorCode.CONFIG_INVALID,
                stage=FailureStage.CONFIG,
            )
        return self._worker_readable

    def _create_pipeline_stages(
        self,
        source_connector: Readable,
        grpc_client: DestinationGRPCClient,
        extract_queue: Queue[Any],
        transform_queue: Queue[Any],
        load_queue: Queue[Any],
        stream_processing_config: dict[str, Any],
        stream_dlq: DeadLetterQueue,
        stream_name: str,
        run_id: str,
        stream_metrics: dict[str, int],
    ) -> list[asyncio.Task[None]]:
        """Create all pipeline stage tasks using factory pattern."""
        return [
            asyncio.create_task(
                self._extract_stage(
                    source_connector, extract_queue, stream_processing_config
                ),
                name=f"extract-{stream_name}",
            ),
            asyncio.create_task(
                self._transform_stage(
                    extract_queue,
                    transform_queue,
                    stream_processing_config,
                    stream_metrics,
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

    def _create_grpc_client(
        self,
        dest_config: dict[str, Any],
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

        host = os.getenv("DESTINATION_GRPC_HOST") or grpc_config.get(
            "host", "localhost"
        )
        port = int(os.getenv("DESTINATION_GRPC_PORT", "0")) or grpc_config.get(
            "port", 50051
        )
        # The engine's ack budget. The client stamps it into the schema
        # handshake so the destination derives its statement timeout from
        # the budget the engine actually waits on (issues #231, #234).
        timeout = resolve_grpc_ack_timeout_seconds()
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

    def _get_stream_name(self, config: dict[str, Any]) -> str:
        """Generate stream name for state management.

        Returns ``endpoint.<scope>:<connection_id>/<endpoint_id>`` (the
        ``endpoint.`` prefix namespaces the key in shared state stores)
        so the metric path is stable across runs.
        """
        source = config.get("source")
        if isinstance(source, dict):
            ref = source.get("endpoint_ref")
            if isinstance(ref, dict):
                return (
                    f"endpoint.{ref.get('scope', '')}:"
                    f"{ref.get('connection_id', '')}/{ref.get('endpoint_id', '')}"
                )
        return str(config.get("pipeline_id", "unknown-stream"))

    def get_metrics(self) -> PipelineMetrics:
        """Get pipeline execution metrics as a validated Pydantic model."""
        return self.metrics

    def get_dominant_stream_error(self) -> BaseException | None:
        """Return the failed-stream exceptions from a partial run, as a group.

        Set when some (not all) streams fail: stream_data logs and returns
        without raising, so the runner reads this and classifies the dominant
        cause across all failed streams to classify the partial run.
        """
        return self._dominant_stream_error

    def get_state_manager(self) -> StateManager:
        """Get the state manager instance."""
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
