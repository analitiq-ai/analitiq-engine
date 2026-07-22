"""Per-stream pipeline execution.

One :class:`StreamProcessor` instance runs one stream for one run: it owns
the stream's extract -> transform -> load -> checkpoint stages, the gRPC ack
protocol, the stream-scoped counters, and the stream's metrics record. The
engine (``engine.py``) only orchestrates processors across streams.
"""

import asyncio
import json
import logging
import os
from asyncio import Queue
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Any

import pyarrow as pa

from cdk.contract import Readable
from cdk.types import CheckpointStore, FailureCategory

from ..grpc.client import (
    BatchResult,
    DestinationGRPCClient,
    generate_record_id,
    resolve_grpc_ack_timeout_seconds,
)
from ..grpc.cursor import compute_max_cursor, cursor_to_state_dict
from ..grpc.generated.analitiq.v1 import AckStatus, Cursor, RetrySemantics
from ..models.metrics import PipelineMetrics
from ..shared.run_id import get_or_generate_run_id
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
    dominant_error_code,
    tag_failure,
)
from ..state.metrics_storage import create_metrics_record, emit_metrics_log
from ..state.state_manager import StateManager
from .data_transformer import compile_transform
from .exceptions import StreamProcessingError

logger = logging.getLogger(__name__)


class _FullRefreshCheckpoint:
    """Checkpoint view for truncate_insert streams: never resumes.

    A full refresh must re-read the source from scratch on every
    (re)start — the destination truncates on the read's first batch, so
    a resumed slice would be the only data left in the target (issue
    #307). ``get_cursor`` therefore always answers ``None`` (full
    re-scan); ``save_cursor`` delegates so in-run watermark tracking stays
    as it is. The durable checkpoint (``save_stream_checkpoint`` on
    destination ACK) does not flow through this wrapper.
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


class _AckDisposition(Enum):
    """Terminal outcome of sending one batch through the ack protocol."""

    COMMITTED = auto()
    ALREADY_COMMITTED = auto()
    RETRIES_EXHAUSTED = auto()
    FATAL = auto()
    UNKNOWN_STATUS = auto()


@dataclass(frozen=True)
class _BatchSendOutcome:
    """How one batch's send ended, and the ack that ended it."""

    disposition: _AckDisposition
    result: BatchResult


@dataclass
class StreamMetrics:
    """Typed per-stream counters feeding the stream's metrics record."""

    records_processed: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    batches_processed: int = 0
    batches_failed: int = 0


class StreamProcessor:
    """Runs one stream's extract-transform-load-checkpoint pipeline.

    Owns everything scoped to a single stream: the typed counters, the
    ack-protocol send loop shared by the batch loop and the zero-batch
    synthetic truncate, and the completion classification.
    """

    def __init__(
        self,
        *,
        stream_id: str,
        stream_config: dict[str, Any],
        pipeline_config: dict[str, Any],
        pipeline_id: str,
        state_manager: StateManager,
        pipeline_metrics: PipelineMetrics,
        worker_readable: Readable,
        dlq_root: str,
        batch_size: int,
        buffer_size: int,
        max_retries: int,
        retry_delay: float,
        error_strategy: str,
    ) -> None:
        self.stream_id = stream_id
        self.stream_name = stream_config.get("name", stream_id)
        # Absent version -> 1, mirroring _split_stream_ref's bare -> 1 rule so
        # an unversioned or minimally-built config stays runnable.
        self.stream_version = stream_config.get("stream_version", 1)
        self.stream_config = stream_config
        self.pipeline_config = pipeline_config
        self.pipeline_id = pipeline_id
        self.state_manager = state_manager
        self.pipeline_metrics = pipeline_metrics
        self.worker_readable = worker_readable
        self.dlq_root = dlq_root
        self.batch_size = batch_size
        self.buffer_size = buffer_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.error_strategy = error_strategy

        self.metrics = StreamMetrics()
        # stream_data starts the state run before any processor is built, so
        # the run id is known at construction time.
        self.run_id = state_manager.current_run_id or get_or_generate_run_id()
        # Created by run() once the destination config is known; stage-level
        # tests inject fakes directly.
        self.grpc_client: DestinationGRPCClient | None = None
        self.stream_dlq: DeadLetterQueue | None = None

        # Set by _load_stage when its batch loop drains with zero batches on
        # a truncate_insert stream. run() sends the synthetic empty batch
        # only AFTER gather succeeds — a failed extract (which also
        # propagates the None sentinel into the batch loop) never triggers
        # the truncate (issue #312).
        self.zero_batch_truncate_needed = False
        # Classified cause of every retry-exhausted batch the dlq/skip
        # strategies dropped without raising (issue #351).
        self.exhausted_failure_codes: list[ErrorCode] = []

    async def run(self) -> ErrorCode | None:
        """Process the stream end to end and emit its metrics record.

        Returns the classified cause when the stream completed partial (a
        dlq/skip strategy exhausted batches without raising — nothing else
        carries that cause to the runner, issue #351), else None.
        """
        logger.info(f"Processing stream: {self.stream_name}")

        tasks: list[asyncio.Task[None]] = []
        start_time = datetime.now(timezone.utc)
        status = "success"
        error_code: ErrorCode | None = None
        error_message: str | None = None
        error_detail: str | None = None

        try:
            source_cfg = self.stream_config["source"]
            destination_cfg = self.stream_config["destination"]

            # Raises a CONFIG_INVALID-tagged ValueError if the source config
            # was never resolved (the real guard for a missing
            # _resolved_source).
            source_readable = self._resolve_source_readable(source_cfg)

            self.stream_dlq = DeadLetterQueue(str(Path(self.dlq_root) / self.stream_id))
            self.grpc_client = self._create_grpc_client(destination_cfg)

            await self._open_destination_stream(destination_cfg)

            tasks = self._create_stage_tasks(source_readable)
            await asyncio.gather(*tasks)

            # A truncate_insert source that emitted zero batches never fires
            # write_batch, so the destination was never told to truncate. The
            # flag is only set when _load_stage exited the batch loop cleanly;
            # if extract failed, gather re-raises before we get here, so the
            # previous run's rows are never wiped by an upstream error
            # (issue #312).
            if self.zero_batch_truncate_needed:
                await self._send_synthetic_truncate()

            (
                status,
                error_code,
                error_message,
                error_detail,
            ) = self._classify_completion()
            # error_code is only non-None on the partial path here; a failed
            # stream raises instead, and the engine reads this return value.
            return error_code

        except Exception as e:
            status = "failed"
            error_code, error_message, error_detail = classify_for_metrics(e)
            logger.exception("Stream %s processing failed: %s", self.stream_name, e)
            # Cancel any running stage tasks, then drive them to completion.
            # The source reader releases its runtime in its own ``finally``,
            # so the cancelled extract task must be awaited here — otherwise
            # the runtime/session could stay open if the run tears down
            # before the cancelled task runs its cleanup.
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            raise

        finally:
            end_time = datetime.now(timezone.utc)
            await self._disconnect()
            self._emit_stream_metrics(
                status=status,
                error_code=error_code,
                error_message=error_message,
                error_detail=error_detail,
                start_time=start_time,
                end_time=end_time,
            )

    def _resolve_source_readable(self, source_cfg: dict[str, Any]) -> Readable:
        """Return the worker-backed Readable for this stream's source.

        Two-step registry resolution (connector_id -> package class, else
        the generic class for the kind) happens inside the spawned worker —
        the engine process never loads connector code.
        """
        if not source_cfg.get("_resolved_source"):
            # The actual raise site for a missing _resolved_source (it runs
            # before any stage); tag it so the metrics record carries the
            # structured config signal instead of falling back to INTERNAL.
            raise tag_failure(
                ValueError("Missing _resolved_source in source config"),
                code=ErrorCode.CONFIG_INVALID,
                stage=FailureStage.CONFIG,
            )
        return self.worker_readable

    def _create_grpc_client(self, dest_config: dict[str, Any]) -> DestinationGRPCClient:
        """Create a gRPC client for destination streaming.

        Configuration priority:
        1. Environment variables (DESTINATION_GRPC_HOST, DESTINATION_GRPC_PORT)
        2. Config dict (destination.grpc.host, destination.grpc.port)
        3. Defaults (localhost:50051)
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
        max_message_size = grpc_config.get("max_message_size", 16 * 1024 * 1024)

        logger.info(f"Creating gRPC client for {host}:{port}")

        return DestinationGRPCClient(
            host=host,
            port=port,
            timeout_seconds=timeout,
            max_retries=self.max_retries,
            max_message_size=max_message_size,
        )

    async def _open_destination_stream(self, destination_cfg: dict[str, Any]) -> None:
        """Connect the gRPC client and run the schema handshake."""
        client = self.grpc_client
        assert client is not None  # created by run() just before this call
        connected = await client.connect()
        if not connected:
            raise tag_failure(
                StreamProcessingError(
                    f"Failed to connect to gRPC destination for stream "
                    f"{self.stream_name}",
                    stream_id=self.stream_id,
                ),
                code=ErrorCode.DESTINATION_WRITE_FAILED,
                stage=FailureStage.DESTINATION_LOAD,
            )

        schema_accepted = await client.start_stream(
            run_id=self.run_id,
            stream_id=self.stream_id,
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
            reason = client.schema_rejection_message
            detail = f": {reason}" if reason else ""
            raise tag_failure(
                StreamProcessingError(
                    f"Destination did not accept the stream for "
                    f"{self.stream_name}{detail}",
                    stream_id=self.stream_id,
                ),
                code=classify_handshake_failure(reason),
                stage=FailureStage.DESTINATION_LOAD,
            )
        logger.info(f"Stream {self.stream_name}: gRPC stream started, schema accepted")
        # Surface the destination's retry-safety verdict per stream
        # (issue #286): an at-least-once stream re-sends committed
        # records on a same-run restart, and the operator should learn
        # that at startup, not from duplicated side effects.
        retry_verdict = client.stream_retry_semantics
        if retry_verdict is not None:
            semantics_value, retry_reason = retry_verdict
            logger.info(
                "Stream %s: retry semantics on a same-run restart: %s (%s)",
                self.stream_name,
                RetrySemantics.Name(semantics_value),
                retry_reason,
            )

    def _create_stage_tasks(
        self, source_readable: Readable
    ) -> list[asyncio.Task[None]]:
        """Create the four pipeline stage tasks wired with fresh queues."""
        extract_queue: Queue[Any] = Queue(maxsize=self.buffer_size)
        transform_queue: Queue[Any] = Queue(maxsize=self.buffer_size)
        load_queue: Queue[Any] = Queue(maxsize=self.buffer_size)
        return [
            asyncio.create_task(
                self._extract_stage(source_readable, extract_queue),
                name=f"extract-{self.stream_name}",
            ),
            asyncio.create_task(
                self._transform_stage(extract_queue, transform_queue),
                name=f"transform-{self.stream_name}",
            ),
            asyncio.create_task(
                self._load_stage(transform_queue, load_queue),
                name=f"load-{self.stream_name}",
            ),
            asyncio.create_task(
                self._checkpoint_stage(load_queue),
                name=f"checkpoint-{self.stream_name}",
            ),
        ]

    def _is_truncate_insert(self) -> bool:
        """Whether this stream's destination write mode is truncate_insert."""
        mode = (self.stream_config.get("destination") or {}).get("write_mode", "")
        return str(mode).lower() == "truncate_insert"

    async def _extract_stage(
        self, source_readable: Readable, queue: Queue[Any]
    ) -> None:
        """Extract data from source in batches with state management."""
        logger.debug(f"Starting extract stage for stream {self.stream_name}")

        try:
            # The connectors read the contract documents directly off the
            # source config (``endpoint_document``, ``stream_source``).
            # No flattening or replication-field injection is needed.
            source_config = self.stream_config["source"]
            resolved_source = source_config["_resolved_source"]
            runtime = resolved_source.runtime
            json_safe_config = {
                k: v for k, v in source_config.items() if k != "_resolved_source"
            }
            partition: dict[str, Any] = {}

            # A truncate_insert stream is a full refresh: the destination
            # truncates on the read's first batch, so the source must read
            # from scratch on every (re)start. Resuming from a persisted
            # cursor would load only the resumed slice into the freshly
            # truncated table (issue #307), so the resume read is disabled;
            # cursor saves still flow through unchanged for watermark
            # emission.
            checkpoint: CheckpointStore = self.state_manager
            if self._is_truncate_insert():
                logger.info(
                    "Stream %s: truncate_insert is a full refresh; "
                    "ignoring any persisted resume cursor",
                    self.stream_name,
                )
                checkpoint = _FullRefreshCheckpoint(self.state_manager)

            batch_count = 0
            async for batch in source_readable.read_batches(
                runtime,
                json_safe_config,
                checkpoint=checkpoint,
                stream_name=self.stream_id,
                partition=partition,
                batch_size=self.batch_size,
            ):
                await queue.put(batch)
                batch_count += 1
                logger.debug(
                    f"Stream {self.stream_name}: Extracted batch {batch_count} "
                    f"with {len(batch)} records"
                )

            # Signal end of stream
            await queue.put(None)
            logger.info(
                f"Stream {self.stream_name}: Extract stage completed "
                f"with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Extract stage failed: %s",
                self.stream_name,
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
        self, input_queue: Queue[Any], output_queue: Queue[Any]
    ) -> None:
        """Transform data with field mappings and validation."""
        logger.debug(f"Starting transform stage for stream {self.stream_name}")

        assignments = (self.stream_config.get("mapping") or {}).get("assignments") or []
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

                self.pipeline_metrics.increment_batches_processed()
                self.metrics.batches_processed += 1

            # Signal end of stream
            await output_queue.put(None)
            logger.info(
                f"Stream {self.stream_name}: Transform stage completed "
                f"with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Transform stage failed: %s",
                self.stream_name,
                e,
            )
            await output_queue.put(None)  # Signal end even on error
            self.pipeline_metrics.increment_batches_failed()
            self.metrics.batches_failed += 1
            # A transform failure is a mapping/type problem. The engine validates
            # no data schema, so this is a configuration defect (CONFIG_INVALID),
            # not a data-vs-schema mismatch. tag_failure is no-overwrite, so a
            # deeper tag still wins.
            tag_failure(e, code=ErrorCode.CONFIG_INVALID, stage=FailureStage.TRANSFORM)
            raise

    def _persist_committed_cursor(
        self, committed_cursor: Any | None
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
                stream_id=self.stream_id,
            )
        hwm = primary["value"]
        self.state_manager.save_stream_checkpoint(
            stream_name=self.stream_id,
            partition={},
            cursor=cursor_data,
            hwm=hwm,
            stream_version=self.stream_version,
        )
        return cursor_data, hwm

    async def _load_stage(
        self, input_queue: Queue[Any], output_queue: Queue[Any]
    ) -> None:
        """Load transformed data to destination via gRPC streaming.

        Implements strict in-order: send batch -> await ACK -> send next.
        """
        # Typed source-read policy (parsed once in PipelineConfigPrep). Record
        # IDs hash the source primary key (stable across the pipeline); the
        # cursor field comes from the same replication policy the connector
        # uses. cursor_field is a contract string|null, so no list handling.
        resolved_source = self.stream_config["source"]["_resolved_source"]
        replication = resolved_source.replication
        cursor_field = replication.cursor_field if replication else None
        tie_breaker_fields = replication.tie_breaker_fields if replication else None
        primary_key_fields = list(resolved_source.primary_keys)

        logger.info(f"Stream {self.stream_name}: Starting gRPC load stage")

        batch_seq = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                batch_seq += 1

                # Materialize once for record-id generation, cursor
                # extraction, and DLQ payloads. The Arrow batch travels
                # the wire untouched.
                record_dicts = batch.to_pylist()

                logger.debug(
                    f"Stream {self.stream_name}: Processing batch {batch_seq} "
                    f"with {batch.num_rows} records"
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

                outcome = await self._send_batch_acked(
                    batch_seq=batch_seq,
                    record_batch=batch,
                    record_ids=record_ids,
                    cursor=cursor,
                    label=f"Batch {batch_seq}",
                )
                await self._handle_send_outcome(
                    outcome,
                    batch_seq=batch_seq,
                    batch=batch,
                    record_dicts=record_dicts,
                    output_queue=output_queue,
                )

            # Record that the batch loop drained with zero batches on a
            # truncate_insert stream. A failed extract also propagates the
            # None sentinel here, so this flag alone cannot prove a clean
            # read — run() sends the synthetic batch only AFTER gather
            # succeeds, which is what keeps a failed extract from ever
            # triggering the truncate (issue #312).
            if batch_seq == 0 and self._is_truncate_insert():
                self.zero_batch_truncate_needed = True

            # Signal end of stream
            await output_queue.put(None)
            logger.info(
                f"Stream {self.stream_name}: gRPC load stage completed "
                f"with {batch_seq} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: gRPC load stage failed: %s",
                self.stream_name,
                e,
            )
            await output_queue.put(None)
            # A load-stage failure is destination-side by construction; tag it so
            # a driver/HTTP code in the cause can never be misread as source auth.
            # A defect the destination declared on the ack (a failure_category
            # the raise site stamped onto the exception, #351) still routes to
            # its own code -- e.g. CONFIG_DEFECT -> CONFIG_INVALID stays
            # user-fixable. tag_failure is no-overwrite, so a deeper tag wins.
            tag_failure(
                e,
                code=classify_destination_failure(e),
                stage=FailureStage.DESTINATION_LOAD,
            )
            raise

    async def _send_batch_acked(
        self,
        *,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
        cursor: Cursor | None,
        label: str,
    ) -> _BatchSendOutcome:
        """Send one batch and drive the ack protocol to a terminal answer.

        The single implementation of send -> ack -> backoff-retry, shared by
        the batch loop and the zero-batch synthetic truncate so the same
        situation always behaves the same way. Policy (error strategy, DLQ,
        classification) stays with the callers.
        """
        client = self.grpc_client
        assert client is not None  # created by run() before any send
        retry_count = 0
        while True:
            result: BatchResult = await client.send_batch(
                run_id=self.run_id,
                stream_id=self.stream_id,
                batch_seq=batch_seq,
                record_batch=record_batch,
                record_ids=record_ids,
                cursor=cursor,
            )
            if result.status == AckStatus.ACK_STATUS_SUCCESS:
                return _BatchSendOutcome(_AckDisposition.COMMITTED, result)
            if result.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED:
                return _BatchSendOutcome(_AckDisposition.ALREADY_COMMITTED, result)
            if result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
                retry_count += 1
                if retry_count > self.max_retries:
                    return _BatchSendOutcome(_AckDisposition.RETRIES_EXHAUSTED, result)
                # Exponential backoff
                delay = self.retry_delay * (2 ** (retry_count - 1))
                logger.warning(
                    f"Stream {self.stream_name}: {label} retryable failure, "
                    f"retry {retry_count}/{self.max_retries} after "
                    f"{delay:.2f}s: {result.failure_summary}"
                )
                await asyncio.sleep(delay)
                continue
            if result.status == AckStatus.ACK_STATUS_FATAL_FAILURE:
                return _BatchSendOutcome(_AckDisposition.FATAL, result)
            return _BatchSendOutcome(_AckDisposition.UNKNOWN_STATUS, result)

    async def _handle_send_outcome(
        self,
        outcome: _BatchSendOutcome,
        *,
        batch_seq: int,
        batch: pa.RecordBatch,
        record_dicts: list[dict[str, Any]],
        output_queue: Queue[Any],
    ) -> None:
        """React to a batch's final ack with the stream's failure policy.

        The transport loop (``_send_batch_acked``) has already finished
        retrying; this is where policy lives: cursor checkpointing and
        metrics on commit, the error strategy on exhaustion, fail-loud on a
        fatal or unknown ack.
        """
        result = outcome.result
        if outcome.disposition is _AckDisposition.COMMITTED:
            cursor_data, hwm = self._persist_committed_cursor(result.committed_cursor)
            logger.debug(
                f"Stream {self.stream_name}: Batch {batch_seq} committed, "
                f"{result.records_written} records written"
            )
            # Order matters: the Pydantic pipeline counter validates the
            # destination-reported count (rejects negatives) before the
            # unguarded stream counter takes it.
            self.pipeline_metrics.increment_records_processed(result.records_written)
            self.metrics.records_processed += result.records_written
            self._emit_batch_metrics(batch_seq, result, cursor_data, hwm)
            await output_queue.put(batch)
            return

        if outcome.disposition is _AckDisposition.ALREADY_COMMITTED:
            # Idempotent replay - batch was already processed
            self._persist_committed_cursor(result.committed_cursor)
            logger.info(
                f"Stream {self.stream_name}: Batch {batch_seq} "
                "already committed (idempotent replay)"
            )
            await output_queue.put(batch)
            return

        record_count = len(record_dicts)
        self.pipeline_metrics.increment_records_failed(record_count)
        self.pipeline_metrics.increment_batches_failed()
        self.metrics.records_failed += record_count
        self.metrics.batches_failed += 1

        if outcome.disposition is _AckDisposition.RETRIES_EXHAUSTED:
            await self._handle_exhausted_batch(batch_seq, result, record_dicts)
            return

        if outcome.disposition is _AckDisposition.FATAL:
            logger.error(
                f"Stream {self.stream_name}: Batch {batch_seq} fatal failure: "
                f"{result.failure_summary}"
            )
            # Send entire batch to DLQ with record_ids for correlation
            if self.error_strategy == "dlq":
                await self._dlq_batch(record_dicts, result.failure_summary)
            # Raise exception to mark stream as failed
            raise StreamProcessingError(
                f"Batch {batch_seq} fatal failure: {result.failure_summary}",
                failure_category=result.failure_category,
            )

        # Unknown status - treat as fatal, and do not trust its advisory
        # failure category (the raise below carries none).
        logger.error(
            f"Stream {self.stream_name}: Batch {batch_seq} unknown status: "
            f"{result.status}"
        )
        if self.error_strategy == "dlq":
            await self._dlq_batch(record_dicts, f"Unknown ACK status: {result.status}")
        raise StreamProcessingError(
            f"Batch {batch_seq} unknown ACK status: {result.status}"
        )

    async def _handle_exhausted_batch(
        self,
        batch_seq: int,
        result: BatchResult,
        record_dicts: list[dict[str, Any]],
    ) -> None:
        """Apply the error strategy to a batch that exhausted its retries.

        Returns normally when the strategy drops the batch and the stream
        should continue (dlq/skip); raises when the stream must stop.
        """
        logger.error(
            f"Stream {self.stream_name}: Batch {batch_seq} failed after "
            f"{self.max_retries} retries: {result.failure_summary}"
        )
        # The dlq/skip strategies return without raising, so this batch's
        # cause would die with this scope. Classify it exactly as the fail
        # strategy's raise would -- declared category first, text fallback
        # for an undeclared ack -- and stash the code for the partial-run
        # classification, so the reported code cannot depend on the error
        # strategy (issue #351).
        exhausted_failure = StreamProcessingError(
            f"Batch {batch_seq} failed after {self.max_retries} "
            f"retries: {result.failure_summary}",
            failure_category=result.failure_category,
        )
        self.exhausted_failure_codes.append(
            classify_destination_failure(exhausted_failure)
        )
        if batch_seq == 1 and self._is_truncate_insert():
            # The destination truncates on batch_seq 1 (issue #307).
            # Dropping the first batch via dlq/skip and continuing would let
            # batch 2 append onto the PREVIOUS refresh's rows — stale data
            # mixed into a partial snapshot. A full refresh that cannot
            # start must fail the stream, whatever the error strategy.
            raise StreamProcessingError(
                f"Batch 1 of a truncate_insert stream "
                f"failed after {self.max_retries} retries; "
                f"dropping it would append the rest of "
                f"the refresh onto the previous run's "
                f"rows: {result.failure_summary}",
                failure_category=result.failure_category,
            )
        if self.error_strategy == "dlq":
            await self._dlq_batch(record_dicts, result.failure_summary)
        elif self.error_strategy == "fail":
            raise exhausted_failure
        elif self.error_strategy == "skip":
            # Skipped batches are dropped, NOT dead-lettered, so track them
            # separately from DLQ'd records (at both stream and pipeline
            # level) to keep the partial-run reporting honest.
            record_count = len(record_dicts)
            self.metrics.records_skipped += record_count
            self.pipeline_metrics.increment_records_skipped(record_count)
            logger.warning(
                f"Stream {self.stream_name}: Batch {batch_seq} skipped "
                f"after {self.max_retries} retries; {record_count} "
                f"records dropped: {result.failure_summary}"
            )
        else:
            # Strategy is contract-validated upstream to {fail, dlq, skip};
            # an unhandled value must fail loud, never silently complete a
            # failed batch.
            raise StreamProcessingError(
                f"Unhandled error strategy {self.error_strategy!r}"
            )

    async def _dlq_batch(
        self, record_dicts: list[dict[str, Any]], failure_summary: str
    ) -> None:
        """Send a failed batch's records to this stream's dead letter queue."""
        assert self.stream_dlq is not None  # created by run() before any stage
        await self.stream_dlq.send_batch(
            record_dicts,
            failure_summary,
            self.pipeline_id,
            stream_id=self.stream_id,
        )

    async def _send_synthetic_truncate(self) -> None:
        """Truncate a zero-batch full refresh via one synthetic empty batch."""
        logger.info(
            "Stream %s: source yielded no batches on a "
            "truncate_insert stream; sending synthetic empty "
            "batch to trigger truncate",
            self.stream_name,
        )
        empty_batch = pa.record_batch([], schema=pa.schema([]))
        outcome = await self._send_batch_acked(
            batch_seq=1,
            record_batch=empty_batch,
            record_ids=[],
            cursor=Cursor(token=b""),
            label="synthetic truncate batch",
        )
        if outcome.disposition is _AckDisposition.COMMITTED:
            logger.info(
                "Stream %s: synthetic truncate batch committed",
                self.stream_name,
            )
            return
        if outcome.disposition is _AckDisposition.ALREADY_COMMITTED:
            logger.info(
                "Stream %s: synthetic truncate already committed "
                "(idempotent replay)",
                self.stream_name,
            )
            return
        # An ack whose status the engine cannot interpret must not have its
        # advisory category trusted -- the same rule as the unknown-status
        # branch of the regular batch loop.
        known_failure = outcome.disposition in (
            _AckDisposition.RETRIES_EXHAUSTED,
            _AckDisposition.FATAL,
        )
        truncate_failure = StreamProcessingError(
            f"Stream {self.stream_name}: zero-batch truncate failed: "
            f"{outcome.result.failure_summary}",
            failure_category=(
                outcome.result.failure_category
                if known_failure
                else FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
            ),
        )
        raise tag_failure(
            truncate_failure,
            code=classify_destination_failure(truncate_failure),
            stage=FailureStage.DESTINATION_LOAD,
        )

    async def _checkpoint_stage(self, input_queue: Queue[Any]) -> None:
        """Checkpoint processing progress with state management."""
        logger.debug(f"Starting checkpoint stage for stream {self.stream_name}")

        batch_count = 0
        try:
            while True:
                batch = await input_queue.get()
                if batch is None:
                    break

                # The durable checkpoint is written on destination ACK in
                # _persist_committed_cursor; this stage only counts drained
                # batches for completion logging
                batch_count += 1

            logger.info(
                f"Stream {self.stream_name}: Checkpoint stage completed "
                f"with {batch_count} batches"
            )

        except Exception as e:
            logger.exception(
                "Stream %s: Checkpoint stage failed: %s",
                self.stream_name,
                e,
            )
            raise

    def _classify_completion(
        self,
    ) -> tuple[str, ErrorCode | None, str | None, str | None]:
        """Name how the stream ended when no stage raised.

        The dlq/skip strategies complete the stream without raising after
        exhausting retries; reflect that it only partly succeeded and carry
        the destination cause rather than reporting success. Every exhausted
        batch was classified when it broke, exactly as the fail strategy's
        raise path would have classified it; the dominant code across them
        (the read_failure_tag rule) names the run, so the same failure
        classifies identically under fail and dlq/skip (issue #351).
        """
        if self.metrics.records_failed == 0:
            logger.info(f"Stream {self.stream_name} completed successfully")
            return "success", None, None, None

        error_code = (
            dominant_error_code(self.exhausted_failure_codes)
            or ErrorCode.DESTINATION_WRITE_FAILED
        )
        # 'skip' drops exhausted batches without a DLQ entry, so those
        # records are NOT recoverable; do not imply dead-lettering. The
        # 'partial' path is only reached via the dlq/skip break, so a
        # non-zero skip count means the strategy was skip. error_detail
        # carries only allowlisted-safe fields; the destination
        # failure_summary stays in the DLQ (when used) and the logs.
        if self.metrics.records_skipped > 0:
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
            f"Stream {self.stream_name} completed partially: "
            f"{self.metrics.records_failed} records {action}"
        )
        return "partial", error_code, customer_message(error_code), error_detail

    async def _disconnect(self) -> None:
        """Disconnect the destination client, logging (not raising) failures."""
        # The source connector is a pure reader: ``read_batches`` materializes
        # and releases its own runtime, so there is nothing to disconnect
        # source-side.
        try:
            if self.grpc_client:
                await self.grpc_client.disconnect()
            logger.debug(
                f"Stream {self.stream_name} connectors disconnected successfully"
            )
        except Exception as e:
            logger.warning(
                "Failed to disconnect connectors for stream %s: %s",
                self.stream_name,
                e,
                exc_info=True,
            )

    def _emit_batch_metrics(
        self,
        batch_seq: int,
        result: BatchResult,
        cursor_data: dict[str, Any],
        hwm: Any,
    ) -> None:
        """Emit the per-batch metrics log line when metrics are enabled."""
        if os.getenv("METRICS_ENABLED", "false").lower() != "true":
            return
        emit_metrics_log(
            {
                "type": "batch",
                "run_id": self.run_id,
                "pipeline_id": self.pipeline_id,
                "stream_id": self.stream_id,
                "batch_seq": batch_seq,
                "records_written": result.records_written,
                "cumulative_records_processed": (
                    self.pipeline_metrics.records_processed
                ),
                "cumulative_records_failed": self.pipeline_metrics.records_failed,
                "cumulative_batches_processed": (
                    self.pipeline_metrics.batches_processed
                ),
                "cursor": json.dumps(cursor_data).encode().hex() if cursor_data else "",
                "cursor_value": hwm,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

    def _emit_stream_metrics(
        self,
        *,
        status: str,
        error_code: ErrorCode | None,
        error_message: str | None,
        error_detail: str | None,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        """Create the stream's metrics record and emit it when enabled."""
        try:
            record = create_metrics_record(
                run_id=self.run_id,
                pipeline_id=self.pipeline_id,
                start_time=start_time,
                end_time=end_time,
                records_processed=self.metrics.records_processed,
                records_failed=self.metrics.records_failed,
                batches_processed=self.metrics.batches_processed,
                status=status,
                error_code=error_code,
                error_message=error_message,
                error_detail=error_detail,
                pipeline_name=self.pipeline_config.get("name"),
            )
            if os.getenv("METRICS_ENABLED", "false").lower() == "true":
                emit_metrics_log(
                    {
                        "type": "stream",
                        "stream_id": self.stream_id,
                        **record.model_dump(),
                    }
                )
                logger.info(f"Emitted stream metrics for {self.stream_name}")
        except Exception as metrics_error:
            logger.error(
                "Failed to emit stream metrics for %s: %s",
                self.stream_name,
                metrics_error,
                exc_info=True,
            )
