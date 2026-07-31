"""Per-stream pipeline execution.

One :class:`StreamProcessor` instance runs one stream for one run: it owns
the stream's extract -> transform -> load -> checkpoint stages, the gRPC ack
protocol, the stream-scoped counters, and the stream's metrics record. The
engine (``engine.py``) only orchestrates processors across streams.
"""

import asyncio
import functools
import json
import logging
import os
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa

from cdk.contract import Readable
from cdk.types import CheckpointStore

from ..grpc.client import (
    BatchResult,
    DestinationGRPCClient,
    generate_record_id,
    resolve_grpc_ack_timeout_seconds,
)
from ..grpc.cursor import compute_max_cursor, cursor_to_state_dict
from ..grpc.generated.analitiq.v1 import Cursor, RetrySemantics
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
from .batch_policy import (
    AlreadyCommitted,
    BatchPolicy,
    Committed,
    DeadLetter,
    Disposition,
    ErrorStrategy,
    FailureKind,
    FailureReport,
    Skipped,
)
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

    @staticmethod
    async def get_cursor(
        stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        return None

    async def save_cursor(
        self,
        stream_name: str,
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
    ) -> None:
        await self._inner.save_cursor(stream_name, partition, cursor)


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
    stream's :class:`BatchPolicy`, and the completion classification. The
    policy decides what a batch's ack costs the stream; this class acts on
    that decision -- it holds the collaborators (dead letter queue, state
    manager, metrics) a decision needs to be carried out.
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
        error_strategy: ErrorStrategy,
        pacing_gate: asyncio.Semaphore | None = None,
    ) -> None:
        self.stream_id = stream_id
        # One slot under the source connection's declared ceiling
        # (concurrency.max_connections, issue #401), shared with every
        # stream on the same connection. Held for the extract stage's
        # lifetime only — the source worker's connection is open exactly
        # that long; holding it through destination load would serialize
        # streams beyond the declared source ceiling.
        self.pacing_gate = pacing_gate
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
        # The client's own connect-retry budget; the batch retry budget is
        # the policy's.
        self.max_retries = max_retries
        # Bound once per stream: every batch of this stream, and the
        # zero-batch synthetic truncate, reach their verdict through it.
        self.batch_policy = BatchPolicy(
            max_retries=max_retries,
            retry_delay=retry_delay,
            error_strategy=error_strategy,
        )

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
        logger.info("Processing stream: %s", self.stream_name)

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

        logger.info("Creating gRPC client for %s:%s", host, port)

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
        logger.info("Stream %s: gRPC stream started, schema accepted", self.stream_name)
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
        extract_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self.buffer_size)
        transform_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self.buffer_size)
        load_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self.buffer_size)
        return [
            asyncio.create_task(
                self._paced(self._extract_stage(source_readable, extract_queue)),
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

    async def _paced(self, stage: Coroutine[Any, Any, None]) -> None:
        """Run *stage* holding a slot under the source connection's ceiling.

        Wraps the extract stage only: the pacing gate bounds live source
        connections, and the extract stage's lifetime is exactly the source
        worker's. The slot is released the moment extraction completes (or
        fails), while transform/load keep draining — a stream slowly
        writing must not block other streams from starting their reads.
        """
        if self.pacing_gate is None:
            return await stage
        async with self.pacing_gate:
            return await stage

    async def _extract_stage(
        self, source_readable: Readable, queue: asyncio.Queue[Any]
    ) -> None:
        """Extract data from source in batches with state management."""
        logger.debug("Starting extract stage for stream %s", self.stream_name)

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
                    "Stream %s: Extracted batch %s with %s records",
                    self.stream_name,
                    batch_count,
                    len(batch),
                )

            # Signal end of stream
            await queue.put(None)
            logger.info(
                "Stream %s: Extract stage completed with %s batches",
                self.stream_name,
                batch_count,
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
        self, input_queue: asyncio.Queue[Any], output_queue: asyncio.Queue[Any]
    ) -> None:
        """Transform data with field mappings and validation."""
        logger.debug("Starting transform stage for stream %s", self.stream_name)

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
                "Stream %s: Transform stage completed with %s batches",
                self.stream_name,
                batch_count,
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

        Reached from the ``Committed`` disposition alone: the checkpoint is
        an artifact of a commit this run confirmed, and no other disposition
        carries a cursor to write.

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
        self, input_queue: asyncio.Queue[Any], output_queue: asyncio.Queue[Any]
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

        logger.info("Stream %s: Starting gRPC load stage", self.stream_name)

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
                    "Stream %s: Processing batch %s with %s records",
                    self.stream_name,
                    batch_seq,
                    batch.num_rows,
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

                # Stamp the batch's emit instant ONCE, before the send/retry
                # loop, so every retry of this batch carries the same value.
                # A time-partitioned destination derives its output path from
                # this instant, so a re-stamp per attempt would drift a
                # replayed batch across an hour/day boundary (issue #353).
                emitted_at = datetime.now(timezone.utc)

                disposition = await self.batch_policy.run(
                    self._bind_send(
                        batch_seq=batch_seq,
                        record_batch=batch,
                        record_ids=record_ids,
                        cursor=cursor,
                        emitted_at=emitted_at,
                    ),
                    label=f"Stream {self.stream_name}: batch {batch_seq}",
                )
                if await self._apply_disposition(
                    disposition,
                    batch_seq=batch_seq,
                    record_dicts=record_dicts,
                ):
                    await output_queue.put(batch)

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
                "Stream %s: gRPC load stage completed with %s batches",
                self.stream_name,
                batch_seq,
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

    def _bind_send(
        self,
        *,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
        cursor: Cursor | None,
        emitted_at: datetime,
    ) -> Callable[[], Awaitable[BatchResult]]:
        """Bind one batch's send so the policy can re-issue it verbatim.

        Every argument is fixed here, before the first attempt, so a retry
        cannot differ from the send it repeats. That is what holds the
        once-stamped ``emitted_at`` steady across attempts: a re-stamp would
        drift a replayed batch across a time-partitioned destination's
        hour/day boundary (issue #353).
        """
        client = self.grpc_client
        assert client is not None  # created by run() before any send
        return functools.partial(
            client.send_batch,
            run_id=self.run_id,
            stream_id=self.stream_id,
            batch_seq=batch_seq,
            record_batch=record_batch,
            record_ids=record_ids,
            cursor=cursor,
            emitted_at=emitted_at,
        )

    async def _apply_disposition(
        self,
        disposition: Disposition,
        *,
        batch_seq: int,
        record_dicts: list[dict[str, Any]],
    ) -> bool:
        """Carry out the policy's verdict for one batch.

        The single reaction ladder, shared by the batch loop and the
        zero-batch synthetic truncate. Returns True when the batch stands
        and should flow to the next stage; returns False when it was
        dropped and the stream continues without it; raises when the stream
        must stop.
        """
        if isinstance(disposition, Committed):
            cursor_data, hwm = self._persist_committed_cursor(disposition.cursor)
            logger.debug(
                "Stream %s: Batch %s committed, %s records written",
                self.stream_name,
                batch_seq,
                disposition.records_written,
            )
            # Order matters: the Pydantic pipeline counter validates the
            # destination-reported count (rejects negatives) before the
            # unguarded stream counter takes it.
            self.pipeline_metrics.increment_records_processed(
                disposition.records_written
            )
            self.metrics.records_processed += disposition.records_written
            self._emit_batch_metrics(
                batch_seq, disposition.records_written, cursor_data, hwm
            )
            return True

        if isinstance(disposition, AlreadyCommitted):
            # An earlier attempt committed this batch, so this run has no
            # confirmed write to checkpoint from; the watermark advances on
            # the next batch this run does commit.
            logger.info(
                "Stream %s: Batch %s already committed (idempotent replay)",
                self.stream_name,
                batch_seq,
            )
            return True

        report = disposition.report
        record_count = len(record_dicts)
        self.pipeline_metrics.increment_records_failed(record_count)
        self.pipeline_metrics.increment_batches_failed()
        self.metrics.records_failed += record_count
        self.metrics.batches_failed += 1

        failure = StreamProcessingError(
            self._failure_message(batch_seq, report),
            failure_category=report.category,
        )
        logger.error("Stream %s: %s", self.stream_name, failure)

        if report.kind is FailureKind.RETRIES_EXHAUSTED:
            # dlq/skip return without raising, so this batch's cause would
            # die with this scope. Classify it exactly as the fail strategy's
            # raise would -- declared category first, text fallback for an
            # undeclared ack -- and stash the code for the partial-run
            # classification, so the reported code cannot depend on the error
            # strategy (issue #351).
            self.exhausted_failure_codes.append(classify_destination_failure(failure))
            if batch_seq == 1 and self._is_truncate_insert():
                # The destination truncates on batch_seq 1 (issue #307).
                # Dropping the first batch and continuing would let batch 2
                # append onto the PREVIOUS refresh's rows -- stale data mixed
                # into a partial snapshot. A full refresh that cannot start
                # must fail the stream, whatever the strategy decided.
                raise StreamProcessingError(
                    f"Batch 1 of a truncate_insert stream failed after "
                    f"{report.attempts} attempts; dropping it would append "
                    f"the rest of the refresh onto the previous run's rows: "
                    f"{report.summary}",
                    failure_category=report.category,
                )

        if isinstance(disposition, DeadLetter):
            await self._dlq_batch(record_dicts, report.summary)
            return False

        if isinstance(disposition, Skipped):
            # Skipped batches are dropped, NOT dead-lettered, so track them
            # separately from DLQ'd records (at both stream and pipeline
            # level) to keep the partial-run reporting honest.
            self.metrics.records_skipped += record_count
            self.pipeline_metrics.increment_records_skipped(record_count)
            logger.warning(
                "Stream %s: Batch %s skipped; %s records dropped: %s",
                self.stream_name,
                batch_seq,
                record_count,
                report.summary,
            )
            return False

        if disposition.dead_letter:
            # Whole-batch DLQ before the stream stops: the rows are preserved
            # for the operator, then the failure is raised.
            await self._dlq_batch(record_dicts, report.summary)
        raise failure

    @staticmethod
    def _failure_message(batch_seq: int, report: FailureReport) -> str:
        """Name a failed batch the way the report's ending reads."""
        if report.kind is FailureKind.RETRIES_EXHAUSTED:
            return (
                f"Batch {batch_seq} failed after {report.attempts} attempts: "
                f"{report.summary}"
            )
        if report.kind is FailureKind.FATAL:
            return f"Batch {batch_seq} fatal failure: {report.summary}"
        return f"Batch {batch_seq} {report.summary}"

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
        disposition = await self.batch_policy.run(
            self._bind_send(
                batch_seq=1,
                record_batch=empty_batch,
                record_ids=[],
                cursor=Cursor(token=b""),
                emitted_at=datetime.now(timezone.utc),
            ),
            label=f"Stream {self.stream_name}: synthetic truncate batch",
        )
        try:
            await self._apply_disposition(disposition, batch_seq=1, record_dicts=[])
        except Exception as e:
            # This send runs outside the load stage, so the stage tag its
            # except clause applies has to be applied here instead --
            # otherwise a failed truncate reaches run() untagged and
            # classifies as INTERNAL. tag_failure is no-overwrite, so a
            # deeper tag still wins.
            tag_failure(
                e,
                code=classify_destination_failure(e),
                stage=FailureStage.DESTINATION_LOAD,
            )
            raise
        if isinstance(disposition, Committed):
            logger.info(
                "Stream %s: synthetic truncate batch committed",
                self.stream_name,
            )
        else:
            # Only Committed and AlreadyCommitted reach here (batch_seq 1 of a
            # truncate_insert stream fails the stream on every other verdict),
            # and a replay ack confirms no commit by THIS run (decision 1.2).
            # Saying "committed" here would tell an operator diagnosing a full
            # refresh that the truncate happened when it may not have.
            logger.info(
                "Stream %s: synthetic truncate batch already committed by an "
                "earlier attempt (idempotent replay); this run truncated "
                "nothing",
                self.stream_name,
            )

    async def _checkpoint_stage(self, input_queue: asyncio.Queue[Any]) -> None:
        """Checkpoint processing progress with state management."""
        logger.debug("Starting checkpoint stage for stream %s", self.stream_name)

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
                "Stream %s: Checkpoint stage completed with %s batches",
                self.stream_name,
                batch_count,
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
            logger.info("Stream %s completed successfully", self.stream_name)
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
            "Stream %s completed partially: %s records %s",
            self.stream_name,
            self.metrics.records_failed,
            action,
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
                "Stream %s connectors disconnected successfully", self.stream_name
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
        records_written: int,
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
                "records_written": records_written,
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
                records_skipped=self.metrics.records_skipped,
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
                logger.info("Emitted stream metrics for %s", self.stream_name)
        except Exception as metrics_error:
            logger.error(
                "Failed to emit stream metrics for %s: %s",
                self.stream_name,
                metrics_error,
                exc_info=True,
            )
