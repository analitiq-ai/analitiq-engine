"""The transform stage disposes of a batch a validation rule rejected.

Issue #468: a rule's effective strategy -- the assignment's
``validate.error_handling.strategy`` override, else the pipeline default --
decides whether the batch is dead-lettered, skipped, or fails the stream.
The batch never reaches the load stage either way.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from src.engine.batch_policy import ErrorStrategy
from src.engine.exceptions import StreamProcessingError
from src.engine.mapping import MappingDocument, compile_mapping
from src.engine.stream_processor import SourceBatch, StreamProcessor
from src.state.error_classification import ErrorCode, FailureStage, read_failure_tag

pytestmark = pytest.mark.unit


def _mapping(error_handling: dict | None) -> MappingDocument:
    validate: dict = {"rules": [{"type": "not_null", "field": ["v"]}]}
    if error_handling is not None:
        validate["error_handling"] = error_handling
    return MappingDocument.parse(
        {
            "assignments": [
                {
                    "target": {"path": "v", "arrow_type": "Int64", "nullable": True},
                    "value": {
                        "kind": "expression",
                        "expression": {"op": "get", "path": ["v"]},
                    },
                    "validate": validate,
                }
            ]
        }
    )


def _stream_config(write_mode: str | None) -> dict:
    config: dict = {"name": "s1"}
    if write_mode is not None:
        resolved_source = MagicMock()
        resolved_source.replication = None
        resolved_source.primary_keys = []
        config["source"] = {"_resolved_source": resolved_source}
        config["destination"] = {"write_mode": write_mode}
    return config


def _processor(
    mapping: MappingDocument, default: str, *, write_mode: str | None = None
) -> StreamProcessor:
    processor = StreamProcessor(
        stream_id="s1",
        stream_config=_stream_config(write_mode),
        mapping=mapping,
        pipeline_config={},
        pipeline_id="p1",
        state_manager=MagicMock(),
        pipeline_metrics=MagicMock(),
        worker_readable=MagicMock(),
        dlq_root=".",
        batch_size=10,
        buffer_size=10,
        max_retries=0,
        retry_delay=0,
        error_strategy=ErrorStrategy(default),
    )
    processor.transform = compile_mapping(
        mapping, default_strategy=processor.error_strategy
    )
    processor.stream_dlq = MagicMock()
    processor.stream_dlq.send_batch = AsyncMock()
    return processor


async def _run_transform(processor: StreamProcessor, batches: list[pa.RecordBatch]):
    """Drive the transform stage over *batches*; return what reached load."""
    inbound: asyncio.Queue = asyncio.Queue()
    outbound: asyncio.Queue = asyncio.Queue()
    for seq, batch in enumerate(batches, start=1):
        inbound.put_nowait(SourceBatch(seq=seq, batch=batch))
    inbound.put_nowait(None)
    await processor._transform_stage(inbound, outbound)
    passed = []
    while (item := outbound.get_nowait()) is not None:
        passed.append(item.batch.to_pylist())
    return passed


_GOOD = pa.RecordBatch.from_pylist([{"v": 1}])
_BAD = pa.RecordBatch.from_pylist([{"v": None}, {"v": 2}])


class TestTransformStageRouting:
    @pytest.mark.asyncio
    async def test_dlq_override_dead_letters_the_batch_and_continues(self):
        processor = _processor(_mapping({"strategy": "dlq"}), default="fail")

        passed = await _run_transform(processor, [_BAD, _GOOD])

        assert passed == [[{"v": 1}]]
        processor.stream_dlq.send_batch.assert_awaited_once()
        rows, summary = processor.stream_dlq.send_batch.await_args.args[:2]
        assert rows == [{"v": None}, {"v": 2}]
        assert "not_null" in summary
        assert processor.metrics.records_failed == 2
        assert processor.metrics.records_skipped == 0
        assert processor.metrics.batches_failed == 1
        assert processor.metrics.batches_processed == 1
        assert [(d.code, d.stage) for d in processor.dropped_batches] == [
            (ErrorCode.CONFIG_INVALID, FailureStage.TRANSFORM)
        ]

    @pytest.mark.asyncio
    async def test_skip_override_drops_the_batch_without_dead_lettering(self):
        processor = _processor(_mapping({"strategy": "skip"}), default="fail")

        passed = await _run_transform(processor, [_BAD, _GOOD])

        assert passed == [[{"v": 1}]]
        processor.stream_dlq.send_batch.assert_not_awaited()
        assert processor.metrics.records_failed == 2
        assert processor.metrics.records_skipped == 2

    @pytest.mark.asyncio
    async def test_fail_override_stops_the_stream_under_a_lenient_default(self):
        processor = _processor(_mapping({"strategy": "fail"}), default="dlq")

        with pytest.raises(StreamProcessingError, match="not_null") as info:
            await _run_transform(processor, [_BAD, _GOOD])

        processor.stream_dlq.send_batch.assert_not_awaited()
        assert read_failure_tag(info.value).stage is FailureStage.TRANSFORM
        assert processor.metrics.batches_failed == 1

    @pytest.mark.asyncio
    async def test_each_rejected_batch_is_named_by_its_own_sequence(self, caplog):
        processor = _processor(_mapping({"strategy": "dlq"}), default="fail")

        with caplog.at_level("ERROR", logger="src.engine.stream_processor"):
            passed = await _run_transform(processor, [_BAD, _GOOD, _BAD])

        assert passed == [[{"v": 1}]]
        named = [
            record.getMessage().split(": ")[1]
            for record in caplog.records
            if "rejected by validation" in record.getMessage()
        ]
        assert named == [
            "Batch 1 rejected by validation",
            "Batch 3 rejected by validation",
        ]

    @pytest.mark.asyncio
    async def test_no_override_takes_the_pipeline_default(self):
        processor = _processor(_mapping(None), default="dlq")

        passed = await _run_transform(processor, [_BAD])

        assert passed == []
        processor.stream_dlq.send_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_pipeline_default_fail_still_fails_the_stream(self):
        processor = _processor(_mapping(None), default="fail")

        with pytest.raises(StreamProcessingError, match="not_null"):
            await _run_transform(processor, [_BAD])

    @pytest.mark.asyncio
    async def test_partial_completion_names_the_transform_stage(self):
        processor = _processor(_mapping({"strategy": "dlq"}), default="fail")

        await _run_transform(processor, [_BAD])
        status, code, _, detail = processor._classify_completion()

        assert status == "partial"
        assert code is ErrorCode.CONFIG_INVALID
        assert detail.startswith("transform/CONFIG_INVALID:")


async def _run_transform_then_load(
    processor: StreamProcessor, batches: list[pa.RecordBatch]
) -> None:
    """Drive the transform and load stages back to back, as run() wires them."""
    inbound: asyncio.Queue = asyncio.Queue()
    between: asyncio.Queue = asyncio.Queue()
    outbound: asyncio.Queue = asyncio.Queue()
    for seq, batch in enumerate(batches, start=1):
        inbound.put_nowait(SourceBatch(seq=seq, batch=batch))
    inbound.put_nowait(None)
    await processor._transform_stage(inbound, between)
    await processor._load_stage(between, outbound)


class TestTruncateInsertWithEveryBatchRejected:
    """A full refresh whose every batch failed validation landed nothing.

    The zero-batch synthetic truncate exists for an EMPTY source; it must
    not fire when the transform stage dropped the batches, or a dlq/skip
    run would wipe the destination, write nothing, and report partial.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("strategy", ["dlq", "skip"])
    async def test_all_rejected_fails_the_stream_and_never_truncates(self, strategy):
        processor = _processor(
            _mapping({"strategy": strategy}),
            default="fail",
            write_mode="truncate_insert",
        )
        processor.grpc_client = MagicMock()
        processor.grpc_client.send_batch = AsyncMock()

        with pytest.raises(StreamProcessingError, match="landed nothing") as info:
            await _run_transform_then_load(processor, [_BAD, _BAD])

        assert processor.zero_batch_truncate_needed is False
        processor.grpc_client.send_batch.assert_not_awaited()
        tag = read_failure_tag(info.value)
        assert (tag.code, tag.stage) == (
            ErrorCode.CONFIG_INVALID,
            FailureStage.TRANSFORM,
        )
        assert processor.metrics.records_failed == 4
        assert processor.stream_dlq.send_batch.await_count == (
            2 if strategy == "dlq" else 0
        )

    @pytest.mark.asyncio
    async def test_all_rejected_on_a_non_refresh_stream_stays_partial(self):
        processor = _processor(
            _mapping({"strategy": "skip"}), default="fail", write_mode="upsert"
        )
        processor.grpc_client = MagicMock()
        processor.grpc_client.send_batch = AsyncMock()

        await _run_transform_then_load(processor, [_BAD])

        assert processor.zero_batch_truncate_needed is False
        assert processor._classify_completion()[0] == "partial"

    @pytest.mark.asyncio
    async def test_a_genuinely_empty_source_still_truncates(self):
        processor = _processor(
            _mapping({"strategy": "skip"}), default="fail", write_mode="truncate_insert"
        )
        processor.grpc_client = MagicMock()
        processor.grpc_client.send_batch = AsyncMock()

        await _run_transform_then_load(processor, [])

        assert processor.zero_batch_truncate_needed is True
        assert processor.dropped_batches == []


class TestSourceSequenceSurvivesADrop:
    @pytest.mark.asyncio
    async def test_load_stage_names_a_batch_by_its_source_sequence(self, caplog):
        """Batch 1 is rejected; batch 2 goes out as wire batch 1 but is still
        logged, and metered, as batch 2."""
        from src.grpc.generated.analitiq.v1 import AckStatus

        processor = _processor(
            _mapping({"strategy": "dlq"}), default="fail", write_mode="upsert"
        )
        result = MagicMock()
        result.status = AckStatus.ACK_STATUS_SUCCESS
        result.success = True
        result.records_written = 1
        result.committed_cursor = None
        processor.grpc_client = MagicMock()
        processor.grpc_client.send_batch = AsyncMock(return_value=result)
        emitted = []
        processor._emit_batch_metrics = lambda seq, *a: emitted.append(seq)

        with caplog.at_level("DEBUG", logger="src.engine.stream_processor"):
            await _run_transform_then_load(processor, [_BAD, _GOOD])

        assert processor.grpc_client.send_batch.await_args.kwargs["batch_seq"] == 1
        assert emitted == [2]
        messages = [r.getMessage() for r in caplog.records]
        assert any("Batch 1 rejected by validation" in m for m in messages)
        assert any("Batch 2 committed" in m for m in messages)
        assert not any("Batch 1 committed" in m for m in messages)
