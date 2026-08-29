"""The engine surfaces what a source page reported about itself (issue #469).

The extract stage reads each batch's ``response.metadata`` slot, logs it,
and keeps the last page's values for the stream's metrics record.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from cdk.batch_metadata import RESPONSE_METADATA_KEY, with_response_metadata
from src.engine.stream_processor import StreamMetrics, StreamProcessor
from src.state.metrics_storage import create_metrics_record

pytestmark = pytest.mark.unit


class _Readable:
    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        self._batches = batches

    async def read_batches(self, runtime, config, **_: Any):
        for batch in self._batches:
            yield batch


def _processor(batches: list[pa.RecordBatch]) -> StreamProcessor:
    processor = object.__new__(StreamProcessor)
    processor.stream_name = "items"
    processor.stream_id = "items"
    processor.batch_size = 10
    processor.metrics = StreamMetrics()
    processor.state_manager = MagicMock()
    resolved = MagicMock()
    resolved.runtime = MagicMock()
    processor.stream_config = {"source": {"_resolved_source": resolved}}
    processor._is_truncate_insert = lambda: False
    return processor


async def _drain(processor: StreamProcessor, readable: _Readable) -> list[Any]:
    queue: asyncio.Queue[Any] = asyncio.Queue()
    await processor._extract_stage(readable, queue)
    out: list[Any] = []
    while not queue.empty():
        out.append(queue.get_nowait())
    return out


@pytest.mark.asyncio
class TestExtractStageSurfacesResponseMetadata:
    async def test_the_last_page_values_reach_the_stream_metrics(self, caplog) -> None:
        first = with_response_metadata(
            pa.RecordBatch.from_pylist([{"id": 1}]), {"total": 2, "remaining": 9}
        )
        last = with_response_metadata(
            pa.RecordBatch.from_pylist([{"id": 2}]), {"total": 2, "remaining": 8}
        )
        processor = _processor([first, last])
        with caplog.at_level(logging.INFO, logger="src.engine.stream_processor"):
            drained = await _drain(processor, _Readable([first, last]))
        assert drained[-1] is None
        assert processor.metrics.response_metadata == {"total": 2, "remaining": 8}
        logged = [
            r.getMessage()
            for r in caplog.records
            if "response metadata" in r.getMessage()
        ]
        assert len(logged) == 2
        assert "'remaining': 9" in logged[0]
        assert "'remaining': 8" in logged[1]

    async def test_a_slot_that_will_not_decode_is_never_published(self) -> None:
        # Transform and load run concurrently with extract; a batch put on
        # the queue before its slot is checked could be committed downstream
        # before the failure is seen.
        bad = pa.RecordBatch.from_pylist([{"id": 1}]).replace_schema_metadata(
            {RESPONSE_METADATA_KEY: b"[1]"}
        )
        processor = _processor([bad])
        queue: asyncio.Queue[Any] = asyncio.Queue()
        with pytest.raises(ValueError, match="not an object"):
            await processor._extract_stage(_Readable([bad]), queue)
        # Only the end-of-stream sentinel was published, never the batch.
        assert queue.get_nowait() is None
        assert queue.empty()

    async def test_a_source_declaring_none_leaves_the_metrics_none(self) -> None:
        batch = pa.RecordBatch.from_pylist([{"id": 1}])
        processor = _processor([batch])
        await _drain(processor, _Readable([batch]))
        assert processor.metrics.response_metadata is None


class TestMetricsRecordCarriesResponseMetadata:
    def test_the_values_land_on_the_record(self) -> None:
        record = create_metrics_record(
            run_id="r1",
            pipeline_id="p1",
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
            records_skipped=0,
            response_metadata={"total": 2},
        )
        assert record.response_metadata == {"total": 2}
        assert record.model_dump()["response_metadata"] == {"total": 2}

    def test_absent_by_default(self) -> None:
        record = create_metrics_record(
            run_id="r1",
            pipeline_id="p1",
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
            records_skipped=0,
        )
        assert record.response_metadata is None
