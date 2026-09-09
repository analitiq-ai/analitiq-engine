"""``_extract_stage`` picks the checkpoint it hands to ``read_batches`` (#489, claim 1).

A truncate_insert stream must never resume from a persisted cursor — the
destination truncates on the read's first batch, so a resumed slice would be
the only data left in the target (issue #307). The dispatch that enforces
this lives in ``_extract_stage`` itself, gated by ``_is_truncate_insert()``;
the existing ``_FullRefreshCheckpoint`` unit test only pins the wrapper's own
behaviour in isolation and never exercises this dispatch. These tests drive
``_extract_stage`` for real, with no stub of ``_is_truncate_insert``, and
inspect the ``checkpoint=`` argument ``read_batches`` actually receives.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from src.engine.stream_processor import (
    StreamMetrics,
    StreamProcessor,
    _FullRefreshCheckpoint,
)

pytestmark = pytest.mark.unit


class _CapturingReadable:
    """Records the ``checkpoint=`` kwarg it was called with, yields nothing."""

    def __init__(self) -> None:
        self.received_checkpoint: Any = None

    async def read_batches(self, runtime, config, **kwargs: Any):
        self.received_checkpoint = kwargs.get("checkpoint")
        return
        yield pa.RecordBatch.from_pylist([])  # pragma: no cover - unreachable


def _processor(write_mode: str, state_manager: MagicMock) -> StreamProcessor:
    processor = object.__new__(StreamProcessor)
    processor.stream_name = "items"
    processor.stream_id = "items"
    processor.batch_size = 10
    processor.metrics = StreamMetrics()
    processor.state_manager = state_manager
    resolved = MagicMock()
    resolved.runtime = MagicMock()
    processor.stream_config = {
        "source": {"_resolved_source": resolved},
        "destination": {"write_mode": write_mode},
    }
    return processor


async def _drive(processor: StreamProcessor, readable: _CapturingReadable) -> None:
    queue: asyncio.Queue[Any] = asyncio.Queue()
    await processor._extract_stage(readable, queue)


@pytest.mark.asyncio
class TestExtractStageCheckpointDispatch:
    async def test_truncate_insert_receives_the_full_refresh_view(self) -> None:
        state_manager = MagicMock()
        processor = _processor("truncate_insert", state_manager)
        readable = _CapturingReadable()

        await _drive(processor, readable)

        checkpoint = readable.received_checkpoint
        assert isinstance(checkpoint, _FullRefreshCheckpoint)
        assert checkpoint._inner is state_manager

    async def test_append_receives_the_bare_state_manager(self) -> None:
        state_manager = MagicMock()
        processor = _processor("append", state_manager)
        readable = _CapturingReadable()

        await _drive(processor, readable)

        assert readable.received_checkpoint is state_manager
