"""Stream fan-out pacing from declared concurrency (issue #401).

The engine builds one gate per source connection whose connector declares
``concurrency.max_connections``; streams sharing that connection run at
most that many at a time. Undeclared means no gate — current behavior.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from src.engine.engine import StreamingEngine
from src.engine.stream_processor import StreamProcessor


def _stream_config(runtime) -> dict:
    return {"source": {"_resolved_source": SimpleNamespace(runtime=runtime)}}


def _runtime(connection_id: str, concurrency: dict | None) -> MagicMock:
    runtime = MagicMock()
    runtime.connection_id = connection_id
    runtime.connector_id = "demo"
    runtime.declared_concurrency = concurrency
    return runtime


class TestSourcePacingGates:
    def test_declared_ceiling_builds_one_shared_gate(self):
        runtime = _runtime("db-1", {"max_connections": 3})
        streams = {
            "s1": _stream_config(runtime),
            "s2": _stream_config(runtime),
        }
        gates = StreamingEngine._source_pacing_gates(streams)
        assert set(gates) == {"db-1"}
        assert isinstance(gates["db-1"], asyncio.Semaphore)
        assert gates["db-1"]._value == 3

    def test_undeclared_ceiling_builds_no_gate(self):
        runtime = _runtime("db-1", None)
        gates = StreamingEngine._source_pacing_gates({"s1": _stream_config(runtime)})
        assert gates == {}

    def test_hand_built_config_without_runtime_builds_no_gate(self):
        gates = StreamingEngine._source_pacing_gates({"s1": {"source": {}}})
        assert gates == {}

    def test_distinct_connections_get_distinct_gates(self):
        streams = {
            "s1": _stream_config(_runtime("db-1", {"max_connections": 2})),
            "s2": _stream_config(_runtime("db-2", {"max_connections": 5})),
        }
        gates = StreamingEngine._source_pacing_gates(streams)
        assert gates["db-1"]._value == 2
        assert gates["db-2"]._value == 5


class TestPacingGateBoundsConcurrency:
    async def test_gate_holds_streams_to_the_ceiling(self):
        # Drive _process_stream's gating shape directly: N tasks under a
        # 2-slot gate never observe more than 2 concurrent entries.
        gate = asyncio.Semaphore(2)
        active = 0
        peak = 0

        async def stream_body():
            nonlocal active, peak
            async with gate:
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.01)
                active -= 1

        await asyncio.gather(*[stream_body() for _ in range(6)])
        assert peak == 2


class TestProcessStreamForwardsTheGate:
    """`_process_stream` hands the gate to the processor, which paces the
    extract stage — a dropped forward would pass the shape tests above
    while unleashing full fan-out."""

    async def test_gate_reaches_the_stream_processor(self, monkeypatch):
        import src.engine.engine as engine_module

        engine = object.__new__(StreamingEngine)
        engine.pipeline_id = "p1"
        engine.state_manager = MagicMock()
        engine.metrics = MagicMock()
        engine._worker_readable = MagicMock()
        engine.dlq_path = "./deadletter/"
        engine.batch_size = 1
        engine.buffer_size = 1
        engine.max_retries = 1
        engine.retry_delay = 0.0
        engine.error_strategy = "fail"
        engine._partial_error_codes = []

        captured: dict = {}

        class _Processor:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            async def run(self):
                return None

        monkeypatch.setattr(engine_module, "StreamProcessor", _Processor)
        gate = asyncio.Semaphore(2)
        await engine._process_stream("s1", {}, {}, pacing_gate=gate)
        assert captured["pacing_gate"] is gate


class TestPacedHoldsTheSlotForExtractOnly:
    """The processor's `_paced` wrapper bounds live source connections:
    the slot is held while the extract coroutine runs and released the
    moment it finishes — a stream still draining its destination must not
    block other streams' reads."""

    def _processor(self, gate) -> StreamProcessor:
        processor = object.__new__(StreamProcessor)
        processor.pacing_gate = gate
        return processor

    async def test_extract_stages_serialize_under_a_one_slot_gate(self):
        gate = asyncio.Semaphore(1)
        active = 0
        peak = 0

        async def extract():
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1

        await asyncio.gather(
            *[self._processor(gate)._paced(extract()) for _ in range(4)]
        )
        assert peak == 1

    async def test_slot_is_released_when_extraction_finishes(self):
        gate = asyncio.Semaphore(1)

        async def extract():
            return None

        await self._processor(gate)._paced(extract())
        # The slot is free again even though the stream's load stage would
        # still be draining at this point.
        assert gate._value == 1

    async def test_no_gate_runs_the_stage_directly(self):
        ran: list[bool] = []

        async def extract():
            ran.append(True)

        await self._processor(None)._paced(extract())
        assert ran == [True]
