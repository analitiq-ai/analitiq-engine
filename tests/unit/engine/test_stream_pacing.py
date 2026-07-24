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
