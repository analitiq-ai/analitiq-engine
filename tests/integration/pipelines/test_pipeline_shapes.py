"""Pipeline-shape integration tests.

Each shape gets a synthesized fixture tree (no on-disk fixture pollution)
loaded through Layer-B factories, plus an end-to-end run with a fake
source connector and a fake gRPC client. Asserts that the typed boundary
holds for every shape and that the engine wires sources/destinations
together without dict-passing artefacts.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional
from unittest.mock import patch

import pytest

from src.engine.engine import StreamingEngine
from src.engine.pipeline_config_prep import PipelineConfigPrep
from src.engine.resolved import (
    ApiReadEndpoint,
    ApiWriteEndpoint,
    DatabaseReadEndpoint,
    DatabaseWriteEndpoint,
    ResolvedPipeline,
    load_resolved_pipeline,
)
from src.grpc.client import BatchResult, DestinationGRPCClient
from src.grpc.cursor import Cursor
from src.grpc.generated.analitiq.v1 import AckStatus
from src.source.connectors.base import BaseConnector

from .fixture_builder import BUILDERS


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeSourceConnector(BaseConnector):
    """Source connector that yields canned batches and ignores the runtime."""

    def __init__(self, batches: List[List[Dict[str, Any]]]):
        super().__init__("FakeSource")
        self._batches = batches

    async def connect(self, runtime) -> None:  # noqa: D401
        self.is_connected = True

    async def disconnect(self) -> None:
        self.is_connected = False

    async def read_batches(
        self,
        source,
        *,
        state_manager,
        stream_id: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        for batch in self._batches:
            yield batch


class _FakeGrpcClient:
    """Drop-in for :class:`DestinationGRPCClient` that records inputs and
    returns SUCCESS ACKs."""

    def __init__(self) -> None:
        self.schemas: List[tuple] = []
        self.batches: List[Dict[str, Any]] = []

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        pass

    async def start_stream(self, run_id, stream_id, destination, endpoint_schema_json):
        self.schemas.append((stream_id, destination, endpoint_schema_json))
        return True

    async def send_batch(self, run_id, stream_id, batch_seq, records, record_ids, cursor, payload_format):
        self.batches.append({
            "run_id": run_id, "stream_id": stream_id, "batch_seq": batch_seq,
            "records": records, "record_ids": record_ids,
        })
        return BatchResult(
            success=True,
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=len(records),
            committed_cursor=None,
            failed_record_ids=[],
            failure_summary="",
        )


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_state(tmp_path, monkeypatch):
    """Move state/, deadletter/, logs/ under tmp_path so concurrent test
    runs and the repo's real ``state/`` directory don't interfere."""
    monkeypatch.chdir(tmp_path)
    yield


def _load(tmp_path: Path, shape: str) -> ResolvedPipeline:
    pipeline_id = BUILDERS[shape](tmp_path)
    return load_resolved_pipeline(tmp_path, pipeline_id)


# ---------------------------------------------------------------------------
# Construction tests (every shape loads via Layer-B factories cleanly)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shape", list(BUILDERS.keys()))
def test_fixture_loads_cleanly(tmp_path, shape: str):
    pipeline = _load(tmp_path, shape)
    assert isinstance(pipeline, ResolvedPipeline)
    assert pipeline.streams
    for stream in pipeline.streams:
        assert stream.source.connection.connection_id
        assert stream.destinations
        for d in stream.destinations:
            assert d.endpoint.endpoint_id


def test_api_to_db_incremental_shape(tmp_path):
    pipeline = _load(tmp_path, "api_to_db_incremental")
    stream = pipeline.streams[0]
    assert isinstance(stream.source.endpoint, ApiReadEndpoint)
    assert isinstance(stream.destinations[0].endpoint, DatabaseWriteEndpoint)
    assert stream.source.replication.method == "incremental"
    assert stream.source.replication.cursor_field == "updated_at"
    assert stream.destinations[0].write.mode == "upsert"


def test_db_to_db_incremental_shape(tmp_path):
    pipeline = _load(tmp_path, "db_to_db_incremental")
    stream = pipeline.streams[0]
    assert isinstance(stream.source.endpoint, DatabaseReadEndpoint)
    assert isinstance(stream.destinations[0].endpoint, DatabaseWriteEndpoint)
    assert stream.source.replication.method == "incremental"
    assert stream.source.endpoint.database_object.name == "orders"


def test_db_to_db_full_shape(tmp_path):
    pipeline = _load(tmp_path, "db_to_db_full")
    stream = pipeline.streams[0]
    assert stream.source.replication.method == "full_refresh"
    assert stream.destinations[0].write.mode == "truncate_insert"


def test_api_to_api_shape(tmp_path):
    pipeline = _load(tmp_path, "api_to_api")
    stream = pipeline.streams[0]
    assert isinstance(stream.source.endpoint, ApiReadEndpoint)
    assert isinstance(stream.destinations[0].endpoint, ApiWriteEndpoint)
    assert stream.destinations[0].endpoint.write_mode == "insert"
    assert stream.destinations[0].endpoint.request.path == "/v1/items"


def test_multi_stream_one_dest_shape(tmp_path):
    pipeline = _load(tmp_path, "multi_stream_one_dest")
    assert len(pipeline.streams) == 3
    # All streams target the same destination connection.
    dest_ids = {s.destinations[0].connection.connection_id for s in pipeline.streams}
    assert len(dest_ids) == 1
    methods = {s.source.replication.method for s in pipeline.streams}
    assert methods == {"incremental", "full_refresh"}


# ---------------------------------------------------------------------------
# PipelineConfigPrep tuple shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shape", list(BUILDERS.keys()))
def test_pipeline_config_prep_returns_typed_tuple(tmp_path, monkeypatch, shape: str):
    pipeline_id = BUILDERS[shape](tmp_path)
    monkeypatch.setenv("PIPELINE_ID", pipeline_id)
    monkeypatch.chdir(tmp_path)
    resolved, runtimes, raw_endpoints = PipelineConfigPrep().create_config()
    assert resolved.pipeline_id == pipeline_id
    # Every stream's connections appear in the runtimes map.
    for stream in resolved.streams:
        assert stream.source.connection.connection_id in runtimes
        for d in stream.destinations:
            assert d.connection.connection_id in runtimes
        src_key = (
            stream.source.endpoint_ref.scope,
            stream.source.endpoint_ref.connection_id,
            stream.source.endpoint_ref.endpoint_id,
        )
        assert src_key in raw_endpoints


# ---------------------------------------------------------------------------
# End-to-end run with fakes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape",
    [
        "api_to_db_incremental",
        "db_to_db_incremental",
        "db_to_db_full",
        "api_to_api",
        "multi_stream_one_dest",
    ],
)
async def test_engine_runs_with_fakes(tmp_path, monkeypatch, shape: str):
    """Patch the source connector and gRPC client to in-process fakes;
    drive `StreamingEngine.run` and assert batches flow through."""
    pipeline_id = BUILDERS[shape](tmp_path)
    monkeypatch.setenv("PIPELINE_ID", pipeline_id)
    monkeypatch.chdir(tmp_path)

    resolved, runtimes, raw_endpoints = PipelineConfigPrep().create_config()
    engine = StreamingEngine.from_resolved(resolved)

    fake_records = [
        {"id": 1, "value": "a", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": 2, "value": "b", "updated_at": "2024-01-02T00:00:00Z"},
    ]
    fake_grpc = _FakeGrpcClient()

    def _fake_source_factory(_self, _source):
        return _FakeSourceConnector(batches=[fake_records])

    monkeypatch.setattr(StreamingEngine, "_create_source_connector", _fake_source_factory)
    monkeypatch.setattr(StreamingEngine, "_create_grpc_client", lambda _self: fake_grpc)

    await engine.run(resolved, runtimes, raw_endpoints)

    # SCHEMA must have been sent for every stream.
    assert len(fake_grpc.schemas) == len(resolved.streams)
    # Each stream sent at least one batch.
    stream_ids = {batch["stream_id"] for batch in fake_grpc.batches}
    assert stream_ids == {s.stream_id for s in resolved.streams}
