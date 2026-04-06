"""Integration-focused tests for the high-level Pipeline abstraction."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

os.environ.setdefault("LOG_LEVEL", "DEBUG")
os.environ.setdefault("METRICS_ENABLED", "false")

import pytest

from src.engine.pipeline import Pipeline
from src.engine.engine import StreamingEngine
from src.source.connectors.base import BaseConnector


class InMemorySourceConnector(BaseConnector):
    """Connector used in tests that yields pre-configured batches."""

    instances: List["InMemorySourceConnector"] = []

    def __init__(self, name: str = "InMemorySourceConnector"):
        super().__init__(name)
        self._batches: List[List[Dict[str, Any]]] = []
        self.is_connected = False
        InMemorySourceConnector.instances.append(self)

    async def connect(self, config: Dict[str, Any]):
        batches = config.get("test_batches")
        if batches is None:
            records = config.get("test_records", [])
            batches = [records] if records else []
        self._batches = [
            [dict(record) for record in batch]
            for batch in batches
        ]
        self.metrics["records_read"] = 0
        self.metrics["batches_read"] = 0
        self.is_connected = True

    async def disconnect(self):
        self.is_connected = False

    async def read_batches(
        self,
        config: Dict[str, Any],
        *,
        batch_size: int = 1000,
        **_: Any,
    ) -> Iterable[List[Dict[str, Any]]]:
        for batch in self._batches:
            current_batch = batch[:batch_size]
            self.metrics["records_read"] += len(current_batch)
            self.metrics["batches_read"] += 1
            yield current_batch

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):  # pragma: no cover - not used
        raise NotImplementedError


@pytest.fixture(autouse=True)
def in_memory_connectors(monkeypatch):
    """Patch engine source connector to use the in-memory implementation."""

    from src.source.connectors import api as api_module

    InMemorySourceConnector.instances.clear()

    monkeypatch.setattr(api_module, "APIConnector", InMemorySourceConnector)

    yield

    InMemorySourceConnector.instances.clear()


@pytest.fixture
def temp_directories(tmp_path):
    """Use an isolated filesystem layout for stateful components."""

    state_dir = tmp_path / "state"
    deadletter_dir = tmp_path / "deadletter"
    logs_dir = tmp_path / "logs"

    for path in (state_dir, deadletter_dir, logs_dir):
        path.mkdir(parents=True, exist_ok=True)

    return {
        "state": state_dir,
        "deadletter": deadletter_dir,
        "logs": logs_dir,
    }


@pytest.fixture
def base_pipeline_config() -> Dict[str, Any]:
    """Base pipeline configuration used across tests."""

    return {
        "pipeline_id": "test-pipeline",
        "name": "Test Pipeline",
        "version": "1.0",
        "engine": {
            "vcpu": 1,
            "memory": 8192
        },
        "runtime": {
            "buffer_size": 256,
            "batching": {
                "batch_size": 2,
                "max_concurrent_batches": 1
            },
            "logging": {
                "log_level": "INFO",
                "metrics_enabled": False
            },
            "error_handling": {
                "strategy": "dlq",
                "max_retries": 3,
                "retry_delay": 1
            }
        },
        "streams": {
            "users": {
                "name": "User Stream",
                "description": "Simple user sync",
                "source": {
                    "endpoint_id": "source-endpoint-123",
                    "replication_key": "updated_at",
                    "cursor_field": "updated_at",
                    "cursor_mode": "inclusive",
                    "safety_window_seconds": 60,
                },
                "destination": {
                    "endpoint_id": "dest-endpoint-456",
                    "refresh_mode": "upsert",
                    "batch_size": 50,
                },
                "mapping": {},
            }
        },
    }


@pytest.fixture
def valid_source_config() -> Dict[str, Any]:
    """Source configuration that feeds deterministic data into the engine."""

    return {
        "type": "api",
        "endpoint": "/users",
        "method": "GET",
        "host": "https://api.example.com",
        "headers": {},
        "pagination": {"type": "page"},
        "endpoint_id": "source-endpoint-123",
        "connection_id": "source-connection-456",
        "replication_method": "incremental",
        "cursor_field": "updated_at",
        "cursor_mode": "inclusive",
        "safety_window_seconds": 60,
        "tie_breaker_fields": [],
        "test_batches": [
            [
                {
                    "id": 1,
                    "name": "Alice",
                    "updated_at": "2024-01-01T00:00:00Z",
                    "created_at": "2023-12-31T23:55:00Z",
                },
                {
                    "id": 2,
                    "name": "Bob",
                    "updated_at": "2024-01-02T00:00:00Z",
                    "created_at": "2024-01-01T08:00:00Z",
                },
            ]
        ],
    }


@pytest.fixture
def valid_destination_config() -> Dict[str, Any]:
    """Destination configuration recorded by the in-memory connector."""

    return {
        "type": "database",
        "driver": "sqlite",
        "host": "localhost",
        "port": 0,
        "database": "testdb",
        "user": "user",
        "password": "pass",
        "endpoint_id": "dest-endpoint-456",
        "connection_id": "dest-connection-789",
        "refresh_mode": "upsert",
        "batch_support": True,
        "batch_size": 50,
        "table": "users",
    }


@pytest.fixture
def pipeline_factory(temp_directories):
    """Factory that creates pipelines and cleans up generated directories."""

    cleanup_paths: set[Path] = set()

    def _create(
        pipeline_config: Dict[str, Any],
        *,
        source_config: Dict[str, Any],
        destination_config: Dict[str, Any],
    ) -> Pipeline:
        cfg = copy.deepcopy(pipeline_config)
        source = copy.deepcopy(source_config)
        destination = copy.deepcopy(destination_config)

        cfg["source"] = copy.deepcopy(source)
        cfg["destination"] = copy.deepcopy(destination)

        for stream in cfg.get("streams", {}).values():
            stream_src = copy.deepcopy(stream.get("source", {})) or {}
            stream_dst = copy.deepcopy(stream.get("destination", {})) or {}

            merged_src = {**source, **stream_src}
            merged_dst = {**destination, **stream_dst}

            stream["source"] = merged_src
            stream["destination"] = merged_dst

        state_path = temp_directories["state"] / cfg["pipeline_id"]
        pipeline = Pipeline(
            cfg,
            state_dir=str(state_path),
        )

        cleanup_paths.add(Path(pipeline.logs_dir))
        cleanup_paths.add(Path(pipeline.dlq_dir))
        return pipeline

    yield _create

    for path in cleanup_paths:
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)


def test_pipeline_initializes_real_engine(
    pipeline_factory,
    base_pipeline_config,
    valid_source_config,
    valid_destination_config,
):
    """Ensure pipeline instantiation wires up the real streaming engine."""

    config = copy.deepcopy(base_pipeline_config)
    config["pipeline_id"] = "init-pipeline"

    pipeline = pipeline_factory(
        config,
        source_config=valid_source_config,
        destination_config=valid_destination_config,
    )

    assert isinstance(pipeline.engine, StreamingEngine)
    assert pipeline.engine.batch_size == config["runtime"]["batching"]["batch_size"]
    assert Path(pipeline.state_dir).exists()
    assert Path(pipeline.logs_dir).exists()
    assert Path(pipeline.dlq_dir).exists()


# NOTE: test_pipeline_run_processes_batches_and_updates_metrics was removed
# because it tested direct mode which is no longer supported.
# Pipeline now always uses gRPC for destination writes.
# gRPC-based integration tests should be added in tests/integration/


