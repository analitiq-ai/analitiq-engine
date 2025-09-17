"""Integration-focused tests for the high-level Pipeline abstraction."""

from __future__ import annotations

import asyncio
import copy
import json
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pytest

from analitiq_stream.core.pipeline import Pipeline
from analitiq_stream.core.engine import StreamingEngine
from analitiq_stream.connectors.base import BaseConnector
from analitiq_stream.fault_tolerance.config_compatibility import ConfigCompatibilityError


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


class InMemoryDestinationConnector(BaseConnector):
    """Connector used in tests to capture written batches."""

    instances: List["InMemoryDestinationConnector"] = []

    def __init__(self, name: str = "InMemoryDestinationConnector"):
        super().__init__(name)
        self.written_batches: List[List[Dict[str, Any]]] = []
        self.is_connected = False
        self.last_config: Optional[Dict[str, Any]] = None
        InMemoryDestinationConnector.instances.append(self)

    async def connect(self, config: Dict[str, Any]):
        self.is_connected = True
        self.last_config = dict(config)

    async def disconnect(self):
        self.is_connected = False

    async def configure(self, config: Dict[str, Any]):  # pragma: no cover - configuration is a no-op for the fake connector
        self.last_config = dict(config)

    async def read_batches(self, config: Dict[str, Any], batch_size: int = 1000, **_: Any):  # pragma: no cover - destination only
        if False:
            yield []

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        current_batch = [dict(record) for record in batch]
        self.written_batches.append(current_batch)
        self.metrics["records_written"] += len(current_batch)
        self.metrics["batches_written"] += 1
        self.last_config = dict(config)


@pytest.fixture(autouse=True)
def in_memory_connectors(monkeypatch):
    """Patch engine connectors to use the in-memory implementations."""

    from analitiq_stream.connectors import api as api_module
    from analitiq_stream.connectors import database as db_module

    InMemorySourceConnector.instances.clear()
    InMemoryDestinationConnector.instances.clear()

    monkeypatch.setattr(api_module, "APIConnector", InMemorySourceConnector)
    monkeypatch.setattr(db_module, "DatabaseConnector", InMemoryDestinationConnector)

    yield

    InMemorySourceConnector.instances.clear()
    InMemoryDestinationConnector.instances.clear()


@pytest.fixture
def temp_directories(tmp_path, monkeypatch):
    """Use an isolated filesystem layout for stateful components."""

    from analitiq_stream import config as global_config

    state_dir = tmp_path / "state"
    deadletter_dir = tmp_path / "deadletter"
    logs_dir = tmp_path / "logs"

    for path in (state_dir, deadletter_dir, logs_dir):
        path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setitem(global_config.DIRECTORIES, "state", state_dir)
    monkeypatch.setitem(global_config.DIRECTORIES, "deadletter", deadletter_dir)
    monkeypatch.setitem(global_config.DIRECTORIES, "logs", logs_dir)

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
        "engine_config": {
            "batch_size": 2,
            "max_concurrent_batches": 1,
            "buffer_size": 256,
        },
        "streams": {
            "users": {
                "name": "User Stream",
                "description": "Simple user sync",
                "src": {
                    "endpoint_id": "source-endpoint-123",
                    "replication_key": "updated_at",
                    "cursor_field": "updated_at",
                    "cursor_mode": "inclusive",
                    "safety_window_seconds": 60,
                },
                "dst": {
                    "endpoint_id": "dest-endpoint-456",
                    "refresh_mode": "upsert",
                    "batch_size": 50,
                },
                "mapping": {},
            }
        },
        "monitoring": {
            "log_level": "INFO",
            "progress_monitoring": "disabled",
        },
    }


@pytest.fixture
def valid_source_config() -> Dict[str, Any]:
    """Source configuration that feeds deterministic data into the engine."""

    return {
        "type": "api",
        "endpoint": "/users",
        "method": "GET",
        "base_url": "https://api.example.com",
        "headers": {},
        "pagination": {"type": "page"},
        "endpoint_id": "source-endpoint-123",
        "host_id": "source-host-456",
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
        "host_id": "dest-host-789",
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
        src = copy.deepcopy(source_config)
        dst = copy.deepcopy(destination_config)

        state_path = temp_directories["state"] / cfg["pipeline_id"]
        pipeline = Pipeline(
            pipeline_config=cfg,
            source_config=src,
            destination_config=dst,
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
    assert pipeline.engine.batch_size == config["engine_config"]["batch_size"]
    assert Path(pipeline.state_dir).exists()
    assert Path(pipeline.logs_dir).exists()
    assert Path(pipeline.dlq_dir).exists()


def test_pipeline_run_processes_batches_and_updates_metrics(
    pipeline_factory,
    base_pipeline_config,
    valid_source_config,
    valid_destination_config,
):
    """Running the pipeline should stream data and update metrics."""

    config = copy.deepcopy(base_pipeline_config)
    config["pipeline_id"] = "run-pipeline"

    pipeline = pipeline_factory(
        config,
        source_config=valid_source_config,
        destination_config=valid_destination_config,
    )

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 2
    assert metrics.batches_processed == 1
    assert metrics.records_failed == 0

    assert InMemoryDestinationConnector.instances, "Destination connector was not instantiated"
    dest_connector = InMemoryDestinationConnector.instances[-1]
    assert dest_connector.written_batches == [
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
    ]

    run_info = pipeline.engine.get_state_manager().get_run_info()
    assert run_info.get("run_id")
    assert run_info.get("pipeline_id") == "run-pipeline"


def test_config_state_validation_without_previous_run(
    pipeline_factory,
    base_pipeline_config,
    valid_source_config,
    valid_destination_config,
):
    """Compatibility validation should be a no-op when no state exists."""

    config = copy.deepcopy(base_pipeline_config)
    config["pipeline_id"] = "first-run"

    pipeline = pipeline_factory(
        config,
        source_config=valid_source_config,
        destination_config=valid_destination_config,
    )

    # No exception should be raised even though no state has been recorded yet.
    pipeline._validate_config_state_compatibility()
    assert pipeline.engine.get_state_manager().get_run_info() == {}


def test_config_state_validation_recovers_breaking_changes(
    pipeline_factory,
    base_pipeline_config,
    valid_source_config,
    valid_destination_config,
):
    """Breaking changes should trigger the real recovery manager."""

    initial_config = copy.deepcopy(base_pipeline_config)
    initial_config["pipeline_id"] = "recoverable-pipeline"

    first_pipeline = pipeline_factory(
        initial_config,
        source_config=valid_source_config,
        destination_config=valid_destination_config,
    )
    asyncio.run(first_pipeline.run())
    first_index = json.loads(first_pipeline.engine.get_state_manager().index_file.read_text())

    updated_config = copy.deepcopy(initial_config)
    updated_config["streams"]["users"]["src"]["replication_key"] = "created_at"
    updated_config["streams"]["users"]["src"]["cursor_field"] = "created_at"

    second_pipeline = pipeline_factory(
        updated_config,
        source_config=valid_source_config,
        destination_config=valid_destination_config,
    )

    # Should not raise because ConfigStateRecoveryManager can handle the change.
    second_pipeline._validate_config_state_compatibility()

    second_index = json.loads(second_pipeline.engine.get_state_manager().index_file.read_text())
    assert second_index.get("streams", {}) == {}
    assert second_index.get("run", {}).get("config_fingerprint") == first_index.get("run", {}).get("config_fingerprint")


def test_config_state_validation_raises_on_pipeline_id_mismatch(
    pipeline_factory,
    base_pipeline_config,
    valid_source_config,
    valid_destination_config,
):
    """Critical compatibility issues should propagate real exceptions."""

    config = copy.deepcopy(base_pipeline_config)
    config["pipeline_id"] = "critical-pipeline"

    first_pipeline = pipeline_factory(
        config,
        source_config=valid_source_config,
        destination_config=valid_destination_config,
    )
    asyncio.run(first_pipeline.run())

    state_manager = first_pipeline.engine.get_state_manager()
    index_path = state_manager.index_file
    index_data = json.loads(index_path.read_text())
    index_data["run"]["pipeline_id"] = "different-pipeline"
    index_path.write_text(json.dumps(index_data))

    second_pipeline = pipeline_factory(
        config,
        source_config=valid_source_config,
        destination_config=valid_destination_config,
    )

    with pytest.raises(ConfigCompatibilityError):
        second_pipeline._validate_config_state_compatibility()
