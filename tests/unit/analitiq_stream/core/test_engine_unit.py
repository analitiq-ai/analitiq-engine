"""Unit tests for StreamingEngine - minimal mocking, real functionality focus."""

from typing import Any

import pytest

from src.engine.engine import StreamingEngine
from src.engine.exceptions import ConfigurationError
from src.models.resolved import BatchingConfig, RuntimeConfig
from src.source.connectors.base import BaseConnector


def _runtime(
    *, batch_size: int, max_concurrent_batches: int, buffer_size: int
) -> RuntimeConfig:
    """Build a RuntimeConfig from the loose tuning values a test cares about."""
    return RuntimeConfig(
        batching=BatchingConfig(
            batch_size=batch_size, max_concurrent_batches=max_concurrent_batches
        ),
        buffer_size=buffer_size,
    )


class InMemorySourceConnector(BaseConnector):
    """Real in-memory source connector for testing."""

    def __init__(self, records: list[dict[str, Any]] = None):
        super().__init__()
        self.records = records or []
        self.connected = False
        self.batch_size = 10

    async def connect(self, config: dict[str, Any]) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def health_check(self) -> bool:
        return self.connected

    async def read_batch(self, batch_size: int = None) -> list[dict[str, Any]]:
        size = batch_size or self.batch_size
        if not self.records:
            return []
        batch = self.records[:size]
        self.records = self.records[size:]
        return batch

    async def read_batches(self, batch_size: int = None):
        """Generator for reading batches."""
        size = batch_size or self.batch_size
        while self.records:
            batch = self.records[:size]
            self.records = self.records[size:]
            yield batch


class InMemoryDestinationConnector(BaseConnector):
    """Real in-memory destination connector for testing."""

    def __init__(self):
        super().__init__()
        self.records_written: list[dict[str, Any]] = []
        self.connected = False
        self.configured = False

    async def connect(self, config: dict[str, Any]) -> None:
        self.connected = True

    async def configure(self, config: dict[str, Any]) -> None:
        self.configured = True

    async def disconnect(self) -> None:
        self.connected = False

    async def health_check(self) -> bool:
        return self.connected

    async def write_batch(self, records: list[dict[str, Any]]) -> int:
        self.records_written.extend(records)
        return len(records)


@pytest.mark.unit
class TestStreamingEngine:
    """Test suite for StreamingEngine functionality."""

    @pytest.fixture
    def engine(self, temp_dir):
        """Create a StreamingEngine instance."""
        return StreamingEngine(
            pipeline_id="test-pipeline",
            runtime=_runtime(batch_size=10, max_concurrent_batches=2, buffer_size=100),
            dlq_path=temp_dir,
        )

    @pytest.fixture
    def sample_records(self):
        """Sample records for testing."""
        return [{"id": i, "name": f"Record {i}", "value": i * 10} for i in range(25)]

    @pytest.mark.asyncio
    async def test_initialization(self, engine):
        """Test proper initialization of StreamingEngine."""
        assert engine.pipeline_id == "test-pipeline"
        assert engine.batch_size == 10
        assert engine.max_concurrent_batches == 2
        assert engine.buffer_size == 100
        assert engine.data_transformer is not None
        assert engine.metrics is not None

    @pytest.mark.asyncio
    async def test_invalid_pipeline_configuration(self, engine):
        """Test error handling for invalid pipeline configuration."""
        invalid_config = {
            "pipeline_id": "test",
            # Missing required fields
        }

        with pytest.raises(ConfigurationError, match="No streams configured"):
            await engine.stream_data(invalid_config)

    @pytest.mark.asyncio
    async def test_no_streams_configuration(self, engine):
        """Test error handling when no streams are configured."""
        config = {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "version": "1.0",
            "source": {"connection_id": "test-src"},
            "destination": {"connection_id": "test-dst"},
            "runtime": {
                "buffer_size": 100,
                "batching": {"batch_size": 10, "max_concurrent_batches": 1},
                "logging": {"log_level": "DEBUG", "metrics_enabled": False},
                "error_handling": {
                    "strategy": "dlq",
                    "max_retries": 3,
                    "retry_delay": 1,
                },
            },
            "streams": {},  # Empty streams
        }

        with pytest.raises(ConfigurationError, match="No streams configured"):
            await engine.stream_data(config)

    def test_create_source_connector_missing_resolved_source(self, engine):
        """Test error handling when _resolved_source is missing."""
        config = {"type": "unknown"}

        with pytest.raises(ValueError, match="Missing _resolved_source"):
            engine._create_source_connector(config)

    def test_create_source_connector_returns_worker_readable(self, engine):
        """Connector code never runs in the engine process: every source —
        regardless of kind — is served by the worker-backed Readable.
        Registry resolution (connector_id -> package class, else the kind's
        generic) happens inside the spawned worker."""
        from unittest.mock import MagicMock

        from src.worker.readable import WorkerReadable

        fake_resolved_source = MagicMock()
        db = engine._create_source_connector({"_resolved_source": fake_resolved_source})
        api = engine._create_source_connector(
            {"_resolved_source": fake_resolved_source}
        )
        assert isinstance(db, WorkerReadable)
        assert isinstance(api, WorkerReadable)
        # One shared client object: per-read state lives in read_batches.
        assert db is api

    def test_create_source_connector_requires_resolved_source(self, engine):
        """A source config without its resolved_source is a configuration error."""
        with pytest.raises(ValueError, match="_resolved_source"):
            engine._create_source_connector({})

    def test_get_stream_name(self, engine):
        """Test stream name generation."""
        # With structured endpoint_ref
        config = {
            "source": {
                "endpoint_ref": {
                    "scope": "connector",
                    "connection_id": "wise",
                    "endpoint_id": "transfers",
                },
            },
        }
        result = engine._get_stream_name(config)
        assert result == "endpoint.connector:wise/transfers"

        # Without endpoint_ref, with pipeline ID
        config = {"pipeline_id": "test-pipeline"}
        result = engine._get_stream_name(config)
        assert result == "test-pipeline"

        # No identifiers
        config = {}
        result = engine._get_stream_name(config)
        assert result == "unknown-stream"

    def test_metrics_initialization(self, engine):
        """Test that metrics are properly initialized."""
        assert engine.metrics.records_processed == 0
        assert engine.metrics.records_failed == 0
        assert engine.metrics.batches_processed == 0
        assert engine.metrics.batches_failed == 0
        assert engine.metrics.streams_processed == 0
        assert engine.metrics.streams_failed == 0


@pytest.mark.unit
class TestEngineMetrics:
    """Test metrics tracking in StreamingEngine."""

    @pytest.fixture
    def engine(self, temp_dir):
        """Create a StreamingEngine instance."""
        return StreamingEngine(
            pipeline_id="metrics-test",
            runtime=_runtime(batch_size=5, max_concurrent_batches=2, buffer_size=100),
            dlq_path=temp_dir,
        )

    def test_initial_metrics(self, engine):
        """Test initial metrics state."""
        metrics = engine.get_metrics()

        assert metrics.records_processed == 0
        assert metrics.records_failed == 0
        assert metrics.batches_processed == 0
        assert metrics.batches_failed == 0
        assert metrics.streams_processed == 0
        assert metrics.streams_failed == 0

    def test_metrics_update(self, engine):
        """Test that metrics can be updated."""
        engine.metrics.records_processed = 100
        engine.metrics.batches_processed = 10
        engine.metrics.streams_processed = 1

        metrics = engine.get_metrics()

        assert metrics.records_processed == 100
        assert metrics.batches_processed == 10
        assert metrics.streams_processed == 1


@pytest.mark.unit
class TestEngineStateManager:
    """Test state manager integration in StreamingEngine."""

    @pytest.fixture
    def engine(self, temp_dir):
        """Create a StreamingEngine instance."""
        return StreamingEngine(
            pipeline_id="state-test",
            runtime=_runtime(batch_size=10, max_concurrent_batches=2, buffer_size=100),
            dlq_path=temp_dir,
        )

    def test_state_manager_exists(self, engine):
        """Test that state manager is initialized."""
        state_manager = engine.get_state_manager()
        assert state_manager is not None

    @pytest.mark.asyncio
    async def test_state_manager_operations(self, engine):
        """Test basic state manager operations through engine."""
        state_manager = engine.get_state_manager()

        # Test starting a run with config dict
        config = {"pipeline_id": "state-test", "version": "1.0"}
        run_id = state_manager.start_run(config)

        # run_id is returned directly as a string like "2025-12-27T08:21:13Z-ebaf"
        assert run_id is not None
        assert isinstance(run_id, str)
        assert "T" in run_id  # ISO timestamp format

        # Test getting run info
        run_info = state_manager.get_run_info()
        assert run_info is not None


@pytest.mark.unit
class TestEngineConfiguration:
    """Test engine configuration validation."""

    def test_engine_with_custom_params(self, temp_dir):
        """Test engine with custom parameters."""
        engine = StreamingEngine(
            pipeline_id="custom-config",
            runtime=_runtime(batch_size=50, max_concurrent_batches=5, buffer_size=500),
            dlq_path=temp_dir,
        )

        assert engine.batch_size == 50
        assert engine.max_concurrent_batches == 5
        assert engine.buffer_size == 500

    def test_engine_default_values(self, temp_dir):
        """A default RuntimeConfig carries the settings-sourced defaults."""
        engine = StreamingEngine(
            pipeline_id="defaults", runtime=RuntimeConfig(), dlq_path=temp_dir
        )

        assert engine.batch_size == 1000  # ANALITIQ_BATCH_SIZE default
        assert engine.max_concurrent_batches == 3  # ANALITIQ_MAX_CONCURRENT_BATCHES
        assert engine.buffer_size == 5000  # ANALITIQ_BUFFER_SIZE default

    def test_engine_with_custom_config_params(self, temp_dir):
        """Test engine with explicit config parameters."""
        engine = StreamingEngine(
            pipeline_id="with-config",
            runtime=_runtime(
                batch_size=200, max_concurrent_batches=4, buffer_size=2000
            ),
            dlq_path=temp_dir,
        )

        assert engine.batch_size == 200
        assert engine.max_concurrent_batches == 4
        assert engine.buffer_size == 2000
