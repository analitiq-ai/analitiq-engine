"""Unit tests for StreamingEngine - minimal mocking, real functionality focus."""

from typing import Any

import pytest

from src.engine.engine import StreamingEngine
from src.engine.exceptions import ConfigurationError
from src.models.resolved import BatchingConfig, RuntimeConfig


def _runtime(*, batch_size: int, buffer_size: int) -> RuntimeConfig:
    """Build a RuntimeConfig from the loose tuning values a test cares about."""
    return RuntimeConfig(
        batching=BatchingConfig(batch_size=batch_size),
        buffer_size=buffer_size,
    )


@pytest.mark.unit
class TestStreamingEngine:
    """Test suite for StreamingEngine functionality."""

    @pytest.fixture
    def engine(self, temp_dir, tmp_project_root):
        """Create a StreamingEngine instance."""
        return StreamingEngine(
            pipeline_id="test-pipeline",
            runtime=_runtime(batch_size=10, buffer_size=100),
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
        assert engine.buffer_size == 100
        assert engine.metrics is not None

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
                "batching": {"batch_size": 10},
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

    def _processor(self, engine, stream_config: dict[str, Any]):
        """Build a StreamProcessor wired the way engine._process_stream does."""
        from src.engine.mapping import MappingDocument
        from src.engine.stream_processor import StreamProcessor

        return StreamProcessor(
            stream_id="s1",
            stream_config=stream_config,
            mapping=MappingDocument(),
            pipeline_config={"pipeline_id": engine.pipeline_id},
            pipeline_id=engine.pipeline_id,
            state_manager=engine.state_manager,
            pipeline_metrics=engine.metrics,
            worker_readable=engine._worker_readable,
            dlq_root=engine.dlq_path,
            batch_size=engine.batch_size,
            buffer_size=engine.buffer_size,
            max_retries=engine.max_retries,
            retry_delay=engine.retry_delay,
            error_strategy=engine.error_strategy,
        )

    def test_resolve_source_readable_missing_resolved_source(self, engine):
        """A source config without its resolved_source is a configuration
        error, raised before any stage starts."""
        processor = self._processor(engine, {"name": "s"})

        with pytest.raises(ValueError, match="Missing _resolved_source"):
            processor._resolve_source_readable({"type": "unknown"})

    def test_resolve_source_readable_returns_worker_readable(self, engine):
        """Connector code never runs in the engine process: every source —
        regardless of kind — is served by the engine's shared worker-backed
        Readable. Registry resolution (connector_id -> package class, else
        the kind's generic) happens inside the spawned worker."""
        from unittest.mock import MagicMock

        from src.worker.readable import WorkerReadable

        fake_resolved_source = MagicMock()
        db = self._processor(engine, {"name": "db"})._resolve_source_readable(
            {"_resolved_source": fake_resolved_source}
        )
        api = self._processor(engine, {"name": "api"})._resolve_source_readable(
            {"_resolved_source": fake_resolved_source}
        )
        assert isinstance(db, WorkerReadable)
        assert isinstance(api, WorkerReadable)
        # One shared client object across streams: per-read state lives in
        # read_batches.
        assert db is api


@pytest.mark.unit
class TestEngineMetrics:
    """Test metrics tracking in StreamingEngine."""

    @pytest.fixture
    def engine(self, temp_dir, tmp_project_root):
        """Create a StreamingEngine instance."""
        return StreamingEngine(
            pipeline_id="metrics-test",
            runtime=_runtime(batch_size=5, buffer_size=100),
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


@pytest.mark.unit
class TestEngineStateManager:
    """Test state manager integration in StreamingEngine."""

    @pytest.fixture
    def engine(self, temp_dir, tmp_project_root):
        """Create a StreamingEngine instance."""
        return StreamingEngine(
            pipeline_id="state-test",
            runtime=_runtime(batch_size=10, buffer_size=100),
            dlq_path=temp_dir,
        )

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

    def test_engine_with_custom_params(self, temp_dir, tmp_project_root):
        """Test engine with custom parameters."""
        engine = StreamingEngine(
            pipeline_id="custom-config",
            runtime=_runtime(batch_size=50, buffer_size=500),
            dlq_path=temp_dir,
        )

        assert engine.batch_size == 50
        assert engine.buffer_size == 500

    def test_engine_default_values(self, temp_dir, tmp_project_root):
        """A default RuntimeConfig carries the settings-sourced defaults."""
        engine = StreamingEngine(
            pipeline_id="defaults", runtime=RuntimeConfig(), dlq_path=temp_dir
        )

        assert engine.batch_size == 1000  # ANALITIQ_BATCH_SIZE default
        assert engine.buffer_size == 5000  # ANALITIQ_BUFFER_SIZE default
