"""Unit tests for StreamingEngine - minimal mocking, real functionality focus."""

import asyncio
from typing import Any, Dict, List

import pytest

from src.engine.engine import StreamingEngine, _deep_merge_dicts
from src.engine.exceptions import ConfigurationError, StreamProcessingError
from src.source.connectors.base import BaseConnector


class InMemorySourceConnector(BaseConnector):
    """Real in-memory source connector for testing."""

    def __init__(self, records: List[Dict[str, Any]] = None):
        super().__init__()
        self.records = records or []
        self.connected = False
        self.batch_size = 10

    async def connect(self, config: Dict[str, Any]) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def health_check(self) -> bool:
        return self.connected

    async def read_batch(self, batch_size: int = None) -> List[Dict[str, Any]]:
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
        self.records_written: List[Dict[str, Any]] = []
        self.connected = False
        self.configured = False

    async def connect(self, config: Dict[str, Any]) -> None:
        self.connected = True

    async def configure(self, config: Dict[str, Any]) -> None:
        self.configured = True

    async def disconnect(self) -> None:
        self.connected = False

    async def health_check(self) -> bool:
        return self.connected

    async def write_batch(self, records: List[Dict[str, Any]]) -> int:
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
            batch_size=10,
            max_concurrent_batches=2,
            buffer_size=100,
            dlq_path=temp_dir
        )

    @pytest.fixture
    def sample_records(self):
        """Sample records for testing."""
        return [
            {"id": i, "name": f"Record {i}", "value": i * 10}
            for i in range(25)
        ]

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

        with pytest.raises(ConfigurationError, match="Invalid pipeline configuration"):
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
            "engine_config": {"batch_size": 10},
            "streams": {}  # Empty streams
        }

        with pytest.raises(ConfigurationError, match="No streams configured"):
            await engine.stream_data(config)

    def test_get_connector_unknown_type(self, engine):
        """Test error handling for missing connector_id."""
        config = {"type": "unknown"}

        with pytest.raises(ValueError, match="missing 'connector_id'"):
            engine._get_connector(config)

    def test_get_stream_name(self, engine):
        """Test stream name generation."""
        # With endpoint ID
        config = {"source": {"endpoint_id": "test-endpoint-123"}}
        result = engine._get_stream_name(config)
        assert result == "endpoint.test-endpoint-123"

        # Without endpoint ID, with pipeline ID
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
class TestDeepMergeDicts:
    """Test _deep_merge_dicts utility function."""

    def test_simple_merge(self):
        """Test merging two simple dicts."""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}

        result = _deep_merge_dicts(base, override)

        assert result == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self):
        """Test merging nested dicts."""
        base = {
            "level1": {
                "level2a": {"key": "base_value"},
                "level2b": "keep_this"
            }
        }
        override = {
            "level1": {
                "level2a": {"key": "override_value", "new_key": "new_value"}
            }
        }

        result = _deep_merge_dicts(base, override)

        assert result["level1"]["level2a"]["key"] == "override_value"
        assert result["level1"]["level2a"]["new_key"] == "new_value"
        assert result["level1"]["level2b"] == "keep_this"

    def test_override_with_non_dict(self):
        """Test that non-dict values override correctly."""
        base = {"a": {"nested": "value"}}
        override = {"a": "simple_value"}

        result = _deep_merge_dicts(base, override)

        assert result["a"] == "simple_value"

    def test_empty_base(self):
        """Test merging with empty base."""
        base = {}
        override = {"a": 1, "b": {"c": 2}}

        result = _deep_merge_dicts(base, override)

        assert result == override

    def test_empty_override(self):
        """Test merging with empty override."""
        base = {"a": 1, "b": {"c": 2}}
        override = {}

        result = _deep_merge_dicts(base, override)

        assert result == base

    def test_credentials_merge_pattern(self):
        """Test the actual credential merging pattern used in pipelines."""
        pipeline_source = {
            "type": "api",
            "host": "https://root.example.com",
            "headers": {
                "Authorization": "Bearer root-token",
                "User-Agent": "root-agent",
                "nested": {"base": True},
            },
            "auth": {"token": "root-token"},
        }

        stream_source = {
            "endpoint_id": "endpoint-123",
            "headers": {
                "User-Agent": "stream-agent",
                "X-Stream": "value",
                "nested": {"override": True},
            },
            "pagination": {"type": "page"},
        }

        result = _deep_merge_dicts(pipeline_source, stream_source)

        # Base values preserved
        assert result["type"] == "api"
        assert result["host"] == "https://root.example.com"
        assert result["auth"]["token"] == "root-token"

        # Overrides applied
        assert result["headers"]["User-Agent"] == "stream-agent"
        assert result["headers"]["X-Stream"] == "value"

        # Deep merge of nested
        assert result["headers"]["nested"]["base"] is True
        assert result["headers"]["nested"]["override"] is True

        # New values added
        assert result["endpoint_id"] == "endpoint-123"
        assert result["pagination"]["type"] == "page"


@pytest.mark.unit
class TestEngineMetrics:
    """Test metrics tracking in StreamingEngine."""

    @pytest.fixture
    def engine(self, temp_dir):
        """Create a StreamingEngine instance."""
        return StreamingEngine(
            pipeline_id="metrics-test",
            batch_size=5,
            max_concurrent_batches=2,
            buffer_size=100,  # Must be >= 100
            dlq_path=temp_dir
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
            batch_size=10,
            max_concurrent_batches=2,
            buffer_size=100,
            dlq_path=temp_dir
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
            batch_size=50,
            max_concurrent_batches=5,
            buffer_size=500,
            dlq_path=temp_dir
        )

        assert engine.batch_size == 50
        assert engine.max_concurrent_batches == 5
        assert engine.buffer_size == 500

    def test_engine_default_values(self, temp_dir):
        """Test engine with default values."""
        engine = StreamingEngine(
            pipeline_id="defaults",
            dlq_path=temp_dir
        )

        assert engine.batch_size == 1000  # Default
        assert engine.max_concurrent_batches == 10  # Default
        assert engine.buffer_size == 10000  # Default

    def test_engine_with_custom_config_params(self, temp_dir):
        """Test engine with explicit config parameters."""
        engine = StreamingEngine(
            pipeline_id="with-config",
            batch_size=200,
            max_concurrent_batches=4,
            buffer_size=2000,
            dlq_path=temp_dir,
        )

        assert engine.batch_size == 200
        assert engine.max_concurrent_batches == 4
        assert engine.buffer_size == 2000
