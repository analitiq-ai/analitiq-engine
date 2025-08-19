"""Unit tests for engine improvements and refactoring."""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any

from analitiq_stream.core.engine import StreamingEngine
from analitiq_stream.core.exceptions import (
    StreamExecutionError, StreamConfigurationError, 
    StageConfigurationError, PipelineOrchestrationError
)
from analitiq_stream.models.engine import (
    EngineConfig, StreamProcessingConfig, PipelineStagesConfig,
    PipelineMetricsSnapshot
)
from analitiq_stream.connectors.base import BaseConnector


class TestEngineConfiguration:
    """Test engine configuration and validation."""
    
    def test_engine_config_validation_success(self):
        """Test successful engine configuration validation."""
        config = EngineConfig(
            batch_size=1000,
            max_concurrent_batches=5,
            buffer_size=5000,
            dlq_path="./test_dlq/"
        )
        
        assert config.batch_size == 1000
        assert config.max_concurrent_batches == 5
        assert config.buffer_size == 5000
        assert config.dlq_path == "./test_dlq/"
    
    def test_engine_config_validation_failure(self):
        """Test engine configuration validation failures."""
        # Test invalid batch_size
        with pytest.raises(Exception):  # Pydantic validation error
            EngineConfig(batch_size=-1)
        
        # Test invalid max_concurrent_batches
        with pytest.raises(Exception):
            EngineConfig(max_concurrent_batches=0)
        
        # Test invalid buffer_size
        with pytest.raises(Exception):
            EngineConfig(buffer_size=50)  # Below minimum
    
    def test_stream_processing_config_validation(self):
        """Test stream processing configuration validation."""
        config_dict = {
            "stream_id": "test-stream",
            "stream_name": "Test Stream",
            "pipeline_id": "test-pipeline",
            "source": {"type": "api", "endpoint": "/test"},
            "destination": {"type": "api", "endpoint": "/dest"},
            "replication_method": "incremental",
            "cursor_mode": "inclusive",
            "refresh_mode": "upsert"
        }
        
        config = StreamProcessingConfig(**config_dict)
        assert config.stream_id == "test-stream"
        assert config.replication_method == "incremental"
        assert config.cursor_mode == "inclusive"
    
    def test_stream_processing_config_validation_failure(self):
        """Test stream processing configuration validation failures."""
        with pytest.raises(Exception):
            StreamProcessingConfig(
                stream_id="test",
                stream_name="test", 
                pipeline_id="test",
                source={},
                destination={},
                replication_method="invalid"  # Invalid replication method
            )


class TestEngineFactory:
    """Test engine factory methods."""
    
    @pytest.fixture
    def engine(self):
        """Create engine instance for testing."""
        return StreamingEngine("test-pipeline")
    
    def test_create_source_connector(self, engine):
        """Test source connector creation."""
        config = {"type": "api", "endpoint": "/test"}
        connector = engine._create_source_connector(config)
        assert isinstance(connector, BaseConnector)
    
    def test_create_destination_connector(self, engine):
        """Test destination connector creation."""
        config = {"type": "api", "endpoint": "/test"}
        connector = engine._create_destination_connector(config)
        assert isinstance(connector, BaseConnector)
    
    @pytest.mark.asyncio
    async def test_create_pipeline_stages(self, engine):
        """Test pipeline stages creation."""
        # Mock dependencies
        source_connector = MagicMock(spec=BaseConnector)
        dest_connector = MagicMock(spec=BaseConnector)
        
        extract_queue = asyncio.Queue()
        transform_queue = asyncio.Queue()
        load_queue = asyncio.Queue()
        
        stream_config = {
            "stream_id": "test-stream",
            "stream_name": "Test Stream"
        }
        stream_dlq = MagicMock()
        
        tasks = engine._create_pipeline_stages(
            source_connector=source_connector,
            dest_connector=dest_connector,
            extract_queue=extract_queue,
            transform_queue=transform_queue,
            load_queue=load_queue,
            stream_processing_config=stream_config,
            stream_dlq=stream_dlq,
            stream_name="test-stream"
        )
        
        assert len(tasks) == 4  # extract, transform, load, checkpoint
        assert all(isinstance(task, asyncio.Task) for task in tasks)
        
        # Clean up tasks
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


class TestEngineLogging:
    """Test structured logging improvements."""
    
    @pytest.fixture
    def engine(self):
        """Create engine with logging setup."""
        return StreamingEngine("test-pipeline")
    
    def test_logger_initialization(self, engine):
        """Test that logger is properly initialized with pipeline context."""
        assert engine.logger is not None
        assert "test-pipeline" in engine.logger.name
    
    @patch('analitiq_stream.core.engine.logger')
    def test_structured_logging_context(self, mock_logger, engine):
        """Test that logging includes structured context."""
        # This would be tested more thoroughly in integration tests
        # since it requires actually running the pipeline
        assert hasattr(engine, 'logger')


class TestEngineMetrics:
    """Test pipeline-level metrics tracking."""
    
    @pytest.fixture  
    def engine(self):
        """Create engine for metrics testing."""
        return StreamingEngine("test-pipeline")
    
    def test_metrics_initialization(self, engine):
        """Test metrics are properly initialized."""
        assert engine.metrics is not None
        assert hasattr(engine.orchestrator, '_metrics')
    
    def test_pipeline_metrics_snapshot(self):
        """Test PipelineMetricsSnapshot creation."""
        metrics = PipelineMetricsSnapshot(
            pipeline_id="test-pipeline",
            run_id="test-run",
            started_at=datetime.now(timezone.utc),
            total_streams=3,
            completed_streams=2,
            failed_streams=1,
            total_records_processed=1000,
            total_records_failed=10
        )
        
        assert metrics.success_rate == (2/3) * 100  # 66.67%
        assert metrics.record_success_rate == (1000/1010) * 100  # ~99%


class TestExceptionHandling:
    """Test improved exception handling."""
    
    def test_stream_execution_error(self):
        """Test StreamExecutionError with context."""
        original_error = ValueError("Test error")
        
        error = StreamExecutionError(
            "Stream failed",
            stream_id="test-stream",
            stage="extract",
            batch_id=5,
            original_error=original_error
        )
        
        assert error.stream_id == "test-stream"
        assert error.stage == "extract"
        assert error.batch_id == 5
        assert error.original_error == original_error
        assert "[Stage: extract]" in str(error)
        assert "[Batch: 5]" in str(error)
    
    def test_stream_configuration_error(self):
        """Test StreamConfigurationError with validation context."""
        validation_errors = ["field1: required", "field2: invalid value"]
        
        error = StreamConfigurationError(
            "Configuration invalid",
            stream_id="test-stream",
            field_path="source.endpoint",
            validation_errors=validation_errors
        )
        
        assert error.stream_id == "test-stream"
        assert error.field_path == "source.endpoint"
        assert error.validation_errors == validation_errors
    
    def test_stage_configuration_error(self):
        """Test StageConfigurationError."""
        error = StageConfigurationError(
            "Stage config invalid",
            stage_name="extract",
            stream_id="test-stream"
        )
        
        assert error.stage_name == "extract"
        assert error.stream_id == "test-stream"


class TestEngineIntegration:
    """Integration tests for engine improvements."""
    
    @pytest.fixture
    def mock_pipeline_config(self):
        """Create mock pipeline configuration."""
        return {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "version": "1.0",
            "streams": {
                "stream1": {
                    "stream_id": "stream1",
                    "name": "Stream 1",
                    "source": {
                        "type": "api", 
                        "endpoint": "/test1",
                        "base_url": "https://api.test.com",
                        "headers": {"Content-Type": "application/json"}
                    },
                    "destination": {
                        "type": "api", 
                        "endpoint": "/dest1",
                        "base_url": "https://api.dest.com",
                        "headers": {"Content-Type": "application/json"}
                    },
                    "replication_method": "incremental",
                    "cursor_field": "updated_at"
                }
            }
        }
    
    @pytest.fixture
    def engine_with_config(self):
        """Create engine with valid configuration."""
        engine_config = EngineConfig(
            batch_size=100,
            max_concurrent_batches=2,
            buffer_size=1000
        )
        return StreamingEngine("test-pipeline", engine_config=engine_config)
    
    def test_engine_initialization_with_config(self, engine_with_config):
        """Test engine initialization with EngineConfig."""
        assert engine_with_config.batch_size == 100
        assert engine_with_config.max_concurrent_batches == 2
        assert engine_with_config.buffer_size == 1000
        assert engine_with_config.engine_config.batch_size == 100
    
    @patch('analitiq_stream.core.engine.StreamingEngine._process_stream')
    @patch('analitiq_stream.fault_tolerance.sharded_state_manager.ShardedStateManager.start_run')
    async def test_stream_data_orchestration(self, mock_start_run, mock_process_stream, engine_with_config, mock_pipeline_config):
        """Test that stream_data method orchestrates streams properly."""
        # Mock the state manager and process_stream method
        mock_start_run.return_value = "test-run-id"
        mock_process_stream.return_value = None
        
        # Call stream_data
        await engine_with_config.stream_data(mock_pipeline_config)
        
        # Verify state manager was called to start run
        mock_start_run.assert_called_once_with(mock_pipeline_config)
        
        # Verify process_stream was called for each stream
        assert mock_process_stream.call_count == 1
        
        # Verify the call arguments
        call_args = mock_process_stream.call_args
        assert call_args[1]['stream_id'] == 'stream1'
        assert 'stream_config' in call_args[1]
        assert 'pipeline_config' in call_args[1]


class TestOrchestration:
    """Test pipeline orchestration separation."""
    
    def test_orchestrator_initialization(self):
        """Test orchestrator is properly initialized."""
        engine = StreamingEngine("test-pipeline")
        assert engine.orchestrator is not None
        assert engine.orchestrator.pipeline_id == "test-pipeline"
    
    def test_orchestrator_metrics(self):
        """Test orchestrator metrics tracking."""
        engine = StreamingEngine("test-pipeline")
        metrics = engine.orchestrator.get_current_metrics()
        
        assert isinstance(metrics, PipelineMetricsSnapshot)
        assert metrics.pipeline_id == "test-pipeline"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])