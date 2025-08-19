"""Unit tests for pipeline orchestrator."""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any

from analitiq_stream.core.orchestrator import PipelineOrchestrator
from analitiq_stream.core.exceptions import (
    PipelineOrchestrationError, StreamExecutionError, 
    PipelineValidationError, StreamConfigurationError
)
from analitiq_stream.models.engine import (
    StreamProcessingConfig, PipelineMetricsSnapshot, TaskExecutionInfo
)


class TestPipelineOrchestrator:
    """Test pipeline orchestration functionality."""
    
    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator instance for testing."""
        return PipelineOrchestrator("test-pipeline")
    
    def test_orchestrator_initialization(self, orchestrator):
        """Test orchestrator proper initialization."""
        assert orchestrator.pipeline_id == "test-pipeline"
        assert orchestrator.run_id is not None
        assert orchestrator.started_at is not None
        assert isinstance(orchestrator._metrics, PipelineMetricsSnapshot)
        assert orchestrator._metrics.pipeline_id == "test-pipeline"
    
    def test_get_current_metrics(self, orchestrator):
        """Test getting current metrics snapshot."""
        metrics = orchestrator.get_current_metrics()
        
        assert isinstance(metrics, PipelineMetricsSnapshot)
        assert metrics.pipeline_id == "test-pipeline"
        assert metrics.run_id == orchestrator.run_id
    
    def test_get_active_tasks(self, orchestrator):
        """Test getting active tasks information."""
        # Initially no active tasks
        active_tasks = orchestrator.get_active_tasks()
        assert isinstance(active_tasks, dict)
        assert len(active_tasks) == 0


class TestConfigurationValidation:
    """Test pipeline configuration validation."""
    
    @pytest.fixture
    def orchestrator(self):
        return PipelineOrchestrator("test-pipeline")
    
    def test_validate_pipeline_config_success(self, orchestrator):
        """Test successful pipeline configuration validation."""
        valid_config = {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "version": "1.0",
            "streams": {
                "stream1": {
                    "name": "Stream 1",
                    "source": {"type": "api"},
                    "destination": {"type": "api"}
                }
            }
        }
        
        result = orchestrator._validate_pipeline_config(valid_config)
        assert result.pipeline_id == "test-pipeline"
        assert result.name == "Test Pipeline"
    
    def test_validate_pipeline_config_failure(self, orchestrator):
        """Test pipeline configuration validation failure."""
        invalid_config = {
            "name": "Missing pipeline_id",
            "streams": {}
        }
        
        with pytest.raises(PipelineValidationError) as exc_info:
            orchestrator._validate_pipeline_config(invalid_config)
        
        error = exc_info.value
        assert "Pipeline configuration validation failed" in str(error)
        assert isinstance(error.errors, dict)


class TestStreamProcessingConfig:
    """Test stream processing configuration building."""
    
    @pytest.fixture
    def orchestrator(self):
        return PipelineOrchestrator("test-pipeline")
    
    def test_build_stream_processing_config_success(self, orchestrator):
        """Test successful stream processing config building."""
        stream_config = {
            "name": "Test Stream",
            "source": {"type": "api", "endpoint": "/test"},
            "destination": {"type": "api", "endpoint": "/dest"},
            "replication_method": "incremental",
            "cursor_mode": "inclusive",
            "refresh_mode": "upsert"
        }
        
        pipeline_config = {"pipeline_id": "test-pipeline"}
        
        result = orchestrator._build_stream_processing_config(
            "stream1", stream_config, pipeline_config
        )
        
        assert isinstance(result, StreamProcessingConfig)
        assert result.stream_id == "stream1"
        assert result.stream_name == "Test Stream"
        assert result.pipeline_id == "test-pipeline"
    
    def test_build_stream_processing_config_failure(self, orchestrator):
        """Test stream processing config building failure."""
        invalid_stream_config = {
            "name": "Test Stream",
            "source": {},
            "destination": {},
            "replication_method": "invalid_method"  # Invalid
        }
        
        pipeline_config = {"pipeline_id": "test-pipeline"}
        
        with pytest.raises(StreamConfigurationError) as exc_info:
            orchestrator._build_stream_processing_config(
                "stream1", invalid_stream_config, pipeline_config
            )
        
        error = exc_info.value
        assert error.stream_id == "stream1"
        assert "Stream configuration validation failed" in str(error)


class TestStreamTaskCreation:
    """Test stream task creation and management."""
    
    @pytest.fixture
    def orchestrator(self):
        return PipelineOrchestrator("test-pipeline")
    
    @pytest.fixture
    def mock_stream_processor_factory(self):
        """Mock stream processor factory."""
        async def mock_factory(config):
            await asyncio.sleep(0.01)  # Simulate work
            return f"processed-{config.stream_id}"
        
        return mock_factory
    
    def test_task_execution_info_model(self):
        """Test TaskExecutionInfo model validation."""
        task_info = TaskExecutionInfo(
            stream_id="test-stream",
            stream_name="Test Stream", 
            task_name="extract-test",
            started_at=datetime.now(timezone.utc),
            status="running"
        )
        
        assert task_info.stream_id == "test-stream"
        assert task_info.status == "running"
    
    def test_task_execution_info_invalid_status(self):
        """Test TaskExecutionInfo with invalid status."""
        with pytest.raises(Exception):  # Pydantic validation error
            TaskExecutionInfo(
                stream_id="test-stream",
                stream_name="Test Stream",
                task_name="extract-test", 
                started_at=datetime.now(timezone.utc),
                status="invalid_status"  # Invalid status
            )
    
    @pytest.mark.asyncio
    async def test_create_stream_tasks(self, orchestrator, mock_stream_processor_factory):
        """Test stream task creation."""
        streams = {
            "stream1": {
                "name": "Stream 1",
                "source": {"type": "api"},
                "destination": {"type": "api"},
                "replication_method": "incremental",
                "cursor_mode": "inclusive",
                "refresh_mode": "upsert"
            }
        }
        
        pipeline_config = {"pipeline_id": "test-pipeline"}
        
        stream_tasks = await orchestrator._create_stream_tasks(
            streams, pipeline_config, mock_stream_processor_factory
        )
        
        assert len(stream_tasks) == 1
        stream_id, stream_name, task = stream_tasks[0]
        assert stream_id == "stream1"
        assert stream_name == "Stream 1"
        assert isinstance(task, asyncio.Task)
        
        # Verify task is tracked
        assert "stream1" in orchestrator.active_tasks
        task_info = orchestrator.active_tasks["stream1"]
        assert task_info.stream_id == "stream1"
        
        # Clean up
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestConcurrentExecution:
    """Test concurrent stream execution."""
    
    @pytest.fixture
    def orchestrator(self):
        return PipelineOrchestrator("test-pipeline")
    
    @pytest.mark.asyncio
    async def test_execute_streams_concurrently_success(self, orchestrator):
        """Test successful concurrent execution."""
        async def mock_stream_task():
            await asyncio.sleep(0.01)
            return "success"
        
        # Create mock tasks
        task1 = asyncio.create_task(mock_stream_task(), name="stream1")
        task2 = asyncio.create_task(mock_stream_task(), name="stream2")
        
        # Setup active tasks
        orchestrator.active_tasks["stream1"] = TaskExecutionInfo(
            stream_id="stream1",
            stream_name="Stream 1",
            task_name="stream1",
            started_at=datetime.now(timezone.utc),
            status="pending"
        )
        orchestrator.active_tasks["stream2"] = TaskExecutionInfo(
            stream_id="stream2", 
            stream_name="Stream 2",
            task_name="stream2",
            started_at=datetime.now(timezone.utc),
            status="pending"
        )
        
        stream_tasks = [
            ("stream1", "Stream 1", task1),
            ("stream2", "Stream 2", task2)
        ]
        
        results = await orchestrator._execute_streams_concurrently(stream_tasks)
        
        assert len(results) == 2
        assert all(result == "success" for result in results)
    
    @pytest.mark.asyncio
    async def test_execute_streams_concurrently_with_failure(self, orchestrator):
        """Test concurrent execution with some failures."""
        async def success_task():
            await asyncio.sleep(0.01)
            return "success"
        
        async def failure_task():
            await asyncio.sleep(0.01)
            raise ValueError("Task failed")
        
        # Create mock tasks
        task1 = asyncio.create_task(success_task(), name="stream1")
        task2 = asyncio.create_task(failure_task(), name="stream2")
        
        # Setup active tasks
        orchestrator.active_tasks["stream1"] = TaskExecutionInfo(
            stream_id="stream1",
            stream_name="Stream 1", 
            task_name="stream1",
            started_at=datetime.now(timezone.utc),
            status="pending"
        )
        orchestrator.active_tasks["stream2"] = TaskExecutionInfo(
            stream_id="stream2",
            stream_name="Stream 2",
            task_name="stream2", 
            started_at=datetime.now(timezone.utc),
            status="pending"
        )
        
        stream_tasks = [
            ("stream1", "Stream 1", task1),
            ("stream2", "Stream 2", task2)
        ]
        
        results = await orchestrator._execute_streams_concurrently(stream_tasks)
        
        assert len(results) == 2
        assert results[0] == "success"
        assert isinstance(results[1], ValueError)


class TestResultProcessing:
    """Test execution result processing."""
    
    @pytest.fixture
    def orchestrator(self):
        return PipelineOrchestrator("test-pipeline")
    
    def test_process_execution_results_all_success(self, orchestrator):
        """Test processing results when all streams succeed."""
        # Setup tasks and results
        task1 = MagicMock()
        task2 = MagicMock()
        
        stream_tasks = [
            ("stream1", "Stream 1", task1),
            ("stream2", "Stream 2", task2)
        ]
        
        results = ["success1", "success2"]
        
        # Setup active tasks
        orchestrator.active_tasks["stream1"] = TaskExecutionInfo(
            stream_id="stream1",
            stream_name="Stream 1",
            task_name="stream1",
            started_at=datetime.now(timezone.utc),
            status="running"
        )
        orchestrator.active_tasks["stream2"] = TaskExecutionInfo(
            stream_id="stream2",
            stream_name="Stream 2", 
            task_name="stream2",
            started_at=datetime.now(timezone.utc),
            status="running"
        )
        
        # Process results
        orchestrator._process_execution_results(stream_tasks, results)
        
        # Verify metrics updated
        assert orchestrator._metrics.completed_streams == 2
        assert orchestrator._metrics.failed_streams == 0
        
        # Verify tasks moved to completed
        assert len(orchestrator.completed_tasks) == 2
        assert len(orchestrator.failed_tasks) == 0
        assert len(orchestrator.active_tasks) == 0
    
    def test_process_execution_results_partial_failure(self, orchestrator):
        """Test processing results with partial failures."""
        # Setup tasks and results
        task1 = MagicMock()
        task2 = MagicMock()
        
        stream_tasks = [
            ("stream1", "Stream 1", task1),
            ("stream2", "Stream 2", task2)
        ]
        
        results = ["success", ValueError("Stream 2 failed")]
        
        # Setup active tasks
        orchestrator.active_tasks["stream1"] = TaskExecutionInfo(
            stream_id="stream1",
            stream_name="Stream 1",
            task_name="stream1",
            started_at=datetime.now(timezone.utc),
            status="running"
        )
        orchestrator.active_tasks["stream2"] = TaskExecutionInfo(
            stream_id="stream2",
            stream_name="Stream 2",
            task_name="stream2",
            started_at=datetime.now(timezone.utc), 
            status="running"
        )
        
        # Process results (should not raise exception for partial failure)
        orchestrator._process_execution_results(stream_tasks, results)
        
        # Verify metrics
        assert orchestrator._metrics.completed_streams == 1
        assert orchestrator._metrics.failed_streams == 1
        
        # Verify task distribution
        assert len(orchestrator.completed_tasks) == 1
        assert len(orchestrator.failed_tasks) == 1
        assert len(orchestrator.active_tasks) == 0
    
    def test_process_execution_results_all_failure(self, orchestrator):
        """Test processing results when all streams fail."""
        task1 = MagicMock()
        task2 = MagicMock()
        
        stream_tasks = [
            ("stream1", "Stream 1", task1),
            ("stream2", "Stream 2", task2)
        ]
        
        results = [ValueError("Stream 1 failed"), RuntimeError("Stream 2 failed")]
        
        # Setup active tasks
        orchestrator.active_tasks["stream1"] = TaskExecutionInfo(
            stream_id="stream1",
            stream_name="Stream 1",
            task_name="stream1", 
            started_at=datetime.now(timezone.utc),
            status="running"
        )
        orchestrator.active_tasks["stream2"] = TaskExecutionInfo(
            stream_id="stream2",
            stream_name="Stream 2",
            task_name="stream2",
            started_at=datetime.now(timezone.utc),
            status="running"
        )
        
        # Should raise ExceptionGroup for total failure
        with pytest.raises(ExceptionGroup) as exc_info:
            orchestrator._process_execution_results(stream_tasks, results)
        
        assert "All streams failed" in str(exc_info.value)
        assert len(exc_info.value.exceptions) == 2


class TestMetricsCalculation:
    """Test metrics calculation and aggregation."""
    
    @pytest.fixture
    def orchestrator(self):
        return PipelineOrchestrator("test-pipeline")
    
    def test_calculate_final_metrics(self, orchestrator):
        """Test final metrics calculation."""
        # Setup some initial metrics
        orchestrator._metrics.total_records_processed = 1000
        orchestrator._metrics.total_batches_processed = 10
        
        # Calculate final metrics
        orchestrator._calculate_final_metrics()
        
        # Verify calculations
        assert orchestrator._metrics.average_batch_size == 100.0  # 1000/10
        assert orchestrator._metrics.records_per_second >= 0  # Should be calculated
    
    def test_pipeline_metrics_properties(self):
        """Test PipelineMetricsSnapshot properties."""
        metrics = PipelineMetricsSnapshot(
            pipeline_id="test",
            run_id="run1",
            started_at=datetime.now(timezone.utc),
            total_streams=4,
            completed_streams=3,
            failed_streams=1,
            total_records_processed=900,
            total_records_failed=100
        )
        
        # Test success rate calculations
        assert metrics.success_rate == 75.0  # 3/4 * 100
        assert metrics.record_success_rate == 90.0  # 900/1000 * 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])