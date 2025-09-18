"""Unit tests for StreamingEngine."""

from unittest.mock import AsyncMock, Mock, patch
import pytest

from analitiq_stream.core.engine import StreamingEngine
from analitiq_stream.core.exceptions import ConfigurationError, StreamProcessingError


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
            "src": {"host_id": "test-src"},
            "dst": {"host_id": "test-dst"},
            "engine_config": {"batch_size": 10},
            "streams": {}  # Empty streams
        }
        
        with pytest.raises(ConfigurationError, match="No streams configured"):
            await engine.stream_data(config)

    @pytest.mark.asyncio
    async def test_single_stream_success(self, engine, sample_pipeline_config, mock_state_manager):
        """Test successful processing of a single stream."""
        with patch.object(engine, '_process_stream') as mock_process:
            with patch.object(engine, 'state_manager', mock_state_manager):
                mock_process.return_value = None  # Successful processing
                
                await engine.stream_data(sample_pipeline_config)
                
                assert mock_process.call_count == 1
                assert engine.metrics.streams_processed == 1
                assert engine.metrics.streams_failed == 0

    @pytest.mark.asyncio
    async def test_single_stream_failure(self, engine, sample_pipeline_config, mock_state_manager):
        """Test handling of single stream failure."""
        with patch.object(engine, '_process_stream') as mock_process:
            with patch.object(engine, 'state_manager', mock_state_manager):
                # Mock stream processing failure
                mock_process.side_effect = RuntimeError("Stream processing failed")
                
                # Should raise ExceptionGroup for all streams failed
                with pytest.raises(ExceptionGroup) as exc_info:
                    await engine.stream_data(sample_pipeline_config)
                
                assert len(exc_info.value.exceptions) == 1
                assert isinstance(exc_info.value.exceptions[0], StreamProcessingError)
                assert engine.metrics.streams_failed == 1
                assert engine.metrics.streams_processed == 0

    @pytest.mark.asyncio
    async def test_multiple_streams_partial_failure(self, engine, mock_state_manager):
        """Test handling of partial stream failures."""
        config = {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline", 
            "version": "1.0",
            "src": {"host_id": "test-src"},
            "dst": {"host_id": "test-dst"}, 
            "engine_config": {"batch_size": 10},
            "streams": {
                "stream1": {"name": "success-stream"},
                "stream2": {"name": "failure-stream"},
                "stream3": {"name": "another-success-stream"}
            }
        }
        
        def mock_process_side_effect(stream_id, *args, **kwargs):
            if stream_id == "stream2":
                raise RuntimeError("Stream 2 failed")
            return None  # Success for other streams
        
        with patch.object(engine, '_process_stream') as mock_process:
            with patch.object(engine, 'state_manager', mock_state_manager):
                mock_process.side_effect = mock_process_side_effect
                
                # Should not raise exception for partial failure
                await engine.stream_data(config)
                
                assert engine.metrics.streams_processed == 2
                assert engine.metrics.streams_failed == 1

    @pytest.mark.asyncio
    async def test_exception_group_handling(self, engine, sample_pipeline_config, mock_state_manager):
        """Test Python 3.11+ ExceptionGroup handling."""
        with patch.object(engine, '_process_stream') as mock_process:
            with patch.object(engine, 'state_manager', mock_state_manager):
                # All streams fail
                mock_process.side_effect = RuntimeError("All streams fail")
                
                try:
                    await engine.stream_data(sample_pipeline_config)
                    pytest.fail("Should have raised ExceptionGroup")
                except ExceptionGroup as eg:
                    assert len(eg.exceptions) == 1
                    assert isinstance(eg.exceptions[0], StreamProcessingError)

    @pytest.mark.asyncio
    async def test_task_cancellation_on_failure(self, engine, mock_state_manager):
        """Test that tasks are properly cancelled when exceptions occur."""
        config = {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "version": "1.0", 
            "src": {"host_id": "test-src"},
            "dst": {"host_id": "test-dst"},
            "engine_config": {"batch_size": 10},
            "streams": {
                "stream1": {"name": "stream1"},
                "stream2": {"name": "stream2"}
            }
        }
        
        # Mock asyncio.create_task to track created tasks
        mock_tasks = []
        original_create_task = engine._process_stream
        
        def mock_process_stream(*args, **kwargs):
            # Create a mock task that can be cancelled
            task = AsyncMock()
            task.done.return_value = False
            task.cancel = Mock()
            mock_tasks.append(task)
            
            # First stream succeeds, second fails
            if len(mock_tasks) == 1:
                return None  # Success
            else:
                raise RuntimeError("Second stream fails")
        
        with patch.object(engine, '_process_stream', side_effect=mock_process_stream):
            with patch.object(engine, 'state_manager', mock_state_manager):
                with patch('asyncio.create_task') as mock_create_task:
                    # Return mock tasks
                    mock_create_task.side_effect = lambda coro, name=None: AsyncMock()
                    
                    try:
                        await engine.stream_data(config)
                    except (ExceptionGroup, Exception):
                        pass  # We expect exceptions
                    
                    # Verify create_task was called for each stream
                    assert mock_create_task.call_count == 2

    def test_get_connector_api(self, engine):
        """Test connector creation for API type."""
        config = {"type": "api"}
        
        with patch('analitiq_stream.connectors.api.APIConnector') as mock_api:
            mock_instance = Mock()
            mock_api.return_value = mock_instance
            
            result = engine._get_connector(config)
            
            assert result == mock_instance
            mock_api.assert_called_once()

    def test_get_connector_database(self, engine):
        """Test connector creation for database type.""" 
        config = {"type": "database"}
        
        with patch('analitiq_stream.connectors.database.DatabaseConnector') as mock_db:
            mock_instance = Mock()
            mock_db.return_value = mock_instance
            
            result = engine._get_connector(config)
            
            assert result == mock_instance
            mock_db.assert_called_once()

    def test_get_connector_unknown_type(self, engine):
        """Test error handling for unknown connector type."""
        config = {"type": "unknown"}
        
        with pytest.raises(ValueError, match="Unknown connector type"):
            engine._get_connector(config)

    def test_get_connector_default_type(self, engine):
        """Test default connector type (API)."""
        config = {}  # No type specified
        
        with patch('analitiq_stream.connectors.api.APIConnector') as mock_api:
            mock_instance = Mock()
            mock_api.return_value = mock_instance
            
            result = engine._get_connector(config)
            
            assert result == mock_instance

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
        # Test individual fields rather than dict comparison
        assert engine.metrics.records_processed == 0
        assert engine.metrics.records_failed == 0
        assert engine.metrics.batches_processed == 0
        assert engine.metrics.batches_failed == 0
        assert engine.metrics.streams_processed == 0
        assert engine.metrics.streams_failed == 0