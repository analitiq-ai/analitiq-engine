"""Unit tests for core exceptions module."""

import pytest

from src.core.exceptions import (
    StreamProcessingError,
    TransformationError,
    ConnectorError,
    ConfigurationError,
    StreamConfigurationError,
    PipelineValidationError,
    StreamExecutionError,
    PipelineOrchestrationError,
    StageConfigurationError
)


class TestStreamProcessingError:
    """Test StreamProcessingError exception."""
    
    def test_basic_exception(self):
        """Test basic exception creation and message."""
        error = StreamProcessingError("Test error")
        
        assert str(error) == "Test error"
        assert error.stream_id is None
        assert error.original_error is None
    
    def test_exception_with_stream_id(self):
        """Test exception with stream ID."""
        error = StreamProcessingError("Test error", stream_id="stream_123")
        
        assert str(error) == "Stream stream_123: Test error"
        assert error.stream_id == "stream_123"
    
    def test_exception_with_original_error(self):
        """Test exception with original error."""
        original = ValueError("Original error")
        error = StreamProcessingError("Test error", original_error=original)
        
        assert str(error) == "Test error (caused by: Original error)"
        assert error.original_error is original
    
    def test_exception_with_all_fields(self):
        """Test exception with all fields."""
        original = ValueError("Original error")
        error = StreamProcessingError(
            "Test error", 
            stream_id="stream_123", 
            original_error=original
        )
        
        expected = "Stream stream_123: Test error (caused by: Original error)"
        assert str(error) == expected
        assert error.stream_id == "stream_123"
        assert error.original_error is original
    
    def test_exception_inheritance(self):
        """Test that StreamProcessingError inherits from Exception."""
        error = StreamProcessingError("Test error")
        
        assert isinstance(error, Exception)
        assert isinstance(error, StreamProcessingError)


class TestTransformationError:
    """Test TransformationError exception."""
    
    def test_inheritance(self):
        """Test TransformationError inheritance."""
        error = TransformationError("Transformation failed")
        
        assert isinstance(error, StreamProcessingError)
        assert isinstance(error, TransformationError)
    
    def test_with_context(self):
        """Test TransformationError with context."""
        original = TypeError("Invalid type")
        error = TransformationError(
            "Field transformation failed",
            stream_id="transform_stream",
            original_error=original
        )
        
        expected = "Stream transform_stream: Field transformation failed (caused by: Invalid type)"
        assert str(error) == expected


class TestConnectorError:
    """Test ConnectorError exception."""
    
    def test_inheritance(self):
        """Test ConnectorError inheritance."""
        error = ConnectorError("Connector failed")
        
        assert isinstance(error, StreamProcessingError)
        assert isinstance(error, ConnectorError)
    
    def test_with_context(self):
        """Test ConnectorError with context."""
        original = ConnectionError("Connection refused")
        error = ConnectorError(
            "Database connection failed",
            stream_id="db_stream",
            original_error=original
        )
        
        expected = "Stream db_stream: Database connection failed (caused by: Connection refused)"
        assert str(error) == expected


class TestConfigurationError:
    """Test ConfigurationError exception."""
    
    def test_basic_exception(self):
        """Test basic configuration error."""
        error = ConfigurationError("Invalid configuration")
        
        assert str(error) == "Invalid configuration"
        assert error.field_path is None
        assert error.validation_errors == []
    
    def test_with_field_path(self):
        """Test configuration error with field path."""
        error = ConfigurationError(
            "Invalid value",
            field_path="pipeline.source.config"
        )
        
        assert str(error) == "Invalid value"
        assert error.field_path == "pipeline.source.config"
    
    def test_with_validation_errors(self):
        """Test configuration error with validation errors."""
        validation_errors = ["Field 'host' is required", "Port must be positive"]
        error = ConfigurationError(
            "Validation failed",
            validation_errors=validation_errors
        )
        
        assert str(error) == "Validation failed"
        assert error.validation_errors == validation_errors
    
    def test_with_all_fields(self):
        """Test configuration error with all fields."""
        validation_errors = ["Required field missing"]
        error = ConfigurationError(
            "Configuration invalid",
            field_path="source.database",
            validation_errors=validation_errors
        )
        
        assert str(error) == "Configuration invalid"
        assert error.field_path == "source.database"
        assert error.validation_errors == validation_errors


class TestStreamConfigurationError:
    """Test StreamConfigurationError exception."""
    
    def test_inheritance(self):
        """Test StreamConfigurationError inheritance."""
        error = StreamConfigurationError("Stream config error")
        
        assert isinstance(error, ConfigurationError)
        assert isinstance(error, StreamConfigurationError)
    
    def test_with_stream_id(self):
        """Test stream configuration error with stream ID."""
        error = StreamConfigurationError(
            "Invalid stream configuration",
            stream_id="stream_456"
        )
        
        assert str(error) == "Invalid stream configuration"
        assert error.stream_id == "stream_456"
    
    def test_with_all_fields(self):
        """Test stream configuration error with all fields."""
        validation_errors = ["Missing required field"]
        error = StreamConfigurationError(
            "Stream validation failed",
            stream_id="stream_789",
            field_path="stream.source.config",
            validation_errors=validation_errors
        )
        
        assert str(error) == "Stream validation failed"
        assert error.stream_id == "stream_789"
        assert error.field_path == "stream.source.config"
        assert error.validation_errors == validation_errors


class TestPipelineValidationError:
    """Test PipelineValidationError exception."""
    
    def test_inheritance(self):
        """Test PipelineValidationError inheritance."""
        error = PipelineValidationError("Pipeline validation failed")
        
        assert isinstance(error, ConfigurationError)
        assert isinstance(error, PipelineValidationError)
    
    def test_basic_error(self):
        """Test basic pipeline validation error."""
        error = PipelineValidationError("Validation failed")
        
        assert str(error) == "Validation failed"
        assert error.errors == {}
    
    def test_with_errors_dict(self):
        """Test pipeline validation error with errors dictionary."""
        errors = {
            "source": ["Missing host", "Invalid port"],
            "destination": ["Missing credentials"]
        }
        error = PipelineValidationError("Multiple validation errors", errors=errors)
        
        assert str(error) == "Multiple validation errors"
        assert error.errors == errors


class TestStreamExecutionError:
    """Test StreamExecutionError exception."""
    
    def test_inheritance(self):
        """Test StreamExecutionError inheritance."""
        error = StreamExecutionError("Execution failed")
        
        assert isinstance(error, StreamProcessingError)
        assert isinstance(error, StreamExecutionError)
    
    def test_with_stage(self):
        """Test stream execution error with stage."""
        error = StreamExecutionError(
            "Processing failed",
            stream_id="exec_stream",
            stage="transform"
        )
        
        assert str(error) == "Stream exec_stream: Processing failed [Stage: transform]"
        assert error.stage == "transform"
    
    def test_with_batch_id(self):
        """Test stream execution error with batch ID."""
        error = StreamExecutionError(
            "Batch processing failed",
            stream_id="batch_stream",
            batch_id=42
        )
        
        assert str(error) == "Stream batch_stream: Batch processing failed [Batch: 42]"
        assert error.batch_id == 42
    
    def test_with_all_fields(self):
        """Test stream execution error with all fields."""
        original = RuntimeError("Underlying error")
        error = StreamExecutionError(
            "Execution failed",
            stream_id="full_stream",
            stage="load",
            batch_id=123,
            original_error=original
        )
        
        expected = "Stream full_stream: Execution failed (caused by: Underlying error) [Stage: load] [Batch: 123]"
        assert str(error) == expected
        assert error.stage == "load"
        assert error.batch_id == 123
        assert error.original_error is original
    
    def test_batch_id_zero(self):
        """Test stream execution error with batch ID zero."""
        error = StreamExecutionError(
            "First batch failed",
            batch_id=0
        )
        
        assert str(error) == "First batch failed [Batch: 0]"
        assert error.batch_id == 0


class TestPipelineOrchestrationError:
    """Test PipelineOrchestrationError exception."""
    
    def test_basic_error(self):
        """Test basic pipeline orchestration error."""
        error = PipelineOrchestrationError("Orchestration failed")
        
        assert str(error) == "Orchestration failed"
        assert error.pipeline_id is None
        assert error.failed_streams == []
    
    def test_with_pipeline_id(self):
        """Test orchestration error with pipeline ID."""
        error = PipelineOrchestrationError(
            "Pipeline execution failed",
            pipeline_id="pipeline_123"
        )
        
        assert str(error) == "Pipeline execution failed"
        assert error.pipeline_id == "pipeline_123"
    
    def test_with_failed_streams(self):
        """Test orchestration error with failed streams."""
        failed_streams = ["stream_1", "stream_2", "stream_3"]
        error = PipelineOrchestrationError(
            "Multiple streams failed",
            failed_streams=failed_streams
        )
        
        assert str(error) == "Multiple streams failed"
        assert error.failed_streams == failed_streams
    
    def test_with_all_fields(self):
        """Test orchestration error with all fields."""
        failed_streams = ["stream_a", "stream_b"]
        error = PipelineOrchestrationError(
            "Pipeline failed with multiple stream failures",
            pipeline_id="complex_pipeline",
            failed_streams=failed_streams
        )
        
        assert str(error) == "Pipeline failed with multiple stream failures"
        assert error.pipeline_id == "complex_pipeline"
        assert error.failed_streams == failed_streams


class TestStageConfigurationError:
    """Test StageConfigurationError exception."""
    
    def test_inheritance(self):
        """Test StageConfigurationError inheritance."""
        error = StageConfigurationError("Stage config error", "extract")
        
        assert isinstance(error, ConfigurationError)
        assert isinstance(error, StageConfigurationError)
    
    def test_with_stage_name(self):
        """Test stage configuration error with stage name."""
        error = StageConfigurationError(
            "Invalid stage configuration",
            stage_name="transform"
        )
        
        assert str(error) == "Invalid stage configuration"
        assert error.stage_name == "transform"
    
    def test_with_stream_id(self):
        """Test stage configuration error with stream ID."""
        error = StageConfigurationError(
            "Stage validation failed",
            stage_name="load",
            stream_id="stage_stream"
        )
        
        assert str(error) == "Stage validation failed"
        assert error.stage_name == "load"
        assert error.stream_id == "stage_stream"


class TestExceptionInteroperability:
    """Test how exceptions work together and with Python's exception system."""
    
    def test_exception_chaining(self):
        """Test exception chaining with raise from."""
        original = ValueError("Original issue")
        
        try:
            try:
                raise original
            except ValueError as e:
                raise StreamProcessingError("Processing failed") from e
        except StreamProcessingError as spe:
            assert spe.__cause__ is original
            assert "Processing failed" in str(spe)
    
    def test_exception_isinstance_checks(self):
        """Test isinstance checks work correctly."""
        error = StreamExecutionError("Test", stage="transform")
        
        assert isinstance(error, StreamExecutionError)
        assert isinstance(error, StreamProcessingError)
        assert isinstance(error, Exception)
        assert not isinstance(error, ConfigurationError)
    
    def test_exception_attributes_preserved(self):
        """Test that exception attributes are preserved when caught."""
        original = RuntimeError("Runtime issue")
        error = StreamExecutionError(
            "Execution failed",
            stream_id="test_stream",
            stage="extract",
            batch_id=99,
            original_error=original
        )
        
        try:
            raise error
        except StreamExecutionError as caught:
            assert caught.stream_id == "test_stream"
            assert caught.stage == "extract"
            assert caught.batch_id == 99
            assert caught.original_error is original
    
    def test_exception_repr(self):
        """Test exception repr for debugging."""
        error = StreamProcessingError("Test error", stream_id="test")
        
        repr_str = repr(error)
        assert "StreamProcessingError" in repr_str
        assert "Test error" in repr_str or "test" in repr_str.lower()


class TestExceptionEdgeCases:
    """Test edge cases and special scenarios."""
    
    def test_empty_message(self):
        """Test exceptions with empty messages."""
        error = StreamProcessingError("")
        
        assert str(error) == ""
        assert isinstance(error, StreamProcessingError)
    
    def test_none_message(self):
        """Test exceptions with None message (should be converted to string)."""
        error = StreamProcessingError(None)
        
        # Exception converts None to "None"
        assert str(error) == "None"
    
    def test_unicode_message(self):
        """Test exceptions with unicode messages."""
        error = StreamProcessingError("Test with unicode: áéíóú 🚀")
        
        assert "áéíóú 🚀" in str(error)
    
    def test_complex_nested_errors(self):
        """Test complex nested error scenarios."""
        # Create a chain of errors
        root_cause = ValueError("Root cause")
        config_error = ConfigurationError("Config failed")  # ConfigurationError doesn't have original_error param
        stream_error = StreamProcessingError(
            "Stream failed", 
            stream_id="nested", 
            original_error=config_error
        )
        
        error_str = str(stream_error)
        assert "Stream nested:" in error_str
        assert "Stream failed" in error_str
        assert "Config failed" in error_str