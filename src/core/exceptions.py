"""Custom exceptions for the streaming engine."""

from typing import Optional, Any, Dict, List


class StreamProcessingError(Exception):
    """Exception for stream processing errors with context."""
    
    def __init__(self, message: str, stream_id: Optional[str] = None, 
                 original_error: Optional[Exception] = None):
        self.stream_id = stream_id
        self.original_error = original_error
        super().__init__(message)
        
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.stream_id:
            base_msg = f"Stream {self.stream_id}: {base_msg}"
        if self.original_error:
            base_msg = f"{base_msg} (caused by: {self.original_error})"
        return base_msg


class TransformationError(StreamProcessingError):
    """Exception for data transformation errors."""
    pass


class ConnectorError(StreamProcessingError):
    """Exception for connector-related errors."""
    pass


class ConfigurationError(Exception):
    """Base exception for pipeline configuration errors."""
    
    def __init__(self, message: str, field_path: Optional[str] = None, 
                 validation_errors: Optional[List[str]] = None):
        self.field_path = field_path
        self.validation_errors = validation_errors or []
        super().__init__(message)


class StreamConfigurationError(ConfigurationError):
    """Exception for stream-specific configuration errors."""
    
    def __init__(self, message: str, stream_id: Optional[str] = None, 
                 field_path: Optional[str] = None, validation_errors: Optional[List[str]] = None):
        self.stream_id = stream_id
        super().__init__(message, field_path, validation_errors)


class PipelineValidationError(ConfigurationError):
    """Exception for pipeline-level validation failures."""
    
    def __init__(self, message: str, errors: Optional[Dict[str, List[str]]] = None):
        self.errors = errors or {}
        super().__init__(message)


class StreamExecutionError(StreamProcessingError):
    """Exception for stream execution failures with detailed context."""
    
    def __init__(self, message: str, stream_id: Optional[str] = None, 
                 stage: Optional[str] = None, batch_id: Optional[int] = None,
                 original_error: Optional[Exception] = None):
        self.stage = stage
        self.batch_id = batch_id
        super().__init__(message, stream_id, original_error)
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.stage:
            base_msg = f"{base_msg} [Stage: {self.stage}]"
        if self.batch_id is not None:
            base_msg = f"{base_msg} [Batch: {self.batch_id}]"
        return base_msg


class PipelineOrchestrationError(Exception):
    """Exception for pipeline orchestration failures."""
    
    def __init__(self, message: str, pipeline_id: Optional[str] = None,
                 failed_streams: Optional[List[str]] = None):
        self.pipeline_id = pipeline_id
        self.failed_streams = failed_streams or []
        super().__init__(message)


class StageConfigurationError(ConfigurationError):
    """Exception for pipeline stage configuration errors."""
    
    def __init__(self, message: str, stage_name: str, stream_id: Optional[str] = None):
        self.stage_name = stage_name
        self.stream_id = stream_id
        super().__init__(message)