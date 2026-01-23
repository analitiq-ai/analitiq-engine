# Engine Architecture Documentation

## Overview

This document describes the modern engine architecture improvements implemented in Analitiq Stream, focusing on the enhanced design patterns, Pydantic v2 validation, pipeline orchestration, and comprehensive observability features.

## Architecture Layers

### 1. Pipeline Layer (`pipeline.py`)
**Responsibility**: Configuration loading, validation, and high-level pipeline management

```python
class Pipeline:
    """High-level pipeline management with configuration validation."""
    
    def __init__(self, pipeline_config, source_config, destination_config):
        # Validates and merges configurations
        # Sets up logging and directory structure
        # Creates StreamingEngine instance
        
    async def run(self):
        # Performs config-state compatibility validation
        # Delegates execution to StreamingEngine
```

**Key Features:**
- UUID-based configuration loading
- Environment variable expansion with `${VAR_NAME}` syntax
- Comprehensive validation with error reporting
- Structured logging setup per pipeline
- Config-state compatibility checking

### 2. Orchestration Layer (`orchestrator.py`)
**Responsibility**: Multi-stream coordination and pipeline-level metrics

```python
class PipelineOrchestrator:
    """Coordinates pipeline execution with multiple streams."""
    
    async def orchestrate_pipeline(self, pipeline_config, stream_processor_factory):
        # Creates and validates stream processing configurations
        # Manages concurrent stream execution
        # Aggregates metrics and handles exceptions
        # Returns comprehensive PipelineMetricsSnapshot
```

**Key Features:**
- Python 3.11+ `ExceptionGroup` pattern for concurrent failures
- Real-time metrics aggregation with Pydantic models
- Structured logging with correlation IDs
- Task lifecycle management (pending → running → completed/failed)
- Pipeline-level performance tracking

### 3. Engine Layer (`engine.py`)
**Responsibility**: Stream processing execution and fault tolerance

```python
class StreamingEngine:
    """Core stream processing with modern architecture."""
    
    def __init__(self, pipeline_id, engine_config: Optional[EngineConfig] = None):
        # Validates engine configuration using Pydantic
        # Initializes orchestrator and fault tolerance components
        # Sets up structured logging with pipeline context
        
    async def stream_data(self, pipeline_config):
        # Traditional interface maintained for backward compatibility
        # Delegates to orchestrator for modern execution patterns
```

**Key Features:**
- Factory methods for connector and stage creation
- Comprehensive type hints throughout
- Pydantic v2 configuration validation
- Enhanced error handling with contextual exceptions
- Integration with orchestration layer

### 4. Connector Layer (`connectors/api.py`)
**Responsibility**: Source/destination abstractions with validation

```python
class APIConnector(BaseConnector):
    """Modern API connector with comprehensive features."""

    async def read_batches(self, config, state_manager, stream_name, partition, batch_size):
        # Handles pagination (cursor, offset, page-based)
        # Implements incremental replication with deduplication
        # Manages rate limiting and fault tolerance

    async def write_batch(self, batch, config):
        # Supports both individual and batch writes
        # Handles rate limiting and error responses
        # Abstract enough for various API patterns
```

**Key Features:**
- Abstract enough for both source and destination usage
- Comprehensive deduplication with tie-breaker fields
- Incremental replication with safety windows
- Flexible pagination support
- Rate limiting and fault tolerance

### 5. gRPC Streaming Layer (`grpc/`, `destination/`)
**Responsibility**: Bidirectional streaming to decoupled destination services

```python
class DestinationGRPCClient:
    """gRPC client for streaming to destination services."""

    async def connect(self) -> bool:
        # Establish gRPC channel and verify health

    async def start_stream(self, run_id, stream_id, schema_config) -> bool:
        # Start bidirectional stream, send schema, wait for ACK

    async def send_batch(self, run_id, stream_id, batch_seq, records, record_ids, cursor) -> BatchResult:
        # Send batch, wait for ACK, handle status codes
```

**Key Features:**
- Bidirectional streaming with Protocol Buffers
- Opaque cursor design (engine produces, destination stores/returns)
- Batch-level idempotency via `(run_id, stream_id, batch_seq)`
- All-or-nothing batch semantics (no partial success)
- Stable record IDs for DLQ correlation

**See [gRPC Streaming Architecture](docs/GRPC_STREAMING_ARCHITECTURE.md) for detailed documentation.**

## Pydantic v2 Models

### Engine Configuration Models

```python
class EngineConfig(BaseModel):
    """Engine configuration with validation."""
    
    model_config = ConfigDict(extra='forbid', validate_assignment=True)
    
    batch_size: int = Field(1000, ge=1, le=100000)
    max_concurrent_batches: int = Field(10, ge=1, le=1000) 
    buffer_size: int = Field(10000, ge=100, le=1000000)
    dlq_path: str = Field("./deadletter/")
    checkpoint_interval: int = Field(100, ge=1)

class StreamProcessingConfig(BaseModel):
    """Complete stream processing configuration."""
    
    stream_id: str = Field(..., description="Unique stream identifier")
    pipeline_id: str = Field(..., description="Pipeline identifier")
    
    # Processing configuration
    source: Dict[str, Any] = Field(..., description="Source configuration")
    destination: Dict[str, Any] = Field(..., description="Destination configuration")
    
    # Replication settings with validation
    replication_method: str = Field("incremental")
    cursor_field: Optional[List[str]] = Field(None)
    # Note: cursor_mode removed - always uses inclusive (>=) for safety
    tie_breaker_fields: Optional[List[str]] = Field(None)
    
    @field_validator("replication_method")
    @classmethod
    def validate_replication_method(cls, v):
        if v not in ["full", "incremental"]:
            raise ValueError("Must be 'full' or 'incremental'")
        return v
```

### Metrics Models

```python
class PipelineMetricsSnapshot(BaseModel):
    """Pipeline-level metrics with computed properties."""
    
    pipeline_id: str = Field(..., description="Pipeline identifier")
    run_id: str = Field(..., description="Current run identifier") 
    started_at: datetime = Field(..., description="Pipeline start time")
    
    # Stream metrics
    total_streams: int = Field(0, ge=0)
    completed_streams: int = Field(0, ge=0)
    failed_streams: int = Field(0, ge=0)
    
    # Performance metrics
    total_records_processed: int = Field(0, ge=0)
    records_per_second: float = Field(0.0, ge=0.0)
    average_batch_size: float = Field(0.0, ge=0.0)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate for streams."""
        if self.total_streams == 0:
            return 100.0
        return (self.completed_streams / self.total_streams) * 100.0

class TaskExecutionInfo(BaseModel):
    """Information about executing tasks."""
    
    stream_id: str = Field(..., description="Stream identifier")
    stream_name: str = Field(..., description="Stream name") 
    task_name: str = Field(..., description="Task name")
    started_at: datetime = Field(..., description="Start time")
    status: str = Field(..., description="Task status")
    
    @field_validator("status")
    @classmethod
    def validate_status(cls, v):
        valid_statuses = ["pending", "running", "completed", "failed", "cancelled"]
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of: {valid_statuses}")
        return v
```

## Exception Hierarchy

### Enhanced Exception Types

```python
class StreamExecutionError(StreamProcessingError):
    """Stream execution failure with detailed context."""
    
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

class StreamConfigurationError(ConfigurationError):
    """Stream-specific configuration errors."""
    
    def __init__(self, message: str, stream_id: Optional[str] = None,
                 field_path: Optional[str] = None, 
                 validation_errors: Optional[List[str]] = None):
        self.stream_id = stream_id
        self.field_path = field_path
        self.validation_errors = validation_errors or []
        super().__init__(message)

class PipelineOrchestrationError(Exception):
    """Pipeline orchestration failures."""
    
    def __init__(self, message: str, pipeline_id: Optional[str] = None,
                 failed_streams: Optional[List[str]] = None):
        self.pipeline_id = pipeline_id
        self.failed_streams = failed_streams or []
        super().__init__(message)
```

### Python 3.11+ Exception Handling

```python
# Modern exception aggregation
try:
    results = await asyncio.gather(*stream_tasks, return_exceptions=True)
    
    # Process results and collect exceptions
    for (stream_id, stream_name, task), result in zip(stream_tasks, results):
        if isinstance(result, Exception):
            stream_error = StreamExecutionError(
                f"Stream execution failed: {result}",
                stream_id=stream_id,
                original_error=result
            )
            stream_exceptions.append(stream_error)
    
    # Handle collected exceptions with ExceptionGroup
    if stream_exceptions:
        if len(stream_exceptions) == len(streams):
            # All streams failed - critical failure
            raise ExceptionGroup("All streams failed", stream_exceptions)
        else:
            # Partial failure - log but continue
            logger.warning(f"Partial failure: {len(stream_exceptions)} streams failed")
            
except* StreamProcessingError as eg:
    # Handle stream processing errors specifically
    logger.error(f"Stream processing errors: {len(eg.exceptions)} streams failed")
    for exc in eg.exceptions:
        logger.error(f"  - {exc}")
    raise
    
except* Exception as eg:
    # Handle unexpected errors
    logger.error(f"Unexpected errors: {len(eg.exceptions)} errors")
    raise
```

## Factory Patterns

### Connector Factory

```python
class StreamingEngine:
    
    def _create_source_connector(self, config: Dict[str, Any]) -> BaseConnector:
        """Create source connector based on configuration."""
        return self._get_connector(config)
    
    def _create_destination_connector(self, config: Dict[str, Any]) -> BaseConnector:
        """Create destination connector based on configuration."""
        return self._get_connector(config)
    
    def _get_connector(self, config: Dict[str, Any]) -> BaseConnector:
        """Factory method for connector creation."""
        connector_type = config.get("type", "api")
        
        if connector_type == "api":
            from ..connectors.api import APIConnector
            return APIConnector()
        elif connector_type == "database":
            from ..connectors.database import DatabaseConnector
            return DatabaseConnector()
        else:
            raise ValueError(f"Unknown connector type: {connector_type}")
```

### Pipeline Stages Factory

```python
def _create_pipeline_stages(
    self,
    source_connector: BaseConnector,
    dest_connector: BaseConnector,
    extract_queue: Queue,
    transform_queue: Queue,
    load_queue: Queue,
    stream_processing_config: Dict[str, Any],
    stream_dlq: 'DeadLetterQueue',
    stream_name: str
) -> List[asyncio.Task]:
    """Create all pipeline stage tasks using factory pattern."""
    
    return [
        asyncio.create_task(
            self._extract_stage(source_connector, extract_queue, stream_processing_config),
            name=f"extract-{stream_name}"
        ),
        asyncio.create_task(
            self._transform_stage(extract_queue, transform_queue, stream_processing_config),
            name=f"transform-{stream_name}"
        ),
        asyncio.create_task(
            self._load_stage(transform_queue, load_queue, dest_connector, stream_processing_config, stream_dlq),
            name=f"load-{stream_name}"
        ),
        asyncio.create_task(
            self._checkpoint_stage(load_queue, stream_processing_config),
            name=f"checkpoint-{stream_name}"
        ),
    ]
```

## Structured Logging

### Correlation IDs

```python
class PipelineOrchestrator:
    
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.run_id = f"{datetime.now(timezone.utc).isoformat()}-{str(uuid4())[:8]}"
        self.logger = logging.getLogger(f"{__name__}.{pipeline_id}")
    
    async def orchestrate_pipeline(self, pipeline_config, stream_processor_factory):
        self.logger.info(
            "Starting pipeline orchestration",
            extra={
                "pipeline_id": self.pipeline_id,
                "run_id": self.run_id,
                "stream_count": len(streams),
                "correlation_id": self.run_id
            }
        )

class StreamingEngine:
    
    async def _process_stream(self, stream_id: str, stream_config: Dict[str, Any], pipeline_config: Dict[str, Any]):
        correlation_id = getattr(self.orchestrator, 'run_id', f"{datetime.now(timezone.utc).isoformat()}-{str(uuid4())[:8]}")
        
        self.logger.info(
            "Processing stream",
            extra={
                "stream_id": stream_id,
                "stream_name": stream_config.get('name', stream_id),
                "correlation_id": correlation_id
            }
        )
```

### Log Format Examples

```
2025-08-18 15:34:27 - src.core.orchestrator.test-pipeline - INFO - Starting pipeline orchestration
  {"pipeline_id": "test-pipeline", "run_id": "2025-08-18T15:34:27-a1b2", "stream_count": 3}

2025-08-18 15:34:28 - src.core.engine.test-pipeline - INFO - Processing stream
  {"stream_id": "transactions", "stream_name": "Transaction Stream", "correlation_id": "2025-08-18T15:34:27-a1b2"}

2025-08-18 15:34:29 - src.connectors.api - DEBUG - Making API request: GET https://api.wise.com/v1/transfers?limit=1000&offset=0

2025-08-18 15:34:30 - src.core.orchestrator.test-pipeline - INFO - Pipeline orchestration completed
  {"success_rate": 100.0, "total_records": 1250, "correlation_id": "2025-08-18T15:34:27-a1b2"}
```

## Performance Improvements

### Concurrent Execution

- **Parallel Stream Processing**: Multiple streams execute concurrently with independent failure handling
- **Factory-Based Creation**: Reduces object creation overhead and improves testability  
- **Pydantic Validation**: Front-loads validation to catch errors early and avoid runtime failures
- **Structured Logging**: Reduces logging overhead with efficient context passing

### Memory Management

- **Async Queues**: Non-blocking queue operations prevent memory buildup
- **Batch Processing**: Configurable batch sizes optimize memory usage vs throughput
- **State Sharding**: Separate state files prevent contention and reduce memory pressure
- **Exception Aggregation**: Efficient collection and handling of concurrent exceptions

### Monitoring Efficiency

- **Real-time Metrics**: Low-overhead metrics collection with Pydantic validation
- **Correlation Tracking**: Efficient request tracing without performance impact
- **Task Lifecycle**: Minimal overhead task monitoring with status updates
- **Error Contextualization**: Rich error information without serialization overhead

## Testing Architecture

### Comprehensive Test Coverage

**Test Modules:**
- `test_engine_improvements.py` - 18 tests covering engine architecture
- `test_orchestrator.py` - 17 tests covering pipeline orchestration  
- `test_duplicate_records_integration.py` - Deduplication and tie-breaker logic
- `test_api_incremental.py` - API connector incremental replication

**Test Categories:**
- **Configuration Validation** - Pydantic model validation, error handling
- **Factory Methods** - Connector creation, stage creation, dependency injection
- **Exception Handling** - Python 3.11+ patterns, contextual errors
- **Metrics & Monitoring** - Performance tracking, structured logging
- **Integration** - End-to-end pipeline execution with mocking

### Testing Patterns

```python
# Pydantic model testing
def test_engine_config_validation():
    config = EngineConfig(batch_size=1000, max_concurrent_batches=5)
    assert config.batch_size == 1000
    
    with pytest.raises(ValidationError):
        EngineConfig(batch_size=-1)  # Invalid value

# Async testing with proper cleanup
@pytest.mark.asyncio
async def test_pipeline_orchestration():
    orchestrator = PipelineOrchestrator("test-pipeline")
    
    # Mock stream processor
    async def mock_processor(config):
        return f"processed-{config.stream_id}"
    
    result = await orchestrator.orchestrate_pipeline(config, mock_processor)
    assert isinstance(result, PipelineMetricsSnapshot)

# Exception testing
def test_stream_execution_error():
    error = StreamExecutionError(
        "Stream failed",
        stream_id="test",
        stage="extract", 
        batch_id=5
    )
    
    assert error.stream_id == "test"
    assert "[Stage: extract]" in str(error)
    assert "[Batch: 5]" in str(error)
```

## Migration Guide

### From Previous Architecture

**Configuration Changes:**
- Engine now accepts `EngineConfig` object for validation
- Factory methods replace direct connector instantiation
- Orchestrator handles multi-stream coordination

**Code Updates:**
```python
# Old approach
engine = StreamingEngine("pipeline-id", batch_size=1000)

# New approach  
engine_config = EngineConfig(batch_size=1000, max_concurrent_batches=5)
engine = StreamingEngine("pipeline-id", engine_config=engine_config)

# Access new metrics
metrics = engine.orchestrator.get_current_metrics()
print(f"Success rate: {metrics.success_rate:.2f}%")
```

**Exception Handling Updates:**
```python
# Old approach - basic exception handling
try:
    await engine.stream_data(config)
except Exception as e:
    logger.error(f"Pipeline failed: {e}")

# New approach - Python 3.11+ patterns
try:
    await engine.stream_data(config) 
except* StreamProcessingError as eg:
    for exc in eg.exceptions:
        logger.error(f"Stream error: {exc}")
except* Exception as eg:
    for exc in eg.exceptions:
        logger.error(f"Unexpected error: {exc}")
```

## Best Practices

### Configuration Management
- Use Pydantic models for all configuration validation
- Leverage `ConfigDict(extra='forbid')` to catch typos
- Implement field validators for complex business rules
- Use descriptive field descriptions for auto-documentation

### Error Handling
- Use specific exception types with contextual information
- Implement Python 3.11+ `except*` patterns for concurrent operations
- Include correlation IDs in all error messages
- Provide actionable error messages with suggested fixes

### Logging
- Use structured logging with consistent extra fields
- Include correlation IDs for request tracing
- Log at appropriate levels (DEBUG for detailed info, INFO for important events)
- Avoid logging sensitive information (passwords, tokens)

### Testing
- Test Pydantic models with both valid and invalid inputs
- Use proper async test patterns with cleanup
- Mock external dependencies consistently
- Test error conditions and edge cases

### Performance
- Use factory methods to reduce object creation overhead
- Implement proper async patterns to avoid blocking
- Monitor metrics regularly to identify bottlenecks
- Use appropriate batch sizes for your workload

---

This architecture provides a solid foundation for building reliable, maintainable, and observable data pipelines while leveraging modern Python features and best practices.