# Analitiq Stream

A high-performance, fault-tolerant data streaming framework for Python 3.11+ that enables reliable data movement between various sources and destinations with **state management**, **strong input validation**, **pipeline orchestration**, and **comprehensive observability**.

## 🚀 Features

### Core Architecture
- **State Management** - Separate state files per stream/partition for scalability
- **Pydantic v2 Validation** - Strong input validation and type safety throughout
- **Config Immutability** - Pipeline configurations are validated and fingerprinted
- **Safe Deserialization** - Protected JSON parsing with comprehensive error handling
- **Pipeline Orchestration** - Modern concurrent execution with Python 3.11+ exception handling
- **Modular Design** - Factory patterns and dependency injection for testability

### Data Pipeline Capabilities  
- **Async/await streaming** with configurable batch processing
- **Incremental replication** with cursor tracking and tie-breaker deduplication
- **UUID-based configuration** for endpoint schemas and host credentials
- **Field mapping & transformations** with computed fields support
- **Comprehensive fault tolerance** (retries, circuit breakers, DLQ)
- **Multi-stream processing** with concurrent execution and independent failure handling

### Reliability & Monitoring
- **Schema drift detection** with hash comparison
- **Rate limiting** with configurable windows
- **Real-time checkpointing** with crash recovery
- **Pipeline-level metrics** with performance tracking and success rates
- **Structured logging** with correlation IDs and contextual information
- **Dead letter queues** for poison record isolation
- **Exception aggregation** with Python 3.11+ ExceptionGroup patterns

#### Circuit Breaker Pattern

The framework implements a sophisticated circuit breaker to prevent cascading failures:

**States & Failure Count Logic:**

- **CLOSED** (Normal operation): Successful calls do NOT reset failure count
- **OPEN** (Failing fast): All calls blocked until recovery timeout
- **HALF_OPEN** (Testing recovery): Limited calls allowed to test service health

**Why failure count persists in CLOSED state:**

```
Scenario: SUCCESS, FAIL, SUCCESS, FAIL, FAIL, SUCCESS, FAIL, FAIL, FAIL

Correct Implementation:
Call 1: SUCCESS → failure_count = 0 (starts at 0)
Call 2: FAIL → failure_count = 1
Call 3: SUCCESS → failure_count = 1 (NO reset) ✅
Call 4: FAIL → failure_count = 2
Call 5: FAIL → failure_count = 3
Call 6: SUCCESS → failure_count = 3 (NO reset) ✅
Call 7: FAIL → failure_count = 4
Call 8: FAIL → failure_count = 5 → CIRCUIT OPENS! 🔴
```

**Benefits:**
- Detects degraded services even with intermittent successes
- Prevents "lucky success" from hiding underlying problems
- Tracks general health trend rather than just last call result

**Failure count resets only when:**
1. Circuit transitions OPEN → HALF_OPEN → CLOSED (full recovery cycle)
2. Manual reset is called
3. Force close is called

## 📦 Installation

```bash
# Install dependencies
poetry install

# Install with optional extras
poetry install -E "kafka cloud analytics"

# Activate virtual environment
poetry shell
```

## 🏗 Architecture

### Modern Engine Architecture

The framework uses a **layered architecture** with clear separation of concerns:

```
┌─────────────────────────────────────┐
│         Pipeline Layer              │  Configuration loading & validation
├─────────────────────────────────────┤
│       Orchestration Layer          │  Multi-stream coordination & metrics
├─────────────────────────────────────┤
│         Engine Layer               │  Stream processing & fault tolerance
├─────────────────────────────────────┤
│       Connector Layer              │  Source/destination abstractions
└─────────────────────────────────────┘
```

**Key Components:**

- **`Pipeline`** - High-level configuration management and validation
- **`PipelineOrchestrator`** - Concurrent stream execution with Python 3.11+ exception handling
- **`StreamingEngine`** - Core stream processing with factory patterns
- **`APIConnector`** - Abstract API connector for sources and destinations
- **`StateManager`** - Scalable state management per stream/partition

### Modern State Management

The framework separates **immutable configuration** from **mutable state** with **concurrent worker support** for horizontal scaling:

```
state/
  wise-to-sevdesk/v1/
  ├── streams/endpoint.5a4b9e21-441f-4bc7-9d5e-41917b4357e6/
  │   ├── partition-default.json    # Single partition state
  │   ├── partition-a1b2c3d4.json   # Account-based partition  
  │   └── partition-e5f6g7h8.json   # Geographic partition
  ├── index.json                    # Manifest with schema hashes
  └── lock                          # Lightweight coordination
```

#### 🔄 Concurrent Worker Architecture

**✅ Zero Contention**: Each partition writes to separate files
- Workers processing different partitions never compete
- Atomic writes prevent corruption during concurrent access
- Horizontal scaling limited only by partition count

**Example Multi-Worker Setup:**
```python
# Worker A processes EU accounts
partition_eu = {"region": "EU", "account_type": "business"}

# Worker B processes US accounts  
partition_us = {"region": "US", "account_type": "business"}

# Each worker checkpoints independently:
# ├── partition-7a4b8c9d.json  # EU partition (Worker A)
# └── partition-2f5e1g3h.json  # US partition (Worker B)
```

#### 📋 State File Schema

**Partition State File** (`partition-{hash}.json`):
```json
{
  "partition": {},
  "cursor": {
    "primary": {
      "field": "created", 
      "value": "2025-08-14T11:58:03Z",
      "inclusive": true
    },
    "tiebreaker": {
      "field": "id",
      "value": 431245
    }
  },
  "hwm": "2025-08-14T11:58:03Z",
  "page_state": {
    "next_token": "eyJ0eXAiOiJKV1QiLCJh...",
    "offset": 1000
  },
  "http_conditionals": {
    "etag": "\"33a64df551425fcc\"",
    "last_modified": "Wed, 21 Oct 2015 07:28:00 GMT"
  },
  "stats": {
    "records_synced": 1250,
    "batches_written": 13,
    "last_checkpoint_at": "2025-08-14T12:05:30Z",
    "errors_since_checkpoint": 0
  },
  "last_updated": "2025-08-14T12:05:30Z"
}
```

**Index Manifest** (`index.json`):
```json
{
  "version": 1,
  "streams": {
    "endpoint.5a4b9e21-441f-4bc7-9d5e-41917b4357e6": {
      "schema_hash": "sha256:a1b2c3d4...",
      "partitions": [
        {
          "partition": {"region": "EU"},
          "file": "wise-to-sevdesk/v1/streams/endpoint.../partition-7a4b8c9d.json"
        }
      ]
    }
  },
  "run": {
    "run_id": "2025-08-14T11:55:00Z-d04f",
    "lease_owner": "worker-12345",
    "started_at": "2025-08-14T11:55:00Z",
    "checkpoint_seq": 42,
    "config_fingerprint": "sha256:e9f8a7b6..."
  }
}

### Configuration Structure

**Pipeline Configuration** (Immutable):
```json
{
  "pipeline_id": "wise-to-sevdesk-transactions",
  "name": "Wise Transactions to SevDesk Bank Transactions", 
  "version": "1.0",
  "src": {
    "host_id": "0e8b1731-479a-4bc0-b056-244cc5d6a53c",
    "endpoint_id": "5a4b9e21-441f-4bc7-9d5e-41917b4357e6",
    "replication_method": "incremental",
    "replication_key": "created",
    "cursor_mode": "inclusive",
    "safety_window_seconds": 120
  },
  "dst": {
    "host_id": "7c1a69eb-239f-45d4-b6c2-3ad4c6e89cfa", 
    "endpoint_id": "1e63d782-4b67-4b7e-b845-4b4de5e4f46e",
    "refresh_mode": "upsert",
    "batch_support": false,
    "batch_size": 1
  },
  "mapping": {
    "field_mappings": { "...": "..." },
    "computed_fields": { "...": "..." }
  }
}
```

**Host Credentials** (`hst_{host_id}.json`):
```json
{
  "base_url": "https://api.example.com",
  "headers": {
    "Content-Type": "application/json",
    "Authorization": "${API_TOKEN}"
  },
  "rate_limit": {
    "max_requests": 100,
    "time_window": 60
  }
}
```

**Endpoint Schema** (`ep_{endpoint_id}.json`):
```json
{
  "type": "api",
  "endpoint": "/v1/transfers", 
  "method": "GET",
  "pagination": {
    "type": "offset",
    "params": {
      "limit_param": "limit",
      "offset_param": "offset"
    }
  },
  "filters": {
    "created": {
      "type": "string",
      "required": false,
      "operators": ["gte"],
      "description": "Filter by creation date"
    }
  }
}
```

## 🔒 Input Validation & Security

### Pydantic v2 Models

All configurations are validated using comprehensive Pydantic models with the latest v2 features:

```python
class EngineConfig(BaseModel):
    """Engine configuration with validation."""
    model_config = ConfigDict(extra='forbid', validate_assignment=True)
    
    batch_size: int = Field(1000, ge=1, le=100000, description="Batch size for processing")
    max_concurrent_batches: int = Field(10, ge=1, le=1000, description="Maximum concurrent batches")
    buffer_size: int = Field(10000, ge=100, le=1000000, description="Buffer size for queues")

class StreamProcessingConfig(BaseModel):
    """Complete configuration for stream processing."""
    stream_id: str = Field(..., description="Unique stream identifier")
    source: Dict[str, Any] = Field(..., description="Source connector configuration")
    destination: Dict[str, Any] = Field(..., description="Destination connector configuration")
    
    @field_validator("replication_method")
    @classmethod
    def validate_replication_method(cls, v):
        if v not in ["full", "incremental"]:
            raise ValueError("replication_method must be 'full' or 'incremental'")
        return v

class PipelineMetricsSnapshot(BaseModel):
    """Pipeline-level metrics snapshot."""
    pipeline_id: str = Field(..., description="Pipeline identifier")
    total_streams: int = Field(0, ge=0, description="Total number of streams")
    completed_streams: int = Field(0, ge=0, description="Successfully completed streams")
    records_per_second: float = Field(0.0, ge=0.0, description="Processing rate")
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate for streams."""
        if self.total_streams == 0:
            return 100.0
        return (self.completed_streams / self.total_streams) * 100.0
```

### Modern Exception Handling

Enhanced error handling with **Python 3.11+ features** and contextual information:

```python
# Specific exception types with detailed context
class StreamExecutionError(StreamProcessingError):
    def __init__(self, message: str, stream_id: Optional[str] = None, 
                 stage: Optional[str] = None, batch_id: Optional[int] = None,
                 original_error: Optional[Exception] = None):
        self.stage = stage
        self.batch_id = batch_id
        super().__init__(message, stream_id, original_error)

# Python 3.11+ ExceptionGroup for concurrent failures
try:
    results = await asyncio.gather(*stream_tasks, return_exceptions=True)
except* StreamProcessingError as eg:
    logger.error(f"Stream processing errors: {len(eg.exceptions)} streams failed")
    for exc in eg.exceptions:
        logger.error(f"  - {exc}")
except* Exception as eg:
    logger.error(f"Unexpected errors: {len(eg.exceptions)} errors")
```

### Safe Deserialization

- **Protected JSON parsing** with error recovery
- **Schema structure validation** before processing  
- **Type checking** for all data structures
- **Bounds checking** for numeric values
- **URL validation** to prevent injection attacks

## 🚦 Usage

### Basic Pipeline Setup

```python
import json
from analitiq_stream import Pipeline

# Load pipeline configuration
with open("pipeline_config.json") as f:
    pipeline_config = json.load(f)

# Create and run pipeline with automatic state management
pipeline = Pipeline(pipeline_config=pipeline_config)
await pipeline.run()
```

### Advanced Usage with Modern Engine

```python
from analitiq_stream.core.engine import StreamingEngine
from analitiq_stream.models.engine import EngineConfig, PipelineMetricsSnapshot
from analitiq_stream.fault_tolerance.state_manager import StateManager

# Create engine with validated configuration
engine_config = EngineConfig(
    batch_size=500,
    max_concurrent_batches=5,
    buffer_size=5000
)

engine = StreamingEngine(
    pipeline_id="my-pipeline",
    engine_config=engine_config
)

# Execute pipeline and get metrics
await engine.stream_data(pipeline_config)

# Access comprehensive metrics
metrics = engine.orchestrator.get_current_metrics()
print(f"Success rate: {metrics.success_rate:.2f}%")
print(f"Records/sec: {metrics.records_per_second:.2f}")
print(f"Completed: {metrics.completed_streams}/{metrics.total_streams}")

# Monitor state management
state_manager = engine.get_state_manager()
resume_info = state_manager.get_resume_info("my-stream")
print(f"Total records synced: {resume_info['total_records_synced']}")
```

### Pipeline Orchestration

```python
from analitiq_stream.core.orchestrator import PipelineOrchestrator

# Direct orchestrator usage for custom workflows
orchestrator = PipelineOrchestrator("custom-pipeline")

async def custom_stream_processor(config):
    """Custom stream processing logic"""
    # Your stream processing implementation
    pass

# Orchestrate multiple streams with exception handling
try:
    metrics = await orchestrator.orchestrate_pipeline(
        pipeline_config, 
        custom_stream_processor
    )
    print(f"Pipeline completed: {metrics.success_rate:.1f}% success")
except ExceptionGroup as eg:
    print(f"Multiple failures occurred: {len(eg.exceptions)} errors")
    for exc in eg.exceptions:
        print(f"  - {exc}")
```

### Generate Configuration Templates

```bash
# Generate credential templates
python examples/generate_credentials_template.py database src_credentials.json
python examples/generate_credentials_template.py api dst_credentials.json
```

## 🧪 Testing & Quality

### Run Tests

**Comprehensive Test Suite** - 35+ tests covering all major features:

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=analitiq_stream --cov-report=html

# Run specific test modules
poetry run pytest tests/test_engine_improvements.py     # Engine architecture (18 tests)
poetry run pytest tests/test_orchestrator.py            # Pipeline orchestration (17 tests)
poetry run pytest tests/test_duplicate_records*.py      # Deduplication logic
poetry run pytest tests/test_api_incremental*.py        # API connector features

# Run by test markers
poetry run pytest -m "unit"           # Unit tests only
poetry run pytest -m "integration"    # Integration tests only  
poetry run pytest -m "not slow"       # Exclude slow tests
poetry run pytest -k "engine"         # Run engine-related tests
poetry run pytest -k "orchestrator"   # Run orchestration tests
```

### Code Quality

```bash
# Format and lint code
poetry run black analitiq_stream/
poetry run isort analitiq_stream/
poetry run mypy analitiq_stream/      # Enhanced with comprehensive type annotations
poetry run flake8 analitiq_stream/

# Run all pre-commit hooks
poetry run pre-commit run --all-files
```

**Test Coverage Areas:**
- ✅ **Engine Configuration** - Pydantic validation, factory methods, type safety
- ✅ **Pipeline Orchestration** - Concurrent execution, exception handling, metrics
- ✅ **API Connector** - Deduplication, incremental replication, error handling  
- ✅ **State Management** - State persistence, cursor tracking, tie-breaker logic
- ✅ **Exception Handling** - Python 3.11+ patterns, contextual errors
- ✅ **Metrics & Monitoring** - Performance tracking, structured logging

## 📊 Monitoring & Observability

### Built-in Metrics

**Pipeline-Level Metrics:**
- **Stream success/failure rates** with detailed breakdown
- **Processing performance** (records/sec, batch sizes, throughput)
- **Error aggregation** with ExceptionGroup support
- **Concurrent task tracking** with status monitoring
- **Real-time metrics** updates during execution

**Stream-Level Metrics:**
- **Records processed/failed** per batch and partition
- **Checkpoint intervals** and success rates per worker
- **API response times** and error rates
- **Schema drift detection** events
- **Dead letter queue** statistics

### Modern Observability

```python
# Pipeline-level monitoring with structured logging
from analitiq_stream.core.engine import StreamingEngine
from analitiq_stream.models.engine import PipelineMetricsSnapshot

engine = StreamingEngine("production-pipeline")

# Real-time metrics during execution
metrics = engine.orchestrator.get_current_metrics()
print(f"Pipeline: {metrics.pipeline_id}")
print(f"Success Rate: {metrics.success_rate:.2f}%")
print(f"Processing Rate: {metrics.records_per_second:.2f} records/sec")
print(f"Streams: {metrics.completed_streams}/{metrics.total_streams}")

# Active task monitoring
active_tasks = engine.orchestrator.get_active_tasks()
for stream_id, task_info in active_tasks.items():
    print(f"Stream {stream_id}: {task_info.status} since {task_info.started_at}")

# Traditional state inspection
state_manager = engine.get_state_manager()
resume_info = state_manager.get_resume_info("my-stream")
print(f"Can resume: {resume_info['can_resume']}")
print(f"Total records: {resume_info['total_records_synced']}")
```

### Structured Logging with Correlation

```python
# Automatic correlation IDs for request tracing
import logging

# Configure structured logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Log entries include correlation context:
# 2025-08-18 15:34:27 - analitiq_stream.core.orchestrator - INFO - Pipeline started
#   {"pipeline_id": "prod-sync", "run_id": "2025-08-18T15:34:27-a1b2", "stream_count": 3}
# 2025-08-18 15:34:28 - analitiq_stream.core.engine - INFO - Stream processing started  
#   {"stream_id": "transactions", "correlation_id": "2025-08-18T15:34:27-a1b2"}
```

### 📋 Technical Documentation

For detailed technical specifications:
- **[State File Specification](STATE_SPECIFICATION.md)** - Complete parameter documentation and concurrent worker architecture
- **Parameter meanings**: Every state file field documented with types, examples, and purposes
- **Concurrency model**: How multiple workers coordinate without contention
- **Operational procedures**: Resume, scale-out, troubleshooting

## 🔧 Configuration Reference

### Incremental Replication Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `replication_key` | Field used for incremental sync | `"created"` |
| `cursor_mode` | Boundary semantics | `"inclusive"` / `"exclusive"` |
| `safety_window_seconds` | Overlap window for late data | `120` |

### Engine Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch_size` | 1000 | Records per batch |
| `max_concurrent_batches` | 10 | Parallel processing limit |
| `buffer_size` | 10000 | Queue buffer size |

### Error Handling Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `dlq` | Send to dead letter queue | Data quality issues |
| `retry` | Retry with exponential backoff | Transient API errors |  
| `skip` | Log and continue | Non-critical validation failures |

### State vs Configuration

**What moved to state files:**
- `bookmarks` - Runtime cursor tracking
- `run` - Current execution metadata
- `config_fingerprint` - Auto-generated
- `updated_at` - Auto-managed timestamp
- `state_file` - Replaced with state management

**What remains in pipeline config:**
- Pipeline identity (`pipeline_id`, `name`, `version`)
- Source/destination configurations
- Field mappings and transformations  
- Engine settings and schedules
- Error handling strategies

## 🤝 Examples

Working examples are available in the `examples/` directory:

- **`basic-pipeline/`** - Simple database to API sync
- **`wise_to_sevdesk/`** - Real-world integration with incremental replication

Run the Wise to SevDesk example:
```bash
python analitiq_stream/examples/wise_to_sevdesk/run_pipeline.py
```

## 📋 Development Commands

```bash
# Environment setup
poetry install
pre-commit install

# Run example pipelines  
python analitiq_stream/examples/basic-pipeline/simple_pipeline.py
python analitiq_stream/examples/wise_to_sevdesk/pipeline.py

# Testing and validation
python test_credentials.py
poetry run pytest --cov=analitiq_stream
```

## 🏛 Project Structure

```
analitiq_stream/
├── core/                    # Core framework components
│   ├── engine.py           # StreamingEngine with modern architecture
│   ├── orchestrator.py     # PipelineOrchestrator for concurrent execution
│   ├── pipeline.py         # High-level configuration interface  
│   ├── exceptions.py       # Enhanced exception hierarchy
│   ├── credentials.py      # Secure authentication management
│   └── data_transformer.py # Field mapping and transformations
├── connectors/             # Data source/destination connectors
│   ├── base.py            # BaseConnector interface
│   ├── api.py             # API connector with validation & deduplication
│   └── database.py        # Database connectors
├── fault_tolerance/        # Comprehensive fault tolerance
│   ├── state_manager.py  # Per-partition state management
│   ├── retry_handler.py   # Exponential backoff retry logic
│   ├── circuit_breaker.py # Failure detection/recovery
│   └── dead_letter_queue.py # Poison record isolation  
├── models/                 # Pydantic v2 validation models
│   ├── state.py           # State management models
│   ├── engine.py          # Engine configuration models
│   ├── api.py             # API connector models
│   └── metrics.py         # Metrics and monitoring models
├── schema/                # Schema management and evolution
│   └── schema_manager.py  # Schema drift detection
├── examples/              # Working examples
│   ├── basic-pipeline/    # Simple integration
│   └── wise_to_sevdesk/   # Production example with tie-breaker deduplication
└── tests/                 # Comprehensive test suite
    ├── test_engine_improvements.py  # Engine architecture tests
    ├── test_orchestrator.py         # Pipeline orchestration tests
    ├── test_api_incremental*.py     # API connector tests
    └── test_duplicate_records*.py   # Deduplication tests
```

## 🔐 Security Features

- **Input validation** with Pydantic v2 models and `ConfigDict(extra='forbid')`
- **Safe JSON deserialization** with error handling and type checking
- **URL validation** to prevent injection attacks and malformed URLs
- **Environment variable expansion** for secrets with `${VAR_NAME}` syntax
- **Type safety** throughout the pipeline with comprehensive annotations
- **Bounds checking** for all numeric inputs with Field constraints
- **Protected file operations** with encoding specification and path validation
- **Exception sanitization** to prevent information leakage in error messages
- **Configuration fingerprinting** for state consistency validation

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Analitiq Stream** - Built for production data pipelines with enterprise-grade reliability, monitoring, and security. 🚀