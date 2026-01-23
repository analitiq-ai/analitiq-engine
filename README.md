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
- **gRPC Streaming** - Decoupled destination services via bidirectional gRPC streaming

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
  "version": 1,
  "pipeline_id": "wise-to-sevdesk-transactions",
  "client_id": "your-client-id",
  "name": "Wise Transactions to SevDesk Bank Transactions",
  "status": "active",
  "is_active": true,
  "connections": {
    "source": { "conn_wise": "0e8b1731-479a-4bc0-b056-244cc5d6a53c" },
    "destinations": [{ "conn_sevdesk": "7c1a69eb-239f-45d4-b6c2-3ad4c6e89cfa" }]
  },
  "runtime": {
    "batching": { "batch_size": 100 },
    "error_handling": { "strategy": "dlq" },
    "schedule": { "type": "interval", "interval_minutes": 60 }
  }
}
```

**Stream Configuration** (`streams/{stream_id}.json`):
```json
{
  "version": 1,
  "stream_id": "transfers-stream",
  "pipeline_id": "wise-to-sevdesk-transactions",
  "status": "active",
  "is_enabled": true,
  "source": {
    "connection_ref": "conn_wise",
    "endpoint_id": "5a4b9e21-441f-4bc7-9d5e-41917b4357e6",
    "primary_key": ["id"],
    "replication": { "method": "incremental", "cursor_field": ["created"] }
  },
  "destinations": [{
    "connection_ref": "conn_sevdesk",
    "endpoint_id": "1e63d782-4b67-4b7e-b845-4b4de5e4f46e",
    "write": { "mode": "upsert" }
  }],
  "mapping": { "assignments": [] }
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
from src import Pipeline

# Load pipeline configuration
with open("pipeline_config.json") as f:
    pipeline_config = json.load(f)

# Create and run pipeline with automatic state management
pipeline = Pipeline(pipeline_config=pipeline_config)
await pipeline.run()
```

### Advanced Usage with Modern Engine

```python
from src.engine.engine import StreamingEngine
from src.models.engine import EngineConfig, PipelineMetricsSnapshot
from src.state.state_manager import StateManager

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
from src.engine.orchestrator import PipelineOrchestrator

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
poetry run pytest --cov=src --cov-report=html

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
poetry run black src/
poetry run isort src/
poetry run mypy src/      # Enhanced with comprehensive type annotations
poetry run flake8 src/

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
from src.engine.engine import StreamingEngine
from src.models.engine import PipelineMetricsSnapshot

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
# 2025-08-18 15:34:27 - src.core.orchestrator - INFO - Pipeline started
#   {"pipeline_id": "prod-sync", "run_id": "2025-08-18T15:34:27-a1b2", "stream_count": 3}
# 2025-08-18 15:34:28 - src.core.engine - INFO - Stream processing started  
#   {"stream_id": "transactions", "correlation_id": "2025-08-18T15:34:27-a1b2"}
```

### 📋 Technical Documentation

For detailed technical specifications:
- **[State File Specification](STATE_SPECIFICATION.md)** - Complete parameter documentation and concurrent worker architecture
- **[Pipeline Metrics & Athena Setup](docs/PIPELINE_METRICS_ATHENA.md)** - Athena table creation, Lambda integration, and example queries for pipeline metrics
- **Parameter meanings**: Every state file field documented with types, examples, and purposes
- **Concurrency model**: How multiple workers coordinate without contention
- **Operational procedures**: Resume, scale-out, troubleshooting

## 🔧 Configuration Reference

### Incremental Replication Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `cursor_field` | Field(s) used for incremental sync | `["created"]` |
| `safety_window_seconds` | Overlap window for late data (optional) | `120` |

**Note**: The engine always uses **inclusive mode** (`>=`) for cursor comparison. This is safer when timestamps can repeat or arrive late. Duplicates are handled via upsert/idempotency.

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
python src/examples/wise_to_sevdesk/run_pipeline.py
```

## 📋 Development Commands

```bash
# Environment setup
poetry install
pre-commit install

# Run example pipelines  
python src/examples/basic-pipeline/simple_pipeline.py
python src/examples/wise_to_sevdesk/pipeline.py

# Testing and validation
python test_credentials.py
poetry run pytest --cov=src
```

## Project Structure

```
src/
├── shared/                  # Shared utilities for source and destination
│   ├── database_utils.py   # SSL mode, identifier validation, table names
│   ├── rate_limiter.py     # Token bucket rate limiter
│   └── type_mapping/       # Unified type mapping system
│       ├── base.py         # BaseTypeMapper ABC
│       ├── postgresql.py   # PostgreSQL: JSONB, TIMESTAMPTZ, arrays
│       ├── mysql.py        # MySQL: JSON, DATETIME
│       ├── snowflake.py    # Snowflake: VARIANT, TIMESTAMP_TZ
│       └── generic.py      # Safe fallback for unknown DBs
├── source/                  # Source connectors (data extraction)
│   ├── connectors/         # Protocol implementations
│   │   ├── base.py         # BaseConnector interface
│   │   ├── api.py          # API connector with validation & deduplication
│   │   └── database.py     # Database connector using driver delegation
│   └── drivers/            # Database-specific drivers
│       ├── base.py         # BaseDatabaseDriver ABC
│       ├── postgresql.py   # PostgreSQL driver
│       └── factory.py      # Driver factory
├── destination/             # gRPC destination service components
│   ├── base_handler.py     # Abstract handler interface
│   ├── server.py           # gRPC server implementation
│   ├── connectors/         # Destination handler implementations
│   │   ├── database.py     # SQLAlchemy-based handler (PostgreSQL, MySQL, SQLite)
│   │   ├── file.py         # File destination (local, S3)
│   │   ├── api.py          # REST API destination
│   │   └── stream.py       # Stdout destination (testing)
│   ├── formatters/         # Output format serializers
│   │   ├── jsonl.py        # JSON Lines formatter
│   │   ├── csv.py          # CSV formatter
│   │   └── parquet.py      # Parquet formatter (requires -E analytics)
│   ├── storage/            # Storage backends
│   │   └── local.py        # Local filesystem storage
│   └── idempotency/        # Idempotency tracking
│       └── manifest.py     # File-based manifest tracker
├── engine/                  # Core framework components (renamed from core/)
│   ├── engine.py           # StreamingEngine with gRPC support
│   ├── orchestrator.py     # PipelineOrchestrator for concurrent execution
│   ├── pipeline.py         # High-level configuration interface
│   ├── exceptions.py       # Enhanced exception hierarchy
│   ├── credentials.py      # Secure authentication management
│   └── data_transformer.py # Field mapping and transformations
├── state/                   # Fault tolerance (renamed from fault_tolerance/)
│   ├── state_manager.py    # Per-partition state management
│   ├── retry_handler.py    # Exponential backoff retry logic
│   ├── circuit_breaker.py  # Failure detection/recovery
│   └── dead_letter_queue.py # Poison record isolation
├── grpc/                    # gRPC client and protocol
│   ├── client.py           # DestinationGRPCClient for engine
│   ├── cursor.py           # Opaque cursor utilities
│   └── generated/          # Generated protobuf code
├── models/                  # Pydantic v2 validation models
│   ├── state.py            # State management models
│   ├── engine.py           # Engine configuration models
│   ├── api.py              # API connector models
│   └── metrics.py          # Metrics and monitoring models
├── schema/                  # Schema management and evolution
│   └── schema_manager.py   # Schema drift detection
├── main.py                  # Unified entrypoint (RUN_MODE dispatch)
└── runner.py                # Pipeline runner

proto/                      # Protocol Buffer definitions
└── analitiq/v1/
    ├── stream.proto       # Message definitions
    └── destination_service.proto  # gRPC service definition

docker/
├── docker-compose.yml     # Engine + Destination + PostgreSQL
├── entrypoint.py          # Container entrypoint
└── .env.example           # Environment template

tests/
├── unit/
│   ├── grpc_tests/        # gRPC client and cursor tests
│   └── ...
├── integration/           # Integration tests
└── e2e/                   # End-to-end tests
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

## gRPC Streaming Architecture

Analitiq Stream supports decoupled destination services via gRPC bidirectional streaming. This enables:

- **Independent scaling** of engine and destination services
- **Easy addition** of new destinations (MySQL, Snowflake, etc.) without engine changes
- **Language-agnostic** destinations (any language with gRPC support)
- **Container isolation** with separate failure domains

### Dual-Mode Docker Image

The same Docker image runs as either engine or destination based on `RUN_MODE`:

```bash
# Engine mode (default) - runs data pipeline
RUN_MODE=engine python -m src.main

# Destination mode - runs gRPC server
RUN_MODE=destination python -m src.main
```

### Architecture Overview

```
+-------------------------------------------------------------+
|                    ENGINE CONTAINER                          |
|  Extract -> Transform -> GrpcLoadStage -> Checkpoint        |
|                    DestinationGRPCClient                    |
+-----------------------------+-------------------------------+
                              | gRPC Bidirectional Stream
+-----------------------------+-------------------------------+
|               DESTINATION CONTAINER                          |
|                    DestinationGRPCServer                    |
|              BaseDestinationHandler (abstract)              |
|         PostgreSQLHandler / MySQLHandler / etc.             |
+-------------------------------------------------------------+
```

### Key Protocol Features

| Feature | Description |
|---------|-------------|
| **Opaque Cursor** | Engine produces cursor, destination stores/returns it unchanged |
| **Batch Idempotency** | `(run_id, stream_id, batch_seq)` uniquely identifies each batch |
| **All-or-Nothing** | No partial success - entire batch succeeds or fails |
| **Record IDs** | Stable IDs for DLQ correlation across retries |
| **ACK Status** | SUCCESS, ALREADY_COMMITTED, RETRYABLE_FAILURE, FATAL_FAILURE |

### Quick Start with gRPC

```bash
# Start destination and database
cd docker
docker compose up -d postgres destination

# Run pipeline with gRPC destination
docker compose run -e PIPELINE_ID=<uuid> engine
```

### Environment Variables

**Engine Mode:**
| Variable | Default | Description |
|----------|---------|-------------|
| `DESTINATION_GRPC_HOST` | - | Hostname of destination service (enables gRPC mode) |
| `DESTINATION_GRPC_PORT` | `50051` | Port of destination service |
| `GRPC_TIMEOUT_SECONDS` | `300` | ACK timeout |
| `MAX_RETRIES` | `3` | Retries for RETRYABLE_FAILURE |

**Destination Mode:**
| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `50051` | Port to listen on |
| `CONNECTOR_TYPE` | `postgresql` | Handler type (postgresql, mysql, etc.) |
| `DB_HOST`, `DB_PORT`, etc. | - | Database connection details |

### Adding New Destinations

The destination system is **config-driven** - most new destinations require zero code:

| Destination Type | Code Required | What You Need |
|------------------|---------------|---------------|
| New SQL database (MySQL, SQLite, etc.) | 0 lines | Connection config with `connector_type: "db"` |
| New API endpoint | 0 lines | Connection config with `connector_type: "api"` |
| New file output | 0 lines | Connection config with `connector_type: "file"` |
| New storage backend (GCS) | ~100 lines | Implement `BaseStorageBackend` |
| New output format (Avro) | ~50 lines | Implement `BaseFormatter` |

**Config-only example (MySQL):**
```json
{
  "connector_type": "db",
  "driver": "mysql",
  "host": "localhost",
  "port": 3306,
  "database": "mydb",
  "username": "${DB_USER}",
  "password": "${DB_PASSWORD}"
}
```

**For custom handlers**, implement `BaseDestinationHandler`:
```python
class CustomHandler(BaseDestinationHandler):
    @property
    def connector_type(self) -> str:
        return "custom"

    async def write_batch(self, run_id, stream_id, batch_seq, records, record_ids, cursor):
        # Implement idempotent batch write
        ...
```

**See [Destination Config](docs/DESTINATION_CONFIG.md) for full configuration reference.**
**See [gRPC Streaming Architecture](docs/GRPC_STREAMING_ARCHITECTURE.md) for protocol details.**

## Docker Deployment

This section covers Docker configuration for deploying Analitiq Stream pipelines in containers.

### Docker Files Overview

- `Dockerfile` - Multi-stage Docker build for the application
- `docker/docker-compose.yml` - Local development and testing configuration
- `docker/docker-compose.aws.yml` - AWS ECS-compatible configuration
- `docker/entrypoint.py` - Container entrypoint script for pipeline execution
- `docker/.env.example` - Example environment variables configuration

### Quick Start

#### Local Development

```bash
# Copy environment template
cp docker/.env.example docker/.env

# Edit base environment variables (AWS config, table names, etc.)
nano docker/.env

# Build the image (for ARM machines like Apple Silicon, specify platform)
docker buildx build --platform linux/amd64 -t analitiq-stream:latest .

# Or use docker compose
docker compose -f docker/docker-compose.yml build

# Run with per-invocation variables
docker compose -f docker/docker-compose.yml run --rm \
  -e PIPELINE_ID=your-pipeline-uuid \
  analitiq-stream
```

#### Using dev.env for AWS Development

```bash
# Copy dev.env to .env for AWS dev environment
cp docker/dev.env docker/.env

# Run with per-invocation variables
docker compose -f docker/docker-compose.yml run --rm \
  -e PIPELINE_ID=your-pipeline-uuid \
  analitiq-stream
```

#### AWS ECS Simulation

```bash
# Use AWS-compatible configuration
docker compose -f docker/docker-compose.aws.yml run --rm \
  -e PIPELINE_ID=your-pipeline-uuid \
  analitiq-stream
```

### Environment Variables

Environment variables are split into two categories:

#### Base Variables (set in .env file or ECS Task Definition)

| Variable | Required | Default                | Description                                    |
|----------|----------|------------------------|------------------------------------------------|
| `ENV` | Yes | `local`                | Deployment environment: `local`, `dev`, `prod` |
| `AWS_REGION` | No | `eu-central-1`         | AWS region for all services                    |
| `LOCAL_CONFIG_MOUNT` | No | `/config`              | Local mount point for configs                  |
| `LOG_LEVEL` | No | `INFO`                 | Logging level                                  |
| `PIPELINES_TABLE` | Yes (dev/prod) | `pipelines`            | DynamoDB table for pipeline configs            |
| `CONNECTIONS_TABLE` | Yes (dev/prod) | `connections`          | DynamoDB table for client connections          |
| `CONNECTORS_TABLE` | Yes (dev/prod) | `connectors`           | DynamoDB table for connector definitions       |
| `ENDPOINTS_TABLE` | Yes (dev/prod) | `connectors_endpoints` | DynamoDB table for connector endpoints         |
| `STREAMS_TABLE` | Yes (dev/prod) | `streams`              | DynamoDB table for streams                     |

#### Per-Invocation Variables (passed at runtime)

| Variable | Required | Description |
|----------|----------|-------------|
| `PIPELINE_ID` | Yes | UUID of the pipeline to execute |

### Configuration Sources

#### Local Mode (`ENV=local`)
- Configurations loaded from mounted volume at `/config`
- Directory structure: `/config/pipelines/`, `/config/connections/`, `/config/connectors_endpoints/`
- Secrets loaded from environment variables

#### AWS Mode (`ENV=dev` or `ENV=prod`)
- Configurations loaded from DynamoDB tables
- Secrets loaded from S3 bucket
- AWS credentials from IAM roles (in ECS) or environment variables

### Resource Configuration

Docker Compose configurations include resource limits that mirror ECS task definitions:

- **CPU**: 0.5-1.0 cores (512-1024 CPU units in ECS)
- **Memory**: 1-2 GB RAM
- **Storage**: Persistent volumes for state, logs, and dead letter queue

### Volume Management

Persistent data is stored in named volumes:
- `analitiq_state`: Pipeline state and checkpoints
- `analitiq_logs`: Application logs
- `analitiq_deadletter`: Failed records for manual review

In ECS, these would typically map to EFS volumes for shared access across tasks.

### Health Checks

The container includes health checks that verify:
- Python environment is working
- Core modules can be imported
- Basic configuration validation

### Logging

- **Local**: JSON file logs with rotation
- **AWS**: CloudWatch logs integration
- **Format**: Structured JSON logs with timestamps, levels, and context

### Debug Mode

Run with debug logging:
```bash
docker compose -f docker/docker-compose.yml run --rm \
  -e PIPELINE_ID=your-pipeline-uuid \
  -e LOG_LEVEL=DEBUG \
  analitiq-stream
```

### Security Considerations

- Container runs as non-root user (`appuser`)
- Secrets should be passed via environment variables in ECS task definitions
- Use IAM roles instead of access keys in ECS
- Network policies should restrict outbound connections to required services

### Troubleshooting

**Configuration Not Found**
- Verify `PIPELINE_ID` exists in the specified config source
- Check mount paths for local mode
- Verify S3 bucket access for AWS mode

**AWS Credentials**
- Ensure IAM roles are properly configured for ECS
- For local AWS testing, verify AWS credentials are set

**Network Connectivity**
- Verify external API endpoints are accessible
- Check security groups and NAT gateway configuration for ECS

## AWS Deployment

Infrastructure (Batch compute environment, job queue, IAM roles) is managed by the Terraform team. This section covers Docker image deployment and integration.

### Build and Push Docker Image

**Important:** When building on Apple Silicon (M1/M2/M3) or other ARM-based machines, you must specify the target platform for AWS compatibility:

```bash
# Build for linux/amd64 (required for AWS ECS/Batch)
docker buildx build --platform linux/amd64 -t analitiq-stream:latest .

# Login to ECR
AWS_PROFILE=your-profile aws ecr get-login-password --region eu-central-1 | \
    docker login --username AWS --password-stdin <account-id>.dkr.ecr.eu-central-1.amazonaws.com

# Tag and push
docker tag analitiq-stream:latest <account-id>.dkr.ecr.eu-central-1.amazonaws.com/analitiq-stream:latest
docker push <account-id>.dkr.ecr.eu-central-1.amazonaws.com/analitiq-stream:latest
```

**Using deploy script:**

```bash
# Deploy to dev (default)
./scripts/deploy-ecr.sh

# Deploy to dev with specific tag
./scripts/deploy-ecr.sh dev v1.2.3

# Deploy to prod
./scripts/deploy-ecr.sh prod latest
```

ECR repository naming convention: `analitiq-stream-{env}` (e.g., `analitiq-stream-dev`, `analitiq-stream-prod`)

### Container Requirements for Batch Job Definition

The Terraform team needs to create a Batch Job Definition with these specifications:

**Runtime Environment Variables (via `containerOverrides`):**

| Variable | Required | Description |
|----------|----------|-------------|
| `PIPELINE_ID` | Yes | UUID of the pipeline to execute |

**Static Environment Variables (baked into job definition):**

| Variable            | Required | Description | Example                |
|---------------------|----------|-------------|------------------------|
| `ENV`               | Yes | Environment name | `dev`, `prod`          |
| `AWS_REGION`        | No | AWS region (default: eu-central-1) | `eu-central-1`         |
| `PIPELINES_TABLE`   | Yes | DynamoDB table for pipeline configs | `pipelines`            |
| `CONNECTIONS_TABLE` | Yes | DynamoDB table for client connections | `client_connections`   |
| `CONNECTORS_TABLE`  | Yes | DynamoDB table for connector definitions | `connectors`           |
| `ENDPOINTS_TABLE`   | Yes | DynamoDB table for service endpoints | `connectors_endpoints` |
| `STREAMS_TABLE`     | Yes | DynamoDB table for service endpoints | `streams`              |
| `LOG_LEVEL`         | No | Logging verbosity (default: INFO) | `INFO`                 |

S3 bucket names are automatically constructed as `analitiq-{purpose}-{env}` (e.g., `analitiq-secrets-dev`, `analitiq-client-pipeline-state-dev`).

**Resource Requirements:**

| Resource | Recommended |
|----------|-------------|
| vCPU | 0.5 - 1.0 |
| Memory | 1024 - 2048 MB |
| Timeout | 3600 seconds |

**IAM Permissions Required (Task Role):**

- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on secrets, state, logs, and DLQ buckets
- `dynamodb:GetItem`, `dynamodb:Query`, `dynamodb:Scan` on `pipelines`, `connections`, `connectors`, `connectors_endpoints` tables
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

### Invoking the Container

The Lambda that triggers pipeline runs should call `batch:SubmitJob` with:

- **Job Queue**: Existing Terraform-managed queue (e.g., `analitiq-pipelines`)
- **Job Definition**: Points to this container image
- **Container Overrides**: Pass `PIPELINE_ID` as environment variable

Example:
```python
import boto3

batch = boto3.client('batch', region_name='eu-central-1')

batch.submit_job(
    jobName='pipeline-run-2025-01-15',
    jobQueue='analitiq-pipelines',
    jobDefinition='analitiq-stream-dev',
    containerOverrides={
        'environment': [
            {'name': 'PIPELINE_ID', 'value': 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'}
        ]
    }
)
```

### Production Deployment (ECS)

For production ECS deployment, the `docker/docker-compose.aws.yml` serves as a reference. Create an ECS task definition with:

1. **Container Definition**:
   - Image: `analitiq-stream:latest`
   - CPU: 1024 units
   - Memory: 2048 MB
   - Base environment variables (ENV, AWS_REGION, table names) in task definition

2. **Container Overrides** (per-invocation):
   - Pass `PIPELINE_ID` when starting tasks via RunTask API or EventBridge

3. **IAM Role**: Attach policy with permissions for:
   - DynamoDB: Read access to configuration tables
   - S3: Read/write access to secrets, state, logs, and DLQ buckets
   - CloudWatch: Write access for logging

4. **VPC Configuration**: Deploy in private subnets with NAT gateway for external API access

5. **Storage**: Use EFS volumes for persistent state if needed

### Local Docker Testing

```shell
docker run --rm -it \
    -e ENV=dev \
    -e PIPELINE_ID=a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
    -e AWS_REGION=eu-central-1 \
    -e AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id)" \
    -e AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key)" \
    -e AWS_SESSION_TOKEN="$(aws configure get aws_session_token)" \
    -e LOG_LEVEL=DEBUG \
    -v analitiq_state:/app/state \
    -v analitiq_logs:/app/logs \
    -v analitiq_deadletter:/app/deadletter \
    analitiq-stream:latest
```