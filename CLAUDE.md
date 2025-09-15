# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Analitiq Stream is a high-performance, fault-tolerant data streaming framework for Python 3.11+ that enables reliable data movement between various sources and destinations. The framework uses Poetry for dependency management and provides async/await streaming with fault tolerance features.

## Configuration Management

The framework uses a modular configuration approach where pipeline configuration is split into separate JSON files for better organization and reusability:

### UUID-Based Configuration
```python
import json
from analitiq_stream import Pipeline

# Load pipeline configuration (contains UUID references to source/destination)
with open("pipeline_config.json") as f:
    pipeline_config = json.load(f)

# Optional: Load additional configurations
with open("validation_config.json") as f:
    validation_config = json.load(f)

# Create pipeline with UUID-based configuration
# Source and destination configurations loaded automatically from UUID files
pipeline = Pipeline(
    pipeline_config=pipeline_config,
    validation_config=validation_config # Optional
)
```

### Credentials File Formats

**Database credentials** (`src_credentials.json`):
```json
{
  "host": "localhost",
  "port": 5432,
  "database": "analytics",
  "user": "postgres", 
  "password": "${DB_PASSWORD}",
  "driver": "postgresql"
}
```

**API credentials** (`dst_credentials.json`):
```json
{
  "base_url": "https://api.example.com",
  "auth": {
    "type": "bearer_token",
    "token": "${API_TOKEN}"
  },
  "headers": {
    "Content-Type": "application/json"
  },
  "rate_limit": {
    "max_requests": 100,
    "time_window": 60
  }
}
```

**Note**: Rate limiting should be configured in API credentials files as it applies to the entire API connection, not individual endpoints.

### Environment Variable Expansion
- Credentials support `${VAR_NAME}` syntax for environment variables
- Variables are expanded at runtime for security
- Use this pattern for sensitive values like passwords and tokens

### Generate Templates
```bash
python examples/generate_credentials_template.py database src_credentials.json
python examples/generate_credentials_template.py api dst_credentials.json
```

## Development Commands

### Environment Setup
```bash
# Install dependencies
poetry install

# Install with optional extras
poetry install -E "kafka cloud analytics"

# Activate virtual environment
poetry shell
```

### Testing
```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=analitiq_stream --cov-report=html

# Run specific test file
poetry run pytest tests/test_engine.py

# Run tests by markers
poetry run pytest -m "not slow"
poetry run pytest -m "unit"
poetry run pytest -m "integration"
```

### Code Quality
```bash
# Format code
poetry run black analitiq_stream/
poetry run isort analitiq_stream/

# Type checking
poetry run mypy analitiq_stream/

# Linting
poetry run flake8 analitiq_stream/

# Run all pre-commit hooks
poetry run pre-commit run --all-files
```

### Installation Commands
```bash
# Install pre-commit hooks
pre-commit install
```

### Running Examples and Testing
```bash
# Test credentials functionality (custom test script)
python test_credentials.py

# Run the basic pipeline example
python analitiq_stream/examples/basic-pipeline/simple_pipeline.py

# Run the Wise to SevDesk example pipeline
python analitiq_stream/examples/wise_to_sevdesk/run_wise_to_sevdesk.py

# Generate credential templates
python examples/generate_credentials_template.py database src_credentials.json
python examples/generate_credentials_template.py api dst_credentials.json
```

**Note**: All pipelines use centralized directories at the project root:
- `state/` - Pipeline state and checkpoints
- `logs/` - Structured logging by pipeline ID
- `deadletter/` - Failed records organized by pipeline ID

## Project Structure

```
analitiq_stream/
├── core/                    # Core framework components
│   ├── engine.py           # StreamingEngine - main ETL pipeline processor
│   ├── pipeline.py         # Pipeline - high-level configuration interface
│   └── credentials.py      # CredentialsManager - secure authentication
├── connectors/             # Data source/destination connectors
│   ├── base.py            # BaseConnector - abstract interface
│   ├── api.py             # REST API connectors with auth support
│   └── database/          # Database connector with driver delegation
│       ├── database_connector.py  # Generic database connector
│       ├── base_driver.py         # Abstract database driver interface
│       ├── postgresql_driver.py   # PostgreSQL-specific implementation
│       └── driver_factory.py      # Driver factory pattern
├── fault_tolerance/        # Comprehensive fault tolerance system
│   ├── sharded_state_manager.py  # Sharded checkpoint persistence
│   ├── retry_handler.py   # Exponential backoff retry logic
│   ├── circuit_breaker.py # Automatic failure detection/recovery
│   └── dead_letter_queue.py # Poison record isolation
├── schema/                 # Schema management and evolution
│   └── schema_manager.py  # Schema drift detection with hash comparison
├── config.py              # Centralized directory configuration
└── examples/              # Working examples and utilities
    ├── basic-pipeline/    # Simple database to API sync
    ├── wise_to_sevdesk/   # Real-world API integration example
    ├── wise_to_postgres/  # Database integration with auto-creation
    └── generate_credentials_template.py # Credential template generator
```

## Architecture

The framework follows a modular architecture with clear separation of concerns:

### Core Components

**StreamingEngine** (`analitiq_stream/core/engine.py`):
- Main async streaming engine with ETL pipeline stages
- Implements extract → transform → load → checkpoint pattern
- Handles backpressure with configurable batch sizes and concurrent processing
- Integrates all fault tolerance components

**Pipeline** (`analitiq_stream/core/pipeline.py`):
- High-level interface for pipeline configuration and execution
- Loads configuration from JSON files or dictionaries
- Validates pipeline configuration and orchestrates engine execution

**BaseConnector** (`analitiq_stream/connectors/base.py`):
- Abstract base class defining connector interface
- Provides common functionality for metrics, health checks, and connection management
- Concrete implementations for database and API connectors

**DatabaseConnector** (`analitiq_stream/connectors/database/database_connector.py`):
- Generic database connector using driver delegation pattern
- Database-agnostic interface with database-specific implementations in drivers
- Auto-creation of schemas, tables, and indexes via `configure` section
- Supports PostgreSQL, MySQL, and other databases through driver pattern
- Pydantic V2 validation for configuration safety

**Database Driver Architecture**:
- **BaseDatabaseDriver** (`analitiq_stream/connectors/database/base_driver.py`): Abstract interface defining driver contract
- **PostgreSQLDriver** (`analitiq_stream/connectors/database/postgresql_driver.py`): PostgreSQL-specific implementation with connection pooling and advanced type mapping
- **DriverFactory** (`analitiq_stream/connectors/database/driver_factory.py`): Factory for creating database drivers dynamically

### Fault Tolerance System

The framework implements comprehensive fault tolerance through multiple components:

- **ShardedStateManager**: Persists sharded per-stream/partition checkpoints for scalable recovery
- **RetryHandler**: Exponential backoff retry logic for transient failures
- **CircuitBreaker**: Prevents cascading failures with automatic recovery
- **DeadLetterQueue**: Isolates poison records for separate handling
- **SchemaManager**: Detects schema drift and manages evolution

### Centralized Directory Structure

All pipeline artifacts are stored in centralized locations at the project root:

```
project-root/
├── state/           # Centralized state management for all pipelines
│   └── {pipeline_id}/
│       └── v1/
│           ├── index.json
│           └── streams/
│               └── stream.{stream_id}/
│                   └── partition-default.json
├── logs/            # Centralized logging for all pipelines  
│   └── {pipeline_id}/
│       ├── pipeline.log
│       └── {stream_id}/
│           └── stream.log
└── deadletter/      # Centralized dead letter queue for failed records
    └── {pipeline_id}/
        └── {stream_id}/
            └── dlq_*.jsonl
```

### Data Flow

1. **Extract Stage**: Reads data from source connectors in configurable batches
2. **Transform Stage**: Applies field mappings, value transformations, and computed fields
3. **Load Stage**: Writes to destination with fault tolerance (retry, circuit breaker, DLQ)
4. **Checkpoint Stage**: Saves progress state for recovery

### Modular Configuration Structure

The framework supports splitting configuration into separate, focused files:

#### 1. Pipeline Configuration (`pipeline_config.json`)
```json
{
  "pipeline_id": "my-pipeline",
  "name": "Pipeline Description",
  "version": "1.0",
  "engine_config": {
    "batch_size": 1000,
    "max_concurrent_batches": 10,
    "buffer_size": 10000,
    "state_file": "state/pipeline_state.json",
    "schedule": {
      "type": "interval",
      "interval_minutes": 60,
      "timezone": "UTC"
    }
  },
  "src": {
    "endpoint_id": "fe38a2f0-c1e3-4e65-a0f1-227b9898a8b8",
    "host_id": "0e8b1731-479a-4bc0-b056-244cc5d6a53c",
    "replication_key": "updatedAt",
    "cursor_mode": "inclusive",
    "safety_window_seconds": 120
  },
  "dst": {
    "endpoint_id": "1e63d782-4b67-4b7e-b845-4b4de5e4f46e",
    "host_id": "7c1a69eb-239f-45d4-b6c2-3ad4c6e89cfa",
    "refresh_mode": "upsert",
    "batch_support": false,
    "batch_size": 100,
    "replication_key": "updatedAt",
    "cursor_mode": "inclusive", 
    "safety_window_seconds": 120
  },
  "bookmarks": [
    {
      "partition": {},
      "cursor": "2025-08-14T11:58:03Z",
      "aux": { "last_id": 431245 }
    }
  ],
  "run": {
    "run_id": "2025-08-14T11:55:00Z-d04f",
    "currently_syncing_partition": {}
  },
  "mapping": {
    "transformations": [
      {
        "type": "field_mapping",
        "mappings": {
          "id": "user_id",
          "name": "full_name"
        }
      },
      {
        "type": "computed_field",
        "field": "sync_timestamp",
        "expression": "now()"
      }
    ]
  },
  "error_handling": {
    "strategy": "dlq",
    "retry_failed_records": true,
    "max_retries": 3
  },
  "monitoring": {
    "metrics_enabled": true,
    "log_level": "INFO",
    "checkpoint_interval": 100
  }
}
```

#### 2. Source Configuration (`source_config.json`)
```json
{
  "endpoint_id": "my-source",
  "type": "database",
  "refresh_mode": "incremental",
  "config": {
    "table": "users",
    "incremental_column": "updated_at"
  }
}
```

#### 3. Destination Configuration (`destination_config.json`)
```json
{
  "endpoint_id": "my-destination",
  "type": "api",
  "config": {
    "endpoint": "/users",
    "method": "POST"
  }
}
```

#### 4. Validation Configuration (`validation_config.json`) - Optional
```json
{
  "rules": [
    {
      "field": "user_id",
      "type": "not_null",
      "error_action": "dlq"
    }
  ]
}
```

#### UUID-Based Destination Configuration

The framework uses UUID-based files to separate endpoint schemas from host credentials:

**API Endpoint Schema File** (`ep_{endpoint_id}.json`):
```json
{
  "endpoint": "/api/v1/CheckAccountTransaction",
  "method": "POST",
  "response_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "API Response Schema",
    "type": "object",
    "properties": { ... }
  }
}
```

**Database Endpoint Schema File** (`ep_{endpoint_id}.json`):
```json
{
  "schema": "wise_data",
  "table": "transactions",
  "primary_key": ["wise_id"],
  "unique_constraints": ["wise_id"],
  "write_mode": "upsert",
  "conflict_resolution": {
    "on_conflict": "wise_id",
    "action": "update",
    "update_columns": ["amount", "status", "updated_at"]
  },
  "configure": {
    "auto_create_schema": true,
    "auto_create_table": true,
    "auto_create_indexes": [
      {
        "name": "idx_transactions_created_at",
        "columns": ["created_at"],
        "type": "btree"
      }
    ]
  },
  "table_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Database Table Schema",
    "type": "object",
    "properties": {
      "wise_id": {
        "type": "integer",
        "database_type": "BIGINT",
        "nullable": false,
        "primary_key": true
      },
      "created_at": {
        "type": "string",
        "format": "date-time",
        "database_type": "TIMESTAMPTZ",
        "nullable": false
      }
    },
    "required": ["wise_id", "created_at"]
  }
}
```

**API Host Credentials File** (`hst_{host_id}.json`):
```json
{
  "base_url": "https://my.sevdesk.de",
  "headers": {
    "Content-Type": "application/json",
    "Authorization": "${API_TOKEN}"
  },
  "rate_limit": {
    "max_requests": 10,
    "time_window": 60
  }
}
```

**Database Host Credentials File** (`hst_{host_id}.json`):
```json
{
  "driver": "postgresql",
  "host": "localhost",
  "port": 5432,
  "database": "analytics",
  "user": "postgres",
  "password": "${DB_PASSWORD}",
  "ssl_mode": "prefer",
  "connection_pool": {
    "min_connections": 2,
    "max_connections": 10,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 3600,
    "pool_pre_ping": true
  }
}
```

### Incremental Replication Configuration

Both `src` and `dst` sections support incremental replication parameters for efficient data synchronization:

#### `replication_key`: "updatedAt"
- **Purpose**: The source field used to order records and track synchronization progress
- **Function**: Enables fetching "only new or changed rows" instead of re-reading all data
- **Common choices**: 
  - Last-modified timestamp: `updatedAt`, `modified`, `lastChanged`
  - Strictly increasing ID: `id`, `sequence_number`
- **Best practice**: If timestamps aren't unique, pair with a tie-breaker field (e.g., `(updatedAt, id)`)

#### `cursor_mode`: "inclusive" | "exclusive" 
- **Purpose**: Controls whether the next synchronization includes or excludes the record at the stored cursor value
- **Options**:
  - **"inclusive" (>=)**: Safer when timestamps can repeat or arrive late. May re-read the last record once; relies on upsert/idempotency to avoid duplicates
  - **"exclusive" (>)**: Fewer duplicates but riskier if multiple rows share the exact cursor value (could skip records)
- **SQL Examples**:
  - Inclusive: `WHERE updatedAt >= '2025-08-14T11:58:03Z'`
  - Exclusive: `WHERE updatedAt > '2025-08-14T11:58:03Z'`
- **Recommendation**: Use "inclusive" with a tie-breaker field for maximum data integrity

#### `safety_window_seconds`: 120
- **Purpose**: Time offset subtracted from stored cursor before next synchronization run
- **Why needed**: Protects against clock skew, eventual consistency, and late-arriving writes
- **How applied**: If saved cursor is `2025-08-14T11:58:03Z` and window is `120`, next query uses `2025-08-14T11:56:03Z`
- **Trade-off**: Intentionally re-reads a small overlap to prevent data loss; handle duplicates via upsert/idempotency or deduplication cache
- **Typical values**: 60-300 seconds depending on source system characteristics

#### `bookmarks`: Array of checkpoint objects
- **Purpose**: Tracks the last known position(s) within the data stream for resumable incremental synchronization
- **Structure**: Each bookmark contains `partition`, `cursor`, and optional `aux` data
- **Why it's an array**: Supports partitioned data streams where different subsets progress independently
- **Use cases**:
  - **Single partition**: `[{"partition": {}, "cursor": "2025-08-14T11:58:03Z"}]` for entire stream
  - **Account-based partitions**: `[{"partition": {"account_id": "123"}, "cursor": "..."}]`
  - **Regional partitions**: `[{"partition": {"region": "EU"}, "cursor": "..."}]`
- **Fields**:
  - `partition`: Object identifying the data subset (empty `{}` for single partition)
  - `cursor`: Last processed value of the replication_key (timestamp, ID, etc.)
  - `aux`: Optional auxiliary data like tie-breakers (`{"last_id": 431245}`)

#### `run`: Current execution state
- **Purpose**: Stores ephemeral information about the current or last pipeline execution
- **Lifecycle**: Short-lived, tied to one execution cycle (unlike persistent bookmarks)
- **Use case**: Enables mid-execution recovery if pipeline is interrupted
- **Fields**:
  - `run_id`: Unique identifier for the current execution (e.g., `"2025-08-14T11:55:00Z-d04f"`)
  - `currently_syncing_partition`: Partition being processed when state was last updated
- **Recovery**: On restart, pipeline can resume from the interrupted partition without reprocessing completed ones

#### Important Configuration Notes:
- **Centralized Directories**: All pipeline artifacts stored at project root:
  - **DLQ Path**: `deadletter/{pipeline_id}/`
  - **State Path**: `state/{pipeline_id}/v1/` (sharded per stream/partition)  
  - **Logs Path**: `logs/{pipeline_id}/`
- **Schedule**: Defined in pipeline config (applies to entire pipeline)
- **Error Handling & Monitoring**: Part of pipeline-level configuration
- **Source Configuration**: Automatically loaded from UUID-based files:
  - `src.endpoint_id`: UUID referencing `ep_{endpoint_id}.json` for source schema
  - `src.host_id`: UUID referencing `hst_{host_id}.json` for source connection details
- **Destination Configuration**: Automatically loaded from UUID-based files:
  - `dst.endpoint_id`: UUID referencing `ep_{endpoint_id}.json` for destination schema  
  - `dst.host_id`: UUID referencing `hst_{host_id}.json` for destination connection details
- **Destination Behavior**: Controlled by fields in pipeline_config.json's `dst` section:
  - `refresh_mode`: Controls write behavior (`insert`, `upsert`, `truncate_insert`)
  - `batch_support`: Boolean flag for API batch vs individual record sending
  - `batch_size`: Destination-specific batch size for processing
- **Database Auto-Configuration**: Controlled by `configure` section in endpoint files:
  - `auto_create_schema`: Boolean flag to auto-create database schema
  - `auto_create_table`: Boolean flag to auto-create database table
  - `auto_create_indexes`: Array of index definitions to auto-create
  - **If `configure` section is missing**: System assumes schema/table/indexes already exist
- **Data Mapping**: Defined in pipeline_config.json's `mapping` section for field transformations and computed fields

### Key Design Patterns

- **Async/Await**: All I/O operations are non-blocking
- **Producer-Consumer**: Stages communicate via async queues
- **Factory Pattern**: Connectors are created based on configuration type
- **Driver Delegation**: Database connector delegates database-specific operations to driver implementations
- **Context Managers**: Proper resource cleanup for connectors
- **State Persistence**: Sharded checkpoints enable crash recovery and incremental processing
- **Pydantic V2 Validation**: Configuration safety with comprehensive type checking

## Testing Configuration

The project uses pytest with the following markers:
- `slow`: Long-running tests (deselect with `-m "not slow"`)
- `integration`: Integration tests requiring external services
- `unit`: Isolated unit tests

Test files follow the pattern `test_*.py` or `*_test.py` in the `tests/` directory.
- You are a senior Python 3.11 engineer and integration-framework architect working on a platfrom for Data Integration (similar in spirit to Airbyte/Singer: sources, destinations, taps/targets, incremental syncs, schemas, and state). Write modular, pythonic code. Strong input validation (JSON Schema or Pydantic v2) and safe deserialization. Deterministic behavior, idempotent writes, safe retries, and consistent state checkpoints. Explicit error paths; use ExceptionGroup and except* where helpful (Py 3.11).
- When making code changes, no need to consider backward compatibility.
- Update documentation when implementiing new features or modifying existing key functionality that previolsy documented in README.md
- Do not keep code for Backward Compatibility. Do not create code that is needed only for Backward Compatibility.
- Do not use empticons in code. Do not use emojis in code.