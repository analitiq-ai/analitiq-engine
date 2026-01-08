# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Analitiq Stream is a high-performance, fault-tolerant data streaming framework for Python 3.11+ that enables reliable data movement between various sources and destinations. The framework uses Poetry for dependency management and provides async/await streaming with fault tolerance features.

## Configuration Management

Configuration is ALWAYS loaded from local filesystem, regardless of environment. The framework uses a modular approach where pipeline configuration is split into separate JSON files.

### Configuration Loading Architecture

```
LOCAL ENVIRONMENT                    CLOUD ENVIRONMENT (dev/prod)
==================                   ==============================
                                     1. config_fetcher.py runs first
                                        - Fetches from DynamoDB/S3
                                        - Writes to LOCAL_CONFIG_MOUNT

2. PipelineConfigPrep                2. PipelineConfigPrep
   - Reads from LOCAL_CONFIG_MOUNT      - Reads from LOCAL_CONFIG_MOUNT
   - Same code path for all envs        - Same code path for all envs
```

**Key Points:**
- `PipelineConfigPrep` always reads from local filesystem
- In cloud environments, `config_fetcher.py` pre-populates local files from DynamoDB/S3
- `ENV` variable only affects storage backends (state, logs, DLQ, metrics), NOT config loading
- Connectors are synced from GitHub and loaded from local `connectors/` directory

### Configuration Directory Structure

```
{local_config_mount}/
├── pipelines/{pipeline_id}.json    # Pipeline configuration
├── streams/{stream_id}.json        # Stream configurations
├── .secrets/{connection_id}        # Secret files (JSON)
└── connectors/                     # Synced from GitHub
    └── {connector_name}/
        ├── connector.json          # Connector template
        └── endpoints/
            └── {endpoint}.json     # Endpoint schemas
```

### Using PipelineConfigPrep

```python
from src.core.pipeline_config_prep import PipelineConfigPrep, PipelineConfigPrepSettings

settings = PipelineConfigPrepSettings(
    env="local",  # or "dev", "prod" - only affects storage backends
    pipeline_id="my-pipeline-uuid",
    local_config_mount="/path/to/config"
)

prep = PipelineConfigPrep(settings)
pipeline_config, stream_configs, resolved_connections, resolved_endpoints = prep.create_config()
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

### Required Environment Variables

**All Environments (for PipelineConfigPrep):**
```bash
export ENV="local"                          # or "dev", "prod" - affects storage backends only
export PIPELINE_ID="your-pipeline-uuid"     # Required
export LOCAL_CONFIG_MOUNT="/path/to/config" # Where configs are loaded from
export CLIENT_ID="your-client-uuid"         # Required for cloud storage paths
```

**Cloud Storage Backends (for state, logs, DLQ, metrics):**
```bash
# AWS region for S3 storage
export AWS_REGION="eu-central-1"

# S3 buckets (auto-constructed as analitiq-{purpose}-{ENV} if not specified)
export STATE_BUCKET="analitiq-client-pipeline-state-dev"
export LOGS_BUCKET="analitiq-client-pipeline-logs-dev"
export DLQ_BUCKET="analitiq-client-pipeline-deadletter-dev"
export ROW_COUNT_BUCKET="analitiq-client-pipeline-row-count"
```

**For config_fetcher.py only (cloud deployments):**
```bash
# DynamoDB table names - only needed by config_fetcher.py
export PIPELINES_TABLE="pipelines"
export STREAMS_TABLE="streams"
export CONNECTIONS_TABLE="connections"
export CONNECTORS_TABLE="connectors"
export ENDPOINTS_TABLE="connectors_endpoints"
export SECRETS_BUCKET="analitiq-secrets-dev"
```

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
poetry run pytest --cov=src --cov-report=html

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
poetry run black src/
poetry run isort src/

# Type checking
poetry run mypy src/

# Linting
poetry run flake8 src/

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
# Run the Wise to SevDesk example pipeline (API to API)
python examples/wise_to_sevdesk/run_wise_to_sevdesk.py

# Run the Wise to PostgreSQL example pipeline (API to Database)
python examples/wise_to_postgres/run_wise_to_postgres.py

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
src/
├── core/                    # Core framework components
│   ├── engine.py           # StreamingEngine - main ETL pipeline processor
│   ├── pipeline.py         # Pipeline - high-level configuration interface
│   ├── pipeline_config_prep.py # PipelineConfigPrep - multi-environment config loading
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
│   ├── state_manager.py  # Checkpoint persistence
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

**StreamingEngine** (`src/core/engine.py`):
- Main async streaming engine with ETL pipeline stages
- Implements extract → transform → load → checkpoint pattern
- Handles backpressure with configurable batch sizes and concurrent processing
- Integrates all fault tolerance components

**Pipeline** (`src/core/pipeline.py`):
- High-level interface for pipeline configuration and execution
- Loads configuration from JSON files or dictionaries
- Validates pipeline configuration and orchestrates engine execution

**BaseConnector** (`src/connectors/base.py`):
- Abstract base class defining connector interface
- Provides common functionality for metrics, health checks, and connection management
- Concrete implementations for database and API connectors

**DatabaseConnector** (`src/connectors/database/database_connector.py`):
- Generic database connector using driver delegation pattern
- Database-agnostic interface with database-specific implementations in drivers
- Auto-creation of schemas, tables, and indexes via `configure` section
- Supports PostgreSQL, MySQL, and other databases through driver pattern
- Pydantic V2 validation for configuration safety

**Database Driver Architecture**:
- **BaseDatabaseDriver** (`src/connectors/database/base_driver.py`): Abstract interface defining driver contract
- **PostgreSQLDriver** (`src/connectors/database/postgresql_driver.py`): PostgreSQL-specific implementation with connection pooling and advanced type mapping
- **DriverFactory** (`src/connectors/database/driver_factory.py`): Factory for creating database drivers dynamically

### Fault Tolerance System

The framework implements comprehensive fault tolerance through multiple components:

- **StateManager**: Persists per-stream/partition checkpoints for scalable recovery
- **RetryHandler**: Exponential backoff retry logic for transient failures
- **CircuitBreaker**: Prevents cascading failures with automatic recovery
- **DeadLetterQueue**: Isolates poison records for separate handling
- **SchemaManager**: Detects schema drift and manages evolution

### State Storage

The StateManager automatically selects the storage backend based on the `ENV` environment variable:

**Local Storage (`ENV=local`, default):**
- State stored in local filesystem at `state/{pipeline_id}/`
- Suitable for development and testing

**S3 Storage (`ENV=dev` or `ENV=prod`):**
- State stored in S3 bucket `analitiq-client-pipeline-state-{env}`
- Path pattern: `s3://{bucket}/{client_id}/{pipeline_id}/`
- S3 versioning can be enabled on the bucket for historical state tracking

**Required Environment Variables for S3 Storage:**
```bash
export ENV="dev"  # or "prod"
export PIPELINE_ID="your-pipeline-uuid"
export AWS_REGION="eu-central-1"
export STATE_BUCKET="analitiq-client-pipeline-state-dev"  # Optional, defaults to analitiq-client-pipeline-state-{ENV}
```

**S3 State Structure:**
```
s3://analitiq-client-pipeline-state-{env}/
  {client_id}/
    {pipeline_id}/
      state.json                        # Active state manifest
      streams/
        stream.{stream_id}/
          partition-default.json
```

**Viewing State History (requires S3 versioning):**
```bash
# List all versions of state.json
aws s3api list-object-versions \
  --bucket analitiq-client-pipeline-state-dev \
  --prefix "{client_id}/{pipeline_id}/state.json"

# Download a specific version
aws s3api get-object \
  --bucket analitiq-client-pipeline-state-dev \
  --key "{client_id}/{pipeline_id}/state.json" \
  --version-id "abc123" \
  old_state.json
```

### Log Storage

Pipeline logs are automatically stored based on the `ENV` environment variable:

**Local Storage (`ENV=local`, default):**
- Logs stored in local filesystem at `logs/{pipeline_id}/`
- Standard file-based logging

**S3 Storage (`ENV=dev` or `ENV=prod`):**
- Logs stored in S3 bucket `analitiq-client-pipeline-logs-{env}`
- Date-based partitioning for S3 lifecycle rules: `year=YYYY/month=MM/day=DD/`
- Buffered writes - logs are flushed to S3 at pipeline completion

**Required Environment Variables for S3 Logs:**
```bash
export ENV="dev"  # or "prod"
export PIPELINE_ID="your-pipeline-uuid"
export AWS_REGION="eu-central-1"
export LOGS_BUCKET="analitiq-client-pipeline-logs-dev"  # Optional, defaults to analitiq-client-pipeline-logs-{ENV}
```

**S3 Log Structure (optimized for lifecycle management):**
```
s3://analitiq-client-pipeline-logs-{env}/
  {client_id}/
    {pipeline_id}/
      year=2025/
        month=01/
          day=15/
            {run_id}_pipeline.log      # Pipeline-level logs
            {stream_id}/
              {run_id}_stream.log      # Stream-specific logs
```

**S3 Lifecycle Policy Example:**
```json
{
  "Rules": [
    {
      "ID": "ExpireOldLogs",
      "Status": "Enabled",
      "Filter": {
        "Prefix": ""
      },
      "Expiration": {
        "Days": 90
      }
    },
    {
      "ID": "TransitionToGlacier",
      "Status": "Enabled",
      "Filter": {
        "Prefix": ""
      },
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "GLACIER"
        }
      ]
    }
  ]
}
```

**Querying Logs:**
```bash
# List logs for a specific date
aws s3 ls s3://analitiq-client-pipeline-logs-dev/{client_id}/{pipeline_id}/year=2025/month=01/day=15/

# Download logs for a specific run
aws s3 cp s3://.../{pipeline_id}/year=2025/month=01/day=15/20250115T103000Z_pipeline.log ./

# Search logs across dates using S3 Select or Athena
```

### Dead Letter Queue (DLQ) Storage

Failed records are automatically stored based on the `ENV` environment variable:

**Local Storage (`ENV=local`, default):**
- DLQ stored in local filesystem at `deadletter/{pipeline_id}/`
- JSONL format with rotation based on file size

**S3 Storage (`ENV=dev` or `ENV=prod`):**
- DLQ stored in S3 bucket `analitiq-client-pipeline-deadletter-{env}`
- Date-based partitioning for S3 lifecycle rules: `year=YYYY/month=MM/day=DD/`
- Buffered writes for efficiency

**Required Environment Variables for S3 DLQ:**
```bash
export ENV="dev"  # or "prod"
export PIPELINE_ID="your-pipeline-uuid"
export AWS_REGION="eu-central-1"
export DLQ_BUCKET="analitiq-client-pipeline-deadletter-dev"  # Optional, defaults to analitiq-client-pipeline-deadletter-{ENV}
```

**S3 DLQ Structure (optimized for lifecycle management):**
```
s3://analitiq-client-pipeline-deadletter-{env}/
  {client_id}/
    {pipeline_id}/
      year=2025/
        month=01/
          day=15/
            {run_id}_dlq.jsonl           # Pipeline-level failed records
            {stream_id}/
              {run_id}_dlq.jsonl         # Stream-specific failed records
```

**DLQ Record Format:**
```json
{
  "id": "uuid",
  "pipeline_id": "pipeline-uuid",
  "original_record": { ... },
  "error": {
    "type": "ValueError",
    "message": "Invalid data format",
    "traceback": [ ... ]
  },
  "timestamp": "2025-01-15T10:30:00Z",
  "retry_count": 0,
  "additional_context": { ... }
}
```

**Querying DLQ:**
```bash
# List DLQ files for a specific date
aws s3 ls s3://analitiq-client-pipeline-deadletter-dev/{client_id}/{pipeline_id}/year=2025/month=01/day=15/

# Download DLQ for analysis
aws s3 cp s3://.../{pipeline_id}/year=2025/month=01/day=15/20250115T103000Z_dlq.jsonl ./

# Count failed records
cat 20250115T103000Z_dlq.jsonl | wc -l
```

### Row Count Metrics Storage (Athena-Queryable)

Pipeline execution metrics (row counts, duration, status) are stored in S3 with Hive-style partitioning optimized for AWS Athena queries.

**Local Storage (`ENV=local`, default):**
- Metrics stored in local filesystem at `metrics/client_id={client_id}/year=.../`
- JSON format with one file per pipeline run

**S3 Storage (`ENV=dev` or `ENV=prod`):**
- Metrics stored in S3 bucket `analitiq-client-pipeline-row-count`
- Hive-style partitioning for automatic Athena partition discovery
- One JSON file per pipeline execution

**Required Environment Variables:**
```bash
export ENV="dev"  # or "prod"
export PIPELINE_ID="your-pipeline-uuid"
export CLIENT_ID="your-client-uuid"
export AWS_REGION="eu-central-1"
export ROW_COUNT_BUCKET="analitiq-client-pipeline-row-count"  # Optional
```

**S3 Structure (optimized for Athena):**
```
s3://analitiq-client-pipeline-row-count/
  client_id={client_id}/
    year=2025/
      month=01/
        day=15/
          {run_id}.json
```

**Metrics Record Format:**
```json
{
  "run_id": "20250115T103000Z-abc12345",
  "pipeline_id": "pipeline-uuid",
  "pipeline_name": "Wise to SevDesk Sync",
  "client_id": "client-uuid",
  "start_time": "2025-01-15T10:30:00+00:00",
  "end_time": "2025-01-15T10:35:00+00:00",
  "duration_seconds": 300.5,
  "records_processed": 1500,
  "records_failed": 5,
  "records_total": 1505,
  "batches_processed": 15,
  "status": "partial",
  "error_message": null,
  "records_per_second": 4.99,
  "environment": "dev"
}
```

**Athena Table Creation:**
```sql
CREATE EXTERNAL TABLE pipeline_metrics (
  run_id STRING,
  pipeline_id STRING,
  pipeline_name STRING,
  start_time STRING,
  end_time STRING,
  duration_seconds DOUBLE,
  records_processed INT,
  records_failed INT,
  records_total INT,
  batches_processed INT,
  status STRING,
  error_message STRING,
  records_per_second DOUBLE,
  environment STRING
)
PARTITIONED BY (
  client_id STRING,
  year STRING,
  month STRING,
  day STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://analitiq-client-pipeline-row-count/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Load partitions
MSCK REPAIR TABLE pipeline_metrics;
```

**Example Athena Queries:**

1. Get all rows exported by day over a time period, for all pipelines:
```sql
SELECT
    year, month, day,
    SUM(records_processed) as total_records,
    COUNT(*) as pipeline_runs,
    SUM(records_failed) as total_failed
FROM pipeline_metrics
WHERE client_id = 'your-client-id'
  AND year = '2025'
  AND month IN ('01', '02', '03')
GROUP BY year, month, day
ORDER BY year, month, day;
```

2. Get all rows exported by a specific pipeline over a time period:
```sql
SELECT
    year, month, day,
    records_processed,
    records_failed,
    duration_seconds,
    status,
    start_time
FROM pipeline_metrics
WHERE client_id = 'your-client-id'
  AND pipeline_id = 'your-pipeline-id'
  AND year = '2025'
ORDER BY start_time DESC;
```

3. Get pipeline performance summary:
```sql
SELECT
    pipeline_id,
    pipeline_name,
    COUNT(*) as total_runs,
    SUM(records_processed) as total_records,
    AVG(records_per_second) as avg_throughput,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs
FROM pipeline_metrics
WHERE client_id = 'your-client-id'
  AND year = '2025'
GROUP BY pipeline_id, pipeline_name
ORDER BY total_records DESC;
```

### Centralized Directory Structure

All pipeline artifacts are stored in centralized locations at the project root:

```
project-root/
├── state/           # Centralized state management for all pipelines
│   └── {pipeline_id}/
│       ├── state.json
│       └── streams/
│           └── stream.{stream_id}/
│               └── partition-default.json
├── logs/            # Centralized logging for all pipelines
│   └── {pipeline_id}/
│       ├── pipeline.log
│       └── {stream_id}/
│           └── stream.log
├── deadletter/      # Centralized dead letter queue for failed records
│   └── {pipeline_id}/
│       └── {stream_id}/
│           └── dlq_*.jsonl
└── metrics/         # Pipeline execution metrics (row counts, duration, etc.)
    └── client_id={client_id}/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── {run_id}.json
```

### Data Flow

1. **Extract Stage**: Reads data from source connectors in configurable batches
2. **Transform Stage**: Applies field mappings, value transformations, and computed fields
3. **Load Stage**: Writes to destination with fault tolerance (retry, circuit breaker, DLQ)
4. **Checkpoint Stage**: Saves progress state for recovery

### Modular Configuration Structure

The framework supports splitting configuration into separate, focused files:

#### 1. Pipeline Configuration (`pipelines/{pipeline_id}.json`)

Pipeline configuration uses the new structure aligned with DynamoDB cloud storage:

```json
{
  "version": 1,
  "pipeline_id": "b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15",
  "client_id": "a4b2c3d4-g5f6-7890-fght-ef1234567890",
  "name": "Wise to SevDesk Integration",
  "description": "Sync Wise transfers to SevDesk",
  "status": "active",
  "is_active": true,
  "tags": ["finance", "wise", "sevdesk"],
  "connections": {
    "source": {
      "conn_wise": "0e8b1731-479a-4bc0-b056-244cc5d6a53c"
    },
    "destinations": [
      {
        "conn_sevdesk": "7c1a69eb-239f-45d4-b6c2-3ad4c6e89cfa"
      }
    ]
  },
  "runtime": {
    "expression": {
      "lang": "jsonata",
      "policy": { "max_depth": 20, "max_ops": 200, "max_string_len": 10000 }
    },
    "logging": {
      "log_level": "INFO",
      "metrics_enabled": true,
      "checkpoint_interval": 50
    },
    "error_handling": { "strategy": "dlq" },
    "retry": { "max_attempts": 5, "backoff": "exponential", "base_delay_ms": 500 },
    "batching": { "batch_size": 100, "max_concurrent_batches": 3 },
    "engine": { "buffer_size": 5000 },
    "schedule": { "type": "interval", "interval_minutes": 1440, "timezone": "UTC" }
  },
  "created_at": "2025-12-25T10:15:00Z",
  "updated_at": "2025-12-25T10:20:00Z"
}
```

**Key Pipeline Fields:**
- `version`: Integer (e.g., `1`), not string
- `is_active`: Boolean indicating if the pipeline is active (replaces `is_enabled`)
- `status`: String status like `"draft"`, `"active"`, `"paused"`
- `connections`: Nested structure with `source` and `destinations` arrays
  - Each connection maps an alias (e.g., `conn_wise`) to a connection UUID
  - Streams reference these aliases via `connection_ref`

#### 2. Stream Configuration (`streams/{stream_id}.json`)

Stream configuration defines how data flows from source to destination(s):

```json
{
  "version": 1,
  "stream_id": "f1a2b3c4-d5e6-7890-abcd-ef1234567891",
  "pipeline_id": "b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15",
  "client_id": "a4b2c3d4-g5f6-7890-fght-ef1234567890",
  "status": "active",
  "is_enabled": true,
  "source": {
    "connection_ref": "conn_wise",
    "endpoint_id": "5a4b9e21-441f-4bc7-9d5e-41917b4357e6",
    "primary_key": ["id"],
    "replication": {
      "method": "incremental",
      "cursor_field": ["created"]
    }
  },
  "destinations": [
    {
      "connection_ref": "conn_sevdesk",
      "endpoint_id": "1e63d782-4b67-4b7e-b845-4b4de5e4f46e",
      "write": {
        "mode": "upsert"
      }
    }
  ],
  "mapping": {
    "assignments": [
      {
        "target": { "path": ["valueDate"], "type": "string", "nullable": true },
        "value": { "kind": "expr", "expr": { "op": "get", "path": ["created"] } }
      },
      {
        "target": { "path": ["amount"], "type": "decimal", "nullable": false },
        "value": { "kind": "expr", "expr": { "op": "get", "path": ["targetValue"] } }
      }
    ]
  },
  "created_at": "2025-12-25T10:15:00Z",
  "updated_at": "2025-12-25T10:20:00Z"
}
```

**Key Stream Fields:**
- `version`: Integer (e.g., `1`), not string
- `is_enabled`: Boolean indicating if the stream is enabled (streams use `is_enabled`, pipelines use `is_active`)
- `status`: String status like `"draft"`, `"active"`, `"paused"`
- `source.connection_ref`: References an alias from pipeline's `connections.source`
- `destinations[].connection_ref`: References an alias from pipeline's `connections.destinations`
- `mapping.assignments`: Array of field mappings with target path and value expressions

#### Directory-Based Configuration Structure

The framework uses a directory-based structure to organize service endpoint schemas and client connection credentials:

```
config-directory/
├── pipelines/
│   └── {pipeline_id}.json
├── connections/
│   ├── {connection_id}.json
│   └── {connection_id}.json
└── connectors_endpoints/
    ├── {endpoint_id}.json
    └── {endpoint_id}.json
```

**API Service Endpoint Schema File** (`service_endpoints/{endpoint_id}.json`):
```json
{
  "endpoint": "/v1/transfers",
  "method": "GET",
  "replication_filter_mapping": {
    "created": "createdDateStart"
  },
  "pagination": {
    "type": "offset",
    "params": {
      "limit_param": "limit",
      "offset_param": "offset"
    }
  },
  "response_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "API Response Schema",
    "type": "array",
    "items": {
      "type": "object",
      "properties": { ... }
    }
  }
}
```

**Database Service Endpoint Schema File** (`service_endpoints/{endpoint_id}.json`):
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

**API Client Connection Credentials File** (`client_connections/{connection_id}.json`):
```json
{
  "host": "https://api.wise.com",
  "type": "api",
  "headers": {
    "Content-Type": "application/json",
    "Authorization": "Bearer ${WISE_API_TOKEN}"
  },
  "rate_limit": {
    "max_requests": 60,
    "time_window": 60
  }
}
```

**Database Client Connection Credentials File** (`client_connections/{connection_id}.json`):
```json
{
  "type": "database",
  "driver": "postgresql",
  "host": "${DB_HOST}",
  "port": 5432,
  "database": "${DB_NAME}",
  "user": "${DB_USER}",
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

**Required Database Connection Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `driver` | string | Database driver (e.g., `postgresql`, `mysql`) |
| `host` | string | Database hostname or IP |
| `port` | integer | Database port (must be integer, not string) |
| `database` | string | Database name |
| `user` | string | Database username |
| `password` | string | Database password (supports `${placeholder}`) |

**Note:** Use `driver`, `database`, `user` (not `provider`, `dbname`, `username`). The `port` must be an integer, not a string.

### Cloud (AWS) Configuration - config_fetcher.py

In cloud deployments, `config_fetcher.py` runs first to fetch configs from DynamoDB/S3 and write them to local filesystem. **This is NOT used by PipelineConfigPrep directly** - it's a pre-processing step.

#### DynamoDB Tables (used by config_fetcher.py)

| Table | Purpose |
|-------|---------|
| `pipelines` | Pipeline configurations |
| `streams` | Stream configurations |
| `connections` | Client-specific connection settings |

#### S3 Secrets (used by config_fetcher.py)

Path pattern: `s3://analitiq-secrets-{env}/connections/{client_id}/{connection_id}`

Secret file format:
```json
{"token": "your-actual-api-token-here"}
```

#### config_fetcher.py Flow

1. Fetch pipeline from DynamoDB `pipelines` table
2. Fetch streams from DynamoDB `streams` table
3. Fetch secrets from S3
4. Write to local filesystem:
   - `{output_dir}/pipelines/{pipeline_id}.json`
   - `{output_dir}/streams/{stream_id}.json`
   - `{output_dir}/.secrets/{connection_id}.json`
5. PipelineConfigPrep then reads from this local filesystem

### Incremental Replication Configuration

Both `source` and `destinaton` sections support incremental replication parameters for efficient data synchronization:

#### `replication_key`: "updatedAt"
- **Purpose**: The source field used to order records and track synchronization progress
- **Function**: Enables fetching "only new or changed rows" instead of re-reading all data
- **Common choices**: 
  - Last-modified timestamp: `updatedAt`, `modified`, `lastChanged`
  - Strictly increasing ID: `id`, `sequence_number`
- **Best practice**: If timestamps aren't unique, pair with a tie-breaker field (e.g., `(updatedAt, id)`)

**Note**: The engine always uses **inclusive mode** (`>=`) for cursor comparison. This is safer when timestamps can repeat or arrive late. Duplicates are handled via upsert/idempotency.

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
  - **State Path**: `state/{pipeline_id}/` (sharded per stream/partition)
  - **Logs Path**: `logs/{pipeline_id}/`
- **Schedule**: Defined in pipeline config (applies to entire pipeline)
- **Error Handling & Monitoring**: Part of pipeline-level configuration
- **Source Configuration**: Automatically loaded from directory-based structure:
  - `source.endpoint_id`: UUID referencing `service_endpoints/{endpoint_id}.json` for source schema
  - `source.connection_id`: UUID referencing `client_connections/{connection_id}.json` for source connection details
- **Destination Configuration**: Automatically loaded from directory-based structure:
  - `destinaton.endpoint_id`: UUID referencing `service_endpoints/{endpoint_id}.json` for destination schema
  - `destinaton.connection_id`: UUID referencing `client_connections/{connection_id}.json` for destination connection details
- **Destination Behavior**: Controlled by fields in pipeline_config.json's `destinaton` section:
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

## AWS Deployment

Infrastructure (Batch compute environment, job queue, IAM roles) is managed by the Terraform team. This section covers Docker image deployment and integration with existing infrastructure.

### Build and Push Docker Image

```bash
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION="eu-central-1"
export ECR_REPO="analitiq-stream"  # Coordinate with Terraform team for exact name

aws ecr get-login-password --region $AWS_REGION | \
    docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

docker build -t $ECR_REPO .
docker tag $ECR_REPO:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest
```

### Container Requirements for Batch Job Definition

The Terraform team needs to create a Batch Job Definition with these specifications:

**Runtime Environment Variables (via `containerOverrides`):**

| Variable | Required | Description |
|----------|----------|-------------|
| `PIPELINE_ID` | Yes | UUID of the pipeline to execute |
| `CLIENT_ID` | Yes | UUID of the client |

**Static Environment Variables (baked into job definition):**

| Variable            | Required | Description | Example                |
|---------------------|----------|-------------|------------------------|
| `ENV`               | Yes | Environment name (affects storage backends) | `dev`, `prod` |
| `AWS_REGION`        | No | AWS region (default: eu-central-1) | `eu-central-1`         |
| `LOG_LEVEL`         | No | Logging verbosity (default: INFO) | `INFO`                 |

**For config_fetcher.py (runs at container startup):**

| Variable            | Required | Description | Example                |
|---------------------|----------|-------------|------------------------|
| `PIPELINES_TABLE`   | Yes | DynamoDB table for pipeline configs | `pipelines`            |
| `STREAMS_TABLE`     | Yes | DynamoDB table for streams | `streams`              |
| `CONNECTIONS_TABLE` | Yes | DynamoDB table for client connections | `connections`          |

**Note:** S3 bucket names are auto-constructed as `analitiq-{purpose}-{env}` (e.g., `analitiq-secrets-dev`, `analitiq-client-pipeline-state-dev`).

**Resource Requirements:**

| Resource | Recommended |
|----------|-------------|
| vCPU | 0.5 - 1.0 |
| Memory | 1024 - 2048 MB |
| Timeout | 3600 seconds |

**IAM Permissions Required (Task Role):**

- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on secrets, state, logs, and DLQ buckets
- `dynamodb:GetItem`, `dynamodb:Query`, `dynamodb:Scan` on `pipelines`, `connections`, `connectors`, `connectors_endpoints`, `streams` tables
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

### Invoking the Container

The Lambda that triggers pipeline runs should call `batch:SubmitJob` with:

- **Job Queue**: Existing Terraform-managed queue (e.g., `analitiq-pipelines`)
- **Job Definition**: Points to this container image
- **Container Overrides**: Pass `PIPELINE_ID` as environment variable

Example:
```python
batch.submit_job(
    jobName='pipeline-run-2025-01-15',
    jobQueue='analitiq-pipelines',
    jobDefinition='analitiq-stream-dev',
    containerOverrides={
        'environment': [
            {'name': 'PIPELINE_ID', 'value': 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'},
            {'name': 'CLIENT_ID', 'value': 'd7a11991-2795-49d1-a858-c7e58ee5ecc6'}
        ]
    }
)
```

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