# CLAUDE.md

## Project Overview

Analitiq Stream is a fault-tolerant data streaming framework for Python 3.11+ enabling reliable data movement between sources and destinations. Uses Poetry for dependency management with async/await streaming.

## Development Commands

```bash
# Setup
poetry install                              # Install dependencies
poetry install -E "kafka cloud analytics"  # With extras
poetry shell                                # Activate venv

# Testing
poetry run pytest                           # All tests
poetry run pytest --cov=src --cov-report=html
poetry run pytest -m "unit"                 # By marker: unit, integration, slow

# Code Quality
poetry run black src/ && poetry run isort src/  # Format
poetry run mypy src/                            # Type check
poetry run flake8 src/                          # Lint
poetry run pre-commit run --all-files           # All hooks
```

## Project Structure

```
src/
├── shared/                      # Shared utilities used by source and destination
│   ├── database_utils.py        # SSL mode, identifiers, table names
│   ├── rate_limiter.py          # Token bucket rate limiter
│   └── type_mapping/            # Unified type mapping system
│       ├── base.py              # BaseTypeMapper ABC
│       ├── postgresql.py        # PostgreSQL: JSONB, TIMESTAMPTZ, arrays
│       ├── mysql.py             # MySQL: JSON, DATETIME
│       ├── snowflake.py         # Snowflake: VARIANT, TIMESTAMP_TZ
│       └── generic.py           # Safe fallback for unknown DBs
│
├── source/                      # Source connectors (read operations)
│   ├── connectors/              # Protocol implementations
│   │   ├── base.py              # BaseConnector ABC
│   │   ├── api.py               # APIConnector
│   │   └── database.py          # DatabaseConnector (uses drivers)
│   └── drivers/                 # Database-specific drivers
│       ├── base.py              # BaseDatabaseDriver ABC
│       ├── postgresql.py        # PostgreSQLDriver
│       └── factory.py           # Driver factory
│
├── destination/                 # Destination components (write operations)
│   ├── base_handler.py          # BaseDestinationHandler ABC
│   ├── server.py                # gRPC server implementation
│   ├── connectors/              # Destination handlers (renamed from handlers/)
│   │   ├── database.py          # DatabaseDestinationHandler (SQLAlchemy)
│   │   ├── api.py               # ApiDestinationHandler
│   │   ├── file.py              # FileDestinationHandler
│   │   └── stream.py            # StreamDestinationHandler
│   ├── formatters/              # JSONL, CSV, Parquet formatters
│   ├── storage/                 # Local, S3 storage backends
│   └── idempotency/             # ManifestTracker for file-based idempotency
│
├── engine/                      # Core engine (renamed from core/)
│   ├── engine.py                # StreamingEngine - ETL pipeline processor
│   ├── pipeline.py              # Pipeline configuration interface
│   ├── pipeline_config_prep.py  # Multi-environment config loading
│   ├── data_transformer.py      # Field transformations
│   └── credentials.py           # Credentials management
│
├── state/                       # Fault tolerance (renamed from fault_tolerance/)
│   ├── state_manager.py         # State management
│   ├── state_storage.py         # Local/S3 state storage
│   ├── retry_handler.py         # Retry handling
│   ├── circuit_breaker.py       # Circuit breaker pattern
│   ├── dead_letter_queue.py     # Dead letter queue
│   └── log_storage.py           # Log storage
│
├── grpc/                        # gRPC client and generated code
├── models/                      # Pydantic models
├── config/                      # Configuration loading
├── secrets/                     # Secret resolvers
├── schema/                      # Schema drift detection
├── mapping/                     # Field mapping processor
├── transformations/             # Transformation registry
├── main.py                      # Entry point (dual-mode: engine/destination)
└── runner.py                    # PipelineRunner
```

### Import Path Changes

| Old Import | New Import |
|------------|------------|
| `from src.connectors` | `from src.source.connectors` |
| `from src.core` | `from src.engine` |
| `from src.fault_tolerance` | `from src.state` |
| `from src.destination.handlers` | `from src.destination.connectors` |

Backward compatibility aliases exist but will emit deprecation warnings.

## Configuration Architecture

Configuration loads from local filesystem when ran locally. In cloud, `config_fetcher.py` pre-populates from DynamoDB/S3 first.

```
{local_config_mount}/
├── pipelines/{pipeline_id}.json
├── streams/{stream_id}.json
├── .secrets/{connection_id}
└── connectors/{connector_name}/connector.json
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ENV` | Yes | `local`, `dev`, `prod` - affects storage backends only |
| `PIPELINE_ID` | Yes | Pipeline UUID |
| `CLIENT_ID` | Yes | Client UUID |
| `LOCAL_CONFIG_MOUNT` | Yes | Config directory path |
| `AWS_REGION` | Cloud | Default: `eu-central-1` |

**Cloud-only (config_fetcher.py):** `PIPELINES_TABLE`, `STREAMS_TABLE`, `CONNECTIONS_TABLE`, `SECRETS_BUCKET`

S3 buckets auto-construct as `analitiq-{purpose}-{env}` (e.g., `analitiq-client-pipeline-state-dev`).

### Using PipelineConfigPrep

```python
from src.engine.pipeline_config_prep import PipelineConfigPrep, PipelineConfigPrepSettings

settings = PipelineConfigPrepSettings(
    env="local", pipeline_id="uuid", local_config_mount="/path/to/config"
)
prep = PipelineConfigPrep(settings)
pipeline_config, stream_configs, connections, endpoints = prep.create_config()
```

## Configuration Schemas

### Pipeline (`pipelines/{id}.json`)

```json
{
  "version": 1,
  "pipeline_id": "uuid",
  "client_id": "uuid",
  "name": "Pipeline Name",
  "is_active": true,
  "connections": {
    "source": { "conn_alias": "connection-uuid" },
    "destinations": [{ "conn_alias": "connection-uuid" }]
  },
  "runtime": {
    "retry": { "max_attempts": 5, "backoff": "exponential" },
    "batching": { "batch_size": 100, "max_concurrent_batches": 3 }
  }
}
```

### Stream (`streams/{id}.json`)

```json
{
  "version": 1,
  "stream_id": "uuid",
  "pipeline_id": "uuid",
  "is_enabled": true,
  "source": {
    "connection_ref": "conn_alias",
    "endpoint_id": "uuid",
    "replication": { "method": "incremental", "cursor_field": ["updated_at"] }
  },
  "destinations": [{
    "connection_ref": "conn_alias",
    "endpoint_id": "uuid",
    "write": { "mode": "upsert" }
  }],
  "mapping": { "assignments": [...] }
}
```

### Connection Credentials

Credentials support `${VAR_NAME}` syntax for environment variable expansion. The `connector_type` field determines which handler processes the connection.

**Database (connector_type: db):**
```json
{
  "connector_type": "db",
  "driver": "postgresql",
  "host": "${DB_HOST}",
  "port": 5432,
  "database": "${DB_NAME}",
  "username": "${DB_USER}",
  "password": "${DB_PASSWORD}"
}
```

**API (connector_type: api):**
```json
{
  "connector_type": "api",
  "host": "https://api.example.com",
  "headers": { "Authorization": "Bearer ${API_TOKEN}" },
  "timeout": 30,
  "rate_limit": { "max_requests": 60, "time_window": 60 }
}
```

**File (connector_type: file):**
```json
{
  "connector_type": "file",
  "file_format": "jsonl",
  "path": "/data/exports"
}
```

**S3 (connector_type: s3):**
```json
{
  "connector_type": "s3",
  "file_format": "parquet",
  "bucket": "data-lake",
  "prefix": "exports",
  "region": "eu-central-1",
  "compression": "snappy"
}
```

**Stdout (connector_type: stdout):**
```json
{
  "connector_type": "stdout",
  "file_format": "jsonl"
}
```

## Storage Backends

All storage (state, logs, DLQ, metrics) follows the same pattern:
- **Local (`ENV=local`):** `{type}/{pipeline_id}/`
- **S3 (`ENV=dev/prod`):** `s3://analitiq-client-pipeline-{type}-{env}/{client_id}/{pipeline_id}/year=.../`

Centralized directories at project root: `state/`, `logs/`, `deadletter/`, `metrics/`

## gRPC Streaming Architecture

Decoupled destination services via bidirectional gRPC streaming.

```
ENGINE CONTAINER                    DESTINATION CONTAINER
Extract -> Transform -> GrpcLoad   DestinationGRPCServer -> Handler
```

**Dual-mode Docker:** Same image runs as engine or destination via `RUN_MODE` env var. Both containers use `PipelineConfigPrep` to load configuration from the same `PIPELINE_ID`.

| Engine Variable | Description |
|-----------------|-------------|
| `DESTINATION_GRPC_HOST` | Enables gRPC mode |
| `DESTINATION_GRPC_PORT` | Default: `50051` |
| `GRPC_TIMEOUT_SECONDS` | Default: `300` |

| Destination Variable | Description |
|---------------------|-------------|
| `GRPC_PORT` | Default: `50051` |
| `DESTINATION_INDEX` | Which destination from pipeline config (default: `0`) |

**Configuration Loading:** Both engine and destination load configuration via `PipelineConfigPrep` using the same `PIPELINE_ID`. The destination uses `DESTINATION_INDEX` to select which destination connection from the pipeline's `connections.destinations` list. Credentials are loaded locally from the config volume, never sent over gRPC.

**Protocol:** Opaque cursors, batch idempotency via `(run_id, stream_id, batch_seq)`, all-or-nothing batches.

See [gRPC Architecture](docs/GRPC_STREAMING_ARCHITECTURE.md) for details.

## Destination Architecture

Configuration-driven destination system with layered abstractions. Single Docker image handles all destination types based on `connector_type` in connection config.

**Handlers:** `db` (SQLAlchemy), `postgresql` (legacy asyncpg), `api`, `file`, `s3`, `stdout`

**Formatters:** `jsonl`, `csv`, `parquet` (requires `-E analytics`)

**Storage:** `local`, `s3`

**Idempotency:** Database handlers use `_batch_commits` table; file handlers use `_manifest.json`

See [Destination Config](docs/DESTINATION_CONFIG.md) for full reference.

## Incremental Replication

- **`replication_key`**: Field for ordering/tracking sync progress (e.g., `updatedAt`)
- **`safety_window_seconds`**: Offset subtracted from cursor (60-300s typical) to handle clock skew
- **`bookmarks`**: Array of `{partition, cursor, aux}` for checkpoint tracking
- Engine uses inclusive mode (`>=`) for cursor comparison; duplicates handled via upsert

## AWS Logs Debugging
Use agent `aws-log-parser` to parse CloudWatch logs.

## Testing
When running tests where client_id is needed, use:
 - client_id = `d7a11991-2795-49d1-a858-c7e58ee5ecc6`

### Running docker test locally
When running docker test, the run should mimic the production environment:
 - start docker coontainer with `ENV=dev`.
 - use aws profile id = `434659057682_AdministratorAccess` if needed.
 - pass the required env vars to the container (including AWS Profile).
 - Docker should then connect to AWS and fetch all required configs and execute the code.
 - examine docker logs for errors.

Example:
```shell
cd docker && \                                                                                                                                          
   PIPELINE_ID=0569c85b-b538-442a-bdc5-726afca08da4 \                                                                                                                                                               
   CLIENT_ID=d7a11991-2795-49d1-a858-c7e58ee5ecc6 \                                                                                                                                                                 
   AWS_PROFILE=434659057682_AdministratorAccess \                                                                                                                                                                   
   docker compose run --rm source_engine 2>&1 
```

## Data Flow

1. **Extract**: Read from source in batches
2. **Transform**: Apply field mappings and transformations
3. **Load**: Write to destination with fault tolerance
4. **Checkpoint**: Save progress state

## AWS Deployment

```bash
# Build and push to ECR
aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com
docker build -t analitiq-stream . && docker push $AWS_ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/analitiq-stream:latest
```

**Batch Job Requirements:**
- Runtime env: `PIPELINE_ID`, `CLIENT_ID`
- Static env: `ENV`, `PIPELINES_TABLE`, `STREAMS_TABLE`, `CONNECTIONS_TABLE`
- Resources: 0.5-1 vCPU, 1-2GB memory, 3600s timeout
- IAM: S3 read/write on buckets, DynamoDB read on tables

## Key Design Patterns

- Async/await for all I/O
- Producer-consumer via async queues
- Factory pattern for connectors and handlers
- Registry pattern for destination handlers, formatters, storage backends
- Composition pattern for FileDestinationHandler (formatter + storage + manifest)
- Driver delegation for databases
- Sharded state checkpoints
- Pydantic V2 validation

## Coding Guidelines

- Python 3.11+ with Pydantic V2 validation
- No backward compatibility code
- No emojis in code
- Modular, pythonic code with explicit error paths
- Use ExceptionGroup and except* where helpful
- Deterministic behavior, idempotent writes, safe retries
- Update README.md when modifying documented functionality
- DO NOT ADD LEGACY SUPPORT OR BACKWARD COMPATIBILITY, UNLESS EXPLICITLY INSTRUCTED!