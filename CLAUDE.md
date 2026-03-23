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
│   └── rate_limiter.py          # Token bucket rate limiter
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
│   ├── schema_contract.py       # Arrow-based type casting (DestinationSchemaContract)
│   ├── connectors/              # Destination handlers (renamed from handlers/)
│   │   ├── database.py          # DatabaseDestinationHandler (SQLAlchemy + Arrow)
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
  "org_id": "uuid",
  "name": "Pipeline Name",
  "status": "active",
  "connections": {
    "source": { "conn_alias": "connection-uuid" },
    "destinations": [{ "conn_alias": "connection-uuid" }]
  },
  "schedule": { "type": "interval", "timezone": "UTC", "interval_minutes": 1440 },
  "engine": { "vcpu": 1, "memory": 8192 },
  "runtime": {
    "buffer_size": 5000,
    "batching": { "batch_size": 100, "max_concurrent_batches": 3 },
    "logging": { "log_level": "INFO", "metrics_enabled": true },
    "error_handling": { "strategy": "dlq", "max_retries": 3, "retry_delay": 5 }
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

### Connectors

Connectors define the type and metadata for each integration. The `connector_type` field determines which handler processes connections using this connector.

```json
{
  "connectors": [
    {"connector_id": "pg-connector", "connector_name": "PostgreSQL", "connector_type": "database", "slug": "postgresql"},
    {"connector_id": "mysql-connector", "connector_name": "MySQL", "connector_type": "database", "slug": "mysql"},
    {"connector_id": "api-connector", "connector_name": "REST API", "connector_type": "api"},
    {"connector_id": "file-connector", "connector_name": "File Export", "connector_type": "file"},
    {"connector_id": "s3-connector", "connector_name": "S3 Export", "connector_type": "s3"},
    {"connector_id": "stdout-connector", "connector_name": "Stdout", "connector_type": "stdout"}
  ]
}
```

### Connection Credentials

Connections reference a connector via `connector_id` and contain credentials. Credentials use `${VAR_NAME}` placeholder syntax. Placeholders are expanded at connection time from secrets files (`.secrets/{connection_id}.json`). Missing placeholders raise `PlaceholderExpansionError`. See `docs/CREDENTIALS_GUIDE.md`.

**Database connection:**
```json
{
  "connection_id": "prod-postgres",
  "connector_id": "pg-connector",
  "host": "${DB_HOST}",
  "port": 5432,
  "database": "${DB_NAME}",
  "username": "${DB_USER}",
  "password": "${DB_PASSWORD}"
}
```

**API connection:**
```json
{
  "connection_id": "external-api",
  "connector_id": "api-connector",
  "host": "https://api.example.com",
  "headers": { "Authorization": "Bearer ${API_TOKEN}" },
  "timeout": 30,
  "rate_limit": { "max_requests": 60, "time_window": 60 }
}
```

**File connection:**
```json
{
  "connection_id": "local-export",
  "connector_id": "file-connector",
  "file_format": "jsonl",
  "path": "/data/exports"
}
```

**S3 connection:**
```json
{
  "connection_id": "s3-export",
  "connector_id": "s3-connector",
  "file_format": "parquet",
  "bucket": "data-lake",
  "prefix": "exports",
  "region": "eu-central-1",
  "compression": "snappy"
}
```

**Stdout connection:**
```json
{
  "connection_id": "debug-output",
  "connector_id": "stdout-connector",
  "file_format": "jsonl"
}
```

## Storage Backends

All storage (state, logs, DLQ, metrics) follows the same pattern:
- **Local (`ENV=local`):** `{type}/{pipeline_id}/`
- **S3 (`ENV=dev/prod`):** `s3://analitiq-client-pipeline-{type}-{env}/{org_id}/{pipeline_id}/year=.../`

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

Configuration-driven destination system with layered abstractions. Single Docker image handles all destination types based on `connector_type` defined in the connector (looked up via `connector_id` on the connection).

**Handlers:** `db` (SQLAlchemy), `postgresql` (legacy asyncpg), `api`, `file`, `s3`, `stdout`

**Formatters:** `jsonl`, `csv`, `parquet`

**Storage:** `local`, `s3`

**Idempotency:** Database handlers use `_batch_commits` table; file handlers use `_manifest.json`

See [Destination Config](docs/DESTINATION_CONFIG.md) for full reference.

## Arrow-Based Type System

Type casting uses PyArrow for efficient columnar conversion instead of row-by-row Python coercion.

**Architecture:**
```
Source (native types) -> Arrow Table (canonical schema) -> Destination (native types)
```

**Key Components:**
- `DestinationSchemaContract` (`src/destination/schema_contract.py`): Builds Arrow schema from endpoint definition, provides vectorized batch casting
- `target.type`: Generic type for Pydantic validation (`integer`, `string`, `datetime`, etc.)
- `target.dest_type`: Native SQL type for DDL/casting (`BIGINT`, `VARCHAR(50)`, `DATETIME`, etc.)

**Type Mapping Flow:**
1. `source_to_generic` mapping provides generic types from source fields
2. `generic_to_destination` mapping provides native destination types per connection
3. `_normalize_mapping_config()` sets both `target.type` and `target.dest_type` on assignments
4. `DestinationSchemaContract` builds Arrow schema from endpoint columns
5. `prepare_records()` performs vectorized casting using PyArrow

**Supported Native Types:**
- Integer: `BIGINT`, `INTEGER`, `SMALLINT`, `TINYINT`
- Float: `FLOAT`, `DOUBLE`, `REAL`, `DECIMAL(p,s)`
- String: `VARCHAR(n)`, `TEXT`, `CHAR`
- Timestamp: `TIMESTAMP`, `TIMESTAMPTZ`, `DATETIME`
- Date/Time: `DATE`, `TIME`
- Boolean: `BOOLEAN`, `BOOL`
- JSON: `JSON`, `JSONB` (stored as string)
- Binary: `BYTEA`, `BLOB`

## Incremental Replication

- **`replication_key`**: Field for ordering/tracking sync progress (e.g., `updatedAt`)
- **`safety_window_seconds`**: Offset subtracted from cursor (60-300s typical) to handle clock skew
- **`bookmarks`**: Array of `{partition, cursor, aux}` for checkpoint tracking
- Engine uses inclusive mode (`>=`) for cursor comparison; duplicates handled via upsert

## AWS Logs Debugging
Use agent `aws-log-parser` to parse CloudWatch logs.

## Testing
When running tests where org_id is needed, use:
 - org_id = `d7a11991-2795-49d1-a858-c7e58ee5ecc6`

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
   AWS_PROFILE=434659057682_AdministratorAccess \
   ENV=dev \
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
- Runtime env: `PIPELINE_ID`, `ORG_ID`
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
- Use @codex-plan-reviewer agent to review plans before presenting them to me for execution.


## Working on Issues:

1. Examine GitHub issues, pick the easist one to implement.
2. Create a new branch for the issue.
3. Implement the issue.
4. Commit your changes and push to the branch.
5. Create a pull request (if not yet created).
6. Run the PR Review Process
7. Wait for feedback from the review executor.

## PR Review Process:

1. Use `/pr-review-toolkit` to review the PR after you have implemented all changes.
2. Wait for feedback from the review executor.
3. Determine if the raised issues are legitimate or not.
   a. if the issue is legitimate and relevant to the PR, fix it.
   b. if the issue is outside the scope of the PR, check if there is a related issue in the GitHub issue tracker. If not, create a new issue in GitHub and move on.
   c. If the issue is not a legitimate problem, summarize your thoughts on the point and move on.
4. Once you fixed all issues that need fixing, commit fixes, push to the branch.
5. Use `/pr-review-toolkit` to review again
6. Continue doing this cycle until the PR is approved by the review executor.
7. Once the PR is approved, run the tests to make sure they all pass.
