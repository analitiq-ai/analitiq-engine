# Destination Configuration Reference

## Overview

Configuration-driven destination system with layered abstractions. Single Docker image handles all destination types based on `connector_type` in connection config.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    TRANSPORT (gRPC Server)                  │
└─────────────────────────────────────────────────────────────┘
                               │
┌─────────────────────────────────────────────────────────────┐
│                   HANDLER LAYER (Orchestration)             │
│  DatabaseDestinationHandler │ FileDestinationHandler │      │
│  ApiDestinationHandler      │ StreamDestinationHandler      │
└─────────────────────────────────────────────────────────────┘
                               │
            ┌──────────────────┴──────────────────┐
            │                                     │
┌───────────────────────┐           ┌─────────────────────────┐
│    WRITER LAYER       │           │    FORMATTER LAYER      │
│  - SQLAlchemy (DB)    │           │  - JsonLinesFormatter   │
│  - FileWriter         │           │  - CsvFormatter         │
│  - ApiWriter          │           │  - ParquetFormatter     │
└───────────────────────┘           └─────────────────────────┘
            │
┌─────────────────────────────────────────────────────────────┐
│                   DRIVER / STORAGE LAYER                    │
│  Database: SQLAlchemy (PostgreSQL, MySQL, SQLite)           │
│  Storage:  LocalFile, S3                                    │
│  API:      aiohttp (REST endpoints)                         │
│  Stream:   Stdout                                           │
└─────────────────────────────────────────────────────────────┘
```

## Environment Variables

### Engine Mode (gRPC Client)

| Variable | Required | Description |
|----------|----------|-------------|
| `DESTINATION_GRPC_HOST` | No | Destination gRPC server host |
| `DESTINATION_GRPC_PORT` | No | Destination gRPC port (default: `50051`) |
| `GRPC_TIMEOUT_SECONDS` | No | gRPC timeout (default: `300`) |
| `MAX_RETRIES` | No | Max retry attempts (default: `3`) |

### Destination Server Mode

| Variable | Required | Description |
|----------|----------|-------------|
| `RUN_MODE` | Yes | Set to `destination` |
| `GRPC_PORT` | No | gRPC listen port (default: `50051`) |
| `LOG_LEVEL` | No | Logging level (default: `INFO`) |

**Note**: Destination connection configuration (connector type, credentials, endpoint details) is determined at runtime from the pipeline config. The engine reads the pipeline JSON, resolves the destination connection, and sends it to the destination server via gRPC `SchemaMessage.destination_config`. The destination server does not require pre-configured connection details.

## Handler Registry

Handler selection based on `connector_type` field in connection config:

```python
from src.destination.connectors import get_handler, HandlerRegistry

# Factory function
handler = get_handler("db")  # Returns DatabaseDestinationHandler

# Registry class methods
handler_class = HandlerRegistry.get_handler_class("api")
handler = HandlerRegistry.create_handler("file", config)
supported = HandlerRegistry.get_supported_types()
```

### Supported Handlers

| connector_type | Handler | Use Case |
|----------------|---------|----------|
| `db` | DatabaseDestinationHandler | SQL databases via SQLAlchemy |
| `postgresql`, `postgres` | DatabaseDestinationHandler | PostgreSQL via SQLAlchemy |
| `mysql`, `mariadb` | DatabaseDestinationHandler | MySQL via SQLAlchemy |
| `api` | ApiDestinationHandler | REST API endpoints |
| `file` | FileDestinationHandler | Local filesystem |
| `s3` | FileDestinationHandler | S3 storage |
| `stdout` | StreamDestinationHandler | Testing/debugging |

### Handler Capabilities

| Handler | Transactions | Upsert | Bulk Load | Primary Use |
|---------|-------------|--------|-----------|-------------|
| Database (db) | Yes | Yes | Yes | SQL databases (PostgreSQL, MySQL, SQLite) |
| API | No | No | Yes (bulk mode) | REST APIs |
| File/S3 | No | No | Yes | Files & object storage |
| Stream | No | No | No | Testing/Debug |

## Formatters

Convert records to output format. Used by File and Stream handlers.

```python
from src.destination.formatters import get_formatter

formatter = get_formatter("jsonl")
data = formatter.serialize_batch(records, schema)
```

| Format | File Extension | Binary | Content-Type | Use Case |
|--------|---------------|--------|--------------|----------|
| `jsonl`, `json` | `.jsonl` | No | `application/x-ndjson` | Streaming, append-friendly |
| `csv` | `.csv` | No | `text/csv` | Tabular data |
| `parquet` | `.parquet` | Yes | `application/vnd.apache.parquet` | Analytics, columnar |

**Parquet requires:** `poetry install -E analytics`

### Formatter Configuration

**JsonLinesFormatter:**
- `ensure_ascii`: Escape non-ASCII characters (default: `false`)
- `sort_keys`: Sort JSON keys (default: `false`)

**CsvFormatter:**
- `delimiter`: Field separator (default: `,`)
- `quotechar`: Quote character (default: `"`)
- `include_header`: Include header row (default: `true`)

**ParquetFormatter:**
- `compression`: `snappy`, `gzip`, `zstd` (default: `snappy`)
- `row_group_size`: Rows per group (default: `10000`)

## Storage Backends

Abstract file storage operations. Used by FileDestinationHandler.

```python
from src.destination.storage import get_storage_backend

storage = get_storage_backend("local")
await storage.connect({"path": "/data", "create_dirs": True})
await storage.write_file("output.jsonl", data, "application/x-ndjson")
```

| Storage Type | Description |
|--------------|-------------|
| `local`, `file` | Local filesystem via aiofiles |
| `s3` | AWS S3 (planned) |

## Connection Configuration

**File:** `connections/{connection_id}.json` or `.secrets/{connection_id}`

Handler selection logic:
```python
connector_type = connection.get("connector_type")
# -> "api" | "db" | "s3" | "file" | "stdout"

# For databases, driver determines SQLAlchemy dialect:
driver = connection.get("driver")
# -> "postgresql", "mysql", "sqlite", "mariadb"

# For file-based, file_format determines formatter:
file_format = connection.get("file_format", "jsonl")
# -> "parquet", "csv", "jsonl"
```

### Database Connection (connector_type: database)

```json
{
  "connection_id": "f1bc3489-...",
  "connector_type": "database",
  "driver": "postgresql",
  "host": "localhost",
  "port": 5432,
  "database": "postgres",
  "username": "postgres",
  "password": "${DB_PASSWORD}",
  "connection_pool": {
    "min_connections": 2,
    "max_connections": 10
  }
}
```

| Field | Required | Description                                          |
|-------|----------|------------------------------------------------------|
| `connector_type` | Yes | `database`                                             |
| `driver` | Yes | `postgresql`, `mysql`, `sqlite`, `mariadb`           |
| `host` | Yes | Database hostname                                    |
| `port` | Yes | Database port                                        |
| `database` | Yes | Database name                                        |
| `username` | Yes | Database user                                        |
| `password` | Yes | Database password (supports `${VAR}`)                |
| `ssl_mode` | No | SSL mode (`prefer`, `require`, `verify-ca` or `verify-full`) |
| `connection_pool.min_connections` | No | Min pool size (default: 2)                           |
| `connection_pool.max_connections` | No | Max pool size (default: 10)                          |
| `command_timeout` | No | Command timeout seconds (default: 300)               |

### API Connection (connector_type: api)

```json
{
  "connection_id": "...",
  "connector_type": "api",
  "host": "https://api.destination.com",
  "headers": { "Authorization": "Bearer ${API_TOKEN}" },
  "timeout": 30,
  "auth": {
    "type": "bearer_token",
    "token": "${TOKEN}"
  },
  "rate_limit": { "max_requests": 100, "time_window": 60 }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `connector_type` | Yes | `api` |
| `host` | Yes | Base URL |
| `headers` | No | HTTP headers |
| `timeout` | No | Request timeout seconds (default: 30) |
| `auth.type` | No | `bearer_token`, `api_key`, `basic` |
| `rate_limit.max_requests` | No | Requests per time window |
| `rate_limit.time_window` | No | Time window in seconds |

### File Connection (connector_type: file)

```json
{
  "connection_id": "...",
  "connector_type": "file",
  "file_format": "jsonl",
  "path": "/data/exports",
  "path_template": "year={year}/month={month}/day={day}",
  "formatter_config": {
    "ensure_ascii": false
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `connector_type` | Yes | `file` |
| `file_format` | No | `jsonl`, `csv`, `parquet` (default: `jsonl`) |
| `path` | Yes | Base output path |
| `path_template` | No | Partition template |
| `formatter_config` | No | Format-specific options |

### S3 Connection (connector_type: s3)

```json
{
  "connection_id": "...",
  "connector_type": "s3",
  "file_format": "parquet",
  "bucket": "data-lake",
  "prefix": "exports",
  "region": "eu-central-1",
  "compression": "snappy"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `connector_type` | Yes | `s3` |
| `file_format` | No | `jsonl`, `csv`, `parquet` (default: `jsonl`) |
| `bucket` | Yes | S3 bucket name |
| `prefix` | No | Key prefix |
| `region` | No | AWS region |
| `compression` | No | Compression for parquet |

### Stdout Connection (connector_type: stdout)

```json
{
  "connection_id": "...",
  "connector_type": "stdout",
  "file_format": "jsonl"
}
```

## Pipeline Configuration

**File:** `pipelines/{pipeline_id}.json`

```json
{
  "connections": {
    "destinations": [
      { "<alias>": "<connection_uuid>" }
    ]
  },
  "runtime": {
    "buffer_size": 5000,
    "batching": { "batch_size": 100, "max_concurrent_batches": 3 },
    "error_handling": { "strategy": "dlq", "max_retries": 3, "retry_delay": 5 }
  }
}
```

## Stream Destination Configuration

**File:** `streams/{stream_id}.json`

| Field | Required | Description |
|-------|----------|-------------|
| `destinations[].connection_ref` | Yes | Connection alias from pipeline |
| `destinations[].endpoint_id` | Yes | Destination endpoint UUID |
| `destinations[].write.mode` | No | `insert`, `upsert`, `truncate_insert` (default: `upsert`) |
| `destinations[].write.conflict_keys` | No | Field paths for conflict resolution |
| `destinations[].batching.size` | No | Batch size (default: 1) |

## Endpoint Configuration

**File:** `connectors/{connector_name}/endpoints/{endpoint_id}.json`

### Database Endpoint

```json
{
  "endpoint_id": "...",
  "type": "database",
  "schema": "public",
  "table": "events",
  "write_mode": "upsert",
  "primary_key": ["id"],
  "conflict_resolution": {
    "on_conflict": "id",
    "action": "update"
  },
  "configure": {
    "auto_create_table": true,
    "auto_create_schema": true
  }
}
```

### API Endpoint

```json
{
  "endpoint_id": "...",
  "endpoint": "/v1/records",
  "method": "POST",
  "batch_mode": "single"
}
```

| Field | Description |
|-------|-------------|
| `batch_mode` | `single` (one request per record), `bulk` (all in one), `batch` (chunks) |

### File/S3 Endpoint

```json
{
  "endpoint_id": "...",
  "path_template": "{stream_id}/year={year}/batch_{batch_seq}"
}
```

## Idempotency

All handlers track `(run_id, stream_id, batch_seq)` for exactly-once delivery.

### Database Idempotency

Uses `_batch_commits` table:

```sql
CREATE TABLE _batch_commits (
  run_id VARCHAR(255),
  stream_id VARCHAR(255),
  batch_seq BIGINT,
  committed_cursor BYTEA,
  records_written INT,
  committed_at TIMESTAMPTZ,
  PRIMARY KEY (run_id, stream_id, batch_seq)
);
```

Process:
1. Check if `(run_id, stream_id, batch_seq)` exists
2. If yes: return `ALREADY_COMMITTED` with stored cursor
3. If no: write data + insert idempotency record in single transaction

### File Idempotency

Uses `_manifest.json` in base path:

```json
{
  "version": 1,
  "commits": [{
    "run_id": "...",
    "stream_id": "...",
    "batch_seq": 1,
    "records_written": 100,
    "cursor_bytes": "...",
    "file_path": "...",
    "committed_at": "2024-01-01T00:00:00"
  }]
}
```

## gRPC Batch Parameters

| Field | Description |
|-------|-------------|
| `run_id` | Unique pipeline run identifier |
| `stream_id` | Stream identifier |
| `batch_seq` | Monotonically increasing batch sequence |
| `cursor` | Opaque cursor token (engine-produced, destination-stored) |
| `record_ids` | Record identifiers for DLQ correlation |

Returns `ACK_STATUS_ALREADY_COMMITTED` for replayed batches.

## Adding New Destinations

| Destination | Code Required | Config Only |
|-------------|---------------|-------------|
| New SQL database | 0 lines | Connection string |
| New API endpoint | 0 lines | JSON config |
| New storage (GCS) | ~100 lines backend | - |
| New format (Avro) | ~50 lines formatter | - |