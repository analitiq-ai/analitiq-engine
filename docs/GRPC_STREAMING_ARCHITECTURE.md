# gRPC Streaming Architecture

## Overview

This document describes the gRPC bidirectional streaming architecture that enables decoupled destination services for Analitiq Stream. This architecture replaces the in-process `asyncio.Queue` pattern with gRPC streaming, allowing destinations to run as separate services.

## Architecture

```
+-------------------------------------------------------------+
|                    ENGINE CONTAINER                          |
|  Extract -> Transform -> GrpcLoadStage -> Checkpoint        |
|                              |                               |
|                    DestinationGRPCClient                    |
+-----------------------------+-------------------------------+
                              | gRPC Bidirectional Stream
                              | (host:port from config)
+-----------------------------+-------------------------------+
|               DESTINATION SERVICE CONTAINER                  |
|                    DestinationGRPCServer                    |
|                              |                               |
|              BaseDestinationHandler (abstract)              |
|                    /                \                        |
|         PostgreSQLHandler      (MySQLHandler future)        |
+-------------------------------------------------------------+
```

**Key Design**: Same Docker image, different `RUN_MODE` environment variable:
- `RUN_MODE=engine` - Runs the data pipeline engine with gRPC client
- `RUN_MODE=destination` - Runs the gRPC destination server

## Benefits

1. **Decoupled Destinations** - Add new destinations (MySQL, Snowflake, etc.) without modifying the engine
2. **Independent Scaling** - Scale engine and destination services independently
3. **Language Agnostic** - Destinations can be implemented in any language supporting gRPC
4. **Container Isolation** - Separate failure domains for engine and destination
5. **Reusable Components** - Same destination service can serve multiple engines

## Protocol Design

### Message Types

| Type | Direction | Purpose |
|------|-----------|---------|
| `SchemaMessage` | Engine -> Destination | Table config, primary keys, write mode (sent once at stream start) |
| `RecordBatch` | Engine -> Destination | Batch of records with payload, cursor, and record IDs |
| `SchemaAck` | Destination -> Engine | Confirmation that schema was accepted |
| `BatchAck` | Destination -> Engine | Status, records written, and committed cursor |

### Protobuf Definitions

#### Payload Format

```protobuf
enum PayloadFormat {
  PAYLOAD_FORMAT_UNSPECIFIED = 0;
  PAYLOAD_FORMAT_JSONL = 1;      // Newline-delimited JSON (default)
  PAYLOAD_FORMAT_MSGPACK = 2;    // MessagePack binary format
  PAYLOAD_FORMAT_AVRO = 3;       // Apache Avro (future)
}
```

#### Cursor (Opaque Token)

```protobuf
// Opaque cursor - produced by engine, stored/returned by destination
// Destination NEVER interprets the cursor - only stores and returns it
message Cursor {
  bytes token = 1;  // Engine-defined format, opaque to destination
}
```

The cursor format (internal to engine):
```json
{
  "field": "created_at",
  "value": "2025-01-08T10:00:00Z",
  "tie_breakers": [{"field": "id", "value": "123"}],
  "encoded_at": "2025-01-08T10:05:00Z"
}
```

#### Record Batch

```protobuf
message RecordBatch {
  // Idempotency keys
  string run_id = 1;              // Unique pipeline run ID
  string stream_id = 2;           // Stream identifier
  uint64 batch_seq = 3;           // Monotonically increasing per stream

  // Payload
  PayloadFormat format = 4;       // Encoding format
  bytes payload = 5;              // Encoded records
  uint32 record_count = 6;        // Number of records in payload

  // Per-record identifiers for DLQ correlation
  repeated string record_ids = 7; // Stable IDs across retries

  // Opaque cursor (MAX watermark in batch)
  Cursor cursor = 8;
}
```

#### ACK Status

```protobuf
enum AckStatus {
  ACK_STATUS_UNSPECIFIED = 0;
  ACK_STATUS_SUCCESS = 1;           // All written, cursor advanced
  ACK_STATUS_ALREADY_COMMITTED = 2; // Idempotent replay, no-op
  ACK_STATUS_RETRYABLE_FAILURE = 3; // Safe to retry entire batch
  ACK_STATUS_FATAL_FAILURE = 4;     // Do not retry, send to DLQ
}
```

#### Batch ACK

```protobuf
message BatchAck {
  string run_id = 1;
  string stream_id = 2;
  uint64 batch_seq = 3;

  AckStatus status = 4;
  uint32 records_written = 5;

  // Returned on SUCCESS and ALREADY_COMMITTED
  Cursor committed_cursor = 6;

  // Optional diagnostic info for FATAL_FAILURE
  repeated string failed_record_ids = 7;
  string failure_summary = 8;
}
```

## Key Design Decisions

### 1. Opaque Cursor

The cursor is an opaque bytes token produced by the engine and stored/returned by the destination. The destination never interprets the cursor.

**Why**:
- Avoids cross-database cursor typing issues
- Engine controls cursor semantics (timestamp, LSN, composite key, etc.)
- Destination only needs to store and return bytes

**Implementation**:
- Engine computes cursor as MAX watermark in batch (not last record, batch may be unordered)
- Cursor is JSON-encoded for debuggability
- Destination stores cursor in `_batch_commits` table and returns it in ACK

### 2. Batch-Level Idempotency

Each batch includes `(run_id, stream_id, batch_seq)` as a unique key.

**Why**:
- Enables exactly-once delivery semantics
- Handles network failures after commit
- Supports safe retries

**Implementation**:
```sql
CREATE TABLE _batch_commits (
    run_id VARCHAR(255),
    stream_id VARCHAR(255),
    batch_seq BIGINT,
    committed_cursor BYTEA,
    records_written INT,
    committed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (run_id, stream_id, batch_seq)
);
```

On batch receipt:
1. Check if `(run_id, stream_id, batch_seq)` exists
2. If exists: return `ALREADY_COMMITTED` with stored cursor
3. If new: write records, insert commit record, return `SUCCESS`

### 3. All-or-Nothing Batches

There is no partial success. The entire batch either succeeds or fails.

**Why**:
- Simplifies cursor semantics - cursor only advances on SUCCESS
- Eliminates complex partial rollback logic
- Makes DLQ handling straightforward - entire batch goes to DLQ

**Error Handling**:
| Error Type | AckStatus | Engine Action |
|------------|-----------|---------------|
| Network failure after commit | `ALREADY_COMMITTED` | Persist cursor, continue |
| Connection failure | `RETRYABLE_FAILURE` | Retry with exponential backoff |
| Timeout | `RETRYABLE_FAILURE` | Retry with exponential backoff |
| Deadlock | `RETRYABLE_FAILURE` | Retry with exponential backoff |
| Constraint violation | `FATAL_FAILURE` | Send entire batch to DLQ |
| Type conversion error | `FATAL_FAILURE` | Send entire batch to DLQ |

### 4. Strict In-Order Processing

Default behavior is strict in-order: send batch -> await ACK -> send next.

**Why**:
- Simplifies cursor management
- Prevents out-of-order commits
- Required for database destinations with cursor-based checkpointing

**Configuration**: `MAX_IN_FLIGHT_BATCHES` (default: 1) can be increased for API destinations that don't require ordering.

### 5. Record IDs for DLQ Correlation

Each record gets a stable ID generated from `(run_id, batch_seq, primary_key or index)`.

**Why**:
- Enables correlation between retries
- Allows DLQ records to be traced back to source
- Survives batch reprocessing

**Implementation**:
```python
def generate_record_id(record, run_id, batch_seq, index, primary_key_fields=None):
    if primary_key_fields:
        pk_values = [str(record.get(f, "")) for f in primary_key_fields]
        hash_input = f"{run_id}:{batch_seq}:{"|".join(pk_values)}"
    else:
        hash_input = f"{run_id}:{batch_seq}:{index}"
    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]
```

### 6. Bytes Payload with Format Enum

Records are sent as `bytes payload` with a `PayloadFormat` enum, not `google.protobuf.Struct`.

**Why**:
- Struct has performance issues at scale
- Bytes allows efficient serialization formats (JSONL, MessagePack, Avro)
- Client and server agree on format via enum

**Supported Formats**:
- `PAYLOAD_FORMAT_JSONL` (default) - Newline-delimited JSON, human-readable
- `PAYLOAD_FORMAT_MSGPACK` - Compact binary, faster parsing

## Component Reference

### Engine Side

#### DestinationGRPCClient

```python
from src.grpc.client import DestinationGRPCClient, BatchResult

client = DestinationGRPCClient(
    host="destination",      # or localhost, or env var
    port=50051,
    timeout_seconds=300,
    max_retries=3,
    max_message_size=16 * 1024 * 1024,  # 16MB
)

# Connect and verify health
await client.connect()

# Start stream with schema
await client.start_stream(
    run_id="2025-01-08T10:00:00Z-abc123",
    stream_id="transactions",
    schema_config={
        "type": "database",
        "driver": "postgresql",
        "schema": "public",
        "table": "transactions",
        "primary_key": ["id"],
        "write_mode": "upsert",
        ...
    }
)

# Send batches
result: BatchResult = await client.send_batch(
    run_id="2025-01-08T10:00:00Z-abc123",
    stream_id="transactions",
    batch_seq=1,
    records=[{"id": 1, "amount": 100}, ...],
    record_ids=["rid1", "rid2", ...],
    cursor=cursor,
)

# Handle result
if result.status == AckStatus.ACK_STATUS_SUCCESS:
    # Persist cursor from result.committed_cursor
    pass
elif result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
    # Retry with backoff
    pass
elif result.status == AckStatus.ACK_STATUS_FATAL_FAILURE:
    # Send to DLQ
    pass

# End stream
await client.end_stream()
await client.disconnect()
```

#### Cursor Utilities

```python
from src.grpc.cursor import (
    encode_cursor,
    decode_cursor,
    compute_max_cursor,
    cursor_to_state_dict,
)

# Compute MAX cursor from batch (batch may be unordered)
cursor = compute_max_cursor(
    batch=records,
    cursor_field="created_at",
    tie_breaker_fields=["id"],
)

# Convert committed cursor to state dict for persistence
state_dict = cursor_to_state_dict(committed_cursor)
# {"cursor": {"primary": {"field": "...", "value": "...", "inclusive": true}}}
```

### Destination Side

#### BaseDestinationHandler

Abstract interface for all destination handlers:

```python
from src.destination.base_handler import BaseDestinationHandler, BatchWriteResult

class MyHandler(BaseDestinationHandler):
    async def connect(self, connection_config: Dict[str, Any]) -> None:
        """Establish connection to destination."""
        ...

    async def disconnect(self) -> None:
        """Close connection."""
        ...

    async def configure_schema(self, schema_msg: SchemaMessage) -> bool:
        """Configure destination schema (create tables, indexes, etc.)."""
        ...

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        records: List[Dict[str, Any]],
        record_ids: List[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        """Write batch with idempotency."""
        ...

    async def health_check(self) -> bool:
        """Check if destination is healthy."""
        ...

    @property
    def connector_type(self) -> str:
        return "mytype"
```

#### PostgreSQLHandler

Built-in PostgreSQL handler with:
- Connection pooling via asyncpg
- Schema/table auto-creation
- ON CONFLICT upsert support
- Batch-level idempotency via `_batch_commits` table

```python
from src.destination.connectors import get_handler, DatabaseDestinationHandler

handler = get_handler("db")  # or DatabaseDestinationHandler()
await handler.connect({
    "driver": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "username": "postgres",
    "password": "secret",
})
```

#### DestinationGRPCServer

```python
from src.destination.server import DestinationGRPCServer
from src.destination.connectors import get_handler

# Simple usage
handler = get_handler("db")
await handler.connect(connection_config)
server = DestinationGRPCServer(handler, port=50051)
await server.start()

# Advanced usage
handler = get_handler("db")
await handler.connect(connection_config)

server = DestinationGRPCServer(handler, port=50051)
await server.start()
await server.wait_for_termination()
```

## Configuration

### Environment Variables

#### Engine Mode

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUN_MODE` | No | `engine` | Set to `engine` for pipeline mode |
| `DESTINATION_GRPC_HOST` | Yes* | - | Hostname of destination gRPC server |
| `DESTINATION_GRPC_PORT` | No | `50051` | Port of destination gRPC server |
| `GRPC_TIMEOUT_SECONDS` | No | `300` | ACK timeout in seconds |
| `MAX_RETRIES` | No | `3` | Max retries for RETRYABLE_FAILURE |
| `RETRY_BASE_DELAY_MS` | No | `500` | Base delay for exponential backoff |

*Required when gRPC mode is enabled

#### Destination Mode

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUN_MODE` | Yes | - | Set to `destination` for server mode |
| `PIPELINE_ID` | Yes | - | Pipeline UUID (same as engine) |
| `ORG_ID` | Cloud | - | Org UUID (required for cloud environments) |
| `DESTINATION_INDEX` | No | `0` | Index of destination in pipeline config |
| `GRPC_PORT` | No | `50051` | Port to listen on |
| `LOG_LEVEL` | No | `INFO` | Logging level |

**Configuration Loading**: Both engine and destination load configuration via `PipelineConfigPrep` using the same `PIPELINE_ID`. The destination uses `DESTINATION_INDEX` to select which destination connection from the pipeline's `connections.destinations` list. Credentials are loaded locally from the config volume and never sent over gRPC.

### Config-Based gRPC Enable

gRPC mode is automatically enabled when:
1. `DESTINATION_GRPC_HOST` environment variable is set, OR
2. Destination config has `grpc.enabled: true`

```json
{
  "destination": {
    "grpc": {
      "enabled": true,
      "host": "destination",
      "port": 50051,
      "timeout_seconds": 300
    }
  }
}
```

## Docker Deployment

### docker-compose.yml

```yaml
services:
  # Destination service - agnostic to actual destination type
  # Connection config is received from engine via gRPC at runtime
  destination:
    image: analitiq-stream:latest
    environment:
      - RUN_MODE=destination
      - GRPC_PORT=50051
    volumes:
      - ../pipelines:/config/pipelines:ro
      - ../streams:/config/streams:ro
      - ../connections:/config/connections:ro
      - ../connectors:/config/connectors:ro
      - ../endpoints:/config/endpoints:ro
      - ../.secrets:/config/.secrets:ro
    expose:
      - "50051"

  # Engine service - reads pipeline config and streams to destination
  engine:
    image: analitiq-stream:latest
    environment:
      - RUN_MODE=engine
      - DESTINATION_GRPC_HOST=destination
      - DESTINATION_GRPC_PORT=50051
    volumes:
      - ../pipelines:/config/pipelines:ro
      - ../streams:/config/streams:ro
      - ../connections:/config/connections:ro
      - ../connectors:/config/connectors:ro
      - ../endpoints:/config/endpoints:ro
      - ../.secrets:/config/.secrets:ro
    depends_on:
      destination:
        condition: service_healthy
```

### Running Locally

```bash
# Start destination service
cd docker
docker compose up -d destination

# Run pipeline (PIPELINE_ID and ORG_ID passed at runtime)
docker compose run -e PIPELINE_ID=<uuid> -e ORG_ID=<uuid> source_engine
```

### ECS Deployment

For same-task deployment (engine + destination in one task):
- Use `DESTINATION_GRPC_HOST=127.0.0.1`
- No TLS needed inside task namespace

For separate-task deployment:
- Use service discovery for `DESTINATION_GRPC_HOST`
- Consider TLS for cross-task communication

## Protocol Flow

### Successful Batch

```
Engine                                    Destination
   |                                           |
   |-- SCHEMA (stream config) ---------------->|
   |                                           |
   |<-- SCHEMA_ACK (accepted=true) ------------|
   |                                           |
   |-- RECORD_BATCH (seq=1) ------------------>|
   |     payload: [record1, record2, ...]      |
   |     cursor: opaque_token                  |
   |                                           |
   |                       [check _batch_commits: not found]
   |                       [INSERT records]
   |                       [INSERT INTO _batch_commits]
   |                       [COMMIT]
   |                                           |
   |<-- BATCH_ACK (seq=1) ---------------------|
   |     status: SUCCESS                       |
   |     committed_cursor: opaque_token        |
   |                                           |
   | [persist committed_cursor to state]       |
   |                                           |
```

### Idempotent Replay

```
Engine                                    Destination
   |                                           |
   |-- RECORD_BATCH (seq=1, same run_id) ----->|
   |                                           |
   |                       [check _batch_commits: FOUND]
   |                       [return stored cursor]
   |                                           |
   |<-- BATCH_ACK (seq=1) ---------------------|
   |     status: ALREADY_COMMITTED             |
   |     committed_cursor: stored_token        |
   |                                           |
   | [no duplicate data written]               |
```

### Retryable Failure

```
Engine                                    Destination
   |                                           |
   |-- RECORD_BATCH (seq=1) ------------------>|
   |                                           |
   |                       [connection lost mid-write]
   |                       [transaction rolled back]
   |                                           |
   |<-- BATCH_ACK (seq=1) ---------------------|
   |     status: RETRYABLE_FAILURE             |
   |     failure_summary: "Connection reset"   |
   |                                           |
   | [exponential backoff]                     |
   | [retry batch]                             |
   |                                           |
```

### Fatal Failure

```
Engine                                    Destination
   |                                           |
   |-- RECORD_BATCH (seq=1) ------------------>|
   |                                           |
   |                       [constraint violation]
   |                       [transaction rolled back]
   |                                           |
   |<-- BATCH_ACK (seq=1) ---------------------|
   |     status: FATAL_FAILURE                 |
   |     failure_summary: "Unique violation"   |
   |                                           |
   | [send entire batch to DLQ]                |
   | [continue with next batch]                |
   |                                           |
```

## Adding New Destinations

### 1. Create Handler

```python
# src/destination/handlers/mysql.py
from ..base_handler import BaseDestinationHandler, BatchWriteResult

class MySQLHandler(BaseDestinationHandler):
    @property
    def connector_type(self) -> str:
        return "mysql"

    async def connect(self, connection_config):
        # Connect to MySQL
        ...

    async def write_batch(self, run_id, stream_id, batch_seq, records, record_ids, cursor):
        # Check idempotency
        # Write records
        # Store cursor
        # Return BatchWriteResult
        ...
```

### 2. Register Handler

```python
# src/destination/handlers/__init__.py
from .mysql import MySQLHandler

def get_handler(connector_type: str):
    handlers = {
        "postgresql": PostgreSQLHandler,
        "mysql": MySQLHandler,
    }
    return handlers[connector_type.lower()]()
```

### 3. Test

```bash
# Run with new handler
CONNECTOR_TYPE=mysql RUN_MODE=destination python -m src.main
```

## Testing

### Unit Tests

```bash
# Run gRPC unit tests
poetry run pytest tests/unit/grpc_tests/ -v

# Output:
# tests/unit/grpc_tests/test_cursor.py - 14 tests
# tests/unit/grpc_tests/test_client.py - 14 tests
```

### Integration Tests

```bash
# Start destination
RUN_MODE=destination DB_HOST=localhost python -m src.main &

# Run integration test
poetry run pytest tests/integration/test_grpc_streaming.py -v
```

## Performance Tuning

### Batch Size

```
max_batch_rows: 5000 (default)
max_batch_bytes: 8MB (default)
```

Smaller batches = more frequent checkpoints, lower memory
Larger batches = better throughput, higher latency

### gRPC Message Size

```python
options = [
    ('grpc.max_send_message_length', 16 * 1024 * 1024),    # 16MB
    ('grpc.max_receive_message_length', 16 * 1024 * 1024),
]
```

### Connection Pooling (PostgreSQL)

```json
{
  "connection_pool": {
    "min_connections": 2,
    "max_connections": 10
  }
}
```

## Troubleshooting

### Connection Refused

```
grpc._channel._InactiveRpcError: failed to connect to all addresses
```

- Check `DESTINATION_GRPC_HOST` and `DESTINATION_GRPC_PORT`
- Ensure destination service is running
- Check network connectivity between containers

### Schema Rejected

```
Schema rejected: Schema configuration failed
```

- Check destination database credentials
- Verify auto_create_schema/auto_create_table settings
- Check endpoint_schema matches database capabilities

### Batch Timeouts

```
Timeout waiting for ACK on batch N
```

- Increase `GRPC_TIMEOUT_SECONDS`
- Reduce batch size
- Check destination database performance

### Idempotency Issues

```
Duplicate key violation in target table
```

- Verify `_batch_commits` table exists
- Check that handler implements idempotency check
- Ensure same `run_id` across retries

## References

- [Protocol Buffers](https://protobuf.dev/)
- [gRPC Python](https://grpc.io/docs/languages/python/)
- [asyncpg](https://magicstack.github.io/asyncpg/)
