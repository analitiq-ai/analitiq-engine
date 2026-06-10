# gRPC Streaming Architecture

**Scope:** This doc owns the engine<->destination gRPC protocol — wire messages, the Arrow IPC payload, cursor encode/decode, batch idempotency / exactly-once semantics, and the ack/retry flow. Environment variables, docker-compose, the handler registry, and per-handler config live in [destination-config.md](destination-config.md).

## Overview

The engine and its destinations run as separate services connected by a gRPC bidirectional stream. The engine extracts and transforms data, then ships each batch to a destination service that writes it. Decoupling the two lets them scale and fail independently, and lets a destination be written in any gRPC-capable language.

```
+-------------------------------------------------------------+
|                    SOURCE_ENGINE CONTAINER                  |
|  Extract -> Transform -> GrpcLoadStage -> Checkpoint        |
|                              |                              |
|                    DestinationGRPCClient                    |
+-----------------------------+-------------------------------+
                              | gRPC bidirectional stream
                              |
+-----------------------------+-------------------------------+
|               DESTINATION SERVICE CONTAINER                 |
|                    DestinationGRPCServer                    |
|                              |                              |
|     handler (CDK BaseDestinationHandler subclass)           |
+-------------------------------------------------------------+
```

Both containers run the same Docker image, toggled by `RUN_MODE` (`source` or `destination`), and load identical config from the same `PIPELINE_ID` via `PipelineConfigPrep`. The destination picks its connection with `DESTINATION_INDEX`. See [destination-config.md](destination-config.md) for the env vars and compose file.

## Wire Protocol

The service is one bidirectional RPC plus side RPCs (`proto/analitiq/v1/`):

```protobuf
service DestinationService {
  rpc StreamRecords(stream StreamRequest) returns (stream StreamResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
  rpc GetCapabilities(GetCapabilitiesRequest) returns (GetCapabilitiesResponse);
  rpc Shutdown(ShutdownRequest) returns (ShutdownAck);
}
```

On `StreamRecords` the engine sends a `StreamRequest` carrying either a `SchemaMessage` (once, at stream start) or a `RecordBatch`; the destination replies with a `StreamResponse` carrying a `SchemaAck` or a `BatchAck`.

| Message | Direction | Purpose |
|---------|-----------|---------|
| `SchemaMessage` | Engine -> Dest | Identifies the stream + write mode (once at stream start) |
| `SchemaAck` | Dest -> Engine | Whether the schema was accepted |
| `RecordBatch` | Engine -> Dest | Arrow IPC payload + idempotency keys + cursor |
| `BatchAck` | Dest -> Engine | Status, records written, committed cursor |

### Payload format: Arrow IPC only

`PayloadFormat` has exactly one real value, `PAYLOAD_FORMAT_ARROW_IPC` (`UNSPECIFIED = 0`). The engine encodes one `pa.RecordBatch` per message with `pa.ipc.new_stream` (`src/grpc/client.py::_encode_arrow_ipc`); the destination decodes it with `pa.ipc.open_stream` (`src/destination/server.py::_decode_arrow_ipc`). The IPC stream carries the Arrow schema in the same buffer, so batch and schema decode together with no out-of-band coordination. Typed columnar data is preserved end-to-end — there is no dict/JSON round-trip on either side.

### SchemaMessage is slim

`SchemaMessage` carries only `stream_id`, `version`, `write_mode`, and the sender's ack budget. It does **not** transmit columns, primary keys, or target table — both sides already hold the contract artifacts (loaded by `PipelineConfigPrep` from the shared `PIPELINE_ID`), so the destination looks up its endpoint document by `stream_id`. The server translates the wire message into the CDK-native `SchemaSpec` before calling the handler (the CDK never imports gRPC types).

```protobuf
message SchemaMessage {
  string stream_id = 1;
  uint32 version = 2;
  WriteMode write_mode = 3;        // INSERT | UPSERT | TRUNCATE_INSERT
  uint32 ack_timeout_seconds = 4;  // sender's gRPC ack budget
}
```

`ack_timeout_seconds` is the budget the sender actually waits for each ack (`GRPC_TIMEOUT_SECONDS` on the engine, default 30). On every handshake the destination servicer derives the per-statement timeout from it (budget minus a 5s margin, or half the budget when it is too small to spare the margin) and applies it via `set_statement_timeout` before `configure_schema` runs DDL, so a blocked `CREATE TABLE` or write is cancelled and reported before the sender abandons the stream (issues #231, #234). The value rides the handshake instead of being read from the destination's own environment, so the two processes cannot drift. A schema message without it is rejected. The destination shell's worker proxy forwards the engine-stamped budget on the worker hop, min'ed with its own wait, so the worker's bound stays below every waiter on the path.

### RecordBatch

```protobuf
message RecordBatch {
  string run_id = 1;              // idempotency key
  string stream_id = 2;           // idempotency key
  uint64 batch_seq = 3;           // idempotency key, monotonic per stream within run
  PayloadFormat format = 4;       // always PAYLOAD_FORMAT_ARROW_IPC
  bytes payload = 5;              // Arrow IPC stream (single record batch)
  uint32 record_count = 6;
  repeated string record_ids = 7; // stable per-record IDs, parallel to payload rows
  Cursor cursor = 8;              // MAX watermark in this batch
}
```

### Cursor (opaque token)

```protobuf
message Cursor { bytes token = 1; }
```

The cursor is opaque to the destination: it stores the bytes and returns them in the ACK, never interpreting them. The engine owns the format, so cross-database cursor typing never leaks into the destination. The token is JSON for debuggability (`src/grpc/cursor.py`):

```json
{"field": "created_at", "value": "2025-01-08T10:00:00Z",
 "tie_breakers": [{"field": "id", "value": "123"}]}
```

The engine computes it as the **MAX** watermark across the batch (`compute_max_cursor`), not the last record — a batch may be unordered. On ACK, `cursor_to_state_dict` decodes the committed token into checkpoint state shaped as `{"cursor": {"primary": {...}, "tiebreakers": [...]}}`.

### BatchAck and AckStatus

```protobuf
enum AckStatus {
  ACK_STATUS_UNSPECIFIED = 0;
  ACK_STATUS_SUCCESS = 1;            // all written, cursor advanced
  ACK_STATUS_ALREADY_COMMITTED = 2;  // idempotent replay, no-op
  ACK_STATUS_RETRYABLE_FAILURE = 3;  // no commit, safe to retry whole batch
  ACK_STATUS_FATAL_FAILURE = 4;      // no commit, do not retry, send to DLQ
}

message BatchAck {
  string run_id = 1; string stream_id = 2; uint64 batch_seq = 3;
  AckStatus status = 4;
  uint32 records_written = 5;
  Cursor committed_cursor = 6;        // returned on SUCCESS and ALREADY_COMMITTED
  repeated string failed_record_ids = 7;  // optional, may be incomplete
  string failure_summary = 8;
}
```

There is no `PARTIAL_SUCCESS` — batches are all-or-nothing (below).

## Key Design Decisions

### No client keepalive on the engine<->destination channel

The engine deliberately does **not** set `grpc.keepalive_*` options on the client channel — it configures only `max_send/receive_message_length` (`src/grpc/client.py::connect`). The server runs with default keepalive enforcement, which treats unsolicited client pings as abuse and sends a `GOAWAY` ("too many pings") that would tear the stream down mid-batch. Leaving client keepalive unset avoids that.

### Opaque cursor

The destination never parses the cursor; the engine controls its semantics (timestamp + tie-breaker, LSN, composite key, ...). The destination stores it in its idempotency table and echoes it back. This keeps cursor typing out of every destination.

### Batch-level idempotency, exactly-once

Each batch is keyed by `(run_id, stream_id, batch_seq)`. Before applying a batch the handler checks whether that key was already committed:

1. Already committed -> return `ALREADY_COMMITTED` with the stored cursor (no rewrite).
2. New -> write records, record the commit, return `SUCCESS`.

This survives a network failure that drops the ACK after the destination already committed: the engine retries, the destination recognizes the key, and no duplicate data lands. The commit-record table is the handler's concern; see [destination-config.md](destination-config.md#idempotency).

### All-or-nothing batches

A batch wholly succeeds or wholly fails. The cursor advances only on `SUCCESS`, there is no partial-rollback bookkeeping, and DLQ routing is per-batch. This is why `AckStatus` has no partial value.

| Failure | AckStatus | Engine action |
|---------|-----------|---------------|
| ACK lost after commit (replay) | `ALREADY_COMMITTED` | Persist cursor, continue |
| Connection drop / timeout / deadlock | `RETRYABLE_FAILURE` | Retry whole batch with backoff |
| Constraint violation / type error | `FATAL_FAILURE` | Send whole batch to DLQ, continue |

### Strict in-order processing

The client sends a batch, awaits its ACK, then sends the next (`send_batch` blocks on the response queue). In-order ACKs keep cursor checkpointing monotonic.

### Record IDs for DLQ correlation

`record_ids` runs parallel to the payload rows and is stable across retries, so a DLQ'd row traces back to its source record even after a batch is reprocessed.

## Type Map (code surfaces)

| Concern | Engine side | Destination side |
|---------|-------------|------------------|
| Client / server | `src/grpc/client.py::DestinationGRPCClient` | `src/destination/server.py::DestinationGRPCServer` |
| Send a batch | `send_batch(run_id, stream_id, batch_seq, record_batch: pa.RecordBatch, record_ids, cursor) -> BatchResult` | handler `write_batch(..., record_batch: pa.RecordBatch, record_ids, cursor) -> BatchWriteResult` |
| Configure stream | `start_stream(...)` -> `SchemaMessage` | handler `configure_schema(schema_spec: SchemaSpec) -> bool` |
| Liveness | `HealthCheck` RPC | handler `health_check() -> bool` |
| Arrow IPC | `_encode_arrow_ipc` (`pa.ipc.new_stream`) | `_decode_arrow_ipc` (`pa.ipc.open_stream`) |
| Cursor | `src/grpc/cursor.py` (`compute_max_cursor`, `encode_cursor`, `cursor_to_state_dict`) | stores/returns the opaque token |

Handlers subclass the CDK base (`from cdk.base_handler import BaseDestinationHandler`; `from cdk.types import Cursor, SchemaSpec, WriteMode`). The SQL destination is `GenericSQLConnector` (`cdk/cdk/sql/generic.py`). The handler registry and how new destinations are wired live in [destination-config.md](destination-config.md#handler-registry).

## Message Flow

### Successful batch
```
Engine                                    Destination
  |-- SchemaMessage (stream_id, write_mode) ->|
  |<-- SchemaAck (accepted=true) -------------|
  |-- RecordBatch (seq=1, Arrow IPC, cursor) ->|
  |                      [key not in commits -> write rows -> record commit]
  |<-- BatchAck (SUCCESS, committed_cursor) --|
  | [persist committed_cursor to state]       |
```

### Idempotent replay
```
  |-- RecordBatch (seq=1, same run_id) ------->|
  |                      [key found -> no rewrite]
  |<-- BatchAck (ALREADY_COMMITTED, stored cursor) -|
```

### Retryable failure
```
  |-- RecordBatch (seq=1) -------------------->|
  |                      [connection dropped mid-write -> rolled back]
  |<-- BatchAck (RETRYABLE_FAILURE) ----------|
  | [backoff, retry same batch]               |
```

### Fatal failure
```
  |-- RecordBatch (seq=1) -------------------->|
  |                      [constraint violation -> rolled back]
  |<-- BatchAck (FATAL_FAILURE, summary) -----|
  | [send whole batch to DLQ, continue]       |
```

## Testing

The gRPC units live in `tests/unit/grpc_tests/` (`poetry run pytest tests/unit/grpc_tests/`). The end-to-end contract is the docker-compose run itself — both services resolve identical config from one `PIPELINE_ID`:

```bash
cd docker && PIPELINE_ID=<id> docker compose run --rm source_engine
```
