# gRPC Streaming Architecture

**Scope:** This doc owns the engine<->destination gRPC protocol — wire messages, the Arrow IPC payload, cursor encode/decode, row-level idempotency semantics, and the ack/retry flow. Environment variables, docker-compose, the handler registry, and per-handler config live in [destination-config.md](destination-config.md).

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
| `SchemaAck` | Dest -> Engine | Whether the schema was accepted, plus the stream's retry-safety verdict (`retry_semantics` + reason) |
| `RecordBatch` | Engine -> Dest | Arrow IPC payload + content-derived record ids + cursor |
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

`ack_timeout_seconds` is the budget the sender actually waits for each ack (`GRPC_TIMEOUT_SECONDS` on the engine, default 30). On every handshake the destination servicer derives the per-statement timeout from it (budget minus a 5s margin, or half the budget when it is too small to spare the margin) and applies it via `set_statement_timeout` before `configure_schema` runs DDL, so a blocked `CREATE TABLE` or write is cancelled and reported before the sender abandons the stream (issues #231, #234). The value rides the handshake instead of being read from the destination's own environment, so the two processes cannot drift. A schema message without it is rejected. The destination shell's worker proxy forwards the engine-stamped budget on the worker hop and adopts it as that hop's own ack wait, so the worker's statement bound tracks the budget the engine actually waits — end-to-end, with no dependence on the destination container's environment.

### RecordBatch

```protobuf
message RecordBatch {
  string run_id = 1;              // routing/scoping identifier (not an idempotency key)
  string stream_id = 2;           // routing/scoping identifier (not an idempotency key)
  uint64 batch_seq = 3;           // monotonic ordering/log sequence per stream within a run (not an idempotency key)
  PayloadFormat format = 4;       // always PAYLOAD_FORMAT_ARROW_IPC
  bytes payload = 5;              // Arrow IPC stream (single record batch)
  uint32 record_count = 6;
  repeated string record_ids = 7; // content-derived row identities (SHA-256), parallel to payload rows
  Cursor cursor = 8;              // MAX watermark in this batch
  int64 emitted_at_unix_ms = 9;   // UTC epoch ms the engine emitted this batch; stamped once, stable across retries
}
```

`emitted_at_unix_ms` is the engine's per-batch emit instant (UTC epoch milliseconds), stamped once in the load stage and re-sent unchanged on every retry of the same batch (`src/engine/stream_processor.py`). The servicer decodes it to a timezone-aware `datetime` and hands it to `write_batch` as `emitted_at`. A time-partitioned destination (file/S3) resolves its `{year}/{month}/{day}/{hour}` placeholders from this value rather than its own write-time clock, so a replayed batch resolves the same output path and overwrites in place instead of drifting into a new partition directory across an hour/day boundary (issue #353). Sinks without time-based partitioning ignore it; a sink whose `path_template` actually substitutes a time placeholder fails the batch loud if the value is unstamped (epoch 0).

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
  ACK_STATUS_ALREADY_COMMITTED = 2;  // idempotent replay, no-op — for a handler that detects a prior commit; no in-tree destination returns it (sinks that dedup — SQL, file — do it in the write and return SUCCESS; per-handler verdicts below)
  ACK_STATUS_RETRYABLE_FAILURE = 3;  // no commit, safe to retry whole batch
  ACK_STATUS_FATAL_FAILURE = 4;      // no commit, do not retry, send to DLQ
}

enum FailureCategory {
  FAILURE_CATEGORY_UNSPECIFIED = 0;    // nothing declared; engine falls back to summary matching
  FAILURE_CATEGORY_CONFIG_DEFECT = 1;  // deterministic, user-fixable configuration defect
  FAILURE_CATEGORY_WRITE_REJECTED = 2; // write attempted and failed: constraint, permission, driver error
  FAILURE_CATEGORY_NOT_READY = 3;      // nothing attempted: never connected / schema never configured
}

message BatchAck {
  string run_id = 1; string stream_id = 2; uint64 batch_seq = 3;
  AckStatus status = 4;
  uint32 records_written = 5;
  Cursor committed_cursor = 6;        // returned on SUCCESS and ALREADY_COMMITTED
  repeated string failed_record_ids = 7;  // optional, may be incomplete
  string failure_summary = 8;
  FailureCategory failure_category = 9;   // machine-readable, set on failure acks (issue #351)
}
```

There is no `PARTIAL_SUCCESS` — batches are all-or-nothing (below).

`failure_summary` answers *what went wrong* (human-readable, free text); `failure_category` answers *who owns the fix* (machine-readable), so the engine maps a failed batch to a customer-facing error code without parsing summary text. The vocabulary is engine-owned: connectors are not asked to self-classify. It is set by the config-defect and write-failure excepts in `cdk/sql/generic.py`, by every pre-flight guard via `reject_batch`, and by the destination servicer's own batch-before-schema rejection. A thick connector that overrides `write_batch` leaves it `UNSPECIFIED` and the engine falls back to matching the summary. The value is range-checked when read off the wire — an unrecognised integer degrades to `UNSPECIFIED` — and an in-range value is used as declared.

## Key Design Decisions

### No client keepalive on the engine<->destination channel

The engine deliberately does **not** set `grpc.keepalive_*` options on the client channel — it configures only `max_send/receive_message_length` (`src/grpc/client.py::connect`). The server runs with default keepalive enforcement, which treats unsolicited client pings as abuse and sends a `GOAWAY` ("too many pings") that would tear the stream down mid-batch. Leaving client keepalive unset avoids that.

### Opaque cursor

The destination never parses the cursor; the engine controls its semantics (timestamp + tie-breaker, LSN, composite key, ...). The destination stores the cursor and echoes it back in the ACK; it keeps no batch idempotency table. This keeps cursor typing out of every destination.

### Row-level, content-derived idempotency

Idempotency is enforced at the destination on **row identity**, not batch position. `batch_seq` is a monotonic ordering/log sequence per stream within a run — it is never the dedup key, and neither are `run_id`/`stream_id`. The SQL destination writes idempotently by content and returns `SUCCESS`; it keeps no per-batch commit ledger. How identity is enforced depends on the write mode:

- **`upsert`** — MERGE / INSERT-or-UPDATE on the stream's `conflict_keys`.
- **`truncate_insert`** — full refresh: TRUNCATE on the read's first batch (`batch_seq` 1, issue #307), plain append after that. `batch_seq` is the engine's own statement of a fresh read — it restarts at 1 only when the engine (re)starts the stream read — so the decision survives engine and destination restarting independently: a destination that restarts mid-refresh keeps appending (committed batches survive), and an engine that restarts re-reads from scratch and re-truncates. The engine never resumes a truncate_insert stream from a cursor.
- **`insert`** — each row is inserted only if its identity is not already present (one `INSERT ... SELECT ... WHERE NOT EXISTS (...)` per row, built with SQLAlchemy core, no dialect-specific SQL). The identity is the contract primary key, or — for a keyless insert stream — a synthetic engine-managed `_record_hash` column (full SHA-256 of the row content) declared as the table's `PRIMARY KEY`, the structural uniqueness backstop. Two byte-identical keyless rows collapse to one.

This survives a network failure that drops the ACK after the destination already wrote: the engine retries, the row identity dedups, and no duplicate data lands. See [destination-config.md](destination-config.md#idempotency).

ADBC-only transports (Snowflake/BigQuery) do not yet do the keyless `insert` anti-join — plain `insert` there is at-least-once (a noted follow-up); `upsert` remains idempotent.

A destination that does not dedup itself is **at-least-once on a same-`RUN_ID` retry**: the engine keeps no in-run pre-send skip, so a restart re-sends already-ACKed batches, and a positional pre-send skip cannot be made correct without reintroducing the row-drop this design removes (an advancing cursor re-batches the same `batch_seq` over different rows). Per handler (issue #286):

- **API, `upsert` mode** — exactly-once: the endpoint dedups on its `conflict_keys`; a re-sent record updates in place.
- **API, `insert` or `upsert` mode with a declared `idempotency` block** — exactly-once within the provider's replay window. The api-endpoint contract's `operations.write.<mode>.idempotency` (`{"in": "header" | "body", "name": "<key>"}`, infra#890) declares **where** the key lands; the engine owns the **value**, per write mode: `insert` sends the identity-derived `record_id` (primary-key fields when declared, else full content — SQL-insert parity: first occurrence of an identity wins), `upsert` sends a full-content hash (an identical replay dedups; a changed row gets a new key so the provider applies the update). Excluded with a `batching` block (the contract has no batching mode — a present block IS the multi-record case; both the schema and `configure_schema` reject the combination, because a restart re-batches records and a per-request key spanning several records can never dedup); `in: "body"` additionally requires a JSON-object request body, and the key name must not collide with engine/connection-owned headers or already-declared body fields.
- **API, `insert` mode without the block** — at-least-once: the engine rents the sink and has no key to dedup on.
- **file / S3** — batch files are content-addressed (the filename carries a hash of the serialized bytes, #319), so a true replay overwrites the same file with the same bytes — atomically: the local backend writes temp-then-rename, so a crash mid-rewrite cannot truncate committed output. There is no batch-level commit ledger, so nothing can misclassify a restart batch as a replay and drop its rows (#306). A same-run restart re-reads the inclusive cursor boundary and writes those rows into a new file — duplicates possible, drops not. Reported as at-least-once.
- **SQL `truncate_insert`** — the truncate runs on the read's first batch only (issue #307) and the append phase has no row-identity dedup (deduping a full refresh would collapse legitimate duplicate rows), so a replayed already-committed later batch re-inserts its rows (a replayed *first* batch re-truncates, landing exactly once). Reported as at-least-once. The restart data-loss case is gone: the engine never resumes a truncate_insert stream from a cursor, so a restart is a fresh full refresh, not a resumed slice.
- **stdout** — at-least-once by construction: it only prints, so a replayed batch prints again.

Every handler reports its verdict per stream in the `SchemaAck` (`retry_semantics` + `retry_semantics_reason`, forwarded verbatim across the worker-proxy hop), and the engine logs it at stream start — the operator learns which streams may duplicate on a restart before any data moves.

### All-or-nothing batches

A batch wholly succeeds or wholly fails. The cursor advances only on `SUCCESS`, there is no partial-rollback bookkeeping, and DLQ routing is per-batch. This is why `AckStatus` has no partial value.

| Failure | AckStatus | Engine action |
|---------|-----------|---------------|
| ACK lost after write (replay), SQL destination | `SUCCESS` | Row identity dedups the rewrite; persist cursor, continue |
| ACK lost after write (replay), file destination | `SUCCESS` | Same bytes hash to the same filename; the rewrite overwrites the same file; persist cursor, continue |
| Connection drop / timeout / deadlock | `RETRYABLE_FAILURE` | Retry whole batch with backoff |
| Constraint violation / type error | `FATAL_FAILURE` | Send whole batch to DLQ, continue |

### Strict in-order processing

The client sends a batch, awaits its ACK, then sends the next (`send_batch` blocks on the response queue). In-order ACKs keep cursor checkpointing monotonic.

### Record IDs for DLQ correlation

`record_ids` are content-derived row identities (SHA-256 of the row content), parallel to the payload rows and stable across retries, so a DLQ'd row traces back to its source record even after a batch is reprocessed. For a keyless `insert` stream the same hash is the `_record_hash` primary-key value written to the destination.

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
  |                      [write rows, deduped by row identity / content]
  |<-- BatchAck (SUCCESS, committed_cursor) --|
  | [persist committed_cursor to state]       |
```

### Idempotent replay
```
  |-- RecordBatch (seq=1, same run_id) ------->|
  |          [identity dedups: SQL MERGE no-ops, file rewrites same bytes]
  |<-- BatchAck (SUCCESS, committed_cursor) --|
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
