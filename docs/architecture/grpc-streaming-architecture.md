# gRPC Streaming Architecture

**Scope:** This doc owns the engine<->destination gRPC protocol — wire messages, the Arrow IPC payload, cursor encode/decode, the protocol-level idempotency design (row identity vs. batch position, how a retry-safety verdict crosses the wire), and the ack/retry flow. Per-handler idempotency mechanics and the handler registry live in [destination-config.md](../config/destination-config.md); the environment-variable catalogue is [`src/config/settings.py`](../../src/config/settings.py) (see also [README.md](../../README.md#environment-variables)); the `docker compose` setup is [`docker/docker-compose.yml`](../../docker/docker-compose.yml).

## Overview

The engine and its destinations run as separate services connected by a gRPC bidirectional stream. The engine extracts and transforms data, then ships each batch to a destination service that writes it (see [ADR 0009](../adr/0009-engine-and-destination-are-separate-services-joined-by-grpc-carrying-arrow-ipc.md)).

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

`RUN_MODE` (`source` or `destination`) selects a container's role, and `DESTINATION_INDEX` the destination's connection. See [`docker/docker-compose.yml`](../../docker/docker-compose.yml) for the compose file and [README.md](../../README.md#environment-variables) for the env vars.

## Wire Protocol

The service is one bidirectional RPC plus side RPCs (`proto/analitiq/v1/`).
The generated Python lives under `src/grpc/generated/` and is committed.
`proto/generate.sh` regenerates the `_pb2` and `_pb2_grpc` modules from every
definition; the package's `__init__.py` re-exports them and is hand-written.
A conformance test holds all three to the definitions -- descriptors, dispatch
paths, and re-exported names -- so an edited `.proto` cannot ship without them.

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
| `SchemaAck` | Dest -> Engine | Whether the schema was accepted, the stream's retry-safety verdict (`retry_semantics` + reason), and on a rejection the `failure_category` naming who owns it |
| `RecordBatch` | Engine -> Dest | Arrow IPC payload + content-derived record ids + cursor |
| `BatchAck` | Dest -> Engine | Status, records written, committed cursor |

### Payload format: Arrow IPC only

The payload is Arrow IPC (see [ADR 0009](../adr/0009-engine-and-destination-are-separate-services-joined-by-grpc-carrying-arrow-ipc.md)). The engine encodes one `pa.RecordBatch` per message with `pa.ipc.new_stream` (`src/grpc/client.py::DestinationGRPCClient.send_batch`); the destination decodes it with `pa.ipc.open_stream` (`src/destination/server.py::DestinationServicer.StreamRecords`).

### SchemaMessage is slim

The destination looks up its endpoint document by `stream_id` (see [ADR 0009](../adr/0009-engine-and-destination-are-separate-services-joined-by-grpc-carrying-arrow-ipc.md)). The server translates the wire message into the CDK-native `SchemaSpec` before calling the handler (the CDK never imports gRPC types).

```protobuf
message SchemaMessage {
  string stream_id = 1;
  uint32 version = 2;
  WriteMode write_mode = 3;        // INSERT | UPSERT | TRUNCATE_INSERT
  uint32 ack_timeout_seconds = 4;  // sender's gRPC ack budget
}
```

`ack_timeout_seconds` is the budget the sender actually waits for each ack (`GRPC_TIMEOUT_SECONDS` on the engine, default 30). On every handshake the destination servicer derives the per-statement timeout from it (budget minus a 5s margin, or half the budget when it is too small to spare the margin) and applies it via `set_statement_timeout` before `configure_schema` runs DDL, so a blocked `CREATE TABLE` or write is cancelled and reported before the sender abandons the stream. The value rides the handshake instead of being read from the destination's own environment, so the two processes cannot drift. A schema message without it is rejected. The destination shell's worker proxy forwards the engine-stamped budget on the worker hop and adopts it as that hop's own ack wait, so the worker's statement bound tracks the budget the engine actually waits — end-to-end, with no dependence on the destination container's environment.

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

`emitted_at_unix_ms` is the engine's per-batch emit instant (UTC epoch milliseconds), stamped once per batch and re-sent unchanged on every retry of it (`src/engine/stream_processor.py`). The servicer decodes it to a timezone-aware `datetime` and hands it to `write_batch` as `emitted_at`. A time-partitioned file destination resolves its `{year}/{month}/{day}/{hour}` placeholders from this value (see [ADR 0014](../adr/0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md)). Sinks without time-based partitioning ignore it; a sink whose `path_template` actually substitutes a time placeholder fails the batch loud if the value is unstamped (epoch 0).

#### Batch-level metadata rides the Arrow schema

An API read's declared `response.metadata` (a provider-reported total, a rate-limit budget) is resolved once per page by the CDK read loop and written into the Arrow schema metadata of the batch that page became, under the key `analitiq.response_metadata` as one JSON object (`cdk/cdk/batch_metadata.py`). Arrow IPC carries schema metadata inside `payload`, so the values cross the source worker -> engine hop with no wire field of their own. The engine's extract stage reads the slot off every batch as it arrives, logs it, and the last page's values land on the stream's metrics record as `response_metadata`. The empty page that ends a read is yielded as a zero-row batch when metadata is declared, so a provider's `total: 0` or its final rate-limit budget still lands; a read that declares no metadata skips that page and its batches carry no key. On a stream with mapping assignments the transform stage produces the mapping's own schema, so the key stops at extract. On a stream with no assignments (`src/engine/stream_processor.py` builds a transform only when there are assignments) the source batch is forwarded untouched, so its schema metadata crosses to the destination worker inside the IPC payload, where nothing reads it: a SQL destination drops it in `cast_arrow_batch`, and file/stdout destinations read `LandingBatch.records` alone.

### Cursor (opaque token)

```protobuf
message Cursor { bytes token = 1; }
```

The CDK's handlers return the token unread in the ACK (see [ADR 0012](../adr/0012-the-cursor-is-opaque-to-the-destination.md)). The token is JSON for debuggability (`src/grpc/cursor.py`):

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
  FAILURE_CATEGORY_UNSPECIFIED = 0;    // nothing declared; the raising stage names the code
  FAILURE_CATEGORY_CONFIG_DEFECT = 1;  // deterministic, user-fixable configuration defect
  FAILURE_CATEGORY_WRITE_REJECTED = 2; // write attempted and failed: constraint, permission, driver error
  FAILURE_CATEGORY_NOT_READY = 3;      // nothing attempted: never connected / schema never configured
  FAILURE_CATEGORY_INTERNAL = 4;       // the engine or the connector has a bug
}

message BatchAck {
  string run_id = 1; string stream_id = 2; uint64 batch_seq = 3;
  AckStatus status = 4;
  uint32 records_written = 5;
  Cursor committed_cursor = 6;        // the checkpoint; read only off a SUCCESS ack
  repeated string failed_record_ids = 7;  // optional, may be incomplete
  string failure_summary = 8;
  FailureCategory failure_category = 9;   // machine-readable, set on failure acks
}
```

There is no partial status (see [ADR 0011](../adr/0011-a-batch-is-accepted-or-rejected-whole.md)).

`failure_summary` is human-readable free text; `failure_category` is machine-readable and names who owns the fix (see [ADR 0001](../adr/0001-two-failure-vocabularies.md)).

Any sender may declare a category — the config-defect and write-failure excepts in `cdk/cdk/sql/generic.py`, every pre-flight guard via `reject_batch`, the destination servicer's own batch-before-schema rejection, and a thick connector that overrides `write_batch`.

`SchemaAck` carries the same field: a destination shell that could not reach its connector worker declares `NOT_READY`, so the engine can tell that refusal apart from one the customer's configuration caused without reading the message text.

## Key Design Decisions

### No client keepalive on the engine<->destination channel

The engine deliberately does **not** set `grpc.keepalive_*` options on the client channel — it configures only `max_send/receive_message_length` (`src/grpc/client.py::connect`). The server (`src/destination/server.py`) sets `grpc.http2.min_ping_interval_without_data_ms` and `grpc.http2.max_ping_strikes` as deliberate hardening against an aggressive client ping schedule — not a gRPC out-of-the-box default — and a client that re-adds keepalive options would trip it: too-frequent pings read as abuse and the server sends a `GOAWAY` ("too many pings") that would tear the stream down mid-batch. Leaving client keepalive unset avoids that.

### Opaque cursor

The cursor is opaque to the destination ([ADR 0012](../adr/0012-the-cursor-is-opaque-to-the-destination.md)).

### Write idempotency

Write idempotency is decided in [ADR 0014](../adr/0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md). Per-handler mechanics, including which write modes report at-least-once, are in [destination-config.md](../config/destination-config.md#idempotency).

Each handler reports a retry-safety verdict per stream in an accepted `SchemaAck` (`retry_semantics` + `retry_semantics_reason`). The worker proxy re-reports the worker's verdict, or the at-least-once default when the worker sent none. The engine reads it only to log it at INFO at stream start, not to gate a retry (see [ADR 0013](../adr/0013-prefer-duplicates-over-data-loss.md)).

### All-or-nothing batches

A batch is accepted or rejected whole, though a rejected batch may be partly written ([ADR 0011](../adr/0011-a-batch-is-accepted-or-rejected-whole.md)).

| Failure | AckStatus | Engine action |
|---------|-----------|---------------|
| ACK lost after write (replay), SQL destination | `SUCCESS` | Row identity dedups the rewrite; persist cursor, continue |
| ACK lost after write (replay), file destination | `SUCCESS` | Same bytes hash to the same filename; the rewrite overwrites the same file; persist cursor, continue |
| Connection drop / timeout / deadlock, reported by the destination | `RETRYABLE_FAILURE` | Retry the whole batch with backoff up to `max_retries`, then the error strategy decides |
| No ack at all: stream torn down, peer closed, ACK timed out, rebuild failed | `RETRYABLE_FAILURE`, synthesized by the client | Same (see [ADR 0013](../adr/0013-prefer-duplicates-over-data-loss.md)) |
| Constraint violation / type error | `FATAL_FAILURE` | The stream fails; under `dlq` the whole batch is written out first |

### One verdict per batch

A batch's verdict comes from `src/engine/batch_policy.py`. A `BatchPolicy` is bound once per stream from `error_strategy` + `max_retries`, owns the whole send -> ack -> backoff-retry loop, and returns one terminal `Disposition` — `Committed(cursor)`, `AlreadyCommitted`, `DeadLetter(report)`, `Skipped(report)` or `Failed(report)`. `StreamProcessor` acts on that verdict, except that a retry-exhausted first batch of a `truncate_insert` stream fails the stream whatever the verdict. Three rules hold it in place.

**A transport failure is retryable, decided where it is detected.** A send that reaches no ack — a torn-down stream, a closed peer, an ACK timeout, a failed rebuild — is answered RETRYABLE in `src/grpc/client.py`, bounded by `max_retries` (see [ADR 0013](../adr/0013-prefer-duplicates-over-data-loss.md)).

**The cursor rides the success variant.** `Committed` is the only disposition with a cursor field (see [ADR 0013](../adr/0013-prefer-duplicates-over-data-loss.md)). An ack that pairs a cursor with a failure status is answered with a contract-violation verdict naming the peer that sent it.

**One loop, one reaction ladder.** The policy executes the send, so there is no second call site to diverge from: the batch loop and the zero-batch synthetic truncate reach their verdict the same way and are acted on by the same ladder. `error_strategy` (the pipeline's `runtime.error_handling.strategy`) belongs to the policy, not to the failure report: it maps a retry-exhausted batch to `DeadLetter` / `Skipped` / `Failed`. A `FATAL_FAILURE` verdict follows [ADR 0011](../adr/0011-a-batch-is-accepted-or-rejected-whole.md).

### Strict in-order processing

One batch is in flight per stream: the stream processor sends the next batch only after `send_batch` returns, which is when its ACK arrives or the send fails (ACK timeout, dead stream) (see [ADR 0010](../adr/0010-one-batch-in-flight-per-stream-strictly-in-order.md)).

### Record IDs

`record_ids` are content-derived row identities, parallel to the payload rows and stable across retries (see [ADR 0014](../adr/0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md)).

## Type Map (code surfaces)

| Concern | Engine side | Destination side |
|---------|-------------|------------------|
| Client / server | `src/grpc/client.py::DestinationGRPCClient` | `src/destination/server.py::DestinationGRPCServer` |
| Send a batch | `send_batch(run_id, stream_id, batch_seq, record_batch: pa.RecordBatch, record_ids, cursor) -> BatchResult` | handler `write_batch(..., record_batch: pa.RecordBatch, record_ids, cursor) -> BatchWriteResult` |
| Configure stream | `start_stream(...)` -> `SchemaMessage` | handler `configure_schema(schema_spec: SchemaSpec) -> bool` |
| Liveness | `HealthCheck` RPC | handler `health_check() -> bool` |
| Arrow IPC | `DestinationGRPCClient.send_batch` (`pa.ipc.new_stream`) | `DestinationServicer.StreamRecords` (`pa.ipc.open_stream`) |
| Cursor | `src/grpc/cursor.py` (`compute_max_cursor`, `encode_cursor`, `cursor_to_state_dict`) | stores/returns the opaque token |

Handlers subclass the CDK base (`from cdk.base_handler import BaseDestinationHandler`; `from cdk.types import Cursor, SchemaSpec, WriteMode`). The SQL destination is `GenericSQLConnector` (`cdk/cdk/sql/generic.py`). The handler registry and how new destinations are wired live in [destination-config.md](../config/destination-config.md#handler-registry).

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
  | [backoff, resend the same batch]          |
  |-- RecordBatch (seq=1) -------------------->|
  |<-- BatchAck (SUCCESS, committed_cursor) --|
  | [persist committed_cursor to state]       |
```

### No ack at all
```
  |-- RecordBatch (seq=1) -------------------->|
  |                      [worker crashed / stream torn down mid-write]
  |            X  (no BatchAck)                |
  | [client synthesizes RETRYABLE; the stream  |
  |  is rebuilt and the batch resent]          |
```

### Retries exhausted
```
  | [max_retries spent on RETRYABLE acks]      |
  | [error strategy decides: dlq -> write the  |
  |  batch out and continue; skip -> drop it   |
  |  and continue; fail -> fail the stream]    |
```

### Fatal failure
```
  |-- RecordBatch (seq=1) -------------------->|
  |                      [constraint violation -> rolled back]
  |<-- BatchAck (FATAL_FAILURE, summary) -----|
  | [fail the stream; under dlq write the      |
  |  whole batch out first]                    |
```

## Testing

The gRPC units live in `tests/unit/grpc_tests/` (`poetry run pytest tests/unit/grpc_tests/`). The end-to-end contract is the `docker compose` run itself — both services resolve identical config from one `PIPELINE_ID`:

```bash
cd docker && PIPELINE_ID=<id> docker compose run --rm source_engine
```
