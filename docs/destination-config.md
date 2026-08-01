# Destination Configuration Reference

**Scope:** this doc owns the destination handler kinds and their config —
write modes, formatters, storage, idempotency, and destination env vars.
For the Arrow type system and the ADBC-vs-SQLAlchemy transport detail see
[`pyarrow-and-destinations.md`](pyarrow-and-destinations.md); for the gRPC
wire protocol see
[`grpc-streaming-architecture.md`](grpc-streaming-architecture.md); for the
CDK connector contract see
[`connector-module-architecture.md`](connector-module-architecture.md).

Destinations are selected at runtime from the pipeline's
`connections.destinations` list (indexed by `DESTINATION_INDEX` env var).
The destination service uses the same Docker image as the source engine
(`RUN_MODE=destination`) and loads its connection via `PipelineConfigPrep`
just like the engine does.

For the connection / connector schema itself, see
[`source-config.md`](source-config.md) and
[`connector-module-architecture.md`](connector-module-architecture.md).

## Architecture

```
+-----------------------------------------------------------+
|                  TRANSPORT (gRPC server)                  |
+-----------------------------------------------------------+
                             |
+-----------------------------------------------------------+
|                  HANDLER LAYER (orchestration)            |
| GenericSQLConnector    GenericAPIConnector                |
| GenericFileConnector   GenericStdoutConnector             |
+-----------------------------------------------------------+
                             |
              +--------------+--------------+
              |                             |
+----------------------+        +----------------------+
|     WRITER LAYER     |        |   FORMATTER LAYER    |
| SQLAlchemy / ADBC    |        | jsonl / csv / parquet|
| aiohttp / file writer|        |                      |
+----------------------+        +----------------------+
```

`GenericSQLConnector` is the single database handler. It serves both the
source and destination roles and supports two transports selected by the
connector definition: `transport_type: "sqlalchemy"` (SQLAlchemy engine —
async for dialects with an async driver such as Postgres asyncpg and MySQL
aiomysql, plain sync for sync-only drivers such as Redshift
`redshift_connector`, dispatched via `asyncio.to_thread`) and
`transport_type: "adbc"` (ADBC DBAPI — Snowflake, BigQuery). The
transport detail lives in
[`pyarrow-and-destinations.md`](pyarrow-and-destinations.md).

`GenericAPIConnector` is the single API connector, and it serves both roles
the same way: one connect, one HTTP round trip, one classification of what a
response status means, one paging loop. What varies between an API read and
an API write is the endpoint document's `operations` block, not the class.

Every connector family lives in the CDK at `cdk/cdk/`, alongside the shared
building blocks. The engine keeps only the gRPC server that fronts them
(`src/destination/server.py`):

- `sql/generic.py` — `GenericSQLConnector` (the database handler).
- `api/generic.py` — `GenericAPIConnector` (the API handler, and the API
  source).
- `file/generic.py` — `GenericFileConnector` (the `file` and `s3` kinds).
- `stdout/generic.py` — `GenericStdoutConnector`.
- `file/backend.py` + `file/local_backend.py` — `BaseStorageBackend`, the
  storage transport seam, and the local filesystem backend behind it.
- `formatters/` — JSONL / CSV / Parquet serializers. Top-level rather than
  inside `file/` because the stdout family serializes batches too, and
  nesting the vocabulary under one family would make the other import it.
- `base_handler.py` — `BaseDestinationHandler` ABC and `BatchWriteResult`.
- `schema_contract.py` — Arrow-based `SchemaContract`.
- `type_map/mapper.py` — `TypeMapper`, the canonical-Arrow <-> native-type
  mapper driven by the connector's own type maps.
- `sql/ddl.py` — `build_create_table_sql`, which renders each column's
  canonical type through `SqlDialect.render_column_type`.
- `registry.py` — `ConnectorRegistry` / `KIND_DEFAULTS` / `build_registries`.

## Environment Variables

### Engine mode (gRPC client)

| Variable | Required | Description |
|----------|----------|-------------|
| `DESTINATION_GRPC_HOST` | When using a remote destination | Hostname of destination gRPC server |
| `DESTINATION_GRPC_PORT` | No | `50051` (default) |
| `GRPC_TIMEOUT_SECONDS` | No | `30` (default) — the engine's ack budget, stamped on the schema handshake |

### Destination server mode

| Variable | Required | Description |
|----------|----------|-------------|
| `RUN_MODE` | Yes | Set to `destination` |
| `PIPELINE_ID` | Yes | Same pipeline ID the engine uses |
| `DESTINATION_INDEX` | No | Index into `pipeline.connections.destinations` (default: `0`) |
| `GRPC_PORT` | No | gRPC listen port (default: `50051`) |
| `LOG_LEVEL` | No | `INFO` (default) |
| `ENV` | No | `loc` (default) — skips the remote config fetch |

Both engine and destination read the same `PIPELINE_ID` and load identical
configuration via `PipelineConfigPrep`. Credentials are read from the
local config volume — they are **never** transmitted over gRPC.

## Handler Registry

Handlers are mapped by the connector's `connector_type` (its *kind*). Both
registries — source and destination — are built in one place,
`build_registries()` in `cdk/cdk/registry.py`, called inside the worker
subprocess because that is where connector classes execute. Nothing else
builds one: the engine and the destination service hold clients, not
connector code.

Kind defaults, named once in `cdk.registry.KIND_DEFAULTS`:

| `connector_type` | Handler | Use Case |
|------------------|---------|----------|
| `database` | `GenericSQLConnector` | All SQL dialects via SQLAlchemy or ADBC (PostgreSQL, MySQL, Snowflake, BigQuery, Redshift) |
| `api` | `GenericAPIConnector` | REST endpoints |
| `file` | `GenericFileConnector` | Local filesystem |
| `s3` | `GenericFileConnector` | Object storage — planned; refused at connect until its storage backend exists |
| `stdout` | `GenericStdoutConnector` | Diagnostics / debugging |

One class serves a kind, and it is registered into the registries whose
capability Protocol it implements: source iff `Readable`, destination iff
`Writable` (`cdk/cdk/contract.py`). `database` and `api` therefore seed the
same object into both, while `file`, `s3` and `stdout` seed the destination
registry only — a `kind: file` *source* raises
`ConnectorNotRegisteredError` rather than resolving a class with no read
path. Reading the answer off the class is what keeps a hand-written table
from disagreeing with the code it describes.

Externally installed connector packages add themselves through the
`analitiq.destination_connectors` entry-point group (discovered at
registry build time).

The set of runnable connector kinds is owned by the worker registry:
a kind that is neither a kind default nor registry-discovered fails
at worker startup with `ConnectorNotRegisteredError`
(`cdk/cdk/registry.py`); neither the engine nor the CDK pins a parallel
kind enum.

### Handler capabilities

| Handler | Transactions | Upsert | Bulk Load |
|---------|--------------|--------|-----------|
| Database | Yes | Yes (via `ON CONFLICT` / `MERGE` / dialect equivalent) | Yes |
| API | No | Contract-driven (`operations.write.upsert`) | Contract-driven (`operations.write.<mode>.batching`) |
| File / S3 | No | No | Yes |
| Stdout | No | No | No |

The API handler sends one request per record unless the endpoint's
`operations.write.<mode>` declares a `batching` block. The contract
shape is `{"max_records": <int >= 2>}` — the provider's maximum records
per request; with the block present, records are sent in chunks of at
most `max_records`. A `batching` block of any other shape fails the
stream at `configure_schema` time.

## Formatters

Used by File and Stream handlers. `parquet` requires
`poetry install -E analytics`.

| Format | Extension | Content-Type | Notes |
|--------|-----------|--------------|-------|
| `jsonl`, `json` | `.jsonl` | `application/x-ndjson` | Default, append-friendly |
| `csv` | `.csv` | `text/csv` | Header on by default |
| `parquet` | `.parquet` | `application/vnd.apache.parquet` | Columnar, snappy by default |

## Connection Configuration

Connections live under `connections/{connection_id}/connection.json`. The
shape is the same on source and destination sides — only the connector
referenced and the endpoints used differ; see [`source-config.md`](source-config.md)
for the connection schema.

### Database destination (PostgreSQL)

```json
{
  "$schema": "https://schemas.analitiq.ai/connection/latest.json",
  "connection_id": "my-postgres",
  "display_name": "My Postgres",
  "connector_id": "postgresql",
  "parameters": {
    "host": "db.example.com",
    "port": 5432,
    "database": "postgres",
    "username": "postgres",
    "ssl_mode": "prefer"
  },
  "secret_refs": {
    "password": "connections/my-postgres/password"
  }
}
```

The connector definition supplies the `transports.database` block that
turns these parameters into a SQLAlchemy DSN; see
`connectors/postgresql/definition/connector.json` and the parameterization
spec for the full mechanism.

### API destination

```json
{
  "$schema": "https://schemas.analitiq.ai/connection/latest.json",
  "connection_id": "my-sevdesk",
  "display_name": "My sevDesk",
  "connector_id": "sevdesk",
  "parameters": {},
  "secret_refs": {
    "api_key": "connections/my-sevdesk/api_key"
  }
}
```

The connector's `transports.api` block (`kind: http`) supplies
`base_url`, `headers` (which can reference `${secrets.api_key}`), and
optional `rate_limit`.

### File destination

`file` and `s3` connector types are routed to `GenericFileConnector`.
The connection config defines `path` (or `prefix`), `file_format`, and an
optional `path_template` for partitioning. Format-specific options
(`compression`, `delimiter`, etc.) are passed through the formatter
config.

The connector kind picks the storage backend that performs the write
(`cdk/cdk/file/__init__.py`). Only `file` has one — the local
filesystem. `s3` is a registered but unbuilt kind, so an `s3` destination
raises `StorageBackendNotBuiltError` at the top of `connect()`, before the
runtime is acquired and before any storage connection is opened; the
message names the kind, names the missing backend, and says it is planned
rather than misconfigured. There is no fallback to local storage. The
refusal happens inside the connector worker, after the engine shell has
already resolved the connection's secrets into the worker's launch
bootstrap.

A `path_template` with time placeholders (`{year}/{month}/{day}/{hour}`)
resolves them from the batch's engine-stamped emit instant, not the
write-time wall clock, so a replayed batch lands in the same partition
directory and overwrites in place. See the `emitted_at_unix_ms`
field in [grpc-streaming-architecture.md](grpc-streaming-architecture.md).

### Stdout destination

`stdout` is intentionally minimal — it serializes batches via the
chosen formatter and prints them. Useful for development and contract
tests.

## Stream — Destination Section

```json
"destinations": [
  {
    "endpoint_ref": {
      "scope": "connection",
      "connection_id": "my-postgres",
      "endpoint_id": "public_wise_transfers"
    },
    "write": {
      "mode": "upsert",
      "conflict_keys": ["id"]
    },
    "execution": {
      "batch_size": 1000
    }
  }
]
```

| Field | Required | Description |
|-------|----------|-------------|
| `endpoint_ref` | Yes | Object `{scope, connection_id, endpoint_id}` (always an object — there is no string form). `scope` is `connection` or `connector`; `connection_id` is the destination connection; `endpoint_id` the endpoint name |
| `write.mode` | No | `insert`, `upsert`, or `truncate_insert` (default: `upsert`) |
| `write.conflict_keys` | When `mode = upsert` | Single composite conflict-key set: a non-empty list of destination field names (e.g. `["id"]` or `["tenant_id", "id"]`) |
| `execution.batch_size` | No | Per-destination batch-size override |

Database destination endpoint files live under
`connections/{alias}/definition/endpoints/{name}.json` and describe the
target table:

```json
{
  "endpoint_name": "public-wise_transfers",
  "table": "wise_transfers",
  "schema": "public",
  "columns": [
    { "name": "id", "native_type": "bigint", "nullable": false },
    { "name": "created", "native_type": "timestamptz", "nullable": false }
  ],
  "primary_key": ["id"]
}
```

`native_type` is mapped to canonical Arrow types via the connection's or
connector's `type-map-read.json` (see `cdk/cdk/type_map/`); when
`auto_create_table` is on, DDL column types come from the dialect's
`render_column_type` backed by the connection-scoped `type-map-write.json`
(connection rules over the connector's) — the same path on both transports. See
[`pyarrow-and-destinations.md`](pyarrow-and-destinations.md) for the full
type-mapping and transport detail.

## Idempotency

### Database (row identity)

The SQL destination dedups on **row identity** (content-derived), not
batch position, and keeps no commit-ledger table. `batch_seq` is only an
ordering sequence on the wire; it is never the dedup key. How identity is
enforced depends on the write mode:

- **`upsert`** — MERGE / INSERT-or-UPDATE on the stream's `conflict_keys`.
- **`truncate_insert`** — full refresh: the target is emptied on the
  read's first batch (`batch_seq` 1) via the dialect's
  target-emptying statement (ANSI `DELETE FROM`, never `TRUNCATE`),
  plain append from the stage after that with no
  row-identity dedup (deduping a full refresh would collapse legitimate
  duplicate rows). `batch_seq` restarts at 1 only when the engine
  (re)starts the read, so the decision survives engine and destination
  restarting independently. The engine never resumes a truncate_insert
  stream from a cursor — a restart re-reads the source from scratch and
  re-truncates.
- **`insert`** — a row is inserted only if its identity is not already
  present: the batch lands in a per-batch stage table and one set-based
  `INSERT ... SELECT ... FROM stage WHERE NOT EXISTS (...)` applies it,
  identically on both transports (plain ANSI over the dialect's quoting,
  no dialect-specific SQL). The identity is
  the contract primary key, or — for a keyless insert stream — a synthetic
  engine-managed `_record_hash` column (full SHA-256 of the row content)
  declared as the table's `PRIMARY KEY`, the structural uniqueness
  backstop. Coalescing is identity-only — a row whose identity already
  exists is skipped without comparing its other columns: two byte-identical
  keyless rows collapse to one, and a keyed `insert` likewise drops a
  same-key row whose content differs (first occurrence wins). `insert` cannot
  tell a retry's re-read from a genuinely conflicting key; a stream that must
  reconcile changed rows should use `upsert`. A keyless insert target created
  before `_record_hash` existed is rejected loudly on the next run (the column
  is the primary key and cannot be back-filled on existing rows); recreate the
  table so the engine can manage it. On a system that does not enforce
  uniqueness (BigQuery's `NOT ENFORCED` keys) the anti-join is a filter,
  not a guarantee, and insert streams report at-least-once.

### File / S3 (content-addressed filenames)

Each batch file's name carries the first 16 hex chars of
SHA-256(serialized bytes), and there is no batch-level
commit ledger. The write itself is the idempotency
mechanism: a true replay serializes to the same bytes, hashes to the
same filename, and overwrites the same file — atomically, via a temp
file renamed into place, so a crash mid-rewrite cannot truncate
committed output — while a same-run restart,
which re-reads the inclusive cursor boundary and re-batches those rows
into different content, lands in a new file instead of being skipped as
a replay (the row-drop class) or overwriting committed
data. Duplicates are possible across a restart, drops are not; the file
destination reports itself as at-least-once in the schema ack
for that handler.

### API (per-record idempotency key)

An API `upsert` is idempotent through the endpoint's own `conflict_keys`.
For `insert`, the api-endpoint contract's
`operations.write.<mode>.idempotency` block declares where a
per-request idempotency key lands:

```json
{ "in": "header", "name": "Idempotency-Key" }
```

`in` is `"header"` (Stripe-style) or `"body"` (Square-style, requires a
JSON-object request body); `name` is the header or top-level body field.
The author declares **placement only** — the key value is engine-owned
and follows the write mode's identity semantics, mirroring the SQL
destination:

- **`insert`** — the identity-derived `record_id` (primary-key fields
  when the source declares them, else the full content): the first
  occurrence of an identity wins, like the SQL insert anti-join; a
  stream that must reconcile changed rows uses `upsert`.
- **`upsert`** — a full-content hash (the `_record_hash`
  canonicalisation): an identical replay dedups, while a changed row
  gets a new key so the provider applies the update instead of
  replaying its cached response.

Either way a re-sent record carries the same key and the provider
dedups it within its replay window. The key name must not collide with
an engine- or connection-owned request header (`Content-Type`, auth
headers, ...) nor with a body field the request body or write input
schema already declares — `configure_schema` rejects those documents.

The block cannot be combined with a `batching` block — the contract has
no batching mode; a present block IS the multi-record case. Both the
published schema and `configure_schema` reject the combination, because
a restart re-batches records and a per-request key spanning several
records cannot dedup. Without the block, API `insert` is
at-least-once on a same-run restart. Every destination reports its
per-stream verdict in the schema ack (`retry_semantics` + reason) and
the engine logs it at stream start.

## gRPC Batch Parameters

| Field | Description |
|-------|-------------|
| `run_id` | Unique pipeline-run identifier (same value on retries); routing/scoping, not a dedup key |
| `stream_id` | Stream identifier; routing/scoping, not a dedup key |
| `batch_seq` | Monotonic ordering/log sequence per stream within a run (not a dedup key) |
| `cursor` | Opaque token produced by the engine, stored verbatim by the destination |
| `record_ids` | Content-derived row identities (SHA-256) for DLQ correlation; the `_record_hash` value for a keyless insert |

The SQL destination writes idempotently by row identity and the file
destination by content-addressed filename; both return
`ACK_STATUS_SUCCESS` on a replay. Destinations with no dedup mechanism
(stdout; API `insert` without an `idempotency` block) replay as
duplicates and say so in their retry verdict. Full protocol semantics
are in
[`grpc-streaming-architecture.md`](grpc-streaming-architecture.md).

## Adding a New Destination

| Destination | Code Required |
|-------------|---------------|
| New SQL dialect (SQLAlchemy or ADBC transport) | 0 lines (point a connector at it) |
| New API endpoint | 0 lines (write a connector + endpoints) |
| New storage backend (e.g. a network share) | New class in `cdk/cdk/file/`, registered in that package's backend table |
| New formatter (e.g. Avro) | New class in `cdk/cdk/formatters/` |
| Brand-new handler family | Subclass `BaseDestinationHandler` and publish the class in the `analitiq.destination_connectors` entry-point group |

### What a new handler implements

`BaseDestinationHandler.write_batch` is the shared preamble: the readiness
guard, the empty-batch success that still advances the cursor, one
materialisation of the Arrow batch, and the mapping from a raised failure to
an ack. A handler supplies only the parts that differ from every other sink:

| Member | Purpose |
|--------|---------|
| `land(batch)` | Put the records in the sink; return how many landed. Called only for a ready handler and a non-empty batch. |
| `not_ready_reason(stream_id)` | Why a batch cannot be taken right now, or `None`. A rejection here attempted nothing, so it acks `NOT_READY`. |
| `land_empty(batch)` | Override only for a per-batch side effect that must happen even with no records — a full refresh whose truncate is keyed to the first batch. |
| `unexpected_write_failure(error, …)` | Override to consult a declared error map before the default fatal verdict. |
| `connect` / `disconnect` / `configure_schema` / `health_check` / `connector_type` | The lifecycle, unchanged. |

`land` receives a `LandingBatch`. It carries the batch both ways round —
`records` for sinks that write dicts, `record_batch` for sinks that stay
Arrow-native — and `records` materialises lazily, so an Arrow-native sink is
not taxed for a representation it never reads.

To refuse a batch, raise `BatchRejected` with the reason and, when the sink
knows it, a `FailureCategory`. It is fatal and destination-owned by default;
a sink that means something else says so. A sink that lands rows one request
at a time passes `records_written` and `failed_record_ids` so the engine
dead-letters exactly what did not land instead of retrying rows that did.

An `OSError` needs no handling: one errno table judges every sink that writes
through a file descriptor, so a full volume is fatal and an unlisted errno is
retryable, identically for files and stdout.

`land` and `write_batch` are alternatives — implement one. A handler that
implements neither is refused when its class is defined, not at its first
batch.

### Forwarded capabilities

A handler that relays another process's advertisement (the destination shell's
worker proxy) returns it from `forwarded_capabilities` and declares
`forwards_capabilities`. Every capability below then reads off that one
object. Such a handler advertises nothing until it has something to relay: the
neutral defaults would have it claim, before it has reached its worker,
capabilities the worker may not have.

## See Also

- [`source-config.md`](source-config.md) — source-side config and pipeline layout
- [`mapping-and-transformations.md`](mapping-and-transformations.md) — assignment AST
- [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md) — engine ↔ destination protocol
- [`connector-module-architecture.md`](connector-module-architecture.md) — CDK boundary, capability contract, registry
