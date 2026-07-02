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
| GenericSQLConnector         ApiDestinationHandler         |
| FileDestinationHandler      StreamDestinationHandler      |
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

Handler source lives under `src/destination/`:

- `connectors/` — handler implementations and the destination registry.
  - `api.py` — `ApiDestinationHandler`.
  - `file.py` — `FileDestinationHandler`.
  - `stream.py` — `StreamDestinationHandler` (stdout).
- `formatters/` — JSONL / CSV / Parquet serializers.
- `storage/` — local filesystem backend.
- `idempotency/` — file `_manifest.json` tracker (the SQL destination dedups on row identity, with no commit-ledger table).
- `server.py` — gRPC server.

Shared building blocks moved to the CDK at `cdk/cdk/`:

- `sql/generic.py` — `GenericSQLConnector` (the database handler).
- `base_handler.py` — `BaseDestinationHandler` ABC and `BatchWriteResult`.
- `schema_contract.py` — Arrow-based `SchemaContract`.
- `sql_types.py` — `arrow_to_sqlalchemy` mapper.
- `registry.py` — `ConnectorRegistry` / `build_registries`.

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

Handlers are mapped by the connector's `connector_type` (its *kind*).
`src/destination/connectors/__init__.py` builds a shared CDK
`ConnectorRegistry` via `build_registries(...)` and exports the
`destination_registry` instance plus a `get_handler(kind)` convenience
factory:

```python
from src.destination.connectors import destination_registry, get_handler

handler = get_handler("database")            # instance (convenience factory)
handler = destination_registry.create("api") # instance
handler_class = destination_registry.get("api")  # class
destination_registry.register("custom", MyHandler)
```

Built-ins (`_DESTINATION_BUILTINS`):

| `connector_type` | Handler | Use Case |
|------------------|---------|----------|
| `database` | `GenericSQLConnector` | All SQL dialects via SQLAlchemy or ADBC (PostgreSQL, MySQL, Snowflake, BigQuery, Redshift) |
| `api` | `ApiDestinationHandler` | REST endpoints |
| `file` | `FileDestinationHandler` | Local filesystem |
| `s3` | `FileDestinationHandler` | Object storage (when storage backend is wired) |
| `stdout` | `StreamDestinationHandler` | Diagnostics / debugging |

Externally installed connector packages add themselves through the
`analitiq.destination_connectors` entry-point group (discovered at
registry build time).

The set of runnable connector kinds is owned by the worker registry:
a kind that is neither a built-in default nor registry-discovered fails
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

`file` and `s3` connector types are routed to `FileDestinationHandler`.
The transport block defines `path` (or bucket/prefix), `file_format`,
and an optional `path_template` for partitioning. Format-specific
options (`compression`, `delimiter`, etc.) are passed through the
formatter config.

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
| `execution.max_concurrent_batches` | No | Per-destination concurrency override |

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
`render_column_type` backed by the connector's `type-map-write.json` —
the same path on both transports. See
[`pyarrow-and-destinations.md`](pyarrow-and-destinations.md) for the full
type-mapping and transport detail.

## Idempotency

### Database (row identity)

The SQL destination dedups on **row identity** (content-derived), not
batch position, and keeps no commit-ledger table. `batch_seq` is only an
ordering sequence on the wire; it is never the dedup key. How identity is
enforced depends on the write mode:

- **`upsert`** — MERGE / INSERT-or-UPDATE on the stream's `conflict_keys`.
- **`truncate_insert`** — full refresh: TRUNCATE once per run on the run's
  first write (issue #307), plain append after that with no row-identity
  dedup (deduping a full refresh would collapse legitimate duplicate
  rows). The engine never resumes a truncate_insert stream from a
  cursor — a restart re-reads the source from scratch and re-truncates.
- **`insert`** — each row is inserted only if its identity is not already
  present: one `INSERT ... SELECT ... WHERE NOT EXISTS (...)` per row,
  built with SQLAlchemy core (no dialect-specific SQL). The identity is
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
  table so the engine can manage it.

ADBC-only transports (Snowflake/BigQuery) do not yet do the keyless
`insert` anti-join — plain `insert` there is at-least-once (a noted
follow-up); `upsert` remains idempotent.

### File / S3 (`_manifest.json`)

A single `_manifest.json` in the base path records `commits[]` with
`run_id`, `stream_id`, `batch_seq`, `records_written`, `cursor_bytes`,
`file_path`, `committed_at`. Replays match by `(run_id, stream_id,
batch_seq)` and become no-ops.

This positional match is sound for an in-run replay of the same batch,
but not across a same-run restart: the source resumes from the committed
cursor while `batch_seq` restarts, so a committed position can re-arrive
carrying different rows and be skipped as a replay. The file destination
therefore reports itself as not replay-safe (at-least-once) in the
schema ack (issue #286).

### API (per-record idempotency key)

An API `upsert` is idempotent through the endpoint's own `conflict_keys`.
For `insert`, the api-endpoint contract's
`operations.write.<mode>.idempotency` block (infra#890) declares where a
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
records cannot dedup (issue #286). Without the block, API `insert` is
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

The SQL destination writes idempotently by row identity and returns
`ACK_STATUS_SUCCESS` on a replay; the file destination returns
`ACK_STATUS_ALREADY_COMMITTED` for replayed batches. Full protocol
semantics are in
[`grpc-streaming-architecture.md`](grpc-streaming-architecture.md).

## Adding a New Destination

| Destination | Code Required |
|-------------|---------------|
| New SQL dialect (SQLAlchemy or ADBC transport) | 0 lines (point a connector at it) |
| New API endpoint | 0 lines (write a connector + endpoints) |
| New storage backend (e.g. GCS) | New class in `src/destination/storage/` |
| New formatter (e.g. Avro) | New class in `src/destination/formatters/` |
| Brand-new handler family | Subclass `BaseDestinationHandler` and `destination_registry.register(...)` |

## See Also

- [`source-config.md`](source-config.md) — source-side config and pipeline layout
- [`mapping-and-transformations.md`](mapping-and-transformations.md) — assignment AST
- [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md) — engine ↔ destination protocol
- [`connector-module-architecture.md`](connector-module-architecture.md) — CDK boundary, capability contract, registry
