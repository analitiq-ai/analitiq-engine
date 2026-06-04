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
connector definition: `transport_type: "sqlalchemy"` (async SQLAlchemy
engine — Postgres asyncpg, MySQL aiomysql) and `transport_type: "adbc"`
(ADBC DBAPI — Snowflake, BigQuery, Postgres-via-ADBC for Redshift). The
transport detail lives in
[`pyarrow-and-destinations.md`](pyarrow-and-destinations.md).

Handler source lives under `src/destination/`:

- `connectors/` — handler implementations and the destination registry.
  - `api.py` — `ApiDestinationHandler`.
  - `file.py` — `FileDestinationHandler`.
  - `stream.py` — `StreamDestinationHandler` (stdout).
- `formatters/` — JSONL / CSV / Parquet serializers.
- `storage/` — local filesystem backend.
- `idempotency/` — `_batch_commits` and `_manifest.json` trackers.
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
| `GRPC_TIMEOUT_SECONDS` | No | `300` (default) |

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

Valid connector types are constrained to the set
`{api, database, file, s3, stdout}` (see
`cdk/cdk/connection_runtime.py` `VALID_CONNECTOR_TYPES`).

### Handler capabilities

| Handler | Transactions | Upsert | Bulk Load |
|---------|--------------|--------|-----------|
| Database | Yes | Yes (via `ON CONFLICT` / `MERGE` / dialect equivalent) | Yes |
| API | No | No | `single` / `bulk` / `batch` posting modes |
| File / S3 | No | No | Yes |
| Stdout | No | No | No |

The API handler posts records in one of three modes (`single`, `bulk`,
`batch` — `BATCH_MODE_*` in `src/destination/connectors/api.py`),
selected from the endpoint's write configuration.

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
      "conflict_keys": [["id"]]
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
| `write.conflict_keys` | When `mode = upsert` | List of token-path field references for conflict resolution (e.g. `[["id"]]`) |
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
connector's `type-map.json` (see `cdk/cdk/type_map/`); on the
SQLAlchemy transport `GenericSQLConnector` uses `arrow_to_sqlalchemy`
(`cdk/cdk/sql_types.py`) to derive DDL types when `auto_create_table` is
on, while the ADBC transport renders native DDL per driver. See
[`pyarrow-and-destinations.md`](pyarrow-and-destinations.md) for the full
type-mapping and transport detail.

## Idempotency

All handlers track `(run_id, stream_id, batch_seq)` for exactly-once
delivery.

### Database (`_batch_commits` table)

```sql
CREATE TABLE _batch_commits (
  run_id          VARCHAR(255),
  stream_id       VARCHAR(255),
  batch_seq       BIGINT,
  committed_cursor BYTEA,
  records_written  INT,
  committed_at    TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (run_id, stream_id, batch_seq)
);
```

On batch receipt:

1. Look up `(run_id, stream_id, batch_seq)`.
2. Hit → return `ALREADY_COMMITTED` with the stored cursor.
3. Miss → write the batch and the commit record in a single transaction;
   return `SUCCESS`.

### File / S3 (`_manifest.json`)

A single `_manifest.json` in the base path records `commits[]` with
`run_id`, `stream_id`, `batch_seq`, `records_written`, `cursor_bytes`,
`file_path`, `committed_at`. Replays match by `(run_id, stream_id,
batch_seq)` and become no-ops.

## gRPC Batch Parameters

| Field | Description |
|-------|-------------|
| `run_id` | Unique pipeline-run identifier (same value on retries) |
| `stream_id` | Stream identifier |
| `batch_seq` | Monotonically increasing batch sequence |
| `cursor` | Opaque token produced by the engine, stored verbatim by the destination |
| `record_ids` | Per-record IDs for DLQ correlation |

Returns `ACK_STATUS_ALREADY_COMMITTED` for replayed batches; full
protocol semantics are in
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
