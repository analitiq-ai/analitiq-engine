# Destination Handlers: Design and Idempotency Semantics

**Scope:** this doc owns the destination handler design — the registry,
write-mode idempotency semantics, and the handler extension contract. For
module layout see
[`engine-architecture.md`](../architecture/engine-architecture.md); for the
Arrow type system and the ADBC-vs-SQLAlchemy transport strategy see
[`arrow-and-transport-strategy.md`](../data-path/arrow-and-transport-strategy.md);
for the SQL write primitive see
[`sql-write-path.md`](../data-path/sql-write-path.md); for the gRPC wire
protocol see
[`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md);
for the CDK connector contract see
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md);
for connection / connector / endpoint schema see
[`source-config.md`](source-config.md). The environment-variable
catalogue is [`src/config/settings.py`](../../src/config/settings.py) (see
also [README.md](../../README.md#environment-variables)); their
resolution order and layering rules are
[`settings-reference.md`](settings-reference.md).

Destinations are selected at runtime from the pipeline's
`connections.destinations` list (indexed by the `DESTINATION_INDEX`
setting). The destination service uses the same Docker image as the source
engine (`RUN_MODE=destination`) and loads its connection via
`PipelineConfigPrep` just like the engine does; credentials are read from
the local config volume and are **never** transmitted over gRPC.

## Architecture

```
+-----------------------------------------------------------+
|                  TRANSPORT (gRPC server)                  |
+-----------------------------------------------------------+
                             |
+-----------------------------------------------------------+
|                  HANDLER LAYER (orchestration)            |
| GenericSQLConnector    GenericAPIConnector                |
| GenericFileConnector   GenericStdoutConnector              |
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

Every connector family lives in the CDK, alongside the shared building
blocks; the engine keeps only the gRPC server that fronts them
(`src/destination/server.py`). The current module breakdown is part of
[`engine-architecture.md`](../architecture/engine-architecture.md#module-layout)'s
module layout, not repeated here.

`GenericSQLConnector` is the single database handler, serving both source
and destination roles over two transports (`sqlalchemy` and `adbc`); the
transport split is specified in
[`arrow-and-transport-strategy.md`](../data-path/arrow-and-transport-strategy.md)
and the write mechanics in
[`sql-write-path.md`](../data-path/sql-write-path.md). `GenericAPIConnector`
is the single API connector and serves both roles the same way — one
connect, one HTTP round trip, one status classification, one paging loop —
with what varies between an API read and an API write confined to the
endpoint document's `operations` block, never the class.

## Handler registry

Handlers are resolved by the connector's `kind`. Both registries — source
and destination — are built in one place, `build_registries()`
(`cdk/cdk/registry.py`), called inside the worker subprocess because that
is where connector classes execute; neither the engine process nor the
destination service holds connector code.

Kind defaults are declared once, in `cdk.registry.KIND_DEFAULTS`, as a
`kind -> (class, roles)` table. **The roles a kind default serves are
declared in that table, not derived from the class at registration** —
reading them off the class would mean importing it, and importing the
class is exactly the cost the table exists to defer. The declaration is
verified, not trusted blindly: the first time a kind default is actually
loaded, its declared roles are checked against the class's own capability
Protocols (`Readable` / `Writable`, `cdk/cdk/contract.py`), and a mismatch
is a registry defect, not a silent divergence. `database` and `api` serve
both roles; `file`, `s3` and `stdout` serve destination only — a
`kind: file` *source* raises `ConnectorNotRegisteredError` rather than
resolving a class with no read path.

Externally installed connector packages add themselves through the
`analitiq.source_connectors` / `analitiq.destination_connectors`
entry-point groups, registering under their `connector_id` — an entry
point never introduces a new *kind*, only a class for a kind the contract
already declares valid. Which `kind` values are valid at all is owned by
the published connector contract: `validate_connector`
(`src/engine/pipeline_config_prep.py::_load_connector`) rejects a
`connector.json` with an unrecognised `kind` before the document ever
reaches the registry. Which of those contract-valid kinds actually has a
class to run is a separate, later question the registry alone answers: a
kind with neither a kind default nor a registry-discovered class fails at
worker startup with `ConnectorNotRegisteredError` — the registry's set of
*runnable* kinds is a subset of the contract's set of *valid* ones, never
a parallel vocabulary to keep in sync.

### Handler capabilities

| Handler | Transactions | Upsert | Bulk Load |
|---------|--------------|--------|-----------|
| Database | Yes | Yes (via `ON CONFLICT` / `MERGE` / dialect equivalent) | Yes |
| API | No | Contract-driven (`operations.write.upsert`) | Contract-driven (`operations.write.<mode>.batching`) |
| File / S3 | No | No | Yes |
| Stdout | No | No | No |

The API handler sends one request per record unless the endpoint's
`operations.write.<mode>` declares a `batching` block
(`{"max_records": <int >= 2>}`, the provider's cap per request); a
`batching` block of any other shape fails the stream at `configure_schema`
time.

## Formatters

Used by File and Stdout handlers. `parquet` requires the CDK's `[arrow]`
extra (`pip install "analitiq-cdk[arrow]"`).

| Format | Extension | Content-Type | Notes |
|--------|-----------|--------------|-------|
| `jsonl`, `json` | `.jsonl` | `application/x-ndjson` | Default, append-friendly |
| `csv` | `.csv` | `text/csv` | Header on by default |
| `parquet` | `.parquet` | `application/vnd.apache.parquet` | Columnar, snappy by default |

## Connection and endpoint shape

Connection, connector, and endpoint document shapes are specified in
[`source-config.md`](source-config.md) — the shape is the same on source
and destination sides, only the connector referenced and the endpoints
used differ. What follows is destination-specific behavior the schema does
not carry:

- **Storage backend selection (file / s3).** The connector kind picks the
  storage backend that performs the write. Only `file` has one — the
  local filesystem. `s3` is a registered but unbuilt kind: an `s3`
  destination raises `StorageBackendNotBuiltError` at the top of
  `connect()`, before the runtime is acquired and before any storage
  connection opens, naming the kind and the missing backend as planned
  rather than misconfigured. There is no fallback to local storage.
- **Partition path stamping.** A `path_template` with time placeholders
  (`{year}/{month}/{day}/{hour}`) resolves them from the batch's
  engine-stamped emit instant, never the write-time wall clock, so a
  replayed batch lands in the same partition directory and overwrites in
  place. See `emitted_at_unix_ms` in
  [`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md).
- **Database DDL type resolution.** When `auto_create_table` is on, DDL
  column types come from the dialect's `render_column_type`, backed by the
  connection-scoped `type-map-write.json` (connection rules over the
  connector's) — the same path on both transports. Full type-mapping and
  transport detail is in
  [`arrow-and-transport-strategy.md`](../data-path/arrow-and-transport-strategy.md).
- **Stdout** serializes batches via the chosen formatter and prints them;
  intentionally minimal, for development and contract tests.

## Idempotency

Every write mode has a defined retry-safety verdict, reported per stream
in the schema ack (`retry_semantics` + reason) and logged at stream start.
The SQL destination dedups on **row identity** (content-derived), never
batch position or a commit-ledger table; `batch_seq` is only an ordering
sequence on the wire.

### Database (row identity)

- **`upsert`** — MERGE / INSERT-or-UPDATE on the stream's `conflict_keys`.
  Exactly-once.
- **`truncate_insert`** — full refresh: the target is emptied on the
  read's first batch (`batch_seq == 1`) via the dialect's target-emptying
  statement (ANSI `DELETE FROM`, never `TRUNCATE`), plain append from the
  stage after that with no row-identity dedup — deduping a full refresh
  would collapse legitimate duplicate rows. `batch_seq` restarts at 1 only
  when the engine (re)starts the read, so the decision survives engine and
  destination restarting independently; the engine never resumes a
  `truncate_insert` stream from a cursor. At-least-once by design.
- **`insert`** — a row lands only if its identity is not already present:
  the batch lands in a per-batch stage table and one set-based
  `INSERT ... SELECT ... FROM stage WHERE NOT EXISTS (...)` applies it,
  identically on both transports. Identity is the contract primary key,
  or — for a keyless stream — a synthetic engine-managed `_record_hash`
  column (SHA-256 of the row content) declared as the table's
  `PRIMARY KEY`. First occurrence of a duplicate identity wins; `insert`
  cannot distinguish a retry's re-read from a genuinely conflicting key —
  a stream that must reconcile changed rows uses `upsert` instead.
  Exactly-once where the system enforces the identity constraint;
  at-least-once where it does not (e.g. BigQuery's `NOT ENFORCED` keys,
  where the anti-join is a filter, not a guarantee).

### File / S3 (content-addressed filenames)

Each batch file's name carries the first 16 hex chars of
SHA-256(serialized bytes); there is no batch-level commit ledger. A true
replay serializes to the same bytes, hashes to the same filename, and
overwrites the same file atomically (temp file renamed into place, so a
crash mid-rewrite cannot truncate committed output). A same-run restart —
which re-reads the inclusive cursor boundary and re-batches those rows
into different content — lands in a new file instead of overwriting
committed data. Duplicates are possible across a restart, drops are not:
reported as at-least-once.

### API (per-record idempotency key)

An API `upsert` is idempotent through the endpoint's own `conflict_keys`.
For `insert` with a declared `idempotency` block, the guarantee is
exactly-once **within the provider's replay window** — a retry after the
provider has expired its idempotency key is not deduped, and may create a
duplicate. The api-endpoint contract's
`operations.write.<mode>.idempotency` block (`{"in": "header" | "body",
"name": "<key>"}`) declares **placement only** — the key value is
engine-owned, following the write mode's identity semantics: `insert`
sends the identity-derived `record_id` (first occurrence wins, mirroring
the SQL anti-join); `upsert` sends a full-content hash, so an identical
replay dedups while a changed row gets a new key and the provider applies
the update. The key name must not collide with an engine- or
connection-owned header or an already-declared body field —
`configure_schema` rejects those documents. The block cannot combine with
a `batching` block: a restart re-batches records, and a per-request key
spanning several records can never dedup. Without the block, API `insert`
is at-least-once on a same-run restart.

### Stdout

At-least-once by construction: the handler only prints, so a replayed
batch prints again — there is no sink state for a replay to dedup against.

## gRPC batch parameters

| Field | Description |
|-------|-------------|
| `run_id` / `stream_id` | Routing/scoping identifiers, never a dedup key |
| `batch_seq` | Monotonic ordering/log sequence per stream within a run, never a dedup key |
| `cursor` | Opaque token produced by the engine, stored verbatim by the destination |
| `record_ids` | Content-derived row identities (SHA-256), for DLQ correlation; the `_record_hash` value for a keyless insert |

Full wire-protocol semantics are in
[`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md).

## Adding a new destination

| Destination | Code required |
|-------------|---------------|
| New SQL dialect (SQLAlchemy or ADBC transport) | 0 lines (point a connector at it) |
| New API endpoint | 0 lines (write a connector + endpoints) |
| New storage backend (e.g. a network share) | New class in `cdk/cdk/file/`, registered in that package's backend table |
| New formatter (e.g. Avro) | New class in `cdk/cdk/formatters/` |
| Brand-new handler family | Subclass `BaseDestinationHandler` and publish the class in the `analitiq.destination_connectors` entry-point group |

### What a new handler implements

`BaseDestinationHandler.write_batch` is the shared preamble: the readiness
guard, the empty-batch success that still advances the cursor, one
materialisation of the Arrow batch, and the mapping from a raised failure
to an ack. A handler supplies only the parts that differ from every other
sink:

| Member | Purpose |
|--------|---------|
| `land(batch)` | Put the records in the sink; return how many landed. Called only for a ready handler and a non-empty batch. |
| `not_ready_reason(stream_id)` | Why a batch cannot be taken right now, or `None`. A rejection here attempted nothing, so it acks `NOT_READY`. |
| `land_empty(batch)` | Override only for a per-batch side effect that must happen even with no records — a full refresh whose truncate is keyed to the first batch. |
| `unexpected_write_failure(error, …)` | Override to consult a declared error map before the default fatal verdict. |
| `connect` / `disconnect` / `configure_schema` / `health_check` / `connector_type` | The lifecycle, unchanged. |

`land` receives a `LandingBatch`, which carries the batch both ways round
— `records` for sinks that write dicts, `record_batch` for sinks that stay
Arrow-native — with `records` materialising lazily so an Arrow-native sink
is not taxed for a representation it never reads.

To refuse a batch, raise `BatchRejected` with the reason and, when the
sink knows it, a `FailureCategory`; it is fatal and destination-owned by
default, and a sink that means something else says so. A sink that lands
rows one request at a time passes `records_written` and
`failed_record_ids` so the engine dead-letters exactly what did not land.
An `OSError` needs no per-sink handling: one errno table judges every sink
that writes through a file descriptor, identically for files and stdout.

`land` and `write_batch` are alternatives — implement one. A handler that
implements neither is refused when its class is defined, not at its first
batch.

### Forwarded capabilities

A handler that relays another process's advertisement (the destination
shell's worker proxy) returns it from `forwarded_capabilities` and
declares `forwards_capabilities`. Every capability then reads off that one
object, so such a handler advertises nothing until it has something to
relay — the neutral defaults would otherwise have it claim, before it has
reached its worker, capabilities the worker may not have.

## See Also

- [`source-config.md`](source-config.md) — source-side config, connection/endpoint schema
- [`mapping-and-transformations.md`](../data-path/mapping-and-transformations.md) — assignment AST
- [`sql-write-path.md`](../data-path/sql-write-path.md) — the SQL write primitive
- [`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md) — engine ↔ destination protocol
- [`connector-module-architecture.md`](../architecture/connector-module-architecture.md) — CDK boundary, capability contract, registry
