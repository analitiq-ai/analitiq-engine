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
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md).
The environment-variable
catalogue is [`src/config/settings.py`](../../src/config/settings.py) (see
also [README.md](../../README.md#environment-variables)); their
resolution order and layering rules are
[`settings-reference.md`](settings-reference.md).

The destination service loads its connection from the pipeline's
`connections.destinations` list, at the `DESTINATION_INDEX` setting (see
[ADR 0009](../adr/0009-engine-and-destination-are-separate-services-joined-by-grpc-carrying-arrow-ipc.md)).

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

The CDK's connector families live alongside its shared building blocks,
and at run time a connector runs in a worker subprocess (see
[ADR 0008](../adr/0008-connector-code-runs-only-in-an-isolated-worker.md)).
The module breakdown is part of
[`engine-architecture.md`](../architecture/engine-architecture.md#module-layout)'s
module layout, not repeated here.

`GenericSQLConnector` (databases, over the `sqlalchemy` and `adbc`
transports) and `GenericAPIConnector` (APIs) each serve both source and
destination roles (see
[ADR 0020](../adr/0020-one-connector-class-serves-both-read-and-write.md) and
[ADR 0015](../adr/0015-every-sql-destination-uses-one-handler-over-two-transports.md)).
The transport split is specified in
[`arrow-and-transport-strategy.md`](../data-path/arrow-and-transport-strategy.md)
and the write mechanics in
[`sql-write-path.md`](../data-path/sql-write-path.md).

## Handler registry

Both registries — source and destination — are built by
`build_registries()` (`cdk/cdk/registry.py`) inside the worker subprocess
(see
[ADR 0008](../adr/0008-connector-code-runs-only-in-an-isolated-worker.md)).
A connector resolves to the class an installed package registered under
its `connector_id`, else to its kind's generic class in `cdk.registry.KIND_DEFAULTS` (see
[ADR 0019](../adr/0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md)).
The `database` and `api` generic classes serve both roles; the `file`,
`s3` and `stdout` ones serve destination only, so a `kind: file` *source*
whose `connector_id` no installed package registers as a source raises
`ConnectorNotRegisteredError` rather than resolving a class with no read
path.

### Handler capabilities

| Handler | Transactions | Upsert | Bulk Load |
|---------|--------------|--------|-----------|
| Database | Yes | Yes (via `ON CONFLICT` / `MERGE` / dialect equivalent) | Yes |
| API | No | Contract-driven (`operations.write.upsert`) | Contract-driven (`operations.write.<mode>.batching`) |
| File / S3 | No | No | Yes |
| Stdout | No | No | No |

The API handler sends one request per record unless the endpoint's
`operations.write.<mode>` declares batching.

## Formatters

Used by File and Stdout handlers. `parquet` requires the CDK's `[arrow]`
extra (`pip install "analitiq-cdk[arrow]"`).

| Format | Extension | Content-Type | Notes |
|--------|-----------|--------------|-------|
| `jsonl`, `json` | `.jsonl` | `application/x-ndjson` | Default, append-friendly |
| `csv` | `.csv` | `text/csv` | Header on by default |
| `parquet` | `.parquet` | `application/vnd.apache.parquet` | Columnar, snappy by default |

## Destination runtime behavior

- **Storage backend selection (file / s3).** The connector kind picks the
  storage backend that performs the write. Only `file` has one — the
  local filesystem. `s3` is a registered but unbuilt kind: an `s3`
  destination raises `StorageBackendNotBuiltError` at the top of
  `connect()`, before the runtime is acquired and before any storage
  connection opens, naming the kind and the missing backend as planned
  rather than misconfigured. There is no fallback to local storage.
- **Partition path stamping.** A `path_template` with time placeholders
  (`{year}/{month}/{day}/{hour}`) resolves them from the batch's
  engine-stamped emit instant, `emitted_at_unix_ms` (see
  [ADR 0014](../adr/0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md)).
- **Database DDL type resolution.** When `auto_create_table` is on, DDL
  column types come from the dialect's `render_column_type`, backed by the
  connection-scoped write type map (connection rules over the
  connector's) — the same path on both transports. Full type-mapping and
  transport detail is in
  [`arrow-and-transport-strategy.md`](../data-path/arrow-and-transport-strategy.md).
- **Stdout** serializes batches via the chosen formatter and prints them;
  intentionally minimal, for development and contract tests.

## Idempotency

Each handler reports a retry-safety verdict per stream in an accepted
schema ack (`retry_semantics` + reason; see
[ADR 0013](../adr/0013-prefer-duplicates-over-data-loss.md)).
`truncate_insert`'s append phase, API `insert` without an `idempotency`
block, file sinks across a restart, and stdout do not dedup (see
[ADR 0014](../adr/0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md)).

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
- **`insert`** — a row lands only if its identity is not already present,
  by an anti-join from the stage (see
  [ADR 0005](../adr/0005-stage-then-merge-is-the-single-sql-write-primitive.md)
  and
  [ADR 0014](../adr/0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md)).
  Exactly-once where the system enforces the identity constraint;
  at-least-once where it does not (e.g. BigQuery's `NOT ENFORCED` keys,
  where the anti-join is a filter, not a guarantee).

### File / S3 (content-addressed filenames)

Each batch file's name carries the first 16 hex chars of
SHA-256(serialized bytes), so a true replay overwrites the same file (see
[ADR 0014](../adr/0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md)).
A same-run restart — which re-reads the inclusive cursor boundary and
re-batches those rows into different content — lands in a new file
instead of overwriting committed data. Duplicates are possible across a
restart, drops are not: reported as at-least-once.

### API (per-record idempotency key)

An API `upsert` is idempotent through the endpoint's own `conflict_keys`.
With a declared `idempotency` block the engine sends a per-record key (see
[ADR 0014](../adr/0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md));
`insert` then reports exactly-once, which holds only if the provider
dedups on that key, and only **within its replay window**.
`configure_schema` refuses a header key name that collides with a header
the connection's transport resolved, and, when the key goes in the body
(`in: body`), a write refuses a resolved request body that is not a JSON
object or already carries the key's field. Without the block, API
`insert` is at-least-once on a same-run restart.

### Stdout

At-least-once by construction: the handler only prints, so a replayed
batch prints again — there is no sink state for a replay to dedup against.

## gRPC batch parameters

| Field | Description |
|-------|-------------|
| `run_id` / `stream_id` | Routing/scoping identifiers |
| `batch_seq` | Per-stream batch sequence; restarts at 1 each time the engine (re)starts the read |
| `cursor` | Opaque token produced by the engine, stored verbatim by the destination |
| `record_ids` | Content-derived row identities (SHA-256), used as the idempotency key of an API insert-mode endpoint that declares one. The SQL `_record_hash` is computed separately, from the cast row |

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
| `connect` / `disconnect` / `configure_schema` / `health_check` / `connector_type` | The lifecycle. |

`land` receives a `LandingBatch`, which carries the batch both ways round
— `records` for sinks that write dicts, `record_batch` for sinks that stay
Arrow-native — with `records` materialising lazily so an Arrow-native sink
is not taxed for a representation it never reads.

To refuse a batch, raise `BatchRejected` with the reason and, when the
sink knows it, a `FailureCategory`; it is fatal and destination-owned by
default, and a sink that means something else says so. A sink that lands
rows one request at a time may also pass `records_written` and
`failed_record_ids`; the engine does not read `failed_record_ids` and
disposes of the whole batch
([ADR 0011](../adr/0011-a-batch-is-accepted-or-rejected-whole.md)).
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

- [`mapping-and-transformations.md`](../data-path/mapping-and-transformations.md) — assignment AST
- [`sql-write-path.md`](../data-path/sql-write-path.md) — the SQL write primitive
- [`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md) — engine ↔ destination protocol
- [`connector-module-architecture.md`](../architecture/connector-module-architecture.md) — CDK boundary, capability contract, registry
