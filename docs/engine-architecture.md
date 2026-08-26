# Engine Architecture

This document describes the engine layout, the pipeline lifecycle, and
the contracts between components. For source-/destination-side schema
details see [`source-config.md`](source-config.md) and
[`destination-config.md`](destination-config.md).

**Scope:** this doc owns the streaming engine — extract / transform /
load / checkpoint orchestration, the producer/consumer flow, the
dual-mode (`RUN_MODE`) runner, and how the engine consumes the CDK and
connections. It defers elsewhere for: destination handler config
([`destination-config.md`](destination-config.md)); the gRPC protocol /
wire format ([`grpc-streaming-architecture.md`](grpc-streaming-architecture.md));
the Arrow type system ([`pyarrow-and-destinations.md`](pyarrow-and-destinations.md));
the CDK boundary / contract
([`connector-module-architecture.md`](connector-module-architecture.md));
and source / stream config ([`source-config.md`](source-config.md)).

## Module Layout

Shared connector machinery lives in the **CDK** package (`cdk/cdk/`,
imported as `from cdk.<x> import ...`). The engine (`src/`) consumes it.
See [`connector-module-architecture.md`](connector-module-architecture.md)
for the CDK boundary in full.

```
cdk/cdk/                     # Connector Development Kit (shared by source + destination)
├── connection_runtime.py    # ConnectionRuntime: reference-counted transport handle
├── transport_factory.py     # Builds SQLAlchemy / aiohttp transports from connector specs
├── database_utils.py        # Pure SQL helpers
├── rate_limiter.py
├── resolver.py              # Typed expression resolver (`ref`/`template`/`literal`/`function`)
├── derived_functions.py     # `lookup`, `basic_auth`, `base64_encode`, `url_encode`
├── type_map/                # TypeMapper, canonical Arrow types
├── schema_contract.py       # Arrow-based vectorized casting
├── base_handler.py          # BaseDestinationHandler ABC
├── contract.py              # Readable / Writable / Discoverable / TableCreator Protocols
├── types.py                 # Shared CDK types
├── registry.py              # ConnectorRegistry + KIND_DEFAULTS + build_registries()
├── secrets/                 # Secret resolvers
├── query_builder.py         # WHERE / SELECT rendering
├── formatters/              # JSONL / CSV / Parquet serializers (file + stdout)
├── sql/                     # GenericSQLConnector + dialects / DDL / discovery / execution
│   └── generic.py           # GenericSQLConnector (source reads + destination writes)
├── api/                     # GenericAPIConnector + page loop / strategies / HTTP / verdicts
│   └── generic.py           # GenericAPIConnector (source reads + destination writes)
├── file/                    # GenericFileConnector + storage backends (file / s3 kinds)
│   ├── generic.py           # GenericFileConnector (destination writes)
│   ├── backend.py           # BaseStorageBackend: the storage transport seam
│   └── local_backend.py     # LocalFileStorage
└── stdout/                  # GenericStdoutConnector
    └── generic.py           # GenericStdoutConnector (destination writes)

src/
├── shared/                  # Engine-local helpers
│   └── run_id.py
│
├── destination/             # Destination side (see destination-config.md)
│   └── server.py                # gRPC server
│
├── engine/                  # Core engine
│   ├── engine.py                # StreamingEngine (fans streams out, aggregates results)
│   ├── stream_processor.py      # StreamProcessor (one stream: extract -> transform -> load -> checkpoint)
│   ├── pipeline_config_prep.py  # Loads manifest/pipelines/streams/connections/connectors
│   ├── mapping.py               # MappingDocument + compile_mapping (mapping AST -> Arrow compute)
│   └── exceptions.py
│
├── worker/                  # Sandboxed connector worker (spawned subprocess)
│   ├── readable.py              # WorkerReadable (engine-side client)
│   ├── source_service.py        # Worker-side read loop
│   ├── proxy.py / shell.py / spawn.py / bootstrap.py
│   └── __init__.py              # package docstring only; the worker binds no connector class
│
├── state/                   # Fault tolerance
│   ├── state_manager.py
│   ├── store.py
│   ├── state_emission.py        # ANALITIQ_STATE:: log lines
│   ├── error_classification.py  # ErrorCode taxonomy + failure tagging
│   ├── dead_letter_queue.py
│   ├── log_emitter.py
│   └── metrics_storage.py       # Emits ANALITIQ_METRICS:: log lines
│
├── grpc/                    # gRPC client and generated stubs
├── models/                  # Pydantic v2 models (engine config, metrics, stream)
├── config/                  # Endpoint resolver, connection loader, validators
├── runner.py                # PipelineRunner (CLI entry from src.main)
├── runtime_archive.py       # Runtime config archive loading (local path or URL)
└── main.py                  # Dual-mode entrypoint (RUN_MODE = source | destination)
```

The engine has zero cloud SDK dependencies. State, logs, DLQ, and
metrics use the local filesystem and stdout; downstream ingestion by an
external log/metrics shipper is a deployment concern, not an engine concern.

### Incremental state restore

An incremental stream's resume cursor is written two ways: to an
`ANALITIQ_STATE` stdout log line the external shipper harvests into durable
storage (cloud), and to a **per-stream checkpoint file**
`state/{pipeline_id}/{stream_id}.json` = `{"cursor": <value>}`
(`CursorStore`, via `StateManager.save_stream_checkpoint`). Each stream owns its
own file and writes it on **every destination ACK**, so concurrent streams never
contend on a shared file and a crash loses at most the last un-ACKed batch.

The stored value is the **committed (destination-ACKed) high-water mark** —
never the source's pre-ACK position (the source advances its cursor as it yields
batches, ahead of the ACK). `save_cursor`, which relays that pre-ACK position
from the source worker, updates only the in-run cache and is never persisted. So
a stream that failed or never ACKed a batch resumes from its last safe bookmark
instead of skipping rows that never landed, and a stream with no checkpoint
resumes with a full re-scan.

Restore is lazy: `get_cursor` reads a stream's checkpoint file at the start of
its run (`src/state/store.py:CursorStore`, `src/state/state_manager.py`). The
two delivery paths converge on the same files: in the cloud each task starts
with an empty `state/`, so the deployment delivers the per-stream files in the
config bundle from whatever it harvested off the prior run; locally the files
the prior run wrote are read directly. Either way the engine only reads resolved
local files and never reaches for cloud storage — exactly as it does for secrets
and config. Delivering the cursors as bundle files rather than an env var also
keeps a high-stream-count pipeline clear of any size limit the deployment
imposes on a task's launch parameters.

Each cursor carries its type. A `datetime`/`date` travels as a tagged
`{"__type__": ..., "value": ...}` value — the same form the on-disk checkpoint
and the gRPC cursor token use — so a timestamp cursor comes back as a
`datetime` (asyncpg rejects a plain string for a timestamp bind) and a string
cursor whose value looks like a date stays a string. The type is carried
end-to-end, never guessed from a value's shape.

A resume reads inclusively (`>=`) from the last committed high-water mark, so
the boundary row is re-read. This keeps a non-unique cursor lossless: a row
that arrives at the boundary value between runs is still read, where an
exclusive `>` would filter it out at the source and drop it. The default
`upsert` write mode dedups the re-read against its `conflict_keys`; an `insert`
stream re-reading the boundary fails loud on the duplicate key rather than
silently losing rows.

## Pipeline Lifecycle

1. `src.main` reads `RUN_MODE`. `source` runs the pipeline engine;
   `destination` runs the gRPC destination server (see
   [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md)).
2. `PipelineRunner` (`src/runner.py`) instantiates `PipelineConfigPrep`,
   which:
   - discovers project root by locating `pipelines/manifest.json`,
   - finds the manifest entry matching `PIPELINE_ID` (must be `active`),
   - loads `pipeline.json` and per-stream files,
   - loads each `connection.json` + its connector definition,
   - builds a `ConnectionRuntime` per connection (with a per-connection
     secrets resolver),
   - resolves every `endpoint_ref` to its endpoint JSON.
3. `PipelineRunner` (`src/runner.py`) translates the resolved contract
   objects into a flat config dict via `_build_config_dict` (and its
   source/destination translation helpers), then constructs a
   `StreamingEngine` with runtime tuning parameters from the pipeline
   config and calls `engine.stream_data(config_dict)`.
4. `StreamingEngine.stream_data` creates one `StreamProcessor`
   (`src/engine/stream_processor.py`) per stream and runs them
   concurrently. Each processor owns everything scoped to its stream —
   counters, the gRPC client, its dead letter queue — and runs four async
   stages — `_extract_stage -> _transform_stage -> _load_stage ->
   _checkpoint_stage` — wired together with async queues. The processor
   compiles the stream's typed mapping document once at construction
   (`compile_mapping`), so a mapping the engine cannot run fails before any
   batch is read; the transform stage then applies it to each batch as
   vectorized Arrow compute.
5. `_load_stage` streams batches over gRPC to the destination service
   with row-level, content-derived idempotency (protocol in
   [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md)).
   Every send — including the synthetic empty batch that truncates a
   zero-batch full refresh — goes through the stream's `BatchPolicy`
   (`src/engine/batch_policy.py`), which owns the send/ack/backoff-retry
   loop and returns one terminal `Disposition` per batch. The processor
   acts on that verdict (checkpoint, dead letter, counters); it never
   re-derives it. See
   [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md#one-verdict-per-batch).
6. Metrics snapshots are emitted to logs as `ANALITIQ_METRICS::{...}`
   lines (batch-level from the engine, pipeline-level from the runner)
   and final pipeline metrics are persisted via
   `state.metrics_storage.save_pipeline_metrics`.

## Exception Hierarchy

Defined in `src/engine/exceptions.py`:

```
StreamProcessingError                   (base for runtime stream failures)
└── TransformationError

ConfigurationError                      (config-time failures)
```

Concurrent stream failures are aggregated with Python 3.11+
`ExceptionGroup` and consumed in callers via `except*`.

```python
try:
    await engine.stream_data(pipeline_config)
except* StreamProcessingError as eg:
    for exc in eg.exceptions:
        logger.error("stream failed: %s", exc)
except* Exception as eg:
    for exc in eg.exceptions:
        logger.error("unexpected error: %s", exc)
```

## Pipeline Error Codes (customer-safe contract)

The pipeline-level metrics record (`state.metrics_storage.PipelineMetricsRecord`,
emitted as `ANALITIQ_METRICS::{"type":"pipeline",...}`) carries a stable,
machine-readable failure category alongside `status` and the counts. The engine
classifies the terminating exception — it is the only layer that sees the
failure's stage and side at the raise site — using
`state.error_classification.classify_exception`.

`ErrorCode` is a **published contract**. The control plane forwards it to
external, API-key customers via the public run-status endpoint, so values are
stable: add members as new failure semantics appear, never rename or repurpose
existing ones. Coordinate additions with the control plane's error-code catalog.

| `error_code` | Meaning |
|---|---|
| `SOURCE_AUTH_FAILED` | Authentication/credentials to the source were rejected |
| `SOURCE_UNREACHABLE` | Source could not be reached (offline, DNS, refused, timeout) |
| `DESTINATION_WRITE_FAILED` | Writing to / reaching the destination failed (incl. a transport-side handshake failure) |
| `RATE_LIMITED` | Source rate-limited / throttled the request |
| `CONFIG_INVALID` | Pipeline/connector/connection config invalid — incl. type-map / mapping defects and destination schema-configuration failures |
| `INTERNAL` | Anything not matched above (treated as an engine-side fault) |

There is deliberately no `SCHEMA_MISMATCH` code: the engine performs no schema
validation. The destination "schema" handshake (`configure_schema`) only prepares
the destination's own table via DDL, so a failed handshake is a destination
*configuration* defect (`CONFIG_INVALID`) or a transport failure
(`DESTINATION_WRITE_FAILED`) — never a data-vs-schema mismatch. Type-map misses
and mapping/transform errors are likewise configuration defects.

Three error fields appear on the record, with distinct audiences:

- `error_code` — the enum above. Customer-safe. Set on `failed` (and `partial`
  where a dominant cause exists); `None` on success.
- `error_message` — a short, fixed, per-code human-readable message. Carries no
  exception text, so it cannot leak secrets, driver internals, or stack traces.
  Customer-safe.
- `error_detail` — a structured failure summary built from allowlisted-safe
  tokens only: stage labels, error codes, and exception *class names* read off
  the live objects (`stage/CODE:ExceptionType`), never message text. It is safe
  by construction — no driver internals, query fragments, or credentials can
  appear, so there is nothing to scrub. **Internal-only** nonetheless: the
  control plane must not forward it externally. The full message text stays in
  the engine logs.

Classification is **structured-first**. The engine stamps a `FailureTag` (a
definite `(error_code, stage)`) on the exception at the raise site, where it
already knows the stage and side: the extract / transform / load stage
boundaries, the destination handshake, and the config phase. `classify_exception`
reads those tags and uses them verbatim — deterministic, no text matching. For an
aggregated `ExceptionGroup` (every stream failed) the highest-priority tag across
the leaves wins, and `error_detail` keeps every per-stream leaf rather than
collapsing to `All streams failed (N sub-exceptions)`.

Four structured signals cross process boundaries so the tag survives isolation:

- The source worker's `deterministic` flag (a config/contract error retrying
  cannot heal) is preserved across the gRPC worker boundary as a `CONFIG_INVALID`
  tag, so a deterministic source-config error classifies as `CONFIG_INVALID`
  regardless of the `ReadError`/`RuntimeError` wrapper its type collapses into.
- The source worker's `declared_category` (`ReadError` wire message, issue
  the worker classifies a read failure at its birth site against the
  connector's declared `error_map` and sends the matched engine-vocabulary
  category; the engine maps it to the published code
  (`source_code_for_declared_category`) and tags both deterministic and
  retryable errors with it — a declared `rate_limited` 403 that exhausts
  retries reports `RATE_LIMITED`, and an undeclared one reports the extract
  stage's own default rather than a reading of "403".
- The destination handshake is classified from its outcome, not its wording.
  The gRPC client records whether it received a `SchemaAck`, or the stream
  died, or nothing came (`SchemaHandshakeOutcome`), because only it can tell
  a destination that refused the stream from one that never answered;
  `classify_handshake_failure` maps that outcome, and the category the
  destination declared on the rejected ack outranks it.
- The ack's `FailureCategory`, carried on both `BatchAck` and `SchemaAck`: the
  side that caught the failure declares config-defect / write-rejected /
  not-ready / internal, and `classify_destination_failure` maps the declared
  category directly (`CONFIG_INVALID` / `DESTINATION_WRITE_FAILED` /
  `INTERNAL`) instead of reading the `failure_summary` prose.

A connector may declare its driver's failure taxonomy as data — the
`error_map` block in `connector.json`: SQLSTATE classes and
states, exception class names, vendor codes, HTTP statuses, each mapped to
an engine-owned category (`transient | config | auth | unreachable |
rate_limited | write_rejected`). The engine alone derives the verdicts
(`AckStatus`, `FailureCategory`, `ErrorCode`) from a declared category;
connectors never self-declare verdicts. Classification happens at the
failure's birth site: the boundary that just caught the driver's error (the
CDK write ladder, the ADBC boundary, the source worker, both API
connectors) matches the immediate exception — plus at most its single
explicit driver link, SQLAlchemy's `orig` or `raise ... from` — against
the declared map, and the verdict crosses process boundaries as the
structured signals above (the deterministic flag, the wire
`declared_category`, the ack's failure category). Nothing downstream
re-derives a declared classification from exception chains or text, so a
declaring connector gets deterministic classification for declared
identifiers with zero connector Python.

Engine-side classification reads no exception type and no message text at
all — there is no phrase table and no class-name table. When nothing was
declared, the code comes from the stage that raised
(`default_code_for_stage`), and every stage boundary tags unconditionally so
that default always exists.

The stage defaults are asymmetric on purpose. A destination-load failure
defaults to `DESTINATION_WRITE_FAILED` because the stage establishes the
whole of that claim: the write did not happen. A source-extract failure
defaults to `INTERNAL`, because each source code names a *mechanism* — the
host did not answer, the credentials were refused, the quota ran out — and
the stage observed none of them. A connector declaring an `error_map`
supplies the mechanism; one that does not leaves a gap `INTERNAL` reports
rather than papers over. The side is not lost either way: it rides the tag's
stage label into `error_detail`, which reads
`source_extract/INTERNAL:ReadError`.

The `error_code` enum is the stable, audited contract. An exception that
reaches the runner with no tag is a raise site missing its boundary — an
engine defect, logged at ERROR and reported `INTERNAL`, so the hole is
findable instead of absorbed into a plausible code. A verdict is never a
secret leak (only class names and codes ever reach `error_detail`) and never
a cross-stage error (the stage is always known from the tag). The two
vocabularies behind this — what a peer declares versus what the customer is
told — are separated in [ADR 0001](adr/0001-two-failure-vocabularies.md).

## ConnectionRuntime and Transports

Each connection loaded by `PipelineConfigPrep` becomes a
`ConnectionRuntime` (`cdk/cdk/connection_runtime.py`). The runtime:

- Holds the resolved connector definition and the user's connection
  document.
- Requires `connector_type` to be a non-empty string. Which kinds are
  runnable is decided by the worker registry (`cdk/cdk/registry.py`),
  not by a hard-coded set, so registry-discovered connector kinds are
  not blocked at config time.
- When the connector declares a `transports` block, builds the actual
  transport (SQLAlchemy async engine, aiohttp ClientSession, etc.) via
  `cdk/cdk/transport_factory.py`. The factory keeps resolution and
  construction apart, so no live object ever crosses to the connector
  side:
  - `resolve_transport_spec` — trusted side. Renders the selected
    transport into a JSON-safe spec (DSN with secrets in place,
    `db_kwargs`, TLS mode + CA PEM, headers, engine kwargs) through a
    `Resolver` carrying `DEFAULT_FUNCTIONS`. Per-kind resolvers
    (`resolve_sqlalchemy_spec`, `resolve_http_spec`) validate as they
    go — `resolve_http_spec` rejects a half-specified `rate_limit`,
    which needs both `max_requests` and `time_window_seconds`.
  - `resolve_transport_specs` — the same, for the whole set a run may
    dispatch through: the default plus every transport an operation's
    `request.transport_ref` names. Resolved together because it is the
    last moment the secrets are in reach; the connection scrubs them as
    soon as materialization ends, so a transport whose spec was not
    resolved by then can never be opened. Which refs those are is
    derived from the run's endpoint documents by
    `src/worker/shell.build_bootstrap` (via
    `cdk.api.request.endpoint_transport_refs`), never by resolving every
    transport the connector declares — a connector's auth, login and
    discovery transports belong to connection setup, and their secrets
    need not be in a run's connection blob at all.
  - `build_transport_from_spec` — connector side. Dispatches on
    `transport_type` to the per-kind builder
    (`build_sqlalchemy_from_spec`, `build_adbc_from_spec`,
    `build_http_from_spec`) that assembles the live transport. The
    default is built at `materialize()`; a named one is built by
    `ConnectionRuntime.http_transport(ref)` on the first request that
    asks for it, so a single-transport connector opens exactly the one
    session it always opened. Both that build and the api connector's
    sender cache are guarded per ref: two streams reaching a transport
    together would otherwise each open a session, and the loser would be
    a connection pool nothing closes.
  - `ca_ssl_context` — builds a verifying `ssl.SSLContext` from a PEM CA
    bundle; the shared helper behind connector packages' own
    `build_tls_connect_arg`.
- Reference-counts handles so multiple streams sharing a connection
  share the same engine / session.

Expression resolution (`ref`, `template`, `literal`, `function`) is
provided by `cdk/cdk/resolver.py`; the `function` registry is in
`cdk/cdk/derived_functions.py` (`lookup`, `basic_auth`,
`base64_encode`, `url_encode`).

The other engine-owned closed registry on the request path is query
serialization (`cdk/cdk/api/query_style.py`). The published schema
requires `style` and `explode` on a query param typed `array` or
`object` but types `style` as a plain string, so the set the engine can
actually spell is closed here: `form`, `spaceDelimited`, `pipeDelimited`
and `deepObject`, in the explode combinations OpenAPI defines. A style
outside it — or a combination OpenAPI leaves undefined — is refused with
the rest of the request block, before a page is fetched.

## Source Connector Layer

The engine ships no source connector. Both families live in the CDK, and
each is one class serving read and write:

- `GenericSQLConnector` (`cdk/cdk/sql/generic.py`) implements four
  capability Protocols from `cdk/cdk/contract.py` (`Readable` / `Writable`
  / `Discoverable` / `TableCreator`) and serves both the SQLAlchemy and
  ADBC transports.
- `GenericAPIConnector` (`cdk/cdk/api/generic.py`) owns one HTTP round
  trip, one classification of what a response status means, incremental
  replication over the engine-supplied safety window, and the five paging
  schemes the endpoint contract declares. Those five run on one loop
  (`cdk/cdk/api/page_loop.py`) with one adapter per scheme — see
  [ADR 0002](adr/0002-one-stop-rule-for-every-paging-scheme.md).

The one attribute either class exposes for a connector package to override
is its dialect (`SqlDialect`, `ApiDialect`): pure translation, no I/O. See
[`connector-module-architecture.md`](connector-module-architecture.md) for
the full CDK contract.

## Connector Registries

Connector classes are resolved through `ConnectorRegistry`
(`cdk/cdk/registry.py`), constructed by `build_registries()`. There
is no `HandlerRegistry`.

The engine binds no connector class. Every kind default is a CDK generic
class named once in `cdk.registry.KIND_DEFAULTS`, which maps a kind to a
`module:ClassName` string plus the extra that importing it needs:

| kind | kind default | extra |
|------|--------------|-------|
| `database` | `cdk.sql.generic:GenericSQLConnector` | `arrow` |
| `api` | `cdk.api.generic:GenericAPIConnector` | `api` |
| `file` | `cdk.file.generic:GenericFileConnector` | `file` |
| `s3` | `cdk.file.generic:GenericFileConnector` | `file` |
| `stdout` | `cdk.stdout.generic:GenericStdoutConnector` | `arrow` |

The table holds strings, not classes, so reading the kind vocabulary costs
no transport: only resolving a kind imports one, and a kind whose transport
is absent fails naming the extra to install. `build_registries()` seeds both
registries from that one table, and **which roles a kind serves is read off
the class**, never declared beside it — a class is registered as a source
default iff it satisfies `Readable` and as a destination default iff it
satisfies `Writable` (`cdk/cdk/contract.py`). So `file`, `s3` and `stdout`
have no source default at all, and a `kind: file` source fails loud instead
of resolving a class with no read path.

`build_registries()` is called inside the spawned worker subprocess, because
that is where connector classes execute. The engine process holds only the
`WorkerReadable` client and never loads connector code.

Externally installed connector packages add themselves on top through the
`analitiq.source_connectors` and `analitiq.destination_connectors`
entry-point groups. A package states its roles by which groups it registers
under; a kind default states them through the Protocols its class
implements.

## Structured Logging

`StreamingEngine` uses structured logging with correlation IDs. The engine
stamps each log with the `run_id` so batches and stream events are joinable
downstream.

```text
... INFO  src.engine.engine.wise-to-postgresql - Starting pipeline: wise-to-postgresql
... INFO  src.engine.engine.wise-to-postgresql - Processing stream: wise-transfers
       {"stream_id": "wise-transfers", "correlation_id": "..."}
```

## Testing

| Suite | Location | Notes |
|-------|----------|-------|
| Unit | `tests/unit/...` | Default, fast; uses Pydantic validation tests, resolver/transport-factory tests |
| Integration | `tests/integration/...` | Real DB / gRPC integration |
| End-to-end | `docker compose run --rm source_engine` with a real `PIPELINE_ID` | The canonical contract test |

Run unit tests with `poetry run pytest`; run end-to-end pipelines with
the docker compose flow described in the project `CLAUDE.md`.

## See Also

- [`source-config.md`](source-config.md)
- [`destination-config.md`](destination-config.md)
- [`mapping-and-transformations.md`](mapping-and-transformations.md)
- [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md)
- [`pyarrow-and-destinations.md`](pyarrow-and-destinations.md)
- [`connector-module-architecture.md`](connector-module-architecture.md)
