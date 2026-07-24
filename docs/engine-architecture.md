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
├── sql_types.py             # arrow_to_sqlalchemy + per-driver native renderers
├── base_handler.py          # BaseDestinationHandler ABC
├── contract.py              # Readable / Writable / Discoverable / TableCreator Protocols
├── types.py                 # Shared CDK types
├── registry.py              # ConnectorRegistry + build_registries(...)
├── secrets/                 # Secret resolvers
├── query_builder.py         # WHERE / SELECT rendering
└── sql/                     # GenericSQLConnector + dialects / DDL / discovery / execution
    └── generic.py           # GenericSQLConnector (source reads + destination writes)

src/
├── shared/                  # Engine-local helpers
│   ├── dict_path.py
│   ├── http_utils.py
│   └── run_id.py
│
├── source/                  # Source side
│   └── connectors/              # BaseConnector, APIConnector (DB source lives in the CDK)
│
├── destination/             # Destination side (see destination-config.md)
│   ├── connectors/              # API / File / Stream handlers + destination_registry / get_handler
│   ├── formatters/              # JSONL / CSV / Parquet
│   ├── storage/                 # Local file storage
│   └── server.py                # gRPC server
│
├── engine/                  # Core engine
│   ├── engine.py                # StreamingEngine (fans streams out, aggregates results)
│   ├── stream_processor.py      # StreamProcessor (one stream: extract -> transform -> load -> checkpoint)
│   ├── pipeline_config_prep.py  # Loads manifest/pipelines/streams/connections/connectors
│   ├── data_transformer.py      # compile_transform (vectorized mapping AST -> Arrow compute)
│   └── exceptions.py
│
├── worker/                  # Sandboxed connector worker (spawned subprocess)
│   ├── readable.py              # WorkerReadable (engine-side client)
│   ├── source_service.py        # Worker-side read loop
│   ├── proxy.py / shell.py / spawn.py / bootstrap.py
│   └── __init__.py              # build_worker_registries (kind + connector_id resolution)
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
   _checkpoint_stage` — wired together with async queues. The transform
   stage compiles the assignment AST once (`compile_transform`) and applies
   it to each batch as vectorized Arrow compute.
5. `_load_stage` streams batches over gRPC to the destination service
   with row-level, content-derived idempotency (protocol in
   [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md)).
   Every send — including the synthetic empty batch that truncates a
   zero-batch full refresh — goes through one shared send/ack/retry loop
   (`_send_batch_acked`), so retry backoff and ack handling behave
   identically everywhere; only the failure policy (fail/dlq/skip,
   classification) differs per call site.
6. Metrics snapshots are emitted to logs as `ANALITIQ_METRICS::{...}`
   lines (batch-level from the engine, pipeline-level from the runner)
   and final pipeline metrics are persisted via
   `state.metrics_storage.save_pipeline_metrics`.

## Exception Hierarchy

Defined in `src/engine/exceptions.py`:

```
StreamProcessingError                   (base for runtime stream failures)
├── TransformationError
├── ConnectorError
└── StreamExecutionError                (carries stage + batch context)

ConfigurationError                      (base for config-time failures)
├── StreamConfigurationError
├── PipelineValidationError
└── StageConfigurationError
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

Three structured signals cross process boundaries so the tag survives isolation:

- The source worker's `deterministic` flag (a config/contract error retrying
  cannot heal) is preserved across the gRPC worker boundary as a `CONFIG_INVALID`
  tag, so a deterministic source-config error classifies as `CONFIG_INVALID`
  regardless of the `ReadError`/`RuntimeError` wrapper its type collapses into.
- The destination-handshake reason (engine/proxy-generated, including the inner
  reason forwarded across the worker proxy) is split transport-vs-config at the
  raise site by `classify_handshake_failure`, so a proxied destination outage
  classifies `DESTINATION_WRITE_FAILED` and a destination-config defect
  `CONFIG_INVALID` — no schema-vs-transport guessing.
- The batch ack's `FailureCategory` (`BatchAck` field 9, issue #351): the
  destination declares config-defect / write-rejected / not-ready where the
  failure is caught, and `classify_destination_failure` maps the declared
  category directly (`CONFIG_INVALID` / `DESTINATION_WRITE_FAILED` /
  `INTERNAL`) instead of substring-matching the `failure_summary` prose.

A connector may declare its driver's failure taxonomy as data — the
`error_map` block in `connector.json` (issue #401): SQLSTATE classes and
states, exception class names, vendor codes, HTTP statuses, each mapped to
an engine-owned category (`transient | config | auth | unreachable |
rate_limited | write_rejected`). The engine alone derives the verdicts
(`AckStatus`, `FailureCategory`, `ErrorCode`) from a declared category;
connectors never self-declare verdicts. The connector-facing boundaries
(the CDK write ladder, the ADBC boundary, the source worker, both API
connectors) consult the declared map before their heuristics, and its
verdict reaches the engine's classifiers as structured signals — the
worker's deterministic flag, the ack's failure category, the extract tag —
so a declaring connector gets deterministic classification for declared
identifiers with zero connector Python.

The name/phrase heuristics remain in three narrow roles only, each running
strictly after the declared map and the structured signals, and each logs
when it decided: the **source-extract fine split**
(`classify_source_extract`), which picks auth-vs-unreachable-vs-rate for an
opaque, undeclared source driver/HTTP error; the **destination-load
fallback** (`classify_destination_failure`) for a batch failure whose ack
declares no category — a thick connector's own ack, or a failure with no
ack at all; and a defensive **fallback** (`classify_exception` over class
names + message text, mirroring `cdk.sql._adbc_utils._is_fatal_adbc_error`)
for any exception that reaches the runner with no tag. Because the source
split runs *only* at the source boundary, a destination port (`host:401`)
or path can never be misread as source auth.

The `error_code` enum is the stable, audited contract. The one residual
best-effort area is the source-extract fine split above, and only for a
failure no declared fact claims — a partial map is the normal case, and a
declared `transient`/`write_rejected` names no source code by design: such
a failure's auth-vs-unreachable-vs-rate is inferred from its text and can
fall to a neighbouring code or `INTERNAL`. It is never a secret leak (only
class names and codes ever reach `error_detail`) and never a cross-stage
error (the stage is always known from the tag).

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
  `cdk/cdk/transport_factory.py`. The factory drives:
  - `_materialize_derived` — fixpoint-evaluates the connector's
    `derived` block.
  - `_ssl_dict_to_context` — builds an `ssl.SSLContext` from declarative
    `{verify_mode, check_hostname}` dicts (CPython-safe ordering).
  - `build_sqlalchemy_transport` and `build_http_transport` — assemble
    the final transport, rejecting half-specified `rate_limit` shapes.
- Reference-counts handles so multiple streams sharing a connection
  share the same engine / session.

Expression resolution (`ref`, `template`, `literal`, `function`) is
provided by `cdk/cdk/resolver.py`; the `function` registry is in
`cdk/cdk/derived_functions.py` (`lookup`, `basic_auth`,
`base64_encode`, `url_encode`).

## Source Connector Layer

`BaseConnector` lives in `src/source/connectors/base.py`. The only
engine-side concrete implementation is `APIConnector`
(`src/source/connectors/api.py`) — it handles cursor / offset / page /
time-window pagination, incremental replication with safety windows and
tie-breaker deduplication, rate limiting, and retry / backoff via
state-layer helpers.

Database sources and destinations are unified in the CDK as
`GenericSQLConnector` (`cdk/cdk/sql/generic.py`), a single class that
implements four capability Protocols from `cdk/cdk/contract.py`
(`Readable` / `Writable` / `Discoverable` / `TableCreator`) and serves
both SQLAlchemy and ADBC transports. The standalone `DatabaseConnector`
and the per-dialect `src/source/drivers/` classes no longer exist. See
[`connector-module-architecture.md`](connector-module-architecture.md)
for the full CDK contract.

## Connector Registries

Connector classes are resolved through `ConnectorRegistry`
(`cdk/cdk/registry.py`), constructed by `build_registries(...)`. There
is no `HandlerRegistry`.

- The **source** registry is built inside the spawned worker subprocess
  (`build_worker_registries` in `src/worker/__init__.py`); the engine
  process holds only the `WorkerReadable` client and never loads
  connector code.
- The **destination** side builds its registry in
  `src/destination/connectors/__init__.py`, exporting
  `destination_registry` and the `get_handler(connector_type)` helper.
  Built-ins map `database -> GenericSQLConnector`, `api`, `file` / `s3`,
  and `stdout`. Externally installed connector packages register
  themselves through the `analitiq.destination_connectors` entry-point
  group.

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
