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
│   ├── placeholder.py           # ${name} expansion shim
│   ├── expressions.py
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
│   ├── idempotency/             # _batch_commits and _manifest.json trackers
│   └── server.py                # gRPC server
│
├── engine/                  # Core engine
│   ├── engine.py                # StreamingEngine (extract -> transform -> load -> checkpoint)
│   ├── pipeline_config_prep.py  # Loads manifest/pipelines/streams/connections/connectors
│   ├── data_transformer.py      # AssignmentTransformer (mapping AST execution)
│   ├── expression_evaluator.py  # SecureExpressionEvaluator (string-form expressions)
│   └── exceptions.py
│
├── state/                   # Fault tolerance
│   ├── state_manager.py
│   ├── state_storage.py
│   ├── retry_handler.py
│   ├── circuit_breaker.py
│   ├── dead_letter_queue.py
│   ├── log_storage.py
│   └── metrics_storage.py       # Emits ANALITIQ_METRICS:: log lines
│
├── grpc/                    # gRPC client and generated stubs
├── models/                  # Pydantic v2 models (engine config, metrics, stream)
├── config/                  # Endpoint resolver, connection loader, validators
├── secrets/                 # Secret resolvers
├── schema/                  # Schema drift detection
├── transformations/         # Transformation registry
├── runner.py                # PipelineRunner (CLI entry from src.main)
└── main.py                  # Dual-mode entrypoint (RUN_MODE = source | destination)
```

The engine has zero cloud SDK dependencies. State, logs, DLQ, and
metrics use the local filesystem and stdout; downstream ingestion by an
external log/metrics shipper is a deployment concern, not an engine concern.

### Incremental state restore

An incremental stream's resume cursor is written two ways: to the local
`state/{pipeline_id}/{stream_id}.json` checkpoint, and to an
`ANALITIQ_STATE` stdout log line the external shipper harvests into durable
storage. On a fresh container (each task starts with an empty `state/`) the
local checkpoint is gone, so restore reads the `RESUME_STATE` environment
variable instead — a JSON object `{stream_id: cursor}` the deployment
injects from whatever it harvested off the prior run. `StateManager` seeds
its cursor cache from it at startup (`src/state/store.py:parse_resume_state`,
`src/state/state_manager.py`). This keeps restore symmetric with emission:
the engine never reaches for cloud storage itself, it only consumes a
resolved value the deployment supplies — exactly as it does for secrets and
config.

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
4. `StreamingEngine` orchestrates multi-stream execution directly in
   `stream_data`. Each stream runs four async stages —
   `_extract_stage -> _transform_stage -> _load_stage ->
   _checkpoint_stage` — wired together with async queues. The transform
   stage uses `AssignmentTransformer` for the assignment AST.
5. `_load_stage` streams batches over gRPC to the destination service
   with batch-level idempotency (protocol in
   [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md)).
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
machine-readable failure category alongside `status` and the counts. The runner
classifies the terminating exception at the catch site — the engine is the only
layer that sees the exception type and the source/destination context — using
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
- `error_detail` — the raw exception text with credential-bearing substrings
  redacted. **Internal-only**: the control plane must not forward it externally.

Classification matches exception *class names* across the whole chain (cause,
context, `ExceptionGroup` members, and the engine's `original_error`) plus the
message text, rather than importing every exception class. This mirrors the
existing name-based pattern in `cdk.sql.generic._is_fatal_adbc_error` and handles
the gRPC worker boundary, where the original source exception type collapses to
`ReadError` / `RuntimeError` but its class name survives as an `error_type:`
prefix in the message. Rules are evaluated in priority order (local-IO, config,
destination handshake, destination write, then the source-side
auth/rate/unreachable buckets), so for an aggregated `ExceptionGroup` the
dominant cause wins.

The `error_code` enum is the stable, audited contract. The *textual* part of
classification (un-typed driver/HTTP errors) and `error_detail` credential
scrubbing are best-effort heuristics over free text: an ambiguous message can
fall to a neighbouring code or `INTERNAL`, and an exotic secret shape (a PEM
block, a password-only URL) may slip past the scrubber -- which is why
`error_detail` is internal-only and never forwarded. The durable fix for both
tails is structured, engine-side error reporting (tag failures with
stage/side/category at the raise site; pass only allowlisted fields), tracked as
a follow-up rather than by widening heuristics here.

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

- The engine builds a **source** registry in `src/engine/engine.py`
  (`self._source_registry, _ = build_registries(...)`) and instantiates
  a stream's source via `self._source_registry.create(connector_type)`.
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
