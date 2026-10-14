# Engine Architecture

This document describes the engine layout, the pipeline lifecycle, and
the contracts between components. For source-/destination-side schema
details see [`source-config.md`](../config/source-config.md) and
[`destination-config.md`](../config/destination-config.md).

**Scope:** this doc owns the streaming engine — extract / transform /
load / checkpoint orchestration, the producer/consumer flow, the
dual-mode (`RUN_MODE`) runner, and how the engine consumes the CDK and
connections. It defers elsewhere for: destination handler config
([`destination-config.md`](../config/destination-config.md)); the gRPC protocol /
wire format ([`grpc-streaming-architecture.md`](grpc-streaming-architecture.md));
the Arrow type system ([`arrow-and-transport-strategy.md`](../data-path/arrow-and-transport-strategy.md));
the CDK boundary / contract
([`connector-module-architecture.md`](connector-module-architecture.md));
and source / stream config ([`source-config.md`](../config/source-config.md)).

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
├── type_map/                # TypeMapper, the arrow_type vocabulary
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
├── destination/             # Destination side (see ../config/destination-config.md)
│   └── server.py                # gRPC server
│
├── engine/                  # Core engine
│   ├── engine.py                # StreamingEngine (fans streams out, aggregates results)
│   ├── stream_processor.py      # StreamProcessor (one stream: extract -> transform -> load -> checkpoint)
│   ├── pipeline_config_prep.py  # Loads manifest/pipelines/streams/connections/connectors
│   ├── mapping.py               # compile_mapping (StreamMapping -> Arrow compute)
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
├── models/                  # Engine-side models (resolved config, metrics, state)
├── config/                  # Workspace request + validator gate, endpoint resolver, settings
├── runner.py                # PipelineRunner (CLI entry from src.main)
├── runtime_archive.py       # Runtime config archive loading (local path or URL)
└── main.py                  # Dual-mode entrypoint (RUN_MODE = source | destination)
```

State and the DLQ are written to the local filesystem; logs, metrics, and
state lines go to stdout (see
[ADR 0007](../adr/0007-the-engine-carries-no-cloud-sdk.md)).

### Incremental state restore

An incremental stream's durable resume cursor advances only when a destination
ACK commits a batch that carries one. The engine then writes it to the stream's
own checkpoint file, `state/{pipeline_id}/{stream_id}.json` =
`{"cursor": <value>}`, and to an `ANALITIQ_STATE` stdout line (see
[ADR 0007](../adr/0007-the-engine-carries-no-cloud-sdk.md)). After a crash,
the next run resumes from the last checkpointed cursor, re-reading the
boundary as [ADR 0013](../adr/0013-prefer-duplicates-over-data-loss.md)
records; a `truncate_insert` stream re-reads from the start.
A stream with no checkpoint resumes with a full re-scan.

Restore is lazy: `get_cursor` reads a stream's checkpoint file at the start of
its run (`src/state/store.py:CursorStore`, `src/state/state_manager.py`). The
two delivery paths converge on the same files: in the cloud each task starts
with an empty `state/`, so the deployment delivers the per-stream files in the
config bundle from whatever it harvested off the prior run; locally the files
the prior run wrote are read directly.

Each cursor carries its type. A `datetime`/`date` travels as a tagged
`{"__type__": ..., "value": ...}` value — the same form the on-disk checkpoint
and the gRPC cursor token use — so a timestamp cursor comes back as a
`datetime` (asyncpg rejects a plain string for a timestamp bind) and a string
cursor whose value looks like a date stays a string. The type is carried
end-to-end, never guessed from a value's shape.

A SQL resume reads inclusively (`>=`) from the checkpointed cursor, so the boundary row is re-read (see
[ADR 0013](../adr/0013-prefer-duplicates-over-data-loss.md)).

## Pipeline Lifecycle

1. `src.main` reads `RUN_MODE`. `source` runs the pipeline engine;
   `destination` runs the gRPC destination server (see
   [ADR 0009](../adr/0009-engine-and-destination-are-separate-services-joined-by-grpc-carrying-arrow-ipc.md)).
2. `PipelineRunner` (`src/runner.py`) instantiates `PipelineConfigPrep`,
   which:
   - discovers project root by locating `pipelines/manifest.json`,
   - finds the manifest entry matching `PIPELINE_ID`, which names the
     pipeline's package directory,
   - gates the run's workspace through the published validator
     (`gate_run`) and parses the pipeline, stream, connection and
     connector documents from the graded texts (see
     [ADR 0016](../adr/0016-the-pinned-validator-is-the-only-document-gate.md)),
   - builds a `ConnectionRuntime` per connection (with a per-connection
     secrets resolver),
   - resolves every `endpoint_ref` to its endpoint JSON.
3. `PipelineRunner.run` (`src/runner.py`) translates the resolved contract
   objects into a flat config dict, then constructs a
   `StreamingEngine` with runtime tuning parameters from the pipeline
   config and calls `engine.stream_data(config_dict)`.
4. `StreamingEngine.stream_data` creates one `StreamProcessor`
   (`src/engine/stream_processor.py`) per stream and runs them
   concurrently via `StreamProcessor.run`. Each processor owns everything
   scoped to its stream — counters, the gRPC client, its dead letter
   queue — and runs four async stages, extract -> transform -> load ->
   checkpoint, wired together with async queues. Before extraction starts,
   `StreamProcessor.run` compiles the stream's typed mapping document once
   (`compile_mapping`), when the stream declares any assignments, so a
   mapping the engine cannot run fails before any batch is read; the
   transform stage then applies the compiled mapping to each batch as
   vectorized Arrow compute. A stream with no assignments skips compilation
   and forwards each source batch untouched.
5. The load stage streams batches over gRPC to the destination service
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
└── TransformationError                 (a mapping defect on a batch)
    └── ValidationFailure               (rows failing a rule; carries the strategy)
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
- `error_message` — a fixed per-code message. Customer-safe.
- `error_detail` — a structured failure summary, such as
  `stage/CODE:ExceptionType`. **Internal-only**.

Neither carries exception message text (see
[ADR 0023](../adr/0023-customer-error-text-is-built-only-from-allowlisted-tokens.md)).

The engine stamps a `FailureTag` (a definite `(error_code, stage)`) on the
exception at the raise site: the extract / transform / load stage boundaries,
the destination handshake, and the config phase. `classify_exception` reads
those tags (see [ADR 0001](../adr/0001-two-failure-vocabularies.md)). For an
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
  connector's declared `error_map` and sends the matched `ErrorCategory`
  value; the engine maps it to the published code
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

When a connector's `error_map` claims a failure, the verdict derived from the
matched category crosses process boundaries as structured signals (see
[ADR 0022](../adr/0022-connectors-declare-driver-failures-as-facts.md)).

For how a code is chosen when nothing was declared, see
[ADR 0001](../adr/0001-two-failure-vocabularies.md).

## ConnectionRuntime and Transports

Each connection loaded by `PipelineConfigPrep` becomes a
`ConnectionRuntime` (`cdk/cdk/connection_runtime.py`). The runtime:

- Holds the resolved connector definition and the user's connection
  document.
- Which connector class runs is decided by the worker registry
  (`cdk/cdk/registry.py`), not by config prep.
- When the connector declares a `transports` block, builds the actual
  transport (SQLAlchemy async engine, aiohttp ClientSession, etc.) via
  `cdk/cdk/transport_factory.py` (see
  [ADR 0008](../adr/0008-connector-code-runs-only-in-an-isolated-worker.md)):
  - `resolve_transport_spec` — trusted side. Renders the selected
    transport into a JSON-safe spec (DSN with secrets in place,
    `db_kwargs`, TLS mode + CA PEM, headers, engine kwargs) through a
    `Resolver` carrying `DEFAULT_FUNCTIONS`, via the per-kind resolvers
    `resolve_sqlalchemy_spec` and `resolve_http_spec`.
  - `resolve_transport_specs` — the same, for the default plus the
    transports named by the `request.transport_ref` of the operations
    the worker's role runs, as derived from the run's endpoint documents by
    `src/worker/shell.build_bootstrap` (via
    `cdk.api.request.endpoint_transport_refs`).
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

The second engine-owned registry on the request path is query
serialization: `cdk/cdk/api/query_style.py` turns a declared
`(style, explode)` into its wire spelling.

A declared param's value keywords (`cdk/cdk/api/param_rules.py`) are the
third. `enum`, `format`, `pattern`, the numeric bounds and the length and
item bounds are published as JSON Schema, so they are enforced with the
reference implementation rather than with rules of the engine's own —
anything else would let a document satisfy the published artifact and fail
here. Two things sit around it. The `format` names the engine enforces are
listed explicitly rather than taken from the library's ambient registry,
which holds whichever checkers the surrounding install happens to supply:
a connector pulling one in for its own reasons would otherwise switch a
format on inside its worker and nowhere else. And numbers are compared in
one decimal model entered from the text both sides were written in — a
declared bound arrives as the binary float nearest the author's decimal,
while a value read back off the wire is an exact `Decimal`, which is how a
keyset cursor reaches the next page. The compiled rules travel on the
param table every request is built from, so no request reaches the wire
without the declarations that judge its values. A refusal names the param,
the endpoint, the keyword and what the author declared, and never the
value: params carry bearer tokens and continuation tokens.

## Source Connector Layer

Both connector families live in the CDK (see
[ADR 0020](../adr/0020-one-connector-class-serves-both-read-and-write.md)):

- `GenericSQLConnector` (`cdk/cdk/sql/generic.py`) implements four
  capability Protocols from `cdk/cdk/contract.py` (`Readable` / `Writable`
  / `Discoverable` / `TableCreator`) and serves both the SQLAlchemy and
  ADBC transports.
- `GenericAPIConnector` (`cdk/cdk/api/generic.py`) owns one HTTP round
  trip, one classification of what a response status means, incremental
  replication over the engine-supplied safety window, and every paging
  scheme. They all run on one loop
  (`cdk/cdk/api/page_loop.py`) with one adapter per scheme — see
  [ADR 0002](../adr/0002-one-stop-rule-for-every-paging-scheme.md).

Each class's dialect (`SqlDialect`, `ApiDialect`) is its per-system
override seam. See
[`connector-module-architecture.md`](connector-module-architecture.md) for
the full CDK contract.

## Connector Registries

Connector classes are resolved through `ConnectorRegistry`
(`cdk/cdk/registry.py`), constructed by `build_registries()`. There
is no `HandlerRegistry`.

Each kind's default is a CDK generic class named in
`cdk.registry.KIND_DEFAULTS`, which maps a kind to a `module:ClassName`
string, its roles, and the extra that importing it needs (see
[ADR 0019](../adr/0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md)):

| kind | kind default | roles | extra |
|------|--------------|-------|-------|
| `database` | `cdk.sql.generic:GenericSQLConnector` | source, destination | `arrow` |
| `api` | `cdk.api.generic:GenericAPIConnector` | source, destination | `api` |
| `file` | `cdk.file.generic:GenericFileConnector` | destination | `file` |
| `s3` | `cdk.file.generic:GenericFileConnector` | destination | `file` |
| `stdout` | `cdk.stdout.generic:GenericStdoutConnector` | destination | `arrow` |

`build_registries()` seeds both registries from that table. A kind default
is imported on first resolve, where a missing transport fails naming the
extra to install and the declared roles are checked against `Readable` /
`Writable`. `file`, `s3` and `stdout` have no source default, so a
`kind: file` source fails loud.

`build_registries()` runs inside the spawned worker subprocess (see
[ADR 0008](../adr/0008-connector-code-runs-only-in-an-isolated-worker.md)).

Externally installed connector packages register through the
`analitiq.source_connectors` and `analitiq.destination_connectors`
entry-point groups (see
[ADR 0004](../adr/0004-capability-is-derived-never-declared.md)).

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
| Unit | `tests/unit/...` | Default, fast; resolver/transport-factory tests |
| Integration | `tests/integration/...` | Real DB / gRPC integration |
| End-to-end | `docker compose run --rm source_engine` with a real `PIPELINE_ID` | The canonical contract test |

Run unit tests with `poetry run pytest`; run end-to-end pipelines with
the docker compose flow described in the project `CLAUDE.md`.

## See Also

- [`source-config.md`](../config/source-config.md)
- [`destination-config.md`](../config/destination-config.md)
- [`mapping-and-transformations.md`](../data-path/mapping-and-transformations.md)
- [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md)
- [`arrow-and-transport-strategy.md`](../data-path/arrow-and-transport-strategy.md)
- [`connector-module-architecture.md`](connector-module-architecture.md)
