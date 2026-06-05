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
│   ├── orchestrator.py          # PipelineOrchestrator (multi-stream coordination)
│   ├── pipeline.py              # Pipeline (config + lifecycle)
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
3. `Pipeline` (`src/engine/pipeline.py`) wraps the loaded config,
   creates a `StreamingEngine`, and calls `engine.stream_data(...)`.
4. `StreamingEngine` delegates multi-stream coordination to
   `PipelineOrchestrator`. Each stream runs four async stages —
   `_extract_stage -> _transform_stage -> _load_stage ->
   _checkpoint_stage` — wired together with async queues. The transform
   stage uses `AssignmentTransformer` for the assignment AST and
   `SecureExpressionEvaluator` for legacy string expressions.
5. `_load_stage` streams batches over gRPC to the destination service
   with batch-level idempotency (protocol in
   [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md)).
6. Metrics snapshots are emitted to logs as `ANALITIQ_METRICS::{...}`
   lines (batch-level from the engine, pipeline-level from the runner)
   and final pipeline metrics are persisted via
   `state.metrics_storage.save_pipeline_metrics`.

## Pydantic Models

Engine configuration uses Pydantic v2. Key models are in
`src/models/engine.py`:

- `StreamStageConfig` — per-stage timeout / retries / metrics.
- `PipelineStagesConfig` — extract/transform/load/checkpoint stages.
- `StreamProcessingConfig` — full per-stream config validated before
  execution. Validates `replication_method ∈ {full, incremental}`,
  `refresh_mode ∈ {insert, upsert, truncate_insert}`, and that source /
  destination dicts include `endpoint_ref` and `connection_id`.
- `PipelineMetricsSnapshot` — pipeline-level metrics snapshot for
  orchestrator output.

```python
from src.models.engine import StreamProcessingConfig

cfg = StreamProcessingConfig(
    stream_id="wise-transfers",
    stream_name="Wise transfers",
    pipeline_id="wise-to-postgresql",
    source={"endpoint_ref": "...", "connection_id": "my-wise"},
    destination={"endpoint_ref": "...", "connection_id": "my-postgres"},
)
```

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

PipelineOrchestrationError              (orchestrator-level failures)
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

`PipelineOrchestrator` and `StreamingEngine` use structured logging with
correlation IDs. The orchestrator stamps each log with the `run_id` so
batches and stream events are joinable downstream.

```text
... INFO  src.engine.orchestrator.wise-to-postgresql - Starting pipeline orchestration
       {"pipeline_id": "wise-to-postgresql", "run_id": "...", "stream_count": 1}
... INFO  src.engine.engine.wise-to-postgresql - Processing stream
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
