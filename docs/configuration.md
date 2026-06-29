# Configuration Reference

This document describes every default the engine ships with: what each one
controls, its built-in value, and how to override it.

## Where defaults live

| Layer | Location | What it holds |
|---|---|---|
| Engine defaults (code) | [`src/config/settings.py`](../src/config/settings.py) | The single source for every engine + infrastructure default, with its environment-variable override. |
| Per-pipeline runtime | `pipelines/{pipeline_id}/pipeline.json` -> `runtime` block | Per-pipeline overrides of the runtime-tuning defaults. |
| Connector / formatter defaults | each connector package, and the destination formatters under `src/destination/formatters/` | Connector- and format-specific defaults. The engine stays connector-agnostic, so these live with their owner, not here. |

## How a value is resolved

Runtime-tuning values are layered, most-specific wins:

```
pipeline.json runtime block  >  environment variable  >  built-in default
```

A key omitted (or set to `null`) in the pipeline's `runtime` block falls
through to the environment variable, then to the built-in default. The overlay
happens once, in `src.engine.pipeline_config_prep._parse_runtime_config`.

Infrastructure values have no per-pipeline layer: they are read straight from
the environment variable, then the built-in default.

Environment variables are read on use (not at import), so a value placed in a
`.env` file - which the runner loads before parsing config - is honoured.

## Runtime tuning

Set these per pipeline under the `runtime` block of `pipeline.json`, or
process-wide with the environment variable. New runtime overrides use the
`ANALITIQ_` prefix.

| Setting | `runtime` key | Env var | Default | Controls |
|---|---|---|---|---|
| Batch size | `batching.batch_size` | `ANALITIQ_BATCH_SIZE` | `1000` | Records read from the source and shipped per batch. |
| Max concurrent batches | `batching.max_concurrent_batches` | `ANALITIQ_MAX_CONCURRENT_BATCHES` | `3` | Batches in flight per stream. |
| Buffer size | `buffer_size` | `ANALITIQ_BUFFER_SIZE` | `5000` | Queue depth between the extract, transform, and load stages. |
| Error strategy | `error_handling.strategy` | `ANALITIQ_ERROR_STRATEGY` | `fail` | What happens to a batch that exhausts its retries: `fail` (raise and stop the stream), `dlq` (write to the dead-letter queue and continue), or `skip` (drop the batch and continue). |
| Max retries | `error_handling.max_retries` | `ANALITIQ_MAX_RETRIES` | `3` | Retry attempts on a retryable failure before the error strategy applies. |
| Retry delay | `error_handling.retry_delay_seconds` | `ANALITIQ_RETRY_DELAY_SECONDS` | `5` | Base seconds for the exponential backoff between retries. |

## Infrastructure

Process-level settings with no per-pipeline layer. Their environment-variable
names are an established deployment contract and are unchanged.

### gRPC transport

| Setting | Env var | Default | Controls |
|---|---|---|---|
| Destination host (engine mode) | `DESTINATION_GRPC_HOST` | `localhost` | Host of the destination gRPC server. A set-but-blank value falls back to localhost. |
| Destination port (engine mode) | `DESTINATION_GRPC_PORT` | `50051` | Port of the destination gRPC server. |
| Server port (destination mode) | `GRPC_PORT` | `50051` | Port the destination gRPC server listens on. |
| Ack timeout | `GRPC_TIMEOUT_SECONDS` | `30` | Engine's ack budget, stamped onto the schema handshake so the destination derives its statement timeout from it. A non-positive value falls back to the default. |
| Max retries | `MAX_RETRIES` | `3` | Retries for gRPC connect/stream operations. (Separate from the runtime `ANALITIQ_MAX_RETRIES`.) |
| Max message size | _(not env-tunable)_ | `16 MiB` | Ceiling for every channel that carries Arrow batches; client and server must match. |

### Worker supervision

| Setting | Env var | Default | Controls |
|---|---|---|---|
| Ready timeout | `WORKER_READY_TIMEOUT_SECONDS` | `300` | Seconds to wait for a worker socket to accept. |
| Kill grace | `WORKER_KILL_GRACE_SECONDS` | `10` | Grace between SIGTERM and SIGKILL on close. |
| Address-space cap | `WORKER_RLIMIT_AS_MB` | _(unset)_ | Optional worker memory cap in MB. |

### Schema contracts and process

| Setting | Env var | Default | Controls |
|---|---|---|---|
| Schema base URL | `ANALITIQ_SCHEMA_BASE_URL` | `https://schemas.analitiq.ai` | Base URL for the published contract schemas. |
| Schema fetch timeout | _(not env-tunable)_ | `15` s | Timeout for fetching a JSON Schema. |
| Log level | `LOG_LEVEL` | `INFO` | Logging level for the process. |
| Run mode | `RUN_MODE` | `source` | Process role: `source` (engine) or `destination` (gRPC server). |
| Destination index | `DESTINATION_INDEX` | `0` | Which destination from the pipeline config to serve. |
| Pipeline id | `PIPELINE_ID` | _(required)_ | Pipeline id from `pipelines/manifest.json` to execute. |

The runtime-archive download timeout (`60` s) lives in the standalone
`src/runtime_archive.py` CLI rather than in `settings`, because that script runs
without the engine package on the path.

## Connector and formatter defaults

These are intentionally not centralised - a connector owns its configuration so
the engine stays connector-agnostic. Examples: the source API connector's
incremental `safety_window_seconds` (`120`), API request timeout/retries, and
the destination formatter defaults (CSV delimiter, Parquet compression and row
group size, JSONL options). Find them in the connector package and in
`src/destination/formatters/`.
