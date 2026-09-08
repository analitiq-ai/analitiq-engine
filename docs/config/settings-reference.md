# Settings: Layering and Resolution

This document specifies how the engine resolves a runtime setting to a value.
It does not enumerate individual settings, their env var names, or their
defaults — those live in code and are read from there, never copied here,
because a copy is a second source of truth and drifts. The canonical list is
[`src/config/settings.py`](../../src/config/settings.py); each setting's
env var name and built-in default are declared once, beside its accessor.

## Where a default is allowed to live

| Layer | Location | Scope |
|---|---|---|
| Engine + infrastructure defaults | `src/config/settings.py` | Every engine-owned setting, with its environment-variable override, declared once. |
| Per-pipeline runtime override | `pipelines/{pipeline_id}/pipeline.json` -> `runtime` block | Overrides the *runtime-tuning* subset only (below) for one pipeline. |
| Connector / formatter defaults | The connector package; the CDK's batch formatters under `cdk/cdk/formatters/` | Connector- and format-specific values (e.g. an API connector's request timeout/retry policy, a formatter's compression). The engine stays connector-agnostic, so these are never centralised in engine settings. |

## Resolution order

Settings split into two kinds, resolved differently:

- **Runtime-tuning** (batch size, buffer size, error-handling strategy and
  retry policy) is layered, most-specific wins:

  ```
  pipeline.json runtime block  >  environment variable  >  built-in default
  ```

  A key omitted (or set to `null`) in the pipeline's `runtime` block falls
  through to the environment variable, then to the built-in default. The
  overlay happens once, in `PipelineConfigPrep.create_config`
  (`src/engine/pipeline_config_prep.py`).

- **Infrastructure** settings (transport addresses, ports, timeouts, process
  role) have no per-pipeline layer: environment variable, then built-in
  default. Their environment-variable names are an established deployment
  contract and are not renamed casually.

Environment variables are read on use, not at import, so a value placed in a
`.env` file — which the runner loads before parsing config — is honoured.

### Runtime-tuning key paths

Which `runtime` block key overrides which environment variable is a
structural fact `settings.py` cannot state on its own — the JSON path and
the env var name are declared in two different places (`pipeline.json`'s
schema and `settings.py` respectively) and only their pairing lives here:

| `runtime` key path | Env var |
|---|---|
| `batching.batch_size` | `ANALITIQ_BATCH_SIZE` |
| `buffer_size` | `ANALITIQ_BUFFER_SIZE` |
| `error_handling.strategy` | `ANALITIQ_ERROR_STRATEGY` |
| `error_handling.max_retries` | `ANALITIQ_MAX_RETRIES` |
| `error_handling.retry_delay_seconds` | `ANALITIQ_RETRY_DELAY_SECONDS` |
| `logging.log_level` | `LOG_LEVEL` |

Defaults for each are in `src/config/settings.py`, not repeated here.

### Environment inputs outside `settings.py`

This is the complete list of Python-side environment reads that
**configure application behavior** and do not go through `settings.py`,
kept here — the one place, not README.md or any other doc — precisely
because every prior attempt to state this list in two places let one of
them go stale. Update this list, not a copy of it, when a new one is
added. Explicitly out of scope: generic subprocess-environment
forwarding when the worker is spawned (`PATH`, `HOME`, `LANG` in
`src/worker/spawn.py::_clean_env`) — inherited shell environment for the
interpreter to run at all, not application configuration, and not
enumerable the way a deliberate input is.

**Engine-owned defaults**, each declared outside `settings.py` for its
own reason:

- The runtime-archive download timeout lives in the standalone
  `src/runtime_archive.py` CLI, not in `settings.py`, because that script
  runs without the engine package on its path — it cannot import
  `src/config/settings.py`.
- The incremental replication safety window
  (`StateReplicationConfig.safety_window_seconds`, default `120`) lives in
  `src/models/state.py`, alongside the resolved-config model it defaults a
  field on, rather than in `settings.py`. It is still engine policy, not
  connector input — see
  [`source-config.md`](source-config.md#replication-semantics) — just
  declared beside its own model instead of in the settings catalogue.
- `METRICS_ENABLED` (default `false`) is read directly by
  `StreamProcessor._emit_batch_metrics` / `_emit_stream_metrics`
  (`src/engine/stream_processor.py`), not through `settings.py`.

**Deployment/platform correlation identifiers** — not settings with a
built-in default, but inputs the deployment supplies for run
identification and log correlation, absent locally by design:

- `RUN_ID` — the run's own identifier. `initialize_run_id`
  (`src/shared/run_id.py`) honours one already set by the deployment;
  otherwise it falls back to `AWS_BATCH_JOB_ID` if present, else
  generates one. Once initialized it is read back via `get_run_id`
  (same module) and `StateManager` (`src/state/state_manager.py`).
- `AWS_BATCH_JOB_ID` — the AWS Batch-injected job id, the fallback source
  for `RUN_ID` above when the deployment hasn't set one directly.
- `INVOCATION_ID` — included in emitted logs only when set (cloud);
  absent locally (`src/state/log_emitter.py`).
- `ORG_ID` — tenant routing for emitted logs, same file; absent locally
  defaults to `0`.

**Worker bootstrap inputs** — forwarded into the isolated connector
worker's environment by `src/worker/spawn.py::_clean_env`, unlike the
generic forwarding this section excludes, because they determine whether
an attach-time-installed connector is importable inside the worker at
all:

- `PYTHONPATH` — forwarded when set, so a connector package installed
  outside the default interpreter path is still importable in the worker.
- `PYTHONUSERBASE` — forwarded when set; `pip install --user` connector
  packages live under it.

Nothing above is a gap to fix; `settings.py` is the catalogue for
environment-overridable *process defaults*, not for every environment
input the engine reads.

## What is deliberately not centralised

Connector- and formatter-owned defaults (e.g. an API request timeout or
retry policy, a formatter's delimiter or row-group size) are not engine
settings: centralising them here would make the engine connector-aware,
which the engine's design refuses (see
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md)).
Find them in the connector package that owns them, or in
`cdk/cdk/formatters/`.
