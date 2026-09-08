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
| Connector / formatter defaults | The connector package; the CDK's batch formatters under `cdk/cdk/formatters/` | Connector- and format-specific values (e.g. an API connector's incremental safety window, a formatter's compression). The engine stays connector-agnostic, so these are never centralised in engine settings. |

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

## What is deliberately not centralised

Connector- and formatter-owned defaults (e.g. an API connector's replication
safety window, a formatter's delimiter or row-group size) are not engine
settings: centralising them here would make the engine connector-aware,
which the engine's design refuses (see
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md)).
Find them in the connector package that owns them, or in
`cdk/cdk/formatters/`.
