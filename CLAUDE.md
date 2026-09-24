# CLAUDE.md

## What This Repo Is

Analitiq Data Sync Engine runs pre-built data pipelines. It reads from a source system (API, database, SFTP), transforms the data, and writes to a destination system. Pipelines are built separately using the [Pipeline Builder plugin](https://github.com/analitiq-ai/ai-plugins-official) for Claude Code.

## Running a Pipeline

Pipelines run in Docker. The only required input is a pipeline ID from `pipelines/manifest.json`.

```shell
cd docker && \
  PIPELINE_ID=my-pipeline-id \
  docker compose run --rm source_engine
```

The engine and destination run from the same Docker image, toggled by `RUN_MODE` (`source` or `destination`). Both containers load config from the same `PIPELINE_ID`.

## Secrets

Secret refs are resolved at connection time; an unresolvable ref fails loud, never falling back to an empty secret.

## Environment Variables

See the table in [README.md](README.md#environment-variables).

## Storage

All runtime data (state, logs, dead letters, metrics) uses local filesystem at project root: `state/`, `logs/`, `deadletter/`, `metrics/`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for coding guidelines, issue workflow, and PR review process.
