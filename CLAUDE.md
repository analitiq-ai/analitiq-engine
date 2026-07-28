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

## Configuration Layout

Configuration is assembled from modular files under `connectors/`, `connections/`, and `pipelines/`. The plugin generates them automatically.

`connector_id` is the connector's canonical identifier and repo name (`postgres`, `mysql`, `xero`, `pipedrive`).

Only pipelines with `status: "active"` in the manifest can be executed.

### Endpoint References

Streams reference endpoints using scoped paths:
- `"connector:{connector_id}/{name}"` — public endpoint from a connector
- `"connection:{alias}/{name}"` — private endpoint from a connection

### Secrets

A connection's `secret_refs.<name>` value carries a scheme that names where the
secret lives: `env:VAR`, `file:./path`, `sidecar:<name>` (an entry in
`connections/{alias}/.secrets/credentials.json`), or `s3://bucket/key` (the
`[s3]` extra). `env:`/`file:`/`sidecar:` are built-in and cloud-free. Refs are
resolved at connection time; an unresolvable ref fails loud, never falling back
to an empty secret.

## Environment Variables

See the table in [README.md](README.md#environment-variables).

## Storage

All runtime data (state, logs, dead letters, metrics) uses local filesystem at project root: `state/`, `logs/`, `deadletter/`, `metrics/`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for coding guidelines, issue workflow, and PR review process.
