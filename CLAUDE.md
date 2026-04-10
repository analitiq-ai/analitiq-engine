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

## How It Works

1. **Extract** — read from source in batches
2. **Transform** — apply field mappings and type conversions
3. **Load** — write to destination with fault tolerance
4. **Checkpoint** — save progress so interrupted runs resume automatically

## Configuration Layout

Configuration is assembled from modular files. The plugin generates all of this automatically.

```
connectors/{slug}/definition/    # Connector definitions (from the registry)
connections/{alias}/             # Connection configs and credentials
pipelines/manifest.json          # Central index of all pipelines
pipelines/{pipeline_id}/         # Pipeline config and stream definitions
```

Only pipelines with `status: "active"` in the manifest can be executed.

### Endpoint References

Streams reference endpoints using scoped paths:
- `"connector:{slug}/{name}"` — public endpoint from a connector
- `"connection:{alias}/{name}"` — private endpoint from a connection

### Secrets

Credentials use `${placeholder}` syntax in connection configs, resolved from `connections/{alias}/.secrets/credentials.json`. Placeholders are expanded at connection time. Missing placeholders raise `PlaceholderExpansionError`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_ID` | *(required)* | Pipeline ID from `manifest.json` |
| `RUN_MODE` | `source` | `source` or `destination` |
| `LOG_LEVEL` | `INFO` | Logging level |
| `DESTINATION_GRPC_HOST` | | Destination service host |
| `DESTINATION_GRPC_PORT` | `50051` | gRPC port |
| `DESTINATION_INDEX` | `0` | Which destination from pipeline config |

## Storage

All runtime data (state, logs, dead letters, metrics) uses local filesystem at project root: `state/`, `logs/`, `deadletter/`, `metrics/`.

## Connector Types

`api`, `database`, `file`, `stdout`

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for coding guidelines, issue workflow, and PR review process.