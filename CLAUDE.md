# CLAUDE.md

## What This Repo Is

Analitiq Data Sync Engine runs pre-built data pipelines. It reads from a source system (API, database, SFTP), transforms the data, and writes to a destination system. Pipelines are built separately using the [Pipeline Builder plugin](https://github.com/analitiq-ai/ai-plugins-official) for Claude Code.

Connectors are pluggable, independently versioned packages. Each targets one system (a database such as `postgres`, or an API such as `xero`) and ships everything that system needs: its definition, its type map, and its own driver. Most connectors are pure declarative config authored against the published schema contract; a connector adds code only when the system is quirky (the thin -> thick gradient). Adding a connector never modifies the engine.

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

Architecture details live in `docs/`: engine lifecycle (`engine-architecture.md`),
CDK and connector packages (`connector-module-architecture.md`), gRPC protocol
(`grpc-streaming-architecture.md`), Arrow and the SQLAlchemy-vs-ADBC transport
strategy (`pyarrow-and-destinations.md`), config shapes (`source-config.md`,
`destination-config.md`), mapping (`mapping-and-transformations.md`).

## Configuration Layout

Configuration is assembled from modular files. The plugin generates all of this automatically.

```
connectors/{connector_id}/       # One installable connector package per system (from the registry)
connections/{alias}/             # Connection configs and credentials
pipelines/manifest.json          # Central index of all pipelines
pipelines/{pipeline_id}/         # Pipeline config and stream definitions
```

`connector_id` is the connector's canonical identifier and repo name (`postgres`, `mysql`, `xero`, `pipedrive`).

Only pipelines with `status: "active"` in the manifest can be executed.

### Endpoint References

Streams reference endpoints using scoped paths:
- `"connector:{connector_id}/{name}"` — public endpoint from a connector
- `"connection:{alias}/{name}"` — private endpoint from a connection

### Secrets

Credentials use `${placeholder}` syntax in connection configs, resolved from `connections/{alias}/.secrets/credentials.json`. Placeholders are expanded at connection time. Missing placeholders raise `PlaceholderExpansionError`.

## Environment Variables

See the table in [README.md](README.md#environment-variables).

## Storage

All runtime data (state, logs, dead letters, metrics) uses local filesystem at project root: `state/`, `logs/`, `deadletter/`, `metrics/`.

## Connector Kinds

`api`, `database`, `file`, `stdout`

A connector resolves in two steps: its `kind` (above) selects the family, and its `connector_id` selects the concrete connector. A `connector_id` with no dedicated class falls back to the generic class for its kind (the thin path); per-system quirks live in that connector's own class, never in the generic base.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for coding guidelines, issue workflow, and PR review process.