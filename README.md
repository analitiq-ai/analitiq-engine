# Analitiq Engine

The open-source engine that moves data between systems using AI (Claude).
- API -> Database
- Databse -> API
- API -> API
- Databse -> Database
- many more (sftp, s3, anything really)

## How Analitiq works

Analitiq is a set of open-source tools for connecting APIs, databases, and storage systems — no coding required.

| Repository | What it does |
|---|---|
| **[AI Plugins](https://github.com/analitiq-ai/ai-plugins-official)** | Claude Code plugins that build connectors and pipelines through conversation. |
| **[DIP Registry](https://github.com/analitiq-dip-registry)** | Open catalog of ready-made connector definitions for common systems. |
| **[Core Engine](https://github.com/analitiq-ai/analitiq-core)** *(this repo)* | Runs the pipelines — reads from sources, transforms data, writes to destinations. |

**Use the plugins** to create connectors and assemble pipelines. **Connectors** live in the registry. **This engine** executes the pipelines. Or skip the setup and use **[Analitiq Cloud](https://analitiq-app.com)** for a fully managed experience.

Learn more at [analitiq.ai](https://analitiq.ai).

---

## Getting Started

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Claude Code](https://claude.ai/code) with the **analitiq-pipeline-builder** plugin installed (see [AI Plugins](https://github.com/analitiq-ai/ai-plugins-official) for installation instructions)

### 1. Create a pipeline

Open Claude Code in this project directory and say:

> *"I need to move data from [source] to [destination]"*

The **analitiq-pipeline-builder** plugin will walk you through choosing connectors, entering credentials, and mapping fields. When it finishes, three directories will appear in the project root:

```
connectors/    # connector definitions (downloaded from the DIP registry)
connections/   # your connection configs and credentials
pipelines/     # pipeline and stream definitions
```

### 2. Build the Docker image

```bash
docker build -t analitiq-engine .
```

### 3. Run the pipeline

Find your pipeline ID in `pipelines/manifest.json`, then:

```bash
docker run --rm \
  -v ./connectors:/app/connectors \
  -v ./connections:/app/connections \
  -v ./pipelines:/app/pipelines \
  -e ENV=local \
  -e PIPELINE_ID=<your-pipeline-id> \
  analitiq-engine
```

The engine reads from the source, transforms the data, and writes to the destination. State is checkpointed so interrupted runs resume where they left off.

## Configuration

The **analitiq-pipeline-builder** plugin generates all configuration files automatically. This section describes the layout for reference.

### Directory structure

```
connectors/{slug}/definition/
├── connector.json              # Connector metadata and auth configuration
├── manifest.json               # Lists available endpoints
└── endpoints/{name}.json       # Endpoint schemas

connections/{alias}/
├── connection.json             # Host, connector_slug, parameters
├── .secrets/
│   └── credentials.json        # Secret key-value pairs
└── endpoints/{name}.json       # Private endpoint schemas (e.g. DB tables)

pipelines/
├── manifest.json               # Central index of all pipelines
└── {pipeline_id}/
    ├── pipeline.json           # Pipeline config (connections, runtime settings)
    └── streams/
        └── {stream_id}.json    # Individual stream config (source, destinations, mapping)
```

### Secrets

Connection configs use `${placeholder}` syntax for sensitive values. Actual secrets live in `connections/{alias}/.secrets/credentials.json` as flat key-value pairs:

```json
{
  "API_TOKEN": "your-token",
  "API_SECRET": "your-secret"
}
```

Placeholders are expanded at connection time (not at config load time). If any `${placeholder}` has no matching key in the secrets file, a `PlaceholderExpansionError` is raised and the connection is never established with raw placeholders.

## Architecture

- Engine and destination run in the same Docker image, toggled by `RUN_MODE` env var
- Engine performs extract -> transform -> load
- Destination receives batches over gRPC and writes idempotently
- State is checkpointed per stream so interrupted runs resume automatically

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ENV` | `local` | Environment: `local`, `dev`, `prod` |
| `PIPELINE_ID` | *(required)* | Pipeline ID from `manifest.json` |
| `RUN_MODE` | `engine` | `engine` or `destination` |
| `LOG_LEVEL` | `INFO` | Logging level |
| `DESTINATION_GRPC_HOST` | | Host of the destination service (engine mode) |
| `DESTINATION_GRPC_PORT` | `50051` | gRPC port (engine mode) |
| `GRPC_PORT` | `50051` | gRPC listen port (destination mode) |
| `DESTINATION_INDEX` | `0` | Which destination from the pipeline config (destination mode) |

## Development

### Setup

```bash
poetry install
poetry shell
```

### Testing

```bash
poetry run pytest                           # all tests
poetry run pytest -m "unit"                 # unit tests only
poetry run pytest --cov=src --cov-report=html
```

### Code quality

```bash
poetry run black src/ && poetry run isort src/
poetry run mypy src/
poetry run flake8 src/
```

## License

Apache License 2.0