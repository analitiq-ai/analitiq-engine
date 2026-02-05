# Analitiq Stream

Analitiq Stream is a fault-tolerant, async-first data streaming framework for Python 3.11+ that moves data between sources and destinations with strong validation, stateful recovery, and gRPC-based decoupling.

## Highlights
- Async/await streaming with configurable batching and backpressure
- Fault tolerance with retries, circuit breaker, dead letter queue, and state checkpoints
- Pydantic v2 validation for all configs and runtime models
- Consolidated configuration model with secrets expansion
- gRPC streaming to decouple engine and destination services
- Deterministic, idempotent batch writes

## Requirements
- Python 3.11+
- Poetry

## Installation

```bash
poetry install
poetry install -E "kafka cloud analytics"
poetry shell
```

## Quick Start (Local)

1. Ensure configuration is present in `pipelines/` and `.secrets/` (see Configuration below).
2. Run the destination server:

```bash
RUN_MODE=destination \
ENV=local \
PIPELINE_ID=<pipeline-id> \
CLIENT_ID=<client-id> \
python -m src.main
```

3. Run the engine:

```bash
RUN_MODE=engine \
ENV=local \
PIPELINE_ID=<pipeline-id> \
CLIENT_ID=<client-id> \
DESTINATION_GRPC_HOST=localhost \
python -m src.main
```

For Docker-based local/dev workflows, see `docker/README.md`.

## Configuration

Configuration paths and runtime directories are defined in `analitiq.yaml`:

```yaml
paths:
  pipelines: "./pipelines"
  secrets: "./.secrets"
  state: "./state"
  logs: "./logs"
  deadletter: "./deadletter"
```

### Consolidated Pipeline File

Each pipeline is a single consolidated JSON file at `pipelines/{pipeline_id}.json` with:

- `pipeline`: pipeline metadata and connection aliases
- `streams`: array of stream configs
- `connections`: connection definitions (without secrets)
- `connectors`: connector metadata (driver, connector_type)
- `endpoints`: endpoint schemas

Minimal example:

```json
{
  "pipeline": {
    "pipeline_id": "<pipeline-id>",
    "client_id": "<client-id>",
    "name": "Example",
    "is_active": true,
    "connections": {
      "source": {"conn_1": "<connection-id-1>"},
      "destinations": [{"conn_2": "<connection-id-2>"}]
    },
    "streams": ["<stream-id>"]
  },
  "streams": [
    {
      "stream_id": "<stream-id>",
      "pipeline_id": "<pipeline-id>",
      "is_enabled": true,
      "source": {
        "connection_ref": "conn_1",
        "endpoint_id": "<endpoint-id>"
      },
      "destinations": [
        {
          "connection_ref": "conn_2",
          "endpoint_id": "<endpoint-id>",
          "write": {"mode": "upsert"}
        }
      ],
      "mapping": {"assignments": []}
    }
  ],
  "connections": [
    {
      "connection_id": "<connection-id-1>",
      "connector_id": "<connector-id>",
      "host": "https://api.example.com",
      "headers": {"Authorization": "Bearer ${API_TOKEN}"}
    }
  ],
  "connectors": [
    {
      "connector_id": "<connector-id>",
      "connector_type": "api",
      "slug": "example"
    }
  ],
  "endpoints": [
    {
      "endpoint_id": "<endpoint-id>",
      "connector_id": "<connector-id>",
      "endpoint": "/v1/items",
      "method": "GET"
    }
  ]
}
```

### Secrets

Secrets live in `.secrets/{connection_id}.json`. Placeholders in connection configs (`${VAR_NAME}`) are expanded from these files first, then environment variables.

Example:

```json
{
  "API_TOKEN": "your-token"
}
```

### Cloud Mode

In `dev`/`prod`, `docker/config_fetcher.py` populates the consolidated pipeline file and secrets before execution. See `docker/README.md`.

## Environment Variables

Common:
- `ENV`: `local`, `dev`, `prod`
- `PIPELINE_ID`: pipeline ID (optionally with version suffix)
- `CLIENT_ID`: client UUID
- `LOG_LEVEL`: logging level

Engine mode:
- `DESTINATION_GRPC_HOST`: host of the destination service
- `DESTINATION_GRPC_PORT`: default `50051`

Destination mode:
- `GRPC_PORT`: default `50051`
- `DESTINATION_INDEX`: destination selection index, default `0`

## Architecture

- Engine and destination run in the same image, toggled by `RUN_MODE`
- Engine performs extract -> transform -> stream
- Destination receives batches over gRPC and writes idempotently

### Run ID Flow

The `run_id` is a unique identifier for each pipeline execution, used for idempotency, metrics, and state management. It is initialized once at startup via `src/shared/run_id.py` and propagated to all components.

```
┌─────────────────────────────────────────────────────────────────┐
│                     ENTRY POINTS                               │
├──────────────────────────────────────────────────────────────────┤
│ cloud_entrypoint.py / main.py                                  │
│ initialize_run_id()                                            │
│ Priority: RUN_ID env > AWS_BATCH_JOB_ID > generate new         │
└──────────────────────────────────────────────────────────────────┘
                      │
                      ▼
            ┌─────────────────────┐
            │   StateManager      │
            │  start_run()        │
            │  (engine.py:155)    │
            └──────────┬──────────┘
                       │
         ┌─────────────┼─────────────────────────────────┐
         │             │                                 │
         ▼             ▼                                 ▼
    ┌─────────┐  ┌──────────────────┐  ┌─────────────────────────┐
    │  STATE  │  │  gRPC CLIENT     │  │ METRICS EMISSION        │
    │  FILES  │  │  (engine sends)  │  │ (log-based)             │
    └────┬────┘  └──────────┬───────┘  └──────────┬──────────────┘
         │                  │                      │
         │                  ▼                      ▼
         │           ┌──────────────────┐   ANALITIQ_METRICS::{}
         │           │ gRPC SERVER      │   
         │           │ (destination)    │
         │           └──────────┬───────┘
         │                      │
         │                      ▼
         │            ┌─────────────────────────┐
         │            │  DESTINATION HANDLER    │
         │            │  (database/file/api)    │
         │            └──────────┬──────────────┘
         │                       │
         ├───────────┬───────────┴─────────────┐
         │           │                         │
         ▼           ▼                         ▼
    state.json  DB BATCH COMMITS      FILE MANIFEST
                _batch_commits_table  _manifest.json
                (run_id, stream_id,   (run_id, stream_id,
                 batch_seq)            batch_seq)
```

Details:
- `docs/GRPC_STREAMING_ARCHITECTURE.md`
- `docs/ENGINE_ARCHITECTURE.md`

## Development Commands

```bash
# Setup
poetry install
poetry install -E "kafka cloud analytics"
poetry shell

# Testing
poetry run pytest
poetry run pytest --cov=src --cov-report=html
poetry run pytest -m "unit"

# Code quality
poetry run black src/ && poetry run isort src/
poetry run mypy src/
poetry run flake8 src/
poetry run pre-commit run --all-files
```

## Project Structure

```
src/
├── shared/                      # Shared utilities
├── source/                      # Source connectors (read)
├── destination/                 # Destination handlers (write)
├── engine/                      # Core pipeline engine
├── state/                       # Fault tolerance and state
├── grpc/                        # gRPC client and generated code
├── models/                      # Pydantic models
├── config/                      # Config loading and validation
├── secrets/                     # Secret resolvers
├── schema/                      # Schema drift detection
├── mapping/                     # Field mapping processor
├── transformations/             # Transformation registry
├── main.py                      # Dual-mode entrypoint
└── runner.py                    # PipelineRunner
```

## Documentation

- `docs/DESTINATION_CONFIG.md`
- `docs/GRPC_STREAMING_ARCHITECTURE.md`
- `docs/ENGINE_ARCHITECTURE.md`
- `docs/SOURCE_CONFIG.md`
- `docs/MAPPING_AND_TRANSFORMATIONS.md`
- `docs/PIPELINE_METRICS_ATHENA.md`
- `docs/PIPELINE.yaml` and `docs/STREAM.yaml`
- `docker/README.md`
