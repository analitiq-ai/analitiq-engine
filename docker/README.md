# Analitiq Stream Docker

Docker support for running Analitiq Stream in two modes:

1. `source_engine` runs the pipeline (extract -> transform -> stream).
2. `destination` runs the gRPC destination server that receives batches.

Both services use the same image and switch behavior with `RUN_MODE`.

## Files

- `docker-compose.yml`: Local orchestration for engine + destination.
- `cloud_entrypoint.py`: Cloud entrypoint that fetches config then runs `src.main`.
- `config_fetcher.py`: Fetches pipeline config + secrets via Lambda/S3 (cloud only).
- `grpc_health_check.py`: gRPC readiness check for destination container.
- `deploy.sh`: Build and push image to ECR.
- `.env.example`, `local.env`, `dev.env`: Environment templates.

## Quick Start (Local)

1. Create `.env`:

```bash
cp .env.example .env
# Set PIPELINE_ID to a pipeline_id from pipelines/manifest.json
```

2. Run both services:

```bash
PIPELINE_ID=mysql-to-postgresql docker compose run --rm source_engine
```

Or run individually:

```bash
# Start destination first
PIPELINE_ID=mysql-to-postgresql docker compose run --rm destination

# Then engine in another terminal
PIPELINE_ID=mysql-to-postgresql docker compose run --rm source_engine
```

Notes:
- `ENV=local` uses local config files from the repo root.
- Both services mount `connectors/`, `connections/`, and `pipelines/` into the container.
- Pipeline discovery uses `pipelines/manifest.json` (convention-based path resolution).
- Secrets are loaded from `connections/{alias}/.secrets/credentials.json`.
- The destination container must be healthy before the engine starts.

## Dev Mode (Cloud-like)

Use the `dev.env` template and let the container fetch config from AWS.

```bash
cp dev.env .env

PIPELINE_ID=<pipeline-uuid> \
AWS_PROFILE=434659057682_AdministratorAccess \
ENV=dev \
docker compose run --rm source_engine
```

This invokes `config_fetcher.py` via `cloud_entrypoint.py` to fetch and write configs.

## Required Environment Variables

Set these at runtime (not in `.env`):

- `PIPELINE_ID`: Must match a `pipeline_id` in `pipelines/manifest.json`.

Optional:

- `ENV`: `local`, `dev`, `prod` (default: `local`).
- `AWS_REGION`: Default `eu-central-1`.
- `AWS_PROFILE`: For local AWS access.
- `DESTINATION_INDEX`: Which destination to use (default: `0`).

## gRPC Health Check

The destination container exposes gRPC on port `50051` and uses a health check:

```bash
python docker/grpc_health_check.py
```

## Deploy to ECR

```bash
./deploy.sh dev latest
```

The script builds the image with `Dockerfile.cloud`, tags it, and pushes to ECR.

## Troubleshooting

- Missing config: ensure `pipelines/manifest.json` exists and the `PIPELINE_ID` matches an active entry.
- AWS auth errors in `dev` mode: verify `AWS_PROFILE` and credentials.
- gRPC connection errors: confirm the destination container is healthy and `DESTINATION_GRPC_HOST=destination`.
