# Analitiq Stream Docker

Docker support for running Analitiq Stream in two modes:

1. `source_engine` runs the pipeline (extract -> transform -> stream).
2. `destination` runs the gRPC destination server that receives batches.

Both services use the same image and switch behavior with `RUN_MODE`.

## Files

- `docker-compose.yml`: Local/dev orchestration for engine + destination.
- `cloud_entrypoint.py`: Cloud entrypoint that fetches config then runs `src.main`.
- `config_fetcher.py`: Fetches consolidated pipeline config + secrets via Lambda/S3.
- `grpc_health_check.py`: gRPC readiness check for destination container.
- `deploy-ecr.sh`: Build and push image to ECR.
- `.env.example`, `local.env`, `dev.env`: Environment templates.

## Quick Start (Local)

1. Create `.env`:

```bash
cp .env.example .env
```

2. Run destination server:

```bash
PIPELINE_ID=<pipeline-uuid> \
docker compose run --rm destination
```

3. Run source engine:

```bash
PIPELINE_ID=<pipeline-uuid> \
docker compose run --rm source_engine
```

Notes:
- `ENV=local` uses local config files under the repo root (for example `pipelines/` and `.secrets/`).
- Both services mount `../src`, `../pipelines`, and `../.secrets` into the container.
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

This invokes `docker/config_fetcher.py` via `cloud_entrypoint.py` and writes:

- Consolidated config to `{paths.pipelines}/{pipeline_id}.json`
- Secrets to `{paths.secrets}/{connection_id}.json`

Paths are loaded from `analitiq.yaml` at the project root.

## Required Environment Variables

Set these at runtime (not in `.env`):

- `PIPELINE_ID`: Pipeline UUID (optionally with version suffix).

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
./deploy-ecr.sh dev latest
```

The script builds the image with `Dockerfile.cloud`, tags it, and pushes to ECR.

## Troubleshooting

- Missing config files in `local` mode: ensure `pipelines/` and `.secrets/` exist in the repo root.
- AWS auth errors in `dev` mode: verify `AWS_PROFILE` and credentials.
- gRPC connection errors: confirm the destination container is healthy and `DESTINATION_GRPC_HOST=destination`.
