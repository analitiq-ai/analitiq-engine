# Test Execution Guide

This document explains how to run different categories of tests in the Analitiq Stream project.

## Standard Test Execution

Run the main test suite:

```bash
poetry run pytest
```

## Test Categories

### Unit Tests
```bash
poetry run pytest tests/unit/
```

### Integration Tests
```bash
poetry run pytest tests/integration/
```

### Conformance Kit Tests
The suite that checks the connector conformance kit itself. Runs as part of the
main suite; its live tier skips unless a PostgreSQL service container is
reachable through the `CONFORMANCE_PG_*` environment variables and the
`asyncpg` driver is installed.

```bash
poetry run pytest tests/conformance_kit/
```

## Fault Tolerance Tests

Fault tolerance tests (rate limiter, dead letter queue) live in
`tests/unit/analitiq_stream/fault_tolerance/` and run as part of the main
suite. To run them alone:

```bash
poetry run pytest tests/unit/analitiq_stream/fault_tolerance/
```

## End-to-End Tests

These two directories hold no test framework code and are not collected by
`pytest`. The test is the engine itself, run from `docker/docker-compose.yml`
against hand-written pipeline config; each directory carries the containers or
seed SQL that the run needs.

### Local databases

`tests/e2e_databases/` starts local Postgres, MySQL and MariaDB containers and
seeds the source table, then moves it with the real engine image. Needs Docker
and a populated `connectors/` checkout. Full instructions, including the
incremental-resume scenario:
[tests/e2e_databases/README.md](e2e_databases/README.md).

```bash
cd tests/e2e_databases && docker compose up -d --wait --remove-orphans
```

### Cloud warehouses

`tests/e2e_cloud_seed/` holds the source-table seed SQL for Snowflake,
BigQuery, Redshift and the Postgres hub. Needs live warehouse credentials
filled into the matching connections. Run the seed file for whichever warehouse
you use as a source, then run the pipeline as described in
[tests/e2e_cloud_seed/README.md](e2e_cloud_seed/README.md).

## Notes

- The full suite is expected to pass with `poetry run pytest`; a handful of
  tests skip when their environment is absent (live databases, optional
  dialect packages, unpopulated `connectors/` checkouts).
- CI runs the same suite plus the pre-commit stack; see `CONTRIBUTING.md`.
