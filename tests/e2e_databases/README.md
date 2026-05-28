# Database matrix end-to-end tests

This framework spins up real source + destination databases, seeds them with a
canonical row set, generates connection / pipeline / stream JSON on the fly,
runs the engine in its production Docker image, and asserts that the destination
ends up with the expected data.

It is **modular**: nothing here runs constantly. Every test brings up only the
DB containers it needs, seeds them, runs one pipeline, asserts, then tears
everything down. There is no shared long-lived state.

## What it tests

For every supported database pair `(source, destination)` and replication mode
`{full_refresh, incremental}`:

1. Seed `e2e_seed_data` in the source.
2. Wire a pipeline that streams `e2e_seed_data` from source to destination.
3. Run the engine.
4. Read the destination back and assert row-for-row equality with the seed.

The matrix shape:

- **Local databases (Postgres, MySQL, DuckDB, SQLite, MongoDB, ClickHouse) — N×N.**
  Every local DB is tested as source against every other local DB as destination.
- **Cloud databases (Snowflake, BigQuery, Redshift) — hub-and-spoke around Postgres.**
  Each cloud DB is tested against Postgres only, in both directions.

Pairs whose connectors are not in the DIP registry skip automatically with a
clear pytest reason. Cloud pairs whose credentials are missing from `.env` also
skip automatically.

## What this framework will NOT do

- It does not modify or recreate connector definitions. Connectors are pulled
  from `connectors/` at the repo root — the DIP registry feeds those, the
  framework only reads them.
- It does not recreate public endpoint contracts. For database endpoints the
  framework writes connection-scoped (private) endpoint JSON describing the
  test seed table; that is exactly what every real user does per connection.
- It does not edit any other directory in the repo. All generated configs live
  under `workspace/` (gitignored).

## Layout

```
tests/e2e_databases/
├── README.md                 # this file
├── .env.example              # cloud creds template
├── pytest.ini                # pytest config + markers
├── conftest.py               # shared fixtures, skip logic
├── docker-compose.yml        # engine + destination services, mounts workspace/
├── databases/                # one module per database — knows how to spin up + seed + read back
│   ├── _base.py              # DatabaseSpec ABC
│   ├── _docker.py            # thin docker compose wrapper
│   ├── postgres.py
│   ├── mysql.py
│   └── ...
├── seeds/
│   └── canonical.sql         # canonical seed table DDL + rows (SQL flavor)
├── factory.py                # generates connection / pipeline / stream / endpoint JSON
├── orchestrator.py           # spin up, seed, run engine, assert, tear down
├── workspace/                # generated configs (gitignored)
└── _tests/
    ├── test_matrix_local.py  # N×N for local DBs
    └── test_matrix_cloud.py  # hub-and-spoke for cloud DBs
```

## The canonical seed table

Every database receives the same logical table:

| column     | type           | notes                                  |
|------------|----------------|----------------------------------------|
| id         | INT (PK)       | row identity                           |
| name       | VARCHAR(100)   |                                        |
| email      | VARCHAR(255)   |                                        |
| score      | INT            | nullable                               |
| created_at | TIMESTAMP      |                                        |
| updated_at | TIMESTAMP      | used as the incremental cursor field   |

Native types are translated per DB by each `DatabaseSpec`.

## Running it

### Prerequisites

- Docker + docker compose
- Poetry environment (`poetry install`)
- Optional: `tests/e2e_databases/.env` with cloud credentials. Copy from
  `.env.example` and fill in what you have. Anything missing -> the cloud
  pairs that need it skip.

### Run the whole matrix

```bash
poetry run pytest tests/e2e_databases/_tests/
```

### Run a single pair

```bash
poetry run pytest tests/e2e_databases/_tests/test_matrix_local.py \
    -k "postgres_to_mysql_incremental"
```

### Run one pipeline manually (no pytest)

```bash
poetry run python -m tests.e2e_databases.orchestrator \
    --source postgres --dest mysql --mode incremental
```

This is useful for debugging: it does not tear down on failure, so you can
inspect the engine logs and the destination DB state.

### Tear everything down

```bash
poetry run python -m tests.e2e_databases.orchestrator --down-all
```

## Adding a new database

1. Add a `DatabaseSpec` subclass in `databases/{slug}.py`.
2. Add source + destination services to `docker-compose.yml` if it runs locally.
3. If the DIP registry has a connector for it, the matrix tests pick it up
   automatically. If not, the spec sets `dip_connector_id = None` and the
   matrix tests for that DB skip with a clear reason.

The matrix discovers DBs by importing every module in `databases/`, so there
is nothing to register manually.

## Why a separate compose / workspace

The engine's production Docker compose at the repo root reads
`../connectors`, `../connections`, `../pipelines`. The test framework needs
its **own** connections + pipelines without touching the user's data, but
**reuses** the project's `connectors/` directory unchanged. The test compose
mounts:

- `../../connectors:/app/connectors`  (read-only, from the repo)
- `./workspace/connections:/app/connections` (written per test run)
- `./workspace/pipelines:/app/pipelines`   (written per test run)
