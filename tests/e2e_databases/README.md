# Database matrix end-to-end tests

This framework spins up real source + destination databases, seeds them with a
canonical row set, generates connection / pipeline / stream JSON on the fly,
runs the engine in its production Docker image, and asserts that the destination
ends up with the expected data.

It is **modular**: nothing here runs constantly. Every test brings up only the
DB containers it needs, seeds them, runs one pipeline, asserts, then tears
everything down. There is no shared long-lived state.

## What it tests

Three test files, each driving the real engine Docker image against real
databases:

**`test_matrix_local.py` / `test_matrix_cloud.py` — full refresh.** For every
supported pair `(source, destination)` and each write mode `{insert, upsert}`:

1. Seed `e2e_seed_data` in the source.
2. Wire a pipeline that streams `e2e_seed_data` from source to destination.
3. Run the engine.
4. Read the destination back and assert row-for-row equality with the seed.

**`test_incremental.py` — true incremental.** A single sync on a fresh table is
indistinguishable from a full refresh, so this runs three syncs and proves the
engine resumes from its cursor bookmark: initial load, then a sentinel planted
below the bookmark plus a source mutation above it (a correct delta sync leaves
the sentinel untouched and moves only the changed/new rows), then a no-op third
sync that must leave the destination byte-for-byte identical. Incremental uses
`upsert` because the inclusive `>=` cursor re-reads the boundary row each sync.

**`test_negative.py` — deliberate failure.** One pair is wired to fail inside
the engine (a source endpoint advertising a column the table lacks). It asserts
the orchestrator surfaces the non-zero engine exit as `PipelineRunFailed` — so a
framework bug that swallowed engine errors could never leave the suite falsely
green.

The matrix shape:

- **Local databases — N×N.** Every local DB spec is tested as source against
  every local DB as destination.
- **Cloud databases — hub-and-spoke around Postgres.** Each cloud DB spec is
  tested against Postgres only, in both directions.

Specs are auto-discovered from `databases/*.py`, so the participating set is a
function of the registry, not a hand-maintained list. Pairs whose connectors
are not in the DIP registry skip automatically with a clear pytest reason, as do
cloud pairs whose credentials are missing from `.env`.

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
│   └── __init__.py           # canonical seed rows (single source of truth)
├── factory.py                # generates connection / pipeline / stream / endpoint JSON
├── orchestrator.py           # spin up, seed, run engine, assert, tear down
├── workspace/                # generated configs (gitignored)
└── _tests/
    ├── test_matrix_local.py  # N×N for local DBs, per write mode
    ├── test_matrix_cloud.py  # hub-and-spoke for cloud DBs, per write mode
    ├── test_incremental.py   # resume-from-bookmark / delta / no-op
    └── test_negative.py      # proves the harness can go red
```

## The canonical seed table

Every database receives the same logical table:

| column     | type           | notes                                          |
|------------|----------------|------------------------------------------------|
| id         | INT (PK)       | row identity                                   |
| name       | VARCHAR(100)   |                                                |
| email      | VARCHAR(255)   |                                                |
| score      | INT            | nullable (one row is NULL)                     |
| created_at | TIMESTAMP(µs)  | distinct, non-zero microseconds per row        |
| updated_at | TIMESTAMP(µs)  | used as the incremental cursor field           |

Native types are translated per DB by each `DatabaseSpec`. Timestamps carry
microsecond precision deliberately: it catches precision-truncating destination
DDL (a MySQL `DATETIME` without `(6)` silently drops the sub-second part), so
the exact-equality read-back goes red if the engine ever loses it.

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

Run the matrix **sequentially** — do not use `pytest-xdist` (`-n`). Every run
shares one `workspace/` and one set of fixed-name compose containers, so
parallel workers would overwrite each other's configs and collide on
containers.

### Run a single pair

```bash
# Full-refresh matrix ids are {source}_to_{dest}_{write_mode}:
poetry run pytest tests/e2e_databases/_tests/test_matrix_local.py \
    -k "postgres_to_mysql_upsert"

# Incremental ids are {source}_to_{dest}:
poetry run pytest tests/e2e_databases/_tests/test_incremental.py \
    -k "postgres_to_mysql"
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

## Why a separate workspace

The engine's production Docker compose at the repo root reads
`../connectors`, `../connections`, `../pipelines`. The test framework needs
its **own** connections + pipelines without touching the user's data, but
**reuses** the project's `connectors/` directory unchanged. The test compose
mounts:

- `../../connectors:/app/connectors`  (read-only, from the repo)
- `./workspace/connections:/app/connections` (written per test run)
- `./workspace/pipelines:/app/pipelines`   (written per test run)
- `./workspace/state:/app/state`   (cursor bookmarks; wiped between runs)
- `./workspace/deadletter:/app/deadletter` (failed records, if any)
