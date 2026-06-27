# End-to-end database tests

The test is the engine itself: it runs in Docker, connects (through a
connector) to a source DB and a destination DB, and moves the canonical 5-row
`e2e_seed` table between them. A run passes if those 5 rows land in the
destination's `e2e_landing` table.

There is **no test framework code here** — only what spins up and seeds the
local databases. Everything else is plain config:

- Connectors: `connectors/` (pulled from the DIP registry)
- Connections + private endpoints: `connections/<id>/`
- Pipelines + streams: `pipelines/<id>/` (+ `pipelines/manifest.json`)

## This directory

```
tests/e2e_databases/
  docker-compose.yml     # local Postgres + MySQL + MariaDB containers
  seed/postgres_init.sql # auto-creates + seeds e2e_seed on container start
  seed/mysql_init.sql    # seeds MySQL and MariaDB (mounted into /docker-entrypoint-initdb.d)
  README.md
```

The engine is NOT defined here — it runs from the project's
`docker/docker-compose.yml`, which mounts `connectors/`, `connections/`, and
`pipelines/`.

## Local DBs (Postgres / MySQL / MariaDB)

Source tables are created and seeded automatically when the containers start;
the engine creates each destination (`e2e_landing`) table itself.

`PIPELINE_ID` is the manifest `pipeline_id` (a UUID), **not** the directory
slug — the engine resolves it against `pipelines/manifest.json`. The snippet
below resolves a slug to its UUID so the commands stay copy-pasteable.

```bash
# 1. start the local databases (auto-seeds e2e_seed). --wait blocks until the
#    healthchecks pass (init scripts done); --remove-orphans clears containers
#    left by older service names under the same analitiq-e2e project.
cd tests/e2e_databases && docker compose up -d --wait --remove-orphans && cd ../..

# 2. run a pipeline (engine in Docker, reaching the DBs via host.docker.internal).
#    Pick a slug from pipelines/manifest.json, e.g. e2e-local-mysql-to-postgres,
#    e2e-local-postgres-to-postgres, e2e-local-postgres-to-mariadb, ... — every
#    Postgres/MySQL/MariaDB pairing works in both directions.
#    The subshell keeps the engine's exit status as the command's status.
SLUG=e2e-local-postgres-to-mysql
PIPELINE_ID=$(python3 -c "import json;print(next(p['pipeline_id'] for p in json.load(open('pipelines/manifest.json'))['pipelines'] if p['path'].startswith('$SLUG/')))")
(cd docker && PIPELINE_ID=$PIPELINE_ID docker compose run --rm source_engine)

# 3. read back e2e_landing in the destination DB to confirm the 5 rows
#    psql    postgres://e2e_user:e2e_password@127.0.0.1:5433/e2e_db -c 'select * from e2e_landing order by id;'
#    mysql   -h127.0.0.1 -P13306 -ue2e_user -pe2e_password e2e_db -e 'select * from e2e_landing order by id;'
#    mariadb -h127.0.0.1 -P13307 -ue2e_user -pe2e_password e2e_db -e 'select * from e2e_landing order by id;'

# 4. before running a DIFFERENT slug: recreate the long-running destination —
#    it keeps the PIPELINE_ID it was created with, so a reused container would
#    serve the previous pipeline's config.
(cd docker && docker compose rm -sf destination)
# ... then repeat step 2 with the new slug.

# 5. tear down
cd tests/e2e_databases && docker compose down -v --remove-orphans && cd ../..
(cd docker && docker compose down)
```

## Cloud DBs (Snowflake / BigQuery / Redshift <-> Postgres)

Cloud warehouses are remote — no containers. Connections live at:

- Snowflake: `connections/11111111-1111-4111-8111-111111111111/`
- BigQuery:  `connections/22222222-2222-4222-8222-222222222222/`
- Redshift:  `connections/33333333-3333-4333-8333-333333333333/`
- Postgres hub (remote): `connections/8687069d-d28d-447a-8c74-3e71661851d1/`

1. Fill every `REPLACE_WITH_*` placeholder in those connections'
   `connection.json` and `.secrets/credentials.json` (and, for BigQuery, the
   dataset in its two endpoint files and in the seed SQL).
2. Seed the source table in whichever warehouse you use as a source, using the
   matching file in `tests/e2e_cloud_seed/` (the engine creates the
   destination table).
3. Run a pipeline (engine from `docker/docker-compose.yml`). As above,
   `PIPELINE_ID` is the manifest UUID resolved from the slug:

   ```bash
   SLUG=e2e-postgres-to-snowflake
   PIPELINE_ID=$(python3 -c "import json;print(next(p['pipeline_id'] for p in json.load(open('pipelines/manifest.json'))['pipelines'] if p['path'].startswith('$SLUG/')))")
   (cd docker && PIPELINE_ID=$PIPELINE_ID docker compose run --rm source_engine)
   ```

   Slugs: `e2e-postgres-to-snowflake`, `e2e-snowflake-to-postgres`,
   `e2e-postgres-to-bigquery`, `e2e-bigquery-to-postgres`,
   `e2e-postgres-to-redshift`, `e2e-redshift-to-postgres`.

## Resume accuracy (incremental)

The runs above are `full_refresh` — they re-read the whole table every time.
This scenario instead verifies that an **incremental** stream resumes correctly
on the *next* run after the previous run finished: it must read only rows past
the saved cursor, lose nothing, and duplicate nothing.

It needs an incremental pipeline — same `e2e_seed -> e2e_landing` shape but with
`source.replication = {"method": "incremental", "cursor_field": "id"}`,
`source.primary_keys = ["id"]`, and an `upsert` destination keyed on `id` (so
the inclusive `>=` re-read of the boundary row dedups instead of duplicating).
Author it with the Pipeline Builder plugin (pipeline/stream config is
user-owned and lives outside this repo); the rest is the seed data here.

`seed/postgres_delta.sql` / `seed/mysql_delta.sql` add the post-cursor rows
(ids 6,7). They are NOT auto-run — apply them by hand between the two runs.

```bash
# 0. start the DB and resolve the incremental pipeline's UUID (replace the slug
#    with whatever you named the incremental pipeline).
cd tests/e2e_databases && docker compose up -d --wait --remove-orphans e2e-postgres && cd ../..
SLUG=e2e-local-postgres-to-postgres-incremental
PIPELINE_ID=$(python3 -c "import json;print(next(p['pipeline_id'] for p in json.load(open('pipelines/manifest.json'))['pipelines'] if p['path'].startswith('$SLUG/')))")

# 1. run 1 over the 5 seeded rows. It lands ids 1-5 and writes the committed
#    cursor to the stream's own checkpoint file
#    state/$PIPELINE_ID/<stream_id>.json on each ACK.
(cd docker && PIPELINE_ID=$PIPELINE_ID docker compose run --rm source_engine)
cat state/$PIPELINE_ID/*.json   # the per-stream checkpoint -> {"cursor": 5}

# 2. add rows past the cursor (ids 6,7).
docker compose -f tests/e2e_databases/docker-compose.yml exec -T e2e-postgres \
  psql -U e2e_user -d e2e_db < tests/e2e_databases/seed/postgres_delta.sql

# 3. (optional, proves the cloud path) reduce local state to ONLY the per-stream
#    checkpoint files -- the fresh-container case where the deployment delivers
#    just those files in the bundle (drop the in-run batch-commit log).
rm -rf state/$PIPELINE_ID/state

# 4. run 2. It reads each stream's checkpoint ("cursor checkpoint ..." / resumes
#    from {"cursor": 5}) and reads only ids 5,6,7 (the inclusive >= boundary),
#    not the whole table.
(cd docker && PIPELINE_ID=$PIPELINE_ID docker compose run --rm source_engine)

# 5. verify: 7 rows, 7 distinct ids (the re-read id=5 deduped, nothing lost).
docker compose -f tests/e2e_databases/docker-compose.yml exec -T e2e-postgres \
  psql -U e2e_user -d e2e_db -c \
  'select count(*) total, count(distinct id) distinct_ids, max(id) from e2e_landing;'
#  total | distinct_ids | max
#  ------+--------------+-----
#      7 |            7 |   7
```

A pass is: run 2 reads only the rows after the cursor (3, not 7), the
destination ends with all 7 rows and no duplicate of the boundary row, and the
stream's `state/$PIPELINE_ID/<stream_id>.json` advances to `{"cursor": 7}`.
