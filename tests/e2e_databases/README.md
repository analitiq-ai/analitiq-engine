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
# 1. start the local databases (auto-seeds e2e_seed)
cd tests/e2e_databases && docker compose up -d && cd ../..

# 2. run a pipeline (engine in Docker, reaching the DBs via host.docker.internal).
#    Pick a slug from pipelines/manifest.json, e.g. e2e-local-mysql-to-postgres,
#    e2e-local-postgres-to-postgres, e2e-local-mariadb-to-postgres, ...
SLUG=e2e-local-postgres-to-mysql
PIPELINE_ID=$(python3 -c "import json;print(next(p['pipeline_id'] for p in json.load(open('pipelines/manifest.json'))['pipelines'] if p['path'].startswith('$SLUG/')))")
cd docker && PIPELINE_ID=$PIPELINE_ID docker compose run --rm source_engine; cd ..

# 3. read back e2e_landing in the destination DB to confirm the 5 rows
#    psql    postgres://e2e_user:e2e_password@127.0.0.1:5433/e2e_db -c 'select * from e2e_landing order by id;'
#    mysql   -h127.0.0.1 -P13306 -ue2e_user -pe2e_password e2e_db -e 'select * from e2e_landing order by id;'
#    mariadb -h127.0.0.1 -P13307 -ue2e_user -pe2e_password e2e_db -e 'select * from e2e_landing order by id;'

# 4. before running a DIFFERENT slug: recreate the long-running destination —
#    it keeps the PIPELINE_ID it was created with, so a reused container would
#    serve the previous pipeline's config.
cd docker && docker compose rm -sf destination; cd ..
# ... then repeat step 2 with the new slug.

# 5. tear down
cd tests/e2e_databases && docker compose down -v && cd ../..
cd docker && docker compose down && cd ..
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
   cd docker && PIPELINE_ID=$PIPELINE_ID docker compose run --rm source_engine; cd ..
   ```

   Slugs: `e2e-postgres-to-snowflake`, `e2e-snowflake-to-postgres`,
   `e2e-postgres-to-bigquery`, `e2e-bigquery-to-postgres`,
   `e2e-postgres-to-redshift`, `e2e-redshift-to-postgres`.
