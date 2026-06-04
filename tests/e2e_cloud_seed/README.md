# Cloud database end-to-end tests (Snowflake / BigQuery / Redshift)

Hand-written test configs that move the canonical 5-row table between each
cloud warehouse and the Postgres hub, in both directions, using the real
engine Docker image.

## What's wired

Connectors (pulled from the public DIP registry into `connectors/`):
- `snowflake` — ADBC, driver `snowflake`
- `bigquery` — ADBC, driver `bigquery`
- `redshift` — SQLAlchemy, driver `redshift+redshift_connector`

Connections (`connections/<uuid>/`, placeholder credentials to fill in):
- `11111111-1111-4111-8111-111111111111` — Snowflake
- `22222222-2222-4222-8222-222222222222` — BigQuery
- `33333333-3333-4333-8333-333333333333` — Redshift
- `8687069d-d28d-447a-8c74-3e71661851d1` — Postgres hub (already existed)

Each connection has two private endpoints: `e2e_seed` (source table) and
`e2e_landing` (destination table the engine creates).

Pipelines (each direction, full-refresh upsert on `id`):
- `e2e-postgres-to-snowflake` / `e2e-snowflake-to-postgres`
- `e2e-postgres-to-bigquery`  / `e2e-bigquery-to-postgres`
- `e2e-postgres-to-redshift`  / `e2e-redshift-to-postgres`

## Fill in credentials

Replace every `REPLACE_WITH_*` placeholder in:
- `connections/<uuid>/connection.json` (host/account/project/dataset, etc.)
- `connections/<uuid>/.secrets/credentials.json` (passwords / service-account JSON)
- BigQuery only: also set the dataset in
  `connections/2222.../definition/endpoints/e2e_seed.json` and `e2e_landing.json`
  (the `database_object.schema`) and in `tests/e2e_cloud_seed/bigquery_seed.sql`.

## Seed the source tables

The engine creates the destination (`e2e_landing`) table itself. Only the
source table (`e2e_seed`) needs seeding. Run the matching SQL file against
each warehouse you intend to use as a source:

- `postgres_seed.sql`  → Postgres hub (needed for every `postgres-to-*` run)
- `snowflake_seed.sql`  → Snowflake (for `snowflake-to-postgres`)
- `bigquery_seed.sql`   → BigQuery  (for `bigquery-to-postgres`)
- `redshift_seed.sql`   → Redshift  (for `redshift-to-postgres`)

## Run a pipeline

```bash
cd docker && \
  PIPELINE_ID=e2e-postgres-to-snowflake \
  docker compose run --rm source_engine
```

Swap `PIPELINE_ID` for any of the six pipeline IDs above. Check the container
logs for errors; on success the 5 rows land in the destination's `e2e_landing`
table, which you can read back to confirm row-for-row equality with the seed.
