# CLAUDE.md

## What This Repo Is

Analitiq Data Sync Engine runs pre-built data pipelines. It reads from a source system (API, database, SFTP), transforms the data, and writes to a destination system. Pipelines are built separately using the [Pipeline Builder plugin](https://github.com/analitiq-ai/ai-plugins-official) for Claude Code.

Connectors are pluggable, independently versioned packages. Each targets one system (a database such as `postgres`, or an API such as `xero`) and ships everything that system needs: its definition, its type map, and its own driver. Most connectors are pure declarative config authored against the published schema contract; a connector adds code only when the system is quirky (the thin -> thick gradient). Adding a connector never modifies the engine.

## Running a Pipeline

Pipelines run in Docker. The only required input is a pipeline ID from `pipelines/manifest.json`.

```shell
cd docker && \
  PIPELINE_ID=my-pipeline-id \
  docker compose run --rm source_engine
```

The engine and destination run from the same Docker image, toggled by `RUN_MODE` (`source` or `destination`). Both containers load config from the same `PIPELINE_ID`.

## How It Works

1. **Extract** — read from source in batches
2. **Transform** — apply field mappings and type conversions
3. **Load** — write to destination with fault tolerance
4. **Checkpoint** — save progress so interrupted runs resume automatically

### For databases that support ADBC (Postgres/BigQuery/Snowflake/DuckDB)

Arrow Database Connectivity (https://arrow.apache.org/adbc/) drivers exist for exactly these. The flow is:

record_batch
→ cast_arrow_batch
→ adbc_conn.adbc_ingest(table, record_batch, mode="append")

The driver hands the Arrow buffers directly to libpq's binary COPY protocol or BigQuery's storage API.

**First-class native drivers**
```text
  ┌────────────┬────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────┐
  │   Driver   │          Package           │                                        Notes                                         │
  ├────────────┼────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
  │ PostgreSQL │ adbc-driver-postgresql     │ Uses libpq COPY BINARY for bulk ingest. Production-ready.                            │                                                                                                                                                                                                                                                        
  ├────────────┼────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
  │ Snowflake  │ adbc-driver-snowflake      │ Native Arrow ingestion via internal Go-Snowflake driver.                             │
  ├────────────┼────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
  │ BigQuery   │ adbc-driver-bigquery       │ Storage Write API (Arrow-native).                                                    │
  ├────────────┼────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
  │ DuckDB     │ shipped with duckdb itself │ Zero-copy in-process.                                                                │
  ├────────────┼────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
  │ SQLite     │ adbc-driver-sqlite         │ Production-ready. Less interesting for "millions of records" but useful for testing. │
  └────────────┴────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────┘
```
These five are the ones where cur.adbc_ingest(...) actually skips a row-by-row insert path.

**Flight SQL driver (works for any DB exposing Flight SQL)**
```text
  ┌────────────────────┬───────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │       Driver       │        Package        │                                                                                    Covers                                                                                     │
  ├────────────────────┼───────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Flight SQL generic │ adbc-driver-flightsql │ Any server that implements the Arrow Flight SQL protocol — currently Dremio, Doris, InfluxDB 3.x, DataBricks (in some configs), and an increasing number of newer warehouses. │
  └────────────────────┴───────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
Caveat: this only helps if the target server actually exposes a Flight SQL endpoint. Most ordinary MySQL/Postgres deployments do not.

**Bridge drivers (broad coverage, NO columnar benefit)**

```text
  ┌─────────────┬──────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │   Driver    │     Package      │                                                                                                                              What it does                                                                                                                              │
  ├─────────────┼──────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ JDBC bridge │ adbc-driver-jdbc │ Wraps any JDBC driver. Gives you "ADBC API surface" over Oracle / MSSQL / MariaDB / MySQL / Redshift / etc. — but underneath it still binds row-by-row through JDBC. Don't pick this for performance. Use it only if you want one ADBC interface across mixed targets. │
  └─────────────┴──────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


### For databases without ADBC: native bulk-load protocols

For PG without ADBC, `COPY FROM stdin BINARY` via `psycopg`. For MySQL, LOAD DATA LOCAL INFILE. Each is ~10x faster than parameterized INSERT, even with batching.
SQLAlchemy as a generic write path should be the fallback, not the default.
```text
  ┌────────────────────┬─────────────────────────────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │         DB         │                              Driver to use                              │                                                                           Bulk path                                                                            │
  ├────────────────────┼─────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ MySQL / MariaDB    │ PyMySQL or mysqlclient via SQLAlchemy                                   │ LOAD DATA LOCAL INFILE via raw cursor — stream Arrow → CSV/TSV → MySQL reads it directly. ~10x faster than INSERT.                                             │
  ├────────────────────┼─────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Oracle             │ python-oracledb via SQLAlchemy                                          │ cursor.executemany(sql, rows) with arraysize tuned. Oracle's batched executemany is the standard fast path; SQL*Loader exists but isn't practical from Python. │
  ├────────────────────┼─────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ MSSQL / SQL Server │ pyodbc via SQLAlchemy                                                   │ Set fast_executemany=True on the cursor — uses TDS batched parameter stream. Big speedup, single-line config change.                                           │
  ├────────────────────┼─────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ ClickHouse         │ clickhouse-connect (skip SQLAlchemy)                                    │ client.insert_arrow(table_name, arrow_table) — first-class Arrow ingest, just not branded ADBC.                                                                │
  ├────────────────────┼─────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Redshift           │ redshift_connector via SQLAlchemy, OR Postgres ADBC driver (unofficial) │ See note below — the real fast path conflicts with your engine policy.                                                                                         │
  └────────────────────┴─────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### For API destinations: skip Arrow after the cast

Once the schema is validated, materialize once and stay row-oriented. Use orjson (C-based, handles datetime/Decimal/UUID natively via `default=`) instead of
stdlib json. This avoids wasting cycles converting decimal → string → dict → json string when it can go decimal → orjson in one step with a custom serializer.

record_batch
→ cast_arrow_batch          # schema validation only
→ to_pylist                 # materialize ONCE
→ orjson.dumps(default=...) # handles Decimal/datetime/UUID without pre-conversion
→ aiohttp

The whole point of orjson's default= hook is to avoid an explicit "Arrow-level cast to JSON shape" pass.

## Configuration Layout

Configuration is assembled from modular files. The plugin generates all of this automatically.

```
connectors/{connector_id}/       # One installable connector package per system (from the registry)
connections/{alias}/             # Connection configs and credentials
pipelines/manifest.json          # Central index of all pipelines
pipelines/{pipeline_id}/         # Pipeline config and stream definitions
```

`connector_id` is the connector's canonical identifier and repo name (`postgres`, `mysql`, `xero`, `pipedrive`).

Only pipelines with `status: "active"` in the manifest can be executed.

### Endpoint References

Streams reference endpoints using scoped paths:
- `"connector:{connector_id}/{name}"` — public endpoint from a connector
- `"connection:{alias}/{name}"` — private endpoint from a connection

### Secrets

Credentials use `${placeholder}` syntax in connection configs, resolved from `connections/{alias}/.secrets/credentials.json`. Placeholders are expanded at connection time. Missing placeholders raise `PlaceholderExpansionError`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_ID` | *(required)* | Pipeline ID from `manifest.json` |
| `RUN_MODE` | `source` | `source` or `destination` |
| `LOG_LEVEL` | `INFO` | Logging level |
| `DESTINATION_GRPC_HOST` | | Destination service host |
| `DESTINATION_GRPC_PORT` | `50051` | gRPC port |
| `DESTINATION_INDEX` | `0` | Which destination from pipeline config |

## Storage

All runtime data (state, logs, dead letters, metrics) uses local filesystem at project root: `state/`, `logs/`, `deadletter/`, `metrics/`.

## Connector Kinds

`api`, `database`, `file`, `stdout`

A connector resolves in two steps: its `kind` (above) selects the family, and its `connector_id` selects the concrete connector. A `connector_id` with no dedicated class falls back to the generic class for its kind (the thin path); per-system quirks live in that connector's own class, never in the generic base.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for coding guidelines, issue workflow, and PR review process.