# PyArrow as the Engine's Data Format: Evaluation and Destination Strategy

**Scope:** this doc owns the Arrow type system, the schema contract, the
ADBC-vs-SQLAlchemy transport strategy, and the API/orjson write path.
For destination handler configuration see
[`destination-config.md`](destination-config.md); for how the CDK package
is bounded and wired see
[`connector-module-architecture.md`](connector-module-architecture.md).

## Question

Is PyArrow the right in-memory data format for an ETL engine that moves
millions of records between APIs and databases in arbitrary combinations?
The current write path performs several conversions per batch and we want
to know whether that overhead is justified or whether a different format
would scale better.

## Current Data Flow

### DB destination (today)

`GenericSQLConnector` (`cdk/cdk/sql/generic.py`), SQLAlchemy write path

```
record_batch
  -> SchemaContract.cast_arrow_batch    # columnar, vectorized cast
  -> to_pylist                          # columnar -> row materialization
  -> decode_json_columns                # JSON strings -> dicts
  -> SQLAlchemy.execute(table.insert(), records)
```

### Source side

- `src/source/connectors/api.py:221` -- HTTP returns `List[Dict]`, then
  `schema_contract.from_pylist(deduped)` rebuilds a `pa.RecordBatch`.
- `GenericSQLConnector.read_batches` (`cdk/cdk/sql/generic.py`) --
  SQLAlchemy returns rows, rows are turned into dicts, then
  `from_pylist(rows)` rebuilds a batch.

### gRPC transport

- Engine encodes with `pa.ipc.new_stream()` (`src/grpc/client.py:551`).
- Destination decodes with `pa.ipc.open_stream()`
  (`src/destination/server.py:349`).

## Where Arrow Earns Its Keep

1. **gRPC IPC payload.** Arrow IPC is compact, schema-aware, and ships
   the type information inline. Anything else (Protobuf-of-rows, JSON)
   would be strictly worse on the wire.
2. **Schema validation cast.** `SchemaContract.cast_arrow_batch`
   (`cdk/cdk/schema_contract.py`) aligns incoming columns with the destination
   schema. Each column conversion is first classified by the conversion matrix
   (`cdk/cdk/type_map/conversions.py`) — the same policy the engine's transform
   retype consults, so the two boundaries cannot disagree — then a permitted
   conversion runs through vectorized `pyarrow.compute.cast(safe=True)`. A
   `forbidden` or `explicit` conversion (e.g. an `Int64 → Utf8` whose mapping
   omitted `to_string`) fails loud here instead of silently stringifying.
   Faster and more correct than a per-row Python coercion loop.
3. **Type vocabulary.** `TypeMapper` (`cdk/cdk/type_map/mapper.py`) ->
   `parse_arrow_type` (`cdk/cdk/type_map/arrow.py`) ->
   `arrow_to_sqlalchemy` (`cdk/cdk/sql_types.py`) is a clean single
   source of truth for types across all connectors. Arrow happens to
   have a good vocabulary to standardize on.

## Where Arrow Is Ceremony

The data path is fundamentally row-oriented at both ends:

- Sources produce rows (HTTP JSON, ORM rows). We pay to columnarize.
- Destinations consume rows (SQLAlchemy binds dict params, aiohttp wants
  JSON). We pay to re-rowify with `to_pylist()`.
- The default `batch_size: 100` is below the threshold where Arrow's
  vectorization wins offset its setup cost. Arrow shines at 5K-50K rows
  per batch.

Conclusion: Arrow is columnar **only in flight and during schema cast**.
It is not pulling its weight as a "universal in-memory format" because
no downstream consumer in this engine is columnar.

## Don't Add More Arrow at the Destination

Specifically: do **not** add an Arrow-level cast to JSON-shape pass
before `to_pylist()` on the API destination. That doubles down on a
columnar format right before we abandon columnar. Instead, materialize
once and use `orjson.dumps(default=...)` with a small encoder hook that
handles `datetime`, `Decimal`, `UUID`, etc. directly from Python.

Recommended API destination flow (implemented in
`src/destination/connectors/api.py`, encoder hook `_orjson_default`):

```
record_batch
  -> cast_arrow_batch           # schema validation only
  -> to_pylist                  # materialize ONCE
  -> orjson.dumps(default=...)  # handles Decimal/datetime/UUID inline
  -> aiohttp
```

## Recommended Destination Strategy: Two Code Paths, Not Five

You do **not** need a custom destination per database. ADBC (Arrow
Database Connectivity) is a standard with a uniform Python API across
drivers, so one method covers many engines.

The engine has one database handler — `GenericSQLConnector`
(`cdk/cdk/sql/generic.py`), which implements both write paths — and the
connector definition picks the path:

```
transport_type: "sqlalchemy"  → SqlAlchemyBackend (stage-then-merge:
                                  every batch lands in a per-batch
                                  stage table, one mode statement
                                  applies it — see
                                  sql-write-path-v2.md). Breadth
                                  layer for every dialect SA covers.
                                  Async engine for dialects with an
                                  async driver (asyncpg, aiomysql);
                                  plain sync engine for sync-only
                                  drivers (Redshift
                                  redshift_connector), run via
                                  asyncio.to_thread.

transport_type: "adbc"        → ADBC machinery (DDL via
                                  cursor.execute, ingest via
                                  cursor.adbc_ingest, upsert via
                                  staged table + MERGE). Depth layer
                                  for dialects with no async SA
                                  driver (Snowflake today). Moves
                                  behind the same TransportBackend
                                  interface in #389.
```

`GenericSQLConnector` is the facade and single semantic owner (write
modes, truncate gating, identity and duplicate rules, refusals, retry
verdicts, timeouts). On the SQLAlchemy transport it builds a
`StageWritePlan` per batch and delegates execution to
`SqlAlchemyBackend` (`cdk/cdk/sql/backend.py`) — the two SQLAlchemy
flavours run one shared sync-`Connection` cycle body (the async engine
enters it via `run_sync`), so their semantics cannot fork. The ADBC
path keeps its own machinery until #389 puts it behind the same
interface; both share the cast/schema-contract logic.

### ADBC coverage (production-ready, 2026)

- PostgreSQL (libpq COPY underneath)
- Snowflake (native Arrow ingestion)
- BigQuery (Storage Write API)
- SQLite, DuckDB (in-process)
- MySQL (newer, via Flight SQL -- verify maturity per release)

### SQLAlchemy stays for

- Oracle, MSSQL, MariaDB, ClickHouse, and any niche dialect ADBC
  doesn't reach yet.
- Low-volume pipelines where the SA path is already fast enough.

### The UPSERT wrinkle

`adbc_ingest` is INSERT/APPEND only. The ADBC path implements upsert
by ingesting into a stage table and emitting `MERGE INTO target USING
stage ON ... WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT` against the
conflict keys. The SQLAlchemy backend runs the same stage shape: the
batch lands in the stage and the dialect's `merge_statement_sql`
renders the declared merge form from stage to target (`INSERT ... ON
CONFLICT DO UPDATE` on Postgres, `INSERT ... ON DUPLICATE KEY UPDATE`
on MySQL).

## What Not to Do

- Don't rip out Arrow. The gRPC payload and schema cast are real wins.
- Don't replace SQLAlchemy. It's the breadth layer that lets any
  reasonable database be a destination on day one.
- Don't add extra Arrow-side conversions to "stay in Arrow longer."
  The columnar advantage ends where the destination consumes rows.
- Don't write five custom destinations. ADBC's uniform API means one
  method covers the high-volume warehouses.

## Mental Model

- SQLAlchemy is the **breadth layer**: any database, any mode, modest
  throughput.
- ADBC is the **depth layer**: the handful of warehouses where volume
  matters, with near-bulk-load throughput.
- They coexist inside one handler (`GenericSQLConnector`). Dispatch,
  don't fork the destination tree.
