# Arrow Type System and Transport Strategy

**Scope:** this doc owns the Arrow type vocabulary, the schema contract's
role at the write boundary, and the two SQL transports (SQLAlchemy and
ADBC). It does not re-derive the
write path or the conversion-matrix rule table — those are owned
elsewhere and linked below, since a second explanation of the same
mechanism is exactly what drifts out of sync with the first. For
destination handler configuration see
[`destination-config.md`](../config/destination-config.md); for the SQL
write primitive (stage-then-merge, the facade/backend split) see
[`sql-write-path.md`](sql-write-path.md); for the conversion matrix
(`identity` / `auto` / `explicit` / `forbidden`) see
[`mapping-and-transformations.md`](mapping-and-transformations.md#type-conversion);
for how the CDK package is bounded and wired see
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md).

## Where Arrow earns its keep

1. **gRPC IPC payload.** Batches cross the engine–destination wire as Arrow
   IPC ([ADR 0009](../adr/0009-engine-and-destination-are-separate-services-joined-by-grpc-carrying-arrow-ipc.md)); see
   [`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md).
2. **Schema validation cast.** `SchemaContract.cast_arrow_batch`
   (`cdk/cdk/schema_contract.py`) casts an arrived batch to the destination
   schema: a column whose arrived type — the Arrow type the driver actually
   produced, which nobody declares — differs from the destination field's is
   cast with `pyarrow.compute.cast(safe=True)`. The conversion
   matrix decides which conversions are permitted ([ADR 0017](../adr/0017-the-engine-owns-the-arrow-type-vocabulary-and-the-conversion-matrix.md)); see
   [`mapping-and-transformations.md`](mapping-and-transformations.md#type-conversion)
   for the mode table.
3. **Type vocabulary.** `arrow_type`, the canonical Arrow type string,
   parses to a `pa.DataType` (`parse_arrow_type`,
   `cdk/cdk/type_map/arrow.py`, which refuses the nested `Object` and `List`
   families), and the connector's own write type map
   renders it back to a `native_type` for DDL on the write side
   (`TypeMapper.to_native_type`, `cdk/cdk/type_map/mapper.py` ->
   `SqlDialect.render_column_type`, `cdk/cdk/sql/dialects.py`). The
   `arrow_family` heads come from `ARROW_FAMILIES`
   (`cdk/cdk/type_map/grammar.py`, [ADR 0017](../adr/0017-the-engine-owns-the-arrow-type-vocabulary-and-the-conversion-matrix.md)): each entry carries its parameter
   grammar and its `conversion_kind`, and may name the pyarrow factory that
   builds it and the `pyarrow.types` predicates that recognise it in a live
   batch.

## Where Arrow is ceremony

The data path is fundamentally row-oriented at most destinations: sources
produce rows (HTTP JSON, ORM rows) and the SQLAlchemy and API write paths
consume rows (SQLAlchemy binds dict params, aiohttp wants JSON). Arrow is
columnar only in flight and during schema cast on those paths — no
consumer downstream of the cast is columnar there — so the design
deliberately stops adding Arrow-space steps once a batch reaches one of
them. The one exception is a connector that declares the `adbc_ingest`
bulk mechanism: `AdbcBackend.land_batch` (`cdk/cdk/sql/adbc_backend.py`)
passes the `pa.RecordBatch` straight to `cursor.adbc_ingest`, no
`to_pylist()` — the batch stays columnar all the way to the driver, which
is exactly the point of a native Arrow ingestion API — see "Landing" (§2)
in [`sql-write-path.md`](sql-write-path.md).

### DB destination

`GenericSQLConnector.write_batch` (`cdk/cdk/sql/generic.py`) casts the
incoming batch (`SchemaContract.cast_arrow_batch`), builds a
`StageWritePlan`, and hands it to the runtime-selected transport backend's
`execute_write`, which lands the batch into a per-batch stage table and
runs one mode statement from stage to target. Full mechanics — the
facade/backend split, the stage lifecycle, transaction boundaries — are
[`sql-write-path.md`](sql-write-path.md)'s.

### API destination

`GenericAPIConnector.land` (`cdk/cdk/api/generic.py`) does **not** cast in
Arrow space before writing. `LandingBatch.records`
(`cdk/cdk/base_handler.py`) materialises the batch once —
`record_batch.to_pylist()`, no intermediate cast — and the API handler
serialises those Python dicts directly with `orjson.dumps(default=...)`
(`cdk/cdk/api/http.py::encode_body`). `orjson` natively handles `datetime`,
`date`, `time`, `UUID`, dataclasses, and enums; the `default` hook
(`_orjson_default`) only has to cover what orjson itself refuses —
`Decimal` and `bytes`/`bytearray`/`memoryview`. Arrow-native Python types
survive into the dicts unchanged, so a pre-cast in Arrow space would be a
second pass for no gain: the API destination never coerces types the way
the SQL destination does, because there is no destination-declared column
schema to cast against — the write input is whatever the endpoint
contract declares.

## Two transports, not five destinations

`GenericSQLConnector` (`cdk/cdk/sql/generic.py`) writes over two transports,
picked per `transport_type` ([ADR 0015](../adr/0015-every-sql-destination-uses-one-handler-over-two-transports.md)):

- **`sqlalchemy`** — the breadth layer: any dialect SQLAlchemy covers, any
  write mode, modest throughput. Async engine for dialects with an async
  driver (asyncpg, aiomysql); plain sync engine for sync-only drivers
  (Redshift `redshift_connector`), run via `asyncio.to_thread`.
- **`adbc`** — the depth layer: the warehouses where volume matters, with
  near-bulk-load throughput via a direct ADBC DBAPI connection. Per-system
  ingestion mechanism (an external-ecosystem fact, not derivable from this
  repo — verify current driver maturity before relying on it): PostgreSQL
  (libpq `COPY`), Snowflake (native Arrow ingestion), BigQuery (Storage
  Write API), SQLite and DuckDB (in-process), MySQL (Flight SQL, newer —
  verify maturity per release).

Both run the stage-then-merge plan of [`sql-write-path.md`](sql-write-path.md)
([ADR 0005](../adr/0005-stage-then-merge-is-the-single-sql-write-primitive.md)).

## Design boundaries

- Arrow IPC on the wire: [ADR 0009](../adr/0009-engine-and-destination-are-separate-services-joined-by-grpc-carrying-arrow-ipc.md).
- One SQL handler over SQLAlchemy and ADBC: [ADR 0015](../adr/0015-every-sql-destination-uses-one-handler-over-two-transports.md).
- No extra Arrow-side conversions are added "to stay in Arrow longer" — the
  columnar advantage ends where the destination consumes rows.
