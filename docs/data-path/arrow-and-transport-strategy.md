# Arrow Type System and Transport Strategy

**Scope:** this doc owns the Arrow type vocabulary, the schema contract's
role at the write boundary, and the rationale for running two SQL
transports (SQLAlchemy and ADBC) rather than one. It does not re-derive the
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

1. **gRPC IPC payload.** Arrow IPC is compact, schema-aware, and ships the
   type information inline. Anything else (Protobuf-of-rows, JSON) would
   be strictly worse on the wire — see
   [`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md).
2. **Schema validation cast.** `SchemaContract.cast_arrow_batch`
   (`cdk/cdk/schema_contract.py`) aligns each arrived column — the Arrow
   type the driver actually produced, which nobody declares — with the
   destination schema, running vectorized `pyarrow.compute.cast(safe=True)`
   instead of a per-row Python coercion loop. Which conversions are
   permitted is the conversion matrix's call, not this boundary's — see
   [`mapping-and-transformations.md`](mapping-and-transformations.md#type-conversion)
   for the full mode table; this boundary and the transform's own retype
   both consult the same policy, so they cannot disagree.
3. **Type vocabulary.** `parse_arrow_type` (`cdk/cdk/type_map/arrow.py`)
   -> `TypeMapper.to_native_type` (`cdk/cdk/type_map/mapper.py`) ->
   `SqlDialect.render_column_type` (`cdk/cdk/sql/dialects.py`) is the
   single source of truth for types across all connectors: `arrow_type`,
   the canonical Arrow type string, parses to a `pa.DataType` on the read
   side, and the connector's own `type-map-write.json` renders it back to a
   `native_type` for DDL on the write side. The `arrow_family` heads are
   declared once, in `ARROW_FAMILIES` (`cdk/cdk/type_map/grammar.py`): each
   entry carries its parameter grammar, its `conversion_kind`, the pyarrow
   factory that builds it, and the `pyarrow.types` predicates that
   recognise it in a live batch. The parser, the conversion matrix,
   `arrow_family`, the conformance probe set, and both published artifacts
   derive from that
   one table, so an `arrow_family` added there needs no second edit.
4. **Wire-format decode/encode.** An `arrow_type` says only the target
   type; it says nothing about the shape of the JSON value that has to
   become it. A field whose `arrow_type` has no direct wire-native
   rendering — the `timestamp`/`date`/`time`/`duration` conversion kinds on
   read, plus `decimal`/`binary` on write — declares an `encoding` (read)
   or `encoding_write` (write) block naming a catalog entry
   (`cdk/cdk/type_map/decoders.py` / `encoders.py`, published as
   `decoders_catalog.json` / `encoders_catalog.json` beside the conversion
   matrix): `iso8601`, `epoch`, `strptime`, `regex_epoch`, `decimal`,
   `bool_map`, `base64`, `iso_duration` on read; the write-side mirror minus
   `regex_epoch`/`iso_duration`. No entry is applied implicitly — a field
   whose kind requires one and declares none raises `MissingEncodingError`
   at `SchemaContract` construction (`check_required_read_encoding`) or at
   the write schema handshake (`check_required_write_encoding`), never at
   the first record that reaches it. A field naming `{"name": "code"}`
   routes instead to a `connector.py` override of `ApiDialect.decode_field`
   / `.encode_field` (`cdk/cdk/api/dialects.py`) for a shape the catalog
   does not cover. This is API-only and orthogonal to item 3's SQL
   `native_type`/`arrow_type` DDL rendering: a SQL driver hands back
   already-typed Python values, so `SchemaContract` never gates a
   `"columns"`-shaped schema on this at all — only the two API call sites
   (`cdk/cdk/api/generic.py`, `cdk/cdk/api/write_plan.py`) do.

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

The engine has one database handler, `GenericSQLConnector`
(`cdk/cdk/sql/generic.py`), which implements both write paths; the
connector definition picks between them per `transport_type`:

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

Both flavours run the identical stage-then-merge plan
([`sql-write-path.md`](sql-write-path.md)) — dispatch, never fork, the
destination tree. ADBC's `adbc_ingest` is INSERT/APPEND only, which is
exactly what the write primitive needs: it lands the batch into the stage,
and the dialect's `merge_statement_sql` renders the declared merge form
from stage to target.

Systems with no async SQLAlchemy driver or no ADBC coverage (Oracle,
MSSQL, MariaDB, ClickHouse, and any niche dialect) stay on the SQLAlchemy
path; low-volume pipelines stay there too, since the SA path is already
fast enough for them. The design does not add a third path: one method
(ADBC) covers the high-volume warehouses, and one method (SQLAlchemy)
covers everything else.

## Design boundaries

- Arrow is not replaced. The gRPC payload and schema cast are real,
  measured wins.
- SQLAlchemy is not replaced. It is the breadth layer that lets any
  reasonable database be a destination on day one.
- No extra Arrow-side conversions are added "to stay in Arrow longer" — the
  columnar advantage ends where the destination consumes rows.
- No per-database custom destination is written where ADBC's uniform API
  already covers the high-volume warehouses.
