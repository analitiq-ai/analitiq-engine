# analitiq-cdk

The **Connector Development Kit**: a vendor-neutral, transport-neutral toolbox of
reusable building blocks a connector uses. It is consumed both for bulk streaming
(the OSS engine reads/writes Arrow batches) and for a synchronous control-plane
(discover schemas / create tables) that needs neither the columnar nor the HTTP
weight.

See `docs/architecture/connector-modules-engine-adr.md` (in the engine repo) for
the design of record.

## The one rule

The dependency points **engine → CDK, never back**. No CDK module may import
anything engine-side (`src/grpc`, `src/state`, `src/models`, `src/source`,
`src/engine/{engine,orchestrator,pipeline}`, `server.py`, `runner.py`,
`main.py`). Anything that must cross from engine to CDK crosses as a plain value
or a CDK-owned type — never an engine object. A connectivity capability whose
implementation is engine- or deployment-specific is expressed as a CDK-owned
seam (Protocol / ABC) the other side implements (`SecretsResolver`,
`CheckpointStore`).

## What lives here

- `cdk/types.py` — CDK-native value types (`AckStatus`, `Cursor`, `SchemaSpec`,
  `BatchWriteResult`, `EndpointScope`, `CheckpointStore`).
- `cdk/contract.py` — capability Protocols (`Readable`, `Writable`,
  `Discoverable`, `TableCreator`, `ColumnDef`).
- transports + `ConnectionRuntime`, the ADBC sub-registry, shared DB helpers,
  the query builder, the rate limiter.
- the expression resolver + derived functions.
- the type-map engine (`type_map/`).
- the secrets seam + local/in-memory resolvers (`secrets/`).
- the abstract destination base (`base_handler.py`).

## Dependency tiers (core + extras)

The CDK ships a small required core and opt-in extras, so a consumer installs
only what its role needs. The split is enforced by **lazy imports**: `cdk.sql`
and `cdk.type_map` resolve the Arrow helpers (and the HTTP transport its
`aiohttp` session) only on first use, so importing the core control-plane
surface never pulls `pyarrow`/`aiohttp`.

| Tier | Pulls | Surface |
|---|---|---|
| core (always) | `sqlalchemy`, `pydantic` | SQL control-plane: `cdk.sql` discovery + standalone `create_table`, `ConnectionRuntime`/transport seam, type-map (string surface), secrets |
| `[arrow]` | `pyarrow` | columnar streaming: `schema_contract`, `sql_types`, `sql.adbc_reader`, `type_map.parse_arrow_type`, `GenericSQLConnector` read/write |
| `[api]` | `aiohttp` | HTTP transport for API connectors |
| `[streaming]` | `pyarrow` + `aiohttp` | full connector surface the engine consumes (`[arrow]` + `[api]`) |

Plus the per-driver DB package a given connector needs (asyncpg, adbc-driver-*, …).

The thin-surface entry points raise `cdk.MissingExtraError` (naming the extra,
e.g. `analitiq-cdk[arrow]`) when the extra is absent, rather than a bare
`No module named 'pyarrow'`: the package-level lazy reexports
(`cdk.sql.AdbcReader` / `open_adbc_reader`, `cdk.type_map.parse_arrow_type` /
`resolve_arrow_type`) and the HTTP transport (`build_http_transport`). A
genuinely-absent extra is relabelled; an *unrelated* or *broken-install*
import failure (a partial build failing on a submodule, a missing transitive
dep) is re-raised untouched so the real cause survives.

Directly importing a streaming module that belongs to an extra
(`cdk.schema_contract`, `cdk.sql_types`, `cdk.sql.adbc_reader`,
`cdk.type_map.arrow`, `cdk.sql.generic`, `cdk.base_handler`) without that extra
raises a plain `ImportError` naming `pyarrow` on the import line — those modules
*are* the extra and require it by design; a thin control-plane consumer reaches
the Arrow surface only through the guarded lazy reexports above, never by
importing these modules directly.

## Install

Control-plane only (discover + create-table, no `pyarrow`/`aiohttp`):

```
pip install "analitiq-cdk @ git+https://github.com/analitiq-ai/analitiq-engine@vX.Y.Z#subdirectory=cdk"
```

Full streaming surface:

```
pip install "analitiq-cdk[streaming] @ git+https://github.com/analitiq-ai/analitiq-engine@vX.Y.Z#subdirectory=cdk"
```

The engine consumes it in-tree (it is on `PYTHONPATH` alongside `src/`) and
declares `pyarrow`/`aiohttp` itself, so the bundled engine image always has the
full surface regardless of the CDK's own extras.
