# Connector Modules: A Shared, Attachable Driver Layer

> **Status:** Design of record (shipped)
> **Scope:** This document **owns** the Connector CDK boundary, the capability
> contract, and the registry / packaging model — it is the design of record for
> that layer. It **defers**: the Arrow type system to
> [pyarrow-and-destinations.md](./pyarrow-and-destinations.md); destination
> handler configuration to [destination-config.md](./destination-config.md);
> source / stream configuration to [source-config.md](./source-config.md); and
> the gRPC streaming protocol to
> [grpc-streaming-architecture.md](./grpc-streaming-architecture.md).
> **Related:** the Arrow type system + connector-owned type maps are covered in
> [pyarrow-and-destinations.md](./pyarrow-and-destinations.md).

## 1. Overview

The **Connector CDK** (`cdk/`) is a standalone, vendor-neutral library of
connector building blocks: the capability contract, transports, the
`ConnectionRuntime`, the type-map engine, the secrets seam, the connector
registry, and the SQL layer (discovery, DDL, read, write). The **Engine**
(`src/`) consumes it in-tree to run pipelines. Both layers are
**cloud-agnostic** — no cloud SDKs, local filesystem and stdout only; where
data ultimately lands is a deployment concern, not a concern of either layer.

The CDK serves two kinds of consumer with the *same* code:

- **Bulk streaming** — the engine's source-read and destination-write paths
  move data in Arrow batches during a pipeline run.
- **Synchronous control-plane** — point operations a caller waits on: list
  schemas, list tables, list columns, create table.

The *capability* is separate from the *deployment*: a `discover` /
`create_table` capability can be served locally, in-tree, by the engine itself,
or driven by a separate synchronous runtime. The same connector code backs
both; deployment and hosting of any such runtime are out of scope here.

### The boundary

The dependency points **one way: engine → CDK, never back.** No CDK module
imports anything engine-side (gRPC, state, models, the engine orchestrator).
Where the CDK needs a capability whose implementation is runtime-specific it
declares a **seam** — a Protocol/ABC the consumer implements (`SecretsResolver`,
`CheckpointStore`) — rather than reaching outward. This keeps the CDK a pure,
reusable contract and each runtime a thin consumer of it.

## 2. The shape: CDK + attachable connector modules

**Drivers are a modular layer — a stable *Connector CDK* plus independently
packaged *connector modules* — that runtimes consume as thin clients. Neither
runtime *owns* drivers.**

```
            ┌───────────────────────────────────────────────┐
            │  Connector CDK  (stable, versioned contract)    │
            │  contract · transport · type-map · secrets          │
            │  discover · create_table · read · write · types │
            └───────────────────────────────────────────────┘
               ▲ implements          ▲ implements
       ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
       │ postgresql   │      │ snowflake    │      │ bigquery     │   attachable
       │ driver+types │      │ driver+types │      │ driver+types │   modules
       │ +definition  │      │ +definition  │      │ +definition  │
       └──────────────┘      └──────────────┘      └──────────────┘
               ▲ consumes                            ▲ consumes
   ┌───────────────────────────┐        ┌───────────────────────────────┐
   │ ENGINE runtime            │        │ CONTROL-PLANE runtime          │
   │ read / write (streaming)  │        │ discover / create_table        │
   │ in-tree                   │        │ separate, synchronous          │
   └───────────────────────────┘        └───────────────────────────────┘
```

Why this rather than "the engine owns drivers and exposes operations":

- It keeps each consumer independent of the heavy engine image — a
  latency-sensitive control-plane call need not load the full streaming
  runtime.
- It keeps the engine deployable as a self-contained Docker image for the
  local experience.
- The same `discover()` capability is reachable locally through the engine —
  the *interface* (a capability) is separate from the *deployment* (who hosts
  the call). Same code, two front doors.

## 3. What this builds on

Grounding the design in the existing code.

### The connector definition is one shared artifact

Consumers load the **same `connector.json` artifact**, reading different
subsets as needed. In the engine, `src/engine/pipeline_config_prep.py` loads
`connectors/{connector_id}/definition/connector.json` **from disk** (only the
connectors referenced by the active pipeline; no directory scan). A separate
control-plane runtime resolves the same artifact from its own registry source —
the shape is identical.

Real top-level shape (engine `connectors/postgres/definition/connector.json`),
abbreviated:

```json
{
  "$schema": "https://schemas.analitiq.ai/connector/latest.json",
  "kind": "database",
  "display_name": "PostgreSQL",
  "version": "1.0.0",
  "default_transport": "database",
  "transports": {
    "database": {
      "transport_type": "sqlalchemy",
      "driver": "postgresql+asyncpg",
      "dsn": { "kind": "url_template", "template": "...", "bindings": { } }
    }
  },
  "auth": { "type": "db" },
  "connection_contract": { "inputs": { }, "validation": { } }
}
```

Two accuracy notes that matter for this design:

- The top-level discriminator is **`kind`** (`database` / `api` / `file` /
  `stdout`) — this is the key the registry maps to a connector class.
- The **type map is NOT referenced inside `connector.json`.** It is a separate,
  *positional* file at `connectors/{connector_id}/definition/type-map-read.json`
  (see [pyarrow-and-destinations.md](./pyarrow-and-destinations.md)). The connector's
  data (definition + type map) is therefore modular and co-located — consumed
  by both sides.

### Already decoupled — transports and secrets

Engine transport objects are frozen dataclasses with no gRPC/streaming imports
(`cdk/cdk/transport_factory.py`). Lifecycle is owned by `ConnectionRuntime`,
not the dataclass:

```python
@dataclass(frozen=True)
class SqlAlchemyTransport:
    engine: AsyncEngine | Engine   # async, or plain sync for sync-only drivers
    driver: str           # e.g. "postgresql+asyncpg", "redshift+redshift_connector"
    dialect: str           # e.g. "postgresql"
    is_async: bool         # which engine flavour `engine` carries

@dataclass(frozen=True)
class AdbcTransport:
    connect: Callable[[], Any]   # call → a fresh DBAPI 2.0 connection (no pool)
    driver: str                  # closed enum: "postgresql" | "snowflake" | "bigquery"
```

`await runtime.materialize()` builds the transport and exposes it via
`runtime.engine` (async SQLAlchemy), `runtime.sync_engine` /
`runtime.is_sync_sqlalchemy` (sync-only SQLAlchemy drivers),
`runtime.open_adbc_connection()` / `runtime.is_adbc` (ADBC), and
`runtime.driver` / `runtime.connector_type`. Lifecycle is ref-counted
(`acquire()` / `close()`).

Secrets sit behind a swappable **ABC** (`cdk/cdk/secrets/protocol.py`), with
local-file and in-memory implementations in-tree — the exact seam a separate
runtime plugs its own resolver into:

```python
class SecretsResolver(ABC):
    @abstractmethod
    async def resolve(
        self, connection_id: str, *, keys: Optional[list[str]] = None
    ) -> Dict[str, str]: ...
    @abstractmethod
    async def close(self) -> None: ...
```

### What shipped

The pieces below were the open gaps in the original design; all are now
implemented in the CDK and wired into the engine. Each note records the shipped
reality.

1. **The code layer is pluggable.** The hardcoded handler dict is gone.
   `ConnectorRegistry` + `build_registries(..., discover=True)`
   (`cdk/cdk/registry.py`) keep a `kind` → connector-class map for each role and
   discover externally pip-installed connectors via setuptools entry points
   (`analitiq.source_connectors` / `analitiq.destination_connectors`). Built-ins
   are seeded explicitly first (always available, works in editable installs and
   under pytest); entry-point discovery is additive and best-effort (a broken
   plugin is logged and skipped, never fatal). The worker subprocess
   (`build_worker_registries` in `src/worker/__init__.py`) and the destination
   (`src/destination/connectors/__init__.py`) both call `build_registries`,
   seeding `{"database": GenericSQLConnector}` and discovering the rest; the
   engine process holds only the `WorkerReadable` client. A duplicate `kind`
   raises rather than silently shadowing.
2. **Introspection operations exist.** `list_schemas` / `list_tables` /
   `list_columns` ship in the CDK (`cdk/cdk/sql/discovery.py`, exposed on
   `GenericSQLConnector`), running `INFORMATION_SCHEMA` queries over the same
   transport the data path uses and canonicalizing native types via the
   connection-scoped read type-map (`runtime.type_mapper_for(scope=CONNECTION)`
   — connection rules over connector rules, since discovery introspects the
   connection's own database; #368). `list_columns` returns **both** the
   columns and the primary keys (`tuple[list[ColumnDef], list[str]]`).
3. **`create_table` is standalone.** DDL is decoupled from the gRPC streaming
   flow: the destination base and the contract speak CDK-native DTOs
   (`SchemaSpec` / `Cursor` / `AckStatus` in `cdk/cdk/types.py`), with the gRPC
   `server.py` translating at the wire boundary. A control-plane caller
   constructs `ColumnDef`s directly and calls `TableCreator.create_table`
   (`cdk/cdk/sql/ddl.py`) with no engine orchestration.
4. **The write-direction type map shipped (#564).** `TypeMapper`
   (`cdk/cdk/type_map/mapper.py`) now exposes `to_native_type()` driven by a
   separate `type-map-write.json` rule set (Arrow canonical → native), the
   inverse `create_table` DDL needs. The two directions are independent rule
   sets, never one inverted at runtime.
5. **There is no capability declaration — by design.** Capability is never a
   static block in `connector.json`, because it conflates two unrelated things
   (see §4): *protocol conformance* (a property of the connector code, derived
   by `isinstance` against the `runtime_checkable` Protocols) and *authorization*
   (a property of the connection's credentials / DB grants, enforced at
   runtime). Neither is declared.

## 4. The contract (Connector CDK)

The shared library is a **CDK** (*connector development kit — a toolbox of
reusable building blocks a connector uses, NOT a central engine it must route
through*). This distinction is the whole design:

- **CDK-as-gatekeeper** (rejected): DB-specific logic lives in the shared
  library, so engineers are in the loop for every new database. This breaks
  user self-service — a brand-new DB (say Clickhouse) wouldn't run until an
  engineer updated the library.
- **CDK-as-toolbox** (this design): **vendor-specific** logic lives **in the
  connector**, generated by the connector-builder plugin. The shared library
  offers the *contract*, vendor-neutral machinery (transports, secrets, the
  type-map engine), and *optional* helpers. A new connector runs on an
  **unchanged** CDK.

Everything in the CDK is therefore **vendor-agnostic** — it may know about SQL
databases *in general*, but nothing about any *specific* database (Postgres vs
Snowflake vs Clickhouse). The test is: **"would this code change when you add
Clickhouse?"** If yes, it's vendor-specific and belongs in the connector; if no,
it's generic and can live in the CDK. By that test, the CDK holds:

1. the **contract** — the operation Protocols (below),
2. the **transport families** — `build_transport()` + `ConnectionRuntime`
   (*the connection plumbing — SQLAlchemy, ADBC, HTTP — into which a connector
   plugs its **own** driver*),
3. the **`SecretsResolver`** ABC (credential fetching seam),
4. the **`TypeMapper` mechanism** (the type-map *engine*, not any mappings) —
   including the **write-direction** support `create_table` needs (§3, item 4 /
   issue #564); the per-DB mappings themselves are the connector's data,
5. **optional reusable building blocks** — e.g. a generic, dialect-agnostic
   SQL-database base a connector can use as-is or subclass (so
   batching/streaming/error handling is battle-tested, not reinvented per
   connector).

Note that items 4–5 *are* database-related — but vendor-neutral. They pass the
test: adding Clickhouse changes none of them.

What is **NOT** in the CDK — the vendor-specific things that change per database:
any database's **driver**, **type-map mappings**, or **dialect SQL**. Those ride
in the connector.

### The contract

Operations are split into **capability protocols** so a connector implements
only what it supports (an HTTP-API connector need not implement `create_table`).
The protocols below are **implemented and shipped** in `cdk/cdk/contract.py`;
they are `runtime_checkable`, so a runtime selects a capability by `isinstance`,
never by a declared block. Method names mirror the engine's source/destination
methods (`read_batches`, `configure_schema`, `write_batch`).

```python
# cdk/cdk/contract.py  (verbatim shape)

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, AsyncIterator, Protocol, runtime_checkable
from .types import BatchWriteResult, CheckpointStore, Cursor, SchemaSpec


@dataclass(frozen=True)
class ColumnDef:
    name: str
    canonical_type: str          # Arrow canonical type string, e.g. "Int64",
                                 # "Decimal128(38, 9)" — symmetric with the type-map
    nullable: bool = True
    primary_key: bool = False


# ---- Capability: DISCOVER (control-plane reads) -------------------------
@runtime_checkable
class Discoverable(Protocol):
    async def list_schemas(self, runtime: "ConnectionRuntime") -> list[str]: ...
    async def list_tables(self, runtime: "ConnectionRuntime", schema: str) -> list[str]: ...
    async def list_columns(
        self, runtime: "ConnectionRuntime", schema: str, table: str
    ) -> tuple[list[ColumnDef], list[str]]:  # (columns, primary_keys)
        ...


# ---- Capability: CREATE (control-plane writes DDL) ----------------------
@runtime_checkable
class TableCreator(Protocol):
    async def create_table(
        self, runtime: "ConnectionRuntime", schema: str, table: str,
        columns: list[ColumnDef], primary_keys: list[str],
    ) -> None: ...


# ---- Capability: READ (engine source) -----------------------------------
@runtime_checkable
class Readable(Protocol):
    async def read_batches(
        self, runtime: "ConnectionRuntime", config: dict[str, Any], *,
        checkpoint: CheckpointStore, stream_name: str,
        partition: dict[str, Any] | None = None, batch_size: int = 1000,
    ) -> AsyncIterator["pa.RecordBatch"]: ...


# ---- Capability: WRITE (engine destination) -----------------------------
@runtime_checkable
class Writable(Protocol):
    async def connect(self, runtime: "ConnectionRuntime") -> None: ...
    async def configure_schema(self, schema_spec: SchemaSpec) -> bool: ...
    async def write_batch(
        self, run_id: str, stream_id: str, batch_seq: int,
        record_batch: "pa.RecordBatch", record_ids: list[str], cursor: Cursor,
    ) -> BatchWriteResult: ...
    async def disconnect(self) -> None: ...
    async def health_check(self) -> bool: ...
```

There is no `finalize`; the write lifecycle is `connect` → `configure_schema`
→ `write_batch`* → `disconnect`, with `health_check` for liveness.
`GenericSQLConnector` (`cdk/cdk/sql/generic.py`) implements **all four**
protocols — a single class serves source reads, destination writes, and the
control-plane discover / create_table operations over both SQLAlchemy and ADBC.

A connector implements **one or more** of these protocols. Crucially, it can do
so two ways — and the plugin picks based on how well-behaved the database is:

- **Reuse the CDK base** — a well-behaved SQL database subclasses the CDK's
  generic SQL building block, supplying only its **data** (connector.json,
  type-map) and **driver**. Almost no new code.
- **Override / bring its own** — a quirky database (unusual dialect SQL, odd
  pagination, exotic types) overrides whatever methods it needs, or implements
  the protocols from scratch. The connector owns this code.

Both run on an **unchanged CDK**. This is what preserves self-service: the
plugin generates a Clickhouse connector — reusing CDK blocks where it can,
overriding where Clickhouse is weird — and it works with **no engineer touching
the shared library**.

> **The one real boundary.** Engineers are needed only if a database needs a
> **brand-new transport family** (*a connection style that is neither SQLAlchemy,
> nor ADBC, nor HTTP*) — rare; almost every DB fits an existing one. A new
> *database* never requires CDK work; only a new connection *paradigm* does. The
> plugin can detect "does this DB fit an existing transport family?" before
> generating.

### Capability is not declared

There is **no capabilities block** — capability is *two* separate things,
neither of which is static config:

- **Protocol conformance** (*does the connector's code implement the
  operation?*) — derived from the class via `isinstance(connector, Discoverable)`
  / `TableCreator`. Code is the single source of truth, so it cannot drift from
  a hand-maintained list. Varies only across `kind` (an API connector may not
  implement `TableCreator`; a stdout destination implements only `Writable`);
  it is uniform within databases.
- **Authorization** (*may this credential do the operation on this instance?*) —
  a property of the **connection's credentials / DB grants**, enforced by the
  database at runtime. A read-only role gets a permission error on
  `create_table`. Never declared; discovered at runtime (or via a preflight
  probe).

```python
# capability check = protocol conformance, not config
if isinstance(connector, Discoverable):
    schemas = await connector.list_schemas(runtime)
# authorization is the DB's call — surfaces as a permission error, not a flag
```

(API connectors are the one place read/write varies *below* the connector level
— but that is **per-endpoint**, already data-driven by the connector's endpoint
definitions, not a connector-wide flag.)

### Key properties

- **`ConnectionRuntime` is the single handle** a consumer holds. Built from
  `(connector definition, connection params, SecretsResolver)`; on
  `materialize()` it exposes the live transport via `runtime.engine` /
  `runtime.open_adbc_connection()` (`runtime.is_adbc` discriminates). Each
  operation constructs a fresh connection per call — **no shared state between
  requests**, which gives the control plane its isolation.
- **Type translation lives with the connector.** The CDK's `TypeMapper`
  provides both `to_arrow_type(native)` (read direction) and `to_native_type()`
  (write direction, canonical → native), the latter what `create_table` DDL
  needs (#564, shipped). Read direction is fed by `type-map-read.json`, write
  direction by a separate `type-map-write.json` — the *mappings* are the
  connector's data, the *mechanism* is the CDK's.
- **The contract is versioned.** Connectors declare a `cdk_version`; runtimes
  refuse connectors outside their supported range. This is the seam that lets the
  CDK and the connectors evolve independently — a contract change is the *other*
  (besides a new transport family) thing that involves engineers.

## 5. Module packaging and layout

### CDK packaging — core + opt-in extras

The CDK (`analitiq-cdk`) is **dependency-tiered** (shipped, PR #141). The core
install pulls only `sqlalchemy` + `pydantic`, so a database-only consumer (e.g.
a control-plane process doing discovery / DDL) stays lightweight. The heavier
capabilities are opt-in extras:

| Extra | Pulls | Enables |
|---|---|---|
| `[arrow]` | `pyarrow` | Arrow columnar read/write batches |
| `[api]` | `aiohttp` | the HTTP transport |
| `[streaming]` | `pyarrow` + `aiohttp` | both of the above |

Imports are lazy at the package seams (`cdk/sql/__init__.py`,
`cdk/type_map/__init__.py`) via PEP 562 `__getattr__`: importing the
string-only surface (`TypeMapper`, the rule parsers, `list_*`, standalone
`create_table`) does **not** pull `pyarrow`. The Arrow builders
(`parse_arrow_type`, `resolve_arrow_type`, `AdbcReader`) resolve on first
access and raise `cdk.MissingExtraError` with the install hint if the extra is
absent.

A connector is a **self-contained, independently releasable unit** carrying
everything DB-specific: definition, type-map, its own driver, and as much or as
little code as that database needs. It depends on the CDK; the CDK never depends
on it.

Two connector shapes, both generated by the plugin, both running on an unchanged
CDK:

**Thin connector** — a well-behaved SQL database. Mostly data + driver; the code
just declares "use the CDK's generic SQL base."

```
connectors/postgresql/
  definition/
    connector.json        # kind, transports (no capabilities block — see §4)
    type-map-read.json         # native <-> arrow mappings (this DB's data; #564 home)
  connector.py            # ~10 lines: subclass the CDK SQL base, no overrides
  requirements.txt        # this DB's driver only (asyncpg / adbc-driver-postgresql)
  pyproject.toml          # packaged as `analitiq-connector-postgresql`
```

**Thick connector** — a quirky database (e.g. Clickhouse). Same layout, but
`connector.py` overrides whatever the CDK base gets wrong for this DB (dialect
DDL, pagination, type quirks). The overrides live **here**, never in the CDK.

```
connectors/clickhouse/
  definition/
    connector.json
    type-map-read.json         # Clickhouse native types -> arrow (plugin-researched)
  connector.py            # subclass CDK base + override create_table DDL, etc.
  requirements.txt        # clickhouse-connect / clickhouse driver
  pyproject.toml
```

> On-disk, `type-map-read.json` sits *inside* `definition/`
> (`connectors/{id}/definition/type-map-read.json`), co-located with `connector.json`
> — the engine's existing layout, preserved.

Two complementary distribution forms:

- **Connector repo** (one per connector in the connector registry) — holds
  **both** the code (`connector.py` + deps) and the data (`definition/`),
  versioned together by **git tag** (§9, decision 3). The installable unit:
  consumers `pip install git+…@vX.Y.Z`.
- **Registry snapshot** — the `definition/` (connector.json + type-map) can be
  snapshotted by a registry source for read paths that only need the data; the
  *code* is pulled from the same repo by tag. No separate package index.

The crucial discipline: **a connector never imports another connector, and
never imports a runtime.** It depends only on the CDK. That one rule is what
keeps the system modular instead of a monolith — and what lets the plugin ship a
new database end-to-end without an engineer.

### What the connector-builder plugin produces (before → after)

This design turns the plugin from a **JSON author** into a **small-package
author**. As a plain JSON author it emitted data only —
`connector.json`, the read-direction `type-map-read.json`, and (for API connectors)
endpoint files.

Under the CDK, the plugin emits the same data **plus** four things:

| New output | Why it's needed |
|---|---|
| **Write-direction `type-map`** (canonical → native) | `create_table` needs the inverse the read map cannot give — the write half of #564 |
| **`requirements.txt`** (this DB's driver) | drivers are no longer baked into the engine; the connector brings its own |
| **`pyproject.toml`** | the connector is now an installable package |
| **`connector.py`** | the code seam — thin (subclass the CDK SQL base, no overrides) for a well-behaved DB, thick (subclass + override dialect DDL / pagination / type quirks) for a quirky one |

What does **not** change: the plugin still authors the connector's *data* and
still needs no engineer. It now also *packages* that data and, only when the
database is quirky, writes the DB-specific code that used to be assumed-generic
and baked into the engine. For a well-behaved database the net addition is the
write-map plus three small boilerplate files; for an exotic one it is that plus
genuine override code — which is precisely what lets a brand-new database ship
on an unchanged CDK.

## 6. Attaching a module to the engine

Goal: a user adds a connector to their self-hosted engine without rebuilding the
world. Modules are **discoverable via Python entry points**, so installing a
package = attaching a connector. Built-in connectors are seeded explicitly;
installed packages are discovered additively.

1. **Install the module** into the engine's environment:

   ```bash
   # into the running engine container, or baked into a custom image layer;
   # git-based from the connector's registry repo at a pinned tag (§9, dec. 3)
   pip install "git+https://…/postgresql@v1.0.0"
   ```

   The package advertises itself via entry points (separate groups per role, so
   a `database` source and destination can resolve to the same or different
   classes):

   ```toml
   # connectors/postgresql/pyproject.toml
   [project.entry-points."analitiq.source_connectors"]
   database = "analitiq_connector_postgresql.connector:PostgresConnector"
   [project.entry-points."analitiq.destination_connectors"]
   database = "analitiq_connector_postgresql.connector:PostgresConnector"
   ```

2. **Engine discovers it at startup.** `build_registries(..., discover=True)`
   seeds the built-ins (`{"database": GenericSQLConnector}`) then scans the
   `analitiq.source_connectors` / `analitiq.destination_connectors` entry-point
   groups, registering each `ConnectorRegistry` by `kind`. A broken plugin is
   logged and skipped; a duplicate `kind` raises rather than silently shadowing.
   Which operations a connector supports is read from the class itself
   (`isinstance` against the Protocols), not from config.

3. **User references it by name** in pipeline config (`connector_ref:
   "postgresql"`). Definition + type map are read from the connector's package
   data (or a mounted `connectors/` dir for local dev).

4. **Discover rides the same install.** Because the registered connector carries
   the `Discoverable` capability, the same CDK code that serves engine reads also
   serves `list_schemas` / `list_tables` / `list_columns` locally — no separate
   implementation.

## 7. Anti-monolith principles (the rules underneath)

1. **Thin runtimes, fat modules.** If logic can live in a connector module, it
   does — runtimes only orchestrate.
2. **Depend on the contract, never the implementation.** A consumer imports the
   CDK interface + a connector by name; it never reaches into a connector's
   internals, and connectors never reach into each other.
3. **Independently releasable connectors** in a registry. Adding a database =
   ship a module = data + a capability implementation, no runtime change.
4. **Capability by protocol conformance, not declaration.** Runtimes check
   `isinstance` against the operation Protocols; authorization is left to the
   DB's grants at runtime. No static capability flags to drift.
5. **Stable, versioned seams.** The CDK contract and the `SecretsResolver` /
   registry protocols are versioned; either side evolves without lockstep
   redeploys.

## 8. Consequences

**Positive**
- One driver codebase; adding a database is a pure module addition.
- A separate synchronous control-plane can reuse the same CDK as a thin client
  instead of reimplementing drivers.
- Engine stays a clean, self-contained deployable.
- Type handling converges (#564) instead of drifting across two stacks.
- Capability stays honest: protocol conformance derived from code, authorization
  owned by DB grants — no static flag to drift or misrepresent.

**Costs / risks**
- A CDK package + release pipeline to own (now in `cdk/`, tag-installable).
- The connector registry is dynamic (entry-point discovery), which adds a
  startup scan and the discipline of not shadowing a `kind`.
- A runtime-specific `SecretsResolver` implementation lives with each consumer.
- Versioning the CDK contract adds release coordination (mitigated by semver +
  supported-range checks).

## 9. Decisions

1. **CDK ownership → the engine repo (`analitiq-core`).** The CDK lives in the
   engine repo (`cdk/`) as a distinct, independently-installable package; the
   engine owns it, and other runtimes consume it. *Rationale:* the engine
   already held the richer ADBC/async driver layer, so this was the
   lowest-friction extraction. *Implication:* the extraction was an **in-repo
   factoring** of the engine's driver/transport code into a clean package
   boundary — not a brand-new repo.

2. **CDK packaging → core + opt-in extras.** The core install is
   `sqlalchemy` + `pydantic`; `[arrow]` (pyarrow), `[api]` (aiohttp), and
   `[streaming]` (both) are opt-in. *Rationale:* a database-only consumer (e.g.
   a control-plane doing discovery / DDL) should not pull Arrow. *Implication:*
   lazy imports at the package seams and `cdk.MissingExtraError` when an Arrow /
   HTTP path is hit without the extra.

3. **Connector distribution → git-based from the registry repos.** Connector
   *code* (`connector.py` + deps) lives **with** its JSON definition in the same
   per-connector repo — one repo per connector, code and data versioned together
   by **git tag**. Consumers install a pinned tag
   (`pip install git+https://…@vX.Y.Z`). *Rationale:* reuses the
   repo-per-connector model already in place; no package index to stand up.
   *Implication:* the connector-builder plugin commits code+data and tags a
   release; the CDK package is pinned the same way.

4. **MSSQL → a first-class module.** SQL Server is a proper connector module
   (driver + type-map), not an optional `pyproject` extra. *Rationale:* users
   expect it; dropping it would be a regression.

## 10. What shipped

The design above is implemented. The work landed as these phases:

- **Write-direction type-map (#124).** `TypeMapper.to_native_type()` +
  `type-map-write.json` rule set — the inverse `create_table` DDL needs (#564).
- **CDK extraction (#115 / #117 / #118).** Transports, `ConnectionRuntime`,
  `SecretsResolver`, `TypeMapper`, and the generic SQL machinery factored into
  the `cdk/` package; engine consumes it in-tree.
- **`Discoverable` + standalone `create_table` (#131).** `list_schemas` /
  `list_tables` / `list_columns` (returning columns **and** primary keys) and
  DDL decoupled from the gRPC flow onto CDK-native DTOs.
- **Dynamic registry + `GenericSQLConnector` (#135).** `ConnectorRegistry` +
  `build_registries(..., discover=True)`; one SQL connector class implementing
  all four protocols over SQLAlchemy and ADBC; the engine and destination wire
  the registries.
- **Core / extras split (#141).** The dependency-tiered packaging in §5 and
  decision 2 — lazy imports + `cdk.MissingExtraError`.
