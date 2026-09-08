# Connector Modules: A Shared, Attachable Driver Layer

> **Scope:** This document **owns** the Connector CDK boundary, the capability
> contract, and the registry / packaging model — it is the specification for
> that layer. It **defers**: the Arrow type system to
> [arrow-and-transport-strategy.md](../data-path/arrow-and-transport-strategy.md);
> destination handler configuration to
> [destination-config.md](../config/destination-config.md);
> source / stream configuration to
> [source-config.md](../config/source-config.md); and
> the gRPC streaming protocol to
> [grpc-streaming-architecture.md](./grpc-streaming-architecture.md).
> The decisions behind this layer's shape are recorded in
> [ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md) and
> [ADR 0004](../adr/0004-capability-is-derived-never-declared.md).

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

Neither runtime owning drivers is a deliberate choice, not the absence of
one — see [ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md).
The same `discover()` capability is reachable locally through the engine —
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

Illustrative top-level shape for a database connector — the transport is
keyed by its own `transport_type`, and the exact required/optional fields
are the published `connector` JSON Schema, not this sketch:

```json
{
  "$schema": "https://schemas.analitiq.ai/connector/latest.json",
  "kind": "database",
  "display_name": "PostgreSQL",
  "version": "1.0.0",
  "default_transport": "sqlalchemy",
  "transports": {
    "sqlalchemy": {
      "transport_type": "sqlalchemy",
      "driver": "postgresql+asyncpg",
      "dsn": { "kind": "url_template", "template": "...", "bindings": { } }
    }
  },
  "auth": { "type": "none" },
  "connection_contract": { "inputs": { }, "validation": { } }
}
```

Two accuracy notes that matter for this design:

- The top-level discriminator is **`kind`** (`database` / `api` / `file` /
  `stdout`) — this is the key the registry maps to a connector class.
- The **type map is NOT referenced inside `connector.json`.** It is a separate,
  *positional* file at `connectors/{connector_id}/definition/type-map-read.json`
  (see [arrow-and-transport-strategy.md](../data-path/arrow-and-transport-strategy.md)). The connector's
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
    driver: str                  # e.g. "postgresql", "snowflake", "bigquery" —
                                 # constrained by the published connector schema's
                                 # AdbcTransport.driver enum, not by this dataclass
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
        self, connection_id: str, secret_refs: Mapping[str, str]
    ) -> dict[str, str]: ...
    @abstractmethod
    async def close(self) -> None: ...
```

### The registry, discovery, and control-plane operations

`ConnectorRegistry` + `build_registries(discover=True)`
(`cdk/cdk/registry.py`) keep a `kind` → connector-class map for each role,
seeded from `KIND_DEFAULTS` and extended by externally pip-installed
connectors via setuptools entry points (`analitiq.source_connectors` /
`analitiq.destination_connectors`). Kind defaults are always available —
`KIND_DEFAULTS` depends on no package metadata, so it works in editable
installs and under pytest; entry-point discovery is additive and
best-effort, so a broken plugin is logged and skipped, never fatal. **The
roles a kind default serves are declared in `KIND_DEFAULTS` itself, not
derived from the class at registration** — reading them off the class
would mean importing it, which is exactly the cost the table exists to
defer. The declaration is checked, not trusted blindly: the first time a
kind default is actually loaded, its declared roles are verified against
the class's own capability Protocols (`issubclass` against
`runtime_checkable` Protocols in `cdk/cdk/contract.py` — the registry holds
a class, not an instance, at this point), and a mismatch is a registry
defect, not a silent divergence. The worker subprocess is the
one caller of `build_registries`, because that is where connector classes
execute; the engine process holds only the `WorkerReadable` client and
imports no connector. A duplicate `kind` raises rather than silently
shadowing.

`list_schemas` / `list_tables` / `list_columns` are introspection
operations exposed on `GenericSQLConnector` (`cdk/cdk/sql/discovery.py`),
running `INFORMATION_SCHEMA` queries over the same transport the data path
uses and canonicalizing native types via the connection-scoped read
type-map (`runtime.type_mapper_for(scope=CONNECTION)` — connection rules
over connector rules, since discovery introspects the connection's own
database). `list_columns` returns **both** the columns and the primary
keys (`tuple[list[ColumnDef], list[str]]`).

`create_table` is a standalone module-level function
(`cdk/cdk/sql/ddl.py::create_table`), decoupled from the gRPC streaming
flow: the destination base and the contract speak CDK-native DTOs
(`SchemaSpec` / `Cursor` / `AckStatus` in `cdk/cdk/types.py`), with the gRPC
`server.py` translating at the wire boundary. A control-plane caller
constructs `ColumnDef`s directly and calls it with no engine orchestration.

`TypeMapper` (`cdk/cdk/type_map/mapper.py`) exposes `to_native_type()`,
driven by a separate `type-map-write.json` rule set (Arrow canonical →
native), the inverse `create_table` DDL needs. The two directions are
independent rule sets, never one inverted at runtime.

**There is no capability declaration, by design.** Capability is never a
static block in `connector.json`, because it conflates two unrelated things
(see §4): *protocol conformance* (a property of the connector code, derived
by `isinstance` against the `runtime_checkable` Protocols) and *authorization*
(a property of the connection's credentials / DB grants, enforced at
runtime). Neither is declared.

## 4. The contract (Connector CDK)

The shared library is a **CDK** (*connector development kit — a toolbox of
reusable building blocks a connector uses, NOT a central engine it must route
through*) rather than a gatekeeper — see
[ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md) for why.

Everything in the CDK is therefore **vendor-agnostic** — it may know about SQL
databases *in general*, but nothing about any *specific* database (Postgres vs
Snowflake vs Clickhouse). The test is: **"would this code change when you add
Clickhouse?"** If yes, it's vendor-specific and belongs in the connector; if no,
it's generic and can live in the CDK. By that test, the CDK holds:

1. the **contract** — the operation Protocols (below),
2. the **transport families** — `transport_factory` + `ConnectionRuntime`
   (*the connection plumbing — SQLAlchemy, ADBC, HTTP — into which a connector
   plugs its **own** driver*),
3. the **`SecretsResolver`** ABC (credential fetching seam),
4. the **`TypeMapper` mechanism** (the type-map *engine*, not any mappings) —
   including the **write-direction** support `create_table` needs (§3). The
   per-DB mappings themselves are the connector's data,
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
# cdk/cdk/contract.py  (shape, not verbatim — read the module for the exact signatures)

from __future__ import annotations
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from .types import BatchWriteResult, CheckpointStore, Cursor, SchemaSpec


@dataclass(frozen=True)
class ColumnDef:
    name: str
    canonical_type: str          # Arrow canonical type string, e.g. "Int64",
                                 # "Decimal128(38, 9)" — symmetric with the type-map
    nullable: bool = True
    primary_key: bool = False
    default: str | None = None   # SQL DEFAULT expression, for create_table DDL


# ---- Capability: DISCOVER (control-plane reads) -------------------------
@runtime_checkable
class Discoverable(Protocol):
    async def list_schemas(self, runtime: "ConnectionRuntime", *, catalog: str = "") -> list[str]: ...
    async def list_tables(self, runtime: "ConnectionRuntime", schema: str, *, catalog: str = "") -> list[str]: ...
    async def list_columns(
        self, runtime: "ConnectionRuntime", schema: str, table: str, *, catalog: str = ""
    ) -> tuple[list[ColumnDef], list[str]]:  # (columns, primary_keys)
        ...


# ---- Capability: CREATE (control-plane writes DDL) ----------------------
@runtime_checkable
class TableCreator(Protocol):
    async def create_table(
        self, runtime: "ConnectionRuntime", schema: str, table: str,
        columns: list[ColumnDef], primary_keys: list[str], *, catalog: str = "",
    ) -> None: ...


# ---- Capability: READ (engine source) -----------------------------------
@runtime_checkable
class Readable(Protocol):
    # Not `async def`: implementors are async generators, so calling
    # read_batches returns the AsyncIterator directly. `async def` would
    # type the call as Coroutine[..., AsyncIterator], breaking `async for`.
    def read_batches(
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
        emitted_at: datetime,
    ) -> BatchWriteResult: ...
    async def disconnect(self) -> None: ...
    async def health_check(self) -> bool: ...
```

The `Writable` protocol itself declares no `finalize`; the connector-visible
write lifecycle is `connect` → `configure_schema` → `write_batch`* →
`disconnect`, with `health_check` for liveness. (The destination *base
class*, `BaseDestinationHandler`, does add a `finalize_run` hook the gRPC
server calls on shutdown — that is engine-side lifecycle, not part of the
`Writable` capability contract a connector implements.)
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

There is **no capabilities block**. Capability is derived, not declared —
see [ADR 0004](../adr/0004-capability-is-derived-never-declared.md) for
why it splits into protocol conformance (code-derived, via
`isinstance(connector, Discoverable)` / `TableCreator`, varying only
across `kind`) and authorization (a property of the connection's
credentials / DB grants, enforced by the database at runtime — never
declared, discovered at runtime or via a preflight probe):

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
  needs. Read direction is fed by `type-map-read.json`, write
  direction by a separate `type-map-write.json` — the *mappings* are the
  connector's data, the *mechanism* is the CDK's.
- **The contract is a versioned package, not an in-document field.** A
  connector declares its `analitiq-cdk` dependency in its own `pyproject.toml`
  like any other Python package; there is no separate `cdk_version` field
  inside `connector.json` for a runtime to check. This is the seam that lets
  the CDK and connectors evolve independently — a breaking contract change is
  the *other* (besides a new transport family) thing that involves engineers.

## 5. Module packaging and layout

### CDK packaging — core + opt-in extras

The CDK (`analitiq-cdk`) is **dependency-tiered**. The core install pulls
`sqlalchemy`, `pydantic`, `analitiq-contract-models`, and `yarl` — no
Arrow, no HTTP client — so a database-only consumer (e.g. a control-plane
process doing discovery / DDL) stays lightweight. The heavier capabilities
are opt-in extras, declared once in `cdk/pyproject.toml` — that file is
authoritative for the exact package list per extra; in outline:

| Extra | Enables |
|---|---|
| `[arrow]` | Arrow columnar read/write batches (pulls `pyarrow`) |
| `[api]` | the HTTP transport and the generic API connector, including JSON-Schema param validation (pulls `pyarrow` as well, since API batches are Arrow-backed) |
| `[file]` | the local file destination |
| `[s3]` | the `s3://` secret-reference scheme |
| `[streaming]` | `[arrow]` + `[api]`, plus the async file I/O the combination needs |
| `[conformance]` | the connector acceptance suite (`cdk.conformance`) |

Imports are lazy at the package seams (`cdk/cdk/sql/__init__.py`,
`cdk/cdk/type_map/__init__.py`) via PEP 562 `__getattr__`: importing the
string-only surface (`TypeMapper`, the rule parsers, `list_*`, standalone
`create_table`) does **not** pull `pyarrow`. The Arrow builders
(`parse_arrow_type`, `resolve_arrow_type`, `AdbcReader`) resolve on first
access and raise `cdk.MissingExtraError` with the install hint if the extra is
absent.

The package also ships three generated contract artifacts beside the modules
that read them: the Arrow type grammar, the conversion matrix, and the
contract-consumption manifest (`cdk/cdk/contract_consumption.json`, the contract
fields the engine reads, rendered from mypy's type map by
`tools/contract_consumption.py` and checked current in CI; a field is claimed
when the engine reads it by any means it actually uses, and an unclaimed field
is one no runtime path reads -- every loaded document is held as its typed
contract model, so there is no dict path the census cannot see). All are data
files, so whether they land in a built distribution is a packaging decision
rather than a code one — and a distribution missing one imports cleanly and
fails on first use, in every consumer at once. The publish workflow therefore
installs the built wheel into a clean environment and reads every document back
through the installed package, and checks the sdist carries them too, before a
release can proceed. The same release publishes the manifest to S3 under
`contract-consumption/v{cdk.__version__}/`, the coordinate the contract repo
pins it by.

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
    type-map-read.json         # native <-> arrow mappings (this DB's data)
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
  versioned together by **git tag** (§8). The installable unit:
  consumers `pip install git+…@vX.Y.Z`.
- **Registry snapshot** — the `definition/` (connector.json + type-map) can be
  snapshotted by a registry source for read paths that only need the data; the
  *code* is pulled from the same repo by tag. No separate package index.

The crucial discipline: **a connector never imports another connector, and
never imports a runtime.** It depends only on the CDK. That one rule is what
keeps the system modular instead of a monolith — and what lets the plugin ship a
new database end-to-end without an engineer.

### What the connector-builder plugin produces

The plugin is a **small-package author**, not a plain JSON author: it emits
`connector.json`, the read-direction `type-map-read.json`, and (for API
connectors) endpoint files, plus the write-direction `type-map-write.json`
(canonical → native — the inverse `create_table` needs and the read map
cannot give), `requirements.txt` (this system's driver — drivers are never
baked into the engine), `pyproject.toml` (the connector is an installable
package), and `connector.py` (the code seam: thin — subclass the CDK SQL
base, no overrides — for a well-behaved system, thick — subclass plus
override dialect DDL / pagination / type quirks — for a quirky one). The
plugin still needs no engineer for any of this: for a well-behaved database
the addition over the write-map is three small boilerplate files; for an
exotic one it is that plus genuine override code — which is what lets a
brand-new database ship on an unchanged CDK.

## 6. Attaching a module to the engine

Goal: a user adds a connector to their self-hosted engine without rebuilding the
world. Modules are **discoverable via Python entry points**, so installing a
package = attaching a connector. Built-in connectors are seeded explicitly;
installed packages are discovered additively.

1. **Install the module** into the engine's environment:

   ```bash
   # into the running engine container, or baked into a custom image layer;
   # git-based from the connector's registry repo at a pinned tag (§8)
   pip install "git+https://…/postgresql@v1.0.0"
   ```

   The package advertises itself via entry points, named by its
   **`connector_id`** (`postgresql`, not the `database` kind). There is a
   group per role because the engine keeps a registry per role, but a
   connector registers the same class in both: one class serves the system
   in both directions, and the conformance suite refuses a connector that
   registers two, because a split there is exactly how the two directions
   drift apart.

   ```toml
   # connectors/postgresql/pyproject.toml
   [project.entry-points."analitiq.source_connectors"]
   postgresql = "analitiq_connector_postgresql.connector:PostgresConnector"
   [project.entry-points."analitiq.destination_connectors"]
   postgresql = "analitiq_connector_postgresql.connector:PostgresConnector"
   ```

2. **Engine discovers it at startup.** `build_registries(discover=True)`
   declares each registry's kind defaults from `KIND_DEFAULTS` (lazily —
   noting which kind each default serves without importing its class), then
   scans the `analitiq.source_connectors` / `analitiq.destination_connectors`
   entry-point groups, registering each entry under its `connector_id`. A
   plugin whose class fails to import, or whose `connector_id` collides with
   one already registered, is logged and skipped — one broken or
   double-published connector package must not abort startup. Resolving a
   connector tries its own `connector_id` first, falling back to the kind
   default only when no specific class is registered (the thin path, §3).
   Which role an entry-point connector serves — source, destination, or
   both — is declared by which entry-point group it registered under, not
   read off the class: the worker invokes the resolved class directly, no
   Protocol check at that call, so a class registered under a role it
   doesn't implement fails at first invocation. `isinstance` against the
   Protocols is for a caller deciding whether to *offer* an optional
   capability before trying it (see [ADR 0004](../adr/0004-capability-is-derived-never-declared.md)) —
   not the invocation gate.

3. **User references it by name** in pipeline config (`connector_id:
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

## 8. Ownership and distribution

The CDK lives in the engine repo (`cdk/`) as a distinct,
independently-installable package; connector *code* (`connector.py` +
deps) lives with its JSON definition in the same per-connector repo, one
repo per connector, code and data versioned together by git tag.
Consumers install a pinned tag (`pip install git+https://…@vX.Y.Z`); the
connector-builder plugin commits code and data together and tags a
release, and the CDK package itself is pinned the same way. See
[ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md) for why
ownership and distribution are shaped this way, including MSSQL's status
as a first-class module rather than an optional extra.
