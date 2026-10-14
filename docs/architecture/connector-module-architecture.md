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

## 1. Overview

The **Connector CDK** (`cdk/`) is a standalone, vendor-neutral library of
connector building blocks: the capability contract, transports, the
`ConnectionRuntime`, the type-map engine, the secrets seam, the connector
registry, and the SQL layer (discovery, DDL, read, write). The **Engine**
(`src/`) consumes it in-tree to run pipelines. Neither layer carries a cloud
SDK in its core dependencies (see
[ADR 0007](../adr/0007-the-engine-carries-no-cloud-sdk.md)).

The CDK serves two kinds of consumer with the *same* code (see
[ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md)):

- **Bulk streaming** — the engine's source-read and destination-write paths
  move data in Arrow batches during a pipeline run.
- **Synchronous control-plane** — point operations a caller waits on: list
  schemas, list tables, list columns, create table.

Deployment and hosting of a control-plane runtime are out of scope here.

### The boundary

The dependency points **one way: engine → CDK, never back.** No CDK module
imports anything engine-side (gRPC, state, models, the engine orchestrator).
Where the CDK needs a capability whose implementation is runtime-specific it
declares a **seam** — a Protocol/ABC the consumer implements (`SecretsResolver`,
`CheckpointStore`) — rather than reaching outward. This keeps the CDK a pure,
reusable contract and each runtime a thin consumer of it.

## 2. The shape: CDK + attachable connector modules

Drivers are a *Connector CDK* plus independently packaged *connector modules*,
consumed by both runtimes (see
[ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md)).

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

## 3. What this builds on

Grounding the design in the existing code.

### The connector definition is one shared artifact

Consumers load the same connector package, reading different subsets as
needed. The engine reads only the packages the run's pipeline references; a
separate control-plane runtime resolves the same package from its own
registry source. How the registry resolves the connector class is recorded
in [ADR 0019](../adr/0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md).

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
    driver: str                  # e.g. "postgresql", "snowflake", "bigquery"
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
`analitiq.destination_connectors`). `KIND_DEFAULTS` depends on no package
metadata, so it works in editable installs and under pytest. The roles a
kind default serves, and their `issubclass` check against the
`runtime_checkable` Protocols in `cdk/cdk/contract.py` when the class is
first loaded, follow
[ADR 0019](../adr/0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md).
`build_registries` runs in the worker subprocess (see
[ADR 0008](../adr/0008-connector-code-runs-only-in-an-isolated-worker.md)).

`list_schemas` / `list_tables` / `list_columns` are introspection
operations exposed on `GenericSQLConnector` (`cdk/cdk/sql/discovery.py`),
running `INFORMATION_SCHEMA` queries over the same transport the data path
uses and mapping each `native_type` to its `arrow_type` via the
connection-scoped read type-map (`runtime.type_mapper_for(scope=CONNECTION)`
— connection rules over connector rules, since discovery introspects the
connection's own database). `list_columns` returns **both** the columns and
the primary keys (`tuple[list[ColumnDef], list[str]]`).

`create_table` is a standalone module-level function
(`cdk/cdk/sql/ddl.py::create_table`), decoupled from the gRPC streaming
flow: the destination base and the contract speak CDK-native DTOs
(`SchemaSpec` / `Cursor` / `AckStatus` in `cdk/cdk/types.py`), with the gRPC
`server.py` translating at the wire boundary. A control-plane caller
constructs `ColumnDef`s directly and calls it with no engine orchestration.

`TypeMapper` (`cdk/cdk/type_map/mapper.py`) exposes `to_native_type()`,
the direction `create_table` DDL needs. `TypeMapper` has two independent
directions, never one inverted at runtime.

## 4. The contract (Connector CDK)

The shared library is a **CDK** (*connector development kit — a toolbox of
reusable building blocks a connector uses, NOT a central engine it must route
through*) rather than a gatekeeper — see
[ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md) for why
and for what belongs in the connector instead. The CDK holds:

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

### The contract

Operations are split into **capability protocols** so a connector implements
only what it supports (an HTTP-API connector need not implement `create_table`).
The protocols below are in `cdk/cdk/contract.py`; they are `runtime_checkable`,
so a class can be checked against them (see
[ADR 0004](../adr/0004-capability-is-derived-never-declared.md)). Method names
mirror the engine's source/destination methods (`read_batches`,
`configure_schema`, `write_batch`).

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
    arrow_type: str              # the canonical Arrow type string, e.g. "Int64",
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
  generic SQL building block, supplying only its **data** and **driver**.
  Almost no new code.
- **Override / bring its own** — a quirky database (unusual dialect SQL, odd
  pagination, exotic types) overrides whatever methods it needs, or implements
  the protocols from scratch. The connector owns this code.

Both run on an unchanged CDK (see
[ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md)).

> **The one real boundary.** Engineers are needed only for a **new transport
> family** (a connection style that is neither SQLAlchemy, ADBC, nor HTTP) or a
> breaking contract change. A new *database* that fits an existing transport
> family needs no CDK work.

### Capability

How a connector's capabilities are decided follows
[ADR 0004](../adr/0004-capability-is-derived-never-declared.md).

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
  (write direction), the latter what `create_table` DDL needs. The
  *mappings* are the connector's data, the *mechanism* is the CDK's.
- **The contract is a versioned package.** The CDK is its own package,
  `analitiq-cdk` (see [ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md)).

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
`tools/contract_consumption.py` and checked current in CI; a model the engine
reads as a JSON grammar rather than field by field, such as `ErrorMap`, is
listed as opaque with no per-field claims — see
[ADR 0016](../adr/0016-the-pinned-validator-is-the-only-document-gate.md)).
The grammar and the matrix are owned by
[ADR 0017](../adr/0017-the-engine-owns-the-arrow-type-vocabulary-and-the-conversion-matrix.md).
All three are data files, and a distribution missing one imports cleanly and
fails on first use, so the publish workflow installs the built wheel into a
clean environment, reads every document back through the installed package,
and checks the sdist carries them too, before a release can proceed.
The same release publishes the manifest as versioned JSON
under `contract-consumption/v{cdk.__version__}/`, the coordinate the contract repo
pins it by.

A connector carries its definition, type-map, driver, and whatever code its
database needs (see
[ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md)).

Two connector shapes, both generated by the plugin, both running on an unchanged
CDK:

**Thin connector** — a well-behaved SQL database. Mostly data + driver; the code
just declares "use the CDK's generic SQL base."

```
connectors/postgresql/
  connector.py            # ~10 lines: subclass the CDK SQL base, no overrides
  requirements.txt        # this DB's driver only (asyncpg / adbc-driver-postgresql)
  pyproject.toml          # packaged as `analitiq-connector-postgresql`
```

**Thick connector** — a quirky database (e.g. Clickhouse). Same layout, but
`connector.py` overrides whatever the CDK base gets wrong for this DB (dialect
DDL, pagination, type quirks).

```
connectors/clickhouse/
  connector.py            # subclass CDK base + override create_table DDL, etc.
  requirements.txt        # clickhouse-connect / clickhouse driver
  pyproject.toml
```

Two complementary distribution forms:

- **Connector repo** — the code and the data at a git tag (see
  [ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md));
  consumers `pip install git+…@vX.Y.Z`.
- **Registry snapshot** — the data can be
  snapshotted by a registry source for read paths that only need the data; the
  *code* is pulled from the same repo by tag. No separate package index.

The crucial discipline: **a connector never imports another connector, and
never imports a runtime.** It depends only on the CDK. That one rule is what
keeps the system modular instead of a monolith — and what lets the plugin ship a
new database end-to-end without an engineer.

## 6. Attaching a module to the engine

Installing a connector package attaches it through Python entry points (see
[ADR 0019](../adr/0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md)).

1. **Install the module** into the engine's environment:

   ```bash
   # into the running engine container, or baked into a custom image layer;
   # git-based from the connector's registry repo at a pinned tag (§8)
   pip install "git+https://…/postgresql@v1.0.0"
   ```

   The package advertises itself via entry points, named by its
   **`connector_id`** (`postgresql`, not the `database` kind), with the same
   class in each role's group (see
   [ADR 0020](../adr/0020-one-connector-class-serves-both-read-and-write.md)).

   ```toml
   # connectors/postgresql/pyproject.toml
   [project.entry-points."analitiq.source_connectors"]
   postgresql = "analitiq_connector_postgresql.connector:PostgresConnector"
   [project.entry-points."analitiq.destination_connectors"]
   postgresql = "analitiq_connector_postgresql.connector:PostgresConnector"
   ```

2. **Engine discovers it at startup.** `build_registries(discover=True)`
   declares each registry's kind defaults from `KIND_DEFAULTS`, then scans
   the `analitiq.source_connectors` / `analitiq.destination_connectors`
   entry-point groups, registering each entry under its `connector_id`.
   Load failures, collisions and resolution order follow
   [ADR 0019](../adr/0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md);
   how the entry-point group sets a connector's role follows
   [ADR 0004](../adr/0004-capability-is-derived-never-declared.md).

3. **Discover rides the same install.** The registered connector's
   `Discoverable` methods serve `list_schemas` / `list_tables` /
   `list_columns` locally (see
   [ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md)).

## 7. Anti-monolith principles (the rules underneath)

1. **Thin runtimes, fat modules** — see
   [ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md).
2. **Depend on the contract, never the implementation.** A consumer imports the
   CDK interface + a connector by name; it never reaches into a connector's
   internals, and connectors never reach into each other.
3. **Independently releasable connectors** — see
   [ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md).
4. **Capability by protocol conformance, not declaration** — see
   [ADR 0004](../adr/0004-capability-is-derived-never-declared.md).
5. **Stable, versioned seams.** The CDK contract and the `SecretsResolver` /
   registry protocols are versioned; either side evolves without lockstep
   redeploys.

## 8. Ownership and distribution

Ownership and distribution of the CDK and connector code follow
[ADR 0003](../adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md). The CDK
is published to PyPI as `analitiq-cdk`, one release per `cdk-v*` tag.
