# ADR: Connector CDK + Module Layer (engine implementation handover)

> **Status:** Draft / for handover to the `analitiq-core` team
> **Audience:** engine implementers (`analitiq-core`)
> **Destination:** this file is authored in `analitiq-infra` because that is
> where the cross-repo design lives; it is intended to be **copied into
> `analitiq-core`** as the engine-side ADR of record.
> **Parents:**
> [connector-module-architecture.md](./connector-module-architecture.md) (the
> cross-repo architecture + decisions §12) and
> [connector-modules-infra-work-breakdown.md](./connector-modules-infra-work-breakdown.md)
> (the infra-side counterpart — explicitly out of scope here).

All file paths below are in `analitiq-core` unless noted. They were verified
against the current engine codebase; signatures are quoted, not paraphrased.
Anything labelled **CONFIRM** is a proposed decision for the engine team to
ratify before implementation.

---

## 1. Context

The same database drivers are maintained twice: the engine's source/destination
connector stack, and infra's `k2m.db` adapter family behind a per-driver
`db-utils-*` Lambda fleet. This ADR makes the engine's driver layer a **shared,
attachable module layer** — a **CDK** (connector development kit: a toolbox of
reusable building blocks a connector uses, not a central engine it routes
through) — consumed by both the engine (bulk streaming) and the infra Lambdas
(synchronous control-plane: list schemas/tables/columns, create table).

Read the parent architecture doc for the full rationale. This ADR is the
engine-side build spec.

## 2. Inherited decisions (from architecture §12)

1. **CDK ownership → this repo (`analitiq-core`).** The CDK is a distinct,
   independently-installable package *inside* `analitiq-core`. The engine
   consumes it locally; infra installs it by pinned git tag.
2. **Cloud Lambdas → all container images** (infra concern; noted for context).
3. **Connector distribution → git-based** from per-connector repos under org
   `analitiq-dip-registry`, code + data versioned together by **git tag**. No
   package index.
4. **MSSQL → first-class module** (driver + read/write type-maps).

## 3. The frozen contract

The CDK defines capability Protocols (`typing.Protocol`, structural — "any class
with these methods conforms"). A connector implements **one or more**. Runtimes
select by `isinstance`, never by a declared capabilities block (none exists, and
none is added — see architecture §4). Method names below mirror the engine's
**existing** source/destination methods where those already exist; the
discover/create operations are **new**.

```python
# cdk/contract.py
from typing import Protocol, runtime_checkable, AsyncIterator, Any
import pyarrow as pa
from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnDef:
    name: str
    canonical_type: str          # Apache Arrow canonical type string (e.g. "Int64",
                                 # "Decimal128(38, 9)", "Timestamp(MICROSECOND, UTC)")
    nullable: bool = True
    primary_key: bool = False


# ---- DISCOVER (control-plane reads; NEW) --------------------------------
@runtime_checkable
class Discoverable(Protocol):
    async def list_schemas(self, runtime: "ConnectionRuntime") -> list[str]: ...
    async def list_tables(self, runtime: "ConnectionRuntime", schema: str) -> list[str]: ...
    async def list_columns(
        self, runtime: "ConnectionRuntime", schema: str, table: str
    ) -> tuple[list[ColumnDef], list[str]]: ...   # (columns, primary_keys)


# ---- CREATE (control-plane DDL; NEW, standalone) ------------------------
@runtime_checkable
class TableCreator(Protocol):
    async def create_table(
        self, runtime: "ConnectionRuntime", schema: str, table: str,
        columns: list[ColumnDef], primary_keys: list[str],
    ) -> None: ...


# ---- READ (engine source; exists today as DatabaseConnector.read_batches)
@runtime_checkable
class Readable(Protocol):
    async def read_batches(
        self, runtime: "ConnectionRuntime", config: dict[str, Any], *,
        checkpoint: "CheckpointStore", stream_name: str,
        partition: dict[str, Any] | None = None, batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]: ...   # checkpoint = CDK seam, not engine state_manager


# ---- WRITE (engine destination; exists today on BaseDestinationHandler) -
@runtime_checkable
class Writable(Protocol):
    async def connect(self, runtime: "ConnectionRuntime") -> None: ...
    async def configure_schema(self, schema_spec: "SchemaSpec") -> bool: ...
    async def write_batch(
        self, run_id: str, stream_id: str, batch_seq: int,
        record_batch: pa.RecordBatch, record_ids: list[str], cursor: "Cursor",
    ) -> "BatchWriteResult": ...
    async def disconnect(self) -> None: ...
    async def health_check(self) -> bool: ...
```

The `Writable` types `SchemaSpec`, `Cursor`, `AckStatus`, and `BatchWriteResult`
are **CDK-native** value types (`cdk/types.py`), **not** the gRPC generated
messages they replace (see CONFIRM-2 and §4):

```python
# cdk/types.py — CDK-native, zero protobuf/grpc/engine dependency
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Any, Protocol, runtime_checkable

class AckStatus(IntEnum):            # mirror the proto enum's integer values 1:1
    ACK_STATUS_UNSPECIFIED = 0
    ACK_STATUS_SUCCESS = 1
    # … remaining statuses, values aligned with stream.proto

@dataclass(frozen=True)
class Cursor:
    token: bytes = b""

@dataclass(frozen=True)
class SchemaSpec:                    # was gRPC SchemaMessage
    stream_id: str
    version: int
    write_mode: int                  # or a WriteMode IntEnum

@dataclass(frozen=True)
class BatchWriteResult:
    status: AckStatus
    committed_cursor: Cursor | None = None

class EndpointScope(StrEnum):        # the value read off the engine's EndpointRef.scope
    CONNECTOR = "connector"
    CONNECTION = "connection"

@runtime_checkable
class CheckpointStore(Protocol):     # provisional read-path seam; engine's StateManager satisfies it
    async def get_cursor(self, stream_name: str, partition: dict[str, Any] | None = None) -> dict[str, Any] | None: ...
    async def save_cursor(self, stream_name: str, partition: dict[str, Any] | None, cursor: dict[str, Any]) -> None: ...
```

**CONFIRM-1:** `list_columns` returns `(columns, primary_keys)` and `ColumnDef`
carries `canonical_type` (Arrow), not the raw native string. The native string
is still available from introspection but the canonical form is what
`create_table` consumes downstream (symmetry with the type-map). The existing
infra `list_columns` returns native `udt_name`; the rebased Lambda will map to
canonical via the read type-map.

**CONFIRM-2 (revised):** `Readable` / `Writable` keep their current concrete
method *shapes* (`read_batches(...)`, `write_batch(run_id, stream_id, batch_seq,
record_batch, record_ids, cursor) -> BatchWriteResult`,
`configure_schema(...) -> bool`) to minimise churn in the streaming path —
**but the parameter/return types swap from the gRPC generated messages to the
CDK-native value types** (`SchemaSpec`, `Cursor`, `AckStatus`,
`BatchWriteResult` in `cdk/types.py`). The CDK cannot import from `src/grpc`
(§4 acceptance), so the gRPC `SchemaMessage` / `Cursor` / `AckStatus` **stay
engine-side** and are translated to/from the CDK types in `server.py` at the
wire boundary (see §4 and §6). This reverses the earlier "keep gRPC
`SchemaMessage` as the contract input" position, which contradicted the §4
transport-neutrality acceptance. The types are structurally identical, so the
translation is a thin, mostly-identity mapping; only `server.py` + the four
concrete handlers change.

## 4. CDK package extraction & the CDK ⇄ engine boundary

### 4.1 The boundary (governing principle)

Two responsibilities, one allowed dependency direction. Everything in §4–§8 is a
consequence of this; when a "which side does X belong on?" question arises,
answer it here first.

**The CDK owns _how to talk to a data system_.** Given a connector definition
and a connection's parameters it can: build a transport (SQLAlchemy / ADBC /
HTTP), resolve credentials through an injected seam, translate native types ↔
Arrow canonical types, and build/parameterise queries. It also **defines** the
read/write contracts (`Readable` / `Writable`) that connectors implement — but
the concrete source/destination handlers that *carry out* read/write stay
engine-side (`src/source/`, `src/destination/connectors/`) and merely **conform**
to these CDK contracts. Phase 1 moves the plumbing and the contracts, **not** the
concrete read/write code. The CDK is **vendor-neutral** (knows SQL-in-general,
never a specific database — the "would this change when you add Clickhouse?"
test, architecture §4) **and transport-neutral** (knows nothing of gRPC, the
streaming wire protocol, or how a run is orchestrated).

**The engine owns _how data flows through a pipeline run_.** Orchestration, the
gRPC server + wire protocol, run/checkpoint state, the pipeline/stream/endpoint
domain models, scheduling, the runner. The engine **consumes** the CDK.

**The one rule — the dependency points one way: engine → CDK, never back.** No
CDK module may import anything engine-side (`src/grpc`, `src/state`,
`src/models`, `src/source`, `src/engine/{engine,orchestrator,pipeline}`,
`src/destination/server.py`, `src/runner.py`, `src/main.py`). Anything that must
cross from engine to CDK crosses as **a plain value or a CDK-owned type — never
an engine object.** This single rule is what every decoupling in §4.3 enforces,
and it is what lets the CDK install into the infra Lambdas without dragging the
engine in.

**Seams (dependency inversion).** Where the CDK needs a capability whose
implementation is engine- or deployment-specific, the CDK defines the abstract
Protocol and the other side supplies the implementation — so the dependency
still points *into* the CDK:

| Capability the CDK needs | CDK-owned seam | Implemented by |
|---|---|---|
| Credential fetching | `SecretsResolver` (ABC, exists) | engine: local/in-memory (OSS) · infra: `S3SecretsResolver` |
| Incremental-read checkpoint/cursor state | `CheckpointStore` (Protocol, **new**) — `get_cursor` / `save_cursor` | engine: `StateManager` |

The connector's type-map **data** (`type-map.json` / `type-map-write.json`) is
plain data the CDK reads from a path — it is not a seam.

**Allowed external (PyPI) dependencies of the CDK**, split into a small required
core and opt-in extras so a consumer pulls only what its role needs:

- **Core (required):** `sqlalchemy` + `pydantic`. This is the SQL *control-plane*
  surface — `cdk.sql` discovery + standalone `create_table` over SQLAlchemy, the
  `ConnectionRuntime`/transport seam, the type-map (string surface) and secrets
  machinery. A consumer that only introspects schemas and creates tables installs
  this and nothing else.
- **`arrow` extra → `pyarrow`:** the columnar streaming surface
  (`schema_contract`, `sql_types`, `sql.adbc_reader`, `type_map.parse_arrow_type`,
  the `GenericSQLConnector` read/write path).
- **`api` extra → `aiohttp`:** the HTTP transport for API connectors.
- **`streaming` extra:** convenience alias for `arrow` + `api` — the full
  connector surface the engine consumes.

Plus the per-driver DB packages a connector pulls in. The split is enforced by
lazy imports: `cdk.sql` and `cdk.type_map` resolve the Arrow helpers (and the
HTTP transport its `aiohttp` session) only on first use, so importing the core
control-plane surface never pulls `pyarrow`/`aiohttp`. Explicitly **not**
`grpcio` / `protobuf` — gRPC is the engine's transport, not a
database-connectivity concern (see §4.3 item 1).

### 4.2 The move

These modules move into the CDK. A row marked **as-is** has no engine-side
imports today (verified by static audit); the three rows marked **(decouple)**
violate §4.1 and must be cleaned first — see §4.3.

| Move into the CDK | Current path | Status |
|---|---|---|
| transport factory + dataclasses + `build_transport()` | `src/shared/transport_factory.py` | as-is |
| `ConnectionRuntime` | `src/shared/connection_runtime.py` | **(decouple)** |
| ADBC driver sub-registry | `src/shared/adbc_registry.py` | as-is |
| shared DB helpers | `src/shared/database_utils.py` | as-is |
| query builder | `src/shared/query_builder.py` | as-is |
| rate limiter (used by transport + runtime) | `src/shared/rate_limiter.py` | as-is |
| expression resolver + derived fns | `src/engine/resolver.py`, `src/engine/derived_functions.py` | as-is |
| type-map engine | `src/engine/type_map/` (`mapper.py`, `loader.py`, `rules.py`, `arrow.py`, `exceptions.py`) | as-is |
| secrets protocol + local/memory resolvers | `src/secrets/` | as-is |
| **CDK-native value types** (`AckStatus`, `Cursor`, `SchemaSpec`, `BatchWriteResult`, `EndpointScope`, `CheckpointStore`) | **new** `cdk/types.py` | new |
| destination base + result type | `src/destination/base_handler.py` | **(decouple)** |

> The **handler registry** (`src/destination/connectors/__init__.py`) is
> **deliberately not** in this list: importing it eagerly pulls in all four
> concrete handlers (which import `src/grpc`, `src/engine/type_map`, …), so under
> §4.1 it cannot move until those handlers are pluggable. That is §7 — Phase 1
> leaves the registry and the four concrete handlers engine-side, untouched.
> See §4.3 item 3.

### 4.3 Per-file decoupling (the three §4.1 violations)

Three **structural** violations were confirmed against the code: `base_handler.py`
and `connection_runtime.py` (in the move-list, fixed below) and the handler
registry (deferred to §7 — §4.2). Each is fixed the same way — by moving a
**value or a CDK-owned type** across the boundary instead of an engine object.

The **exact** import-closure (which small leaf utilities must travel with the
seeds) is **not** hand-maintained here; it is computed and frozen by the
import-linter gate (§4.4) run against the real engine tree at implementation
time. The gate, not this prose, is the source of truth for completeness.

1. **`base_handler.py` → gRPC types.** It imports `AckStatus`, `Cursor`,
   `SchemaMessage` from `src/grpc/generated/…` and uses them in its public
   `configure_schema` / `write_batch` signatures and in `BatchWriteResult`.
   Fix: introduce the CDK-native equivalents (`cdk/types.py`, §3) and rewrite the
   **abstract** `base_handler.py` + `BatchWriteResult` onto them. The protobuf
   messages stay engine-side. **Two translation sites, not one:** `server.py`
   converts protobuf ↔ `cdk/types.py` at the wire boundary, **and** the four
   concrete destination handlers (`database` / `stream` / `file` / `api`, which
   stay engine-side and also speak gRPC types directly today) are ported to the
   CDK types in their signatures to satisfy the abstract base. Keep the
   `AckStatus` integer values aligned with `stream.proto` so the mapping is
   mostly identity.

2. **`connection_runtime.py` → engine model `EndpointRef`.** `type_mapper_for()`
   lazily imports `EndpointRef` from `src/models/stream.py` and reads only
   `ref.scope`. Fix: pass the value, not the model —
   `type_mapper_for(*, scope: EndpointScope)`, where `EndpointScope` is a **new**
   CDK-native `StrEnum` (`"connector"` / `"connection"`) in `cdk/types.py`. The
   engine maps `EndpointScope(ref.scope)` at the boundary; constructing the enum
   raises on an unknown value, so the `scope ∈ {connector, connection}` check is
   preserved (today it lives in `EndpointRef.__post_init__`, which also enforces
   non-empty `connection_id` / `endpoint_id`). `EndpointRef` + `from_dict()`
   (dict type-guard, unknown-key and required-key checks) stay engine-side. With
   the lazy import gone, **`src/models/stream.py` does not move to the CDK and
   the CDK references no engine model.**

3. **`connectors/__init__.py` → concrete handlers (registry).** The registry
   *mechanism* (a string→class map + register/lookup) is vendor-neutral and
   CDK-appropriate, but the file eagerly imports the four concrete handlers,
   which pull in `src/grpc`, `src/engine/type_map`, `src/secrets`, … Extracting
   it is inseparable from making the handlers pluggable, which is §7. **Phase 1
   leaves it engine-side, unchanged.** §7 lifts the mechanism into the CDK and
   replaces the static imports with entry-point discovery.

**Stays in the engine** (consumes the CDK): `src/grpc/`,
`src/destination/server.py`, `src/state/`, `src/engine/engine.py`,
`src/engine/orchestrator.py`, `src/engine/pipeline.py`, `src/runner.py`,
`src/main.py`.

Existing public shapes the CDK must preserve (verified):

```python
# transport_factory.py
@dataclass(frozen=True)
class SqlAlchemyTransport:  engine: AsyncEngine; driver: str; dialect: str
@dataclass(frozen=True)
class AdbcTransport:        connect: Callable[[], Any]; driver: str
@dataclass(frozen=True)
class HttpTransport:        session; base_url; headers; rate_limiter=None

# connection_runtime.py — after `await runtime.materialize()`:
runtime.engine                 # AsyncEngine (SA path)
runtime.is_adbc                # bool
runtime.open_adbc_connection() # fresh DBAPI conn (call in asyncio.to_thread)
runtime.driver / .driver_string / .connector_type
runtime.connector_type_mapper  # TypeMapper
runtime.acquire() / runtime.close()

# secrets/protocol.py
class SecretsResolver(ABC):
    async def resolve(self, connection_id: str, *, keys: list[str] | None = None) -> dict[str, str]: ...
    async def close(self) -> None: ...
```

- **Package layout (CONFIRM-3):** `cdk/` (or `analitiq_connectors/`) as a
  top-level installable package within `analitiq-core`, with its own
  `pyproject.toml`, so infra can `pip install "git+…/analitiq-core@vX.Y.Z#subdirectory=cdk"`
  (or a dedicated published artifact). Engine depends on it in-tree.
- **Acceptance:** engine test suite green after the engine switches to importing
  from the CDK package; no engine runtime module imports the CDK's *internals*
  (only its public surface); and **the §4.1 one-rule holds** — the CDK imports
  nothing engine-side (`src/grpc`, `src/state`, `src/models`, `src/source`,
  `src/engine/{engine,orchestrator,pipeline}`, `server.py`, `runner.py`,
  `main.py`).
- **Import-audit gate (enforces §4.1):** a CI check — an `import-linter`
  "forbidden" contract (CDK ✗→ engine) or equivalent — fails the build if any
  CDK module imports an engine-side package. This is the one rule as code: it
  must be green before §4 is considered done, and it catches the next stray wire
  at PR time rather than in design review.

**Open items the engine team must confirm at implementation** (drafting audits
were inconsistent on these — verify against the real tree, the gate will force
resolution):

- **Leaf closure.** Run the gate and add every clean stdlib-only leaf the seeds
  import (candidate: a `src/shared/retry.py` retry helper, if it exists and is
  imported by transport/runtime/db-utils). These are trivial moves, but the
  list must be closed for the gate to pass.
- **`type_map/loader.py` path resolution.** If `loader.py` imports
  `src/connectors/registry.py` (`resolve_connector_dir`) to locate type-map
  files, **inject** the resolved directory/`Path` into the loader (a seam)
  instead of importing the engine resolver — the loader must be *given* its path,
  not reach for it.
- **`EndpointScope` may already exist** in `src/models/stream.py` (used by
  `connection_runtime`). If so, **relocate** it into `cdk/types.py` (engine
  imports it back) rather than authoring a duplicate, and fix
  `connection_runtime`'s top-level import alongside the lazy `EndpointRef` one.
- **`CheckpointStore` final method set.** The incremental DB read path may also
  call `get_high_water_mark` / `set_high_water_mark` beyond `get_cursor` /
  `save_cursor`; the API path uses `save_stream_checkpoint`. Finalise the seam's
  methods when the read/source path is actually extracted (a later phase).

## 5. Component: write-direction type-map (#564 — Slice 1)

Today `TypeMapper` (`src/engine/type_map/mapper.py`) provides **only**
`to_arrow_type(native) -> str` (native → Arrow canonical, the **read**
direction). `create_table` needs the **inverse** (canonical → native), which the
read map cannot supply. This is the unbuilt half of #564 and the smallest,
highest-value first slice.

- **New artifact:** `connectors/{id}/definition/type-map-write.json`, co-located
  with the read `type-map.json`. Same rule grammar as the read map but inverted:

  ```json
  [
    { "match": "exact", "native": "BOOLEAN",  "canonical": "Boolean" }   // READ (existing)
  ]
  ```
  ```json
  [
    { "match": "exact", "canonical": "Boolean",            "native": "BOOLEAN" },
    { "match": "exact", "canonical": "Int64",              "native": "BIGINT" },
    { "match": "regex", "canonical": "^Decimal128\\((?<p>\\d+), (?<s>\\d+)\\)$",
                        "native": "NUMERIC(${p}, ${s})" },
    { "match": "exact", "canonical": "Utf8",               "native": "VARCHAR(${length})" }
  ]
  ```
  Write rules match on the **canonical** Arrow type (exact or regex with named
  groups) and render the **native** DDL string, substituting `${…}` from the
  canonical parameters (precision/scale) and from per-column hints (e.g.
  `length`). First match wins, mirroring the read map.

- **`TypeMapper` API addition:** `to_native_type(canonical: str, *, params: dict |
  None = None) -> str`, fed by the write map. Raise an explicit
  `UnmappedTypeError` on a miss (no silent default), consistent with
  `to_arrow_type`.

- **`create_table` rendering:** for each `ColumnDef`, `to_native_type` →
  dialect-quoted `"name" <native>[ NOT NULL]`; append the PK clause. Quoting +
  any dialect-specific clause (e.g. BigQuery `NOT ENFORCED`) come from the
  connector / dialect strategy, not hardcoded in the CDK.

- **CONFIRM-4:** sibling file `type-map-write.json` vs extending `type-map.json`
  into a `{read:[…], write:[…]}` object. The architecture doc says "fed by the
  connector's co-located type-map" — the sibling-file form keeps the published
  read schema (a top-level array) unbroken; recommended. Infra publishes the
  matching write schema (infra breakdown §3.1) — **keep the two in lockstep.**

- **Acceptance:** `to_native_type` round-trips the canonical vocabulary for
  postgres + snowflake; `create_table` emits dialect-correct, idempotent DDL with
  **no LLM call**; unmapped canonical types raise, not default.

## 6. Component: `Discoverable` + standalone `create_table`

- **No introspection exists today.** Build `list_schemas` / `list_tables` /
  `list_columns` on the raw transport — there is no "execute arbitrary SQL" on
  `DatabaseConnector`, but `AdbcReader.fetch_page(sql, params)`
  (`src/source/drivers/adbc_reader.py`) and SQLAlchemy `conn.execute(text(sql))`
  both run arbitrary SQL. Issue `INFORMATION_SCHEMA` (or dialect equivalent)
  queries; map native column types to `canonical_type` via the read `TypeMapper`.
- **`create_table` is gRPC-shaped today.** `DatabaseDestinationHandler.configure_schema`
  takes a gRPC `SchemaMessage` and does `CREATE TABLE IF NOT EXISTS` inside the
  streaming flow. Extract a standalone `TableCreator.create_table` that the
  control-plane calls directly (it constructs DDL from `ColumnDef`s + the write
  map per §5); `configure_schema` may delegate to it internally. Post-decoupling
  (§4), `configure_schema` consumes the CDK-native `SchemaSpec`, not the gRPC
  `SchemaMessage`; `server.py` maps the wire message to `SchemaSpec` before
  dispatch.
- **Acceptance:** the three discover ops return parity with today's infra
  `db-utils` payloads for fixture DBs; `create_table` is callable with no gRPC
  server running.

## 7. Component: dynamic `HandlerRegistry` + entry-point loader

- **Boundary (per §4.1 / §4.3 item 3):** the registry *mechanism* (the string→
  class map + `register` / lookup) is vendor-neutral and lands **in the CDK**
  here; only its *population* changes — the four concrete handlers (engine-side
  until they become modules) are discovered via entry points instead of static
  imports. Phase 1 deliberately left `connectors/__init__.py` engine-side; this
  is where it splits.
- **Today:** `HandlerRegistry` (`src/destination/connectors/__init__.py`) is a
  hardcoded `{kind: HandlerClass}` dict; connectors are baked into the image and
  loaded on demand by id (no startup scan). `register_transport_kind()` exists
  but nothing calls it.
- **Change:** discover connector modules via Python **entry points** (group
  `analitiq.connectors`), register each at startup, replacing the hardcoded
  dict. Also support a **mounted connectors directory** for local dev (scan both).
  Validate at registration that the class conforms to the protocols its `kind`
  implies (via `isinstance`); fail fast on mismatch.
- **CONFIRM-5:** entry-point group name `analitiq.connectors` and the registration
  key (`kind` vs connector id).
- **Acceptance:** installing a connector package makes it usable with no engine
  code change; a malformed module fails loudly at startup, not at first use.

## 8. Component: generic SQL base (CDK building block)

A vendor-neutral, **dialect-agnostic** SQL-database base class in the CDK that
implements `Readable`/`Writable`/`Discoverable`/`TableCreator` against the
transport + `QueryBuilder` + `TypeMapper`. A **thin** connector subclasses it
with no overrides (data + driver only); a **thick** connector overrides the
dialect-specific bits (DDL quoting, pagination, type quirks). The base must
never contain anything vendor-specific (the "would this change when you add
Clickhouse?" test — architecture §4).

- **Acceptance:** postgres works as a thin connector (subclass, no overrides);
  a deliberately-quirky fixture works by overriding only `create_table` DDL.

## 9. Secrets seam (note)

The CDK ships the `SecretsResolver` ABC + local/in-memory resolvers (OSS). The
**cloud** `S3SecretsResolver` is implemented **infra-side** (infra breakdown
§3.4) against this protocol — no engine change. Keep `ConnectionRuntime`'s
construction `(definition, connection params, SecretsResolver)` so the resolver
is injectable per use.

## 10. Sequencing

1. **§5 write-map (Slice 1)** — smallest, proves connector-owned data; can ship
   inside the current `TypeMapper` before the package is fully carved out.
2. **§4 CDK extraction** — carve the package; engine consumes it (no behaviour
   change).
3. **§7 dynamic registry** — entry-point loader; convert built-in connectors to
   modules.
4. **§6 Discoverable + standalone create_table** — on the transport's raw-SQL
   primitive.
5. **§8 generic SQL base** — factor the shared SQL implementation; postgres becomes
   thin.
6. Hand the published CDK + modules to infra for the fleet rebase (infra
   breakdown).

No backwards-compat shims (repo policy): cut over hard, don't dual-support.

### 10.1 Status against the plan (as of 2026-06-01)

Phase names follow the unified plan in
[connector-module-architecture.md](./connector-module-architecture.md) (Phase
0–5); the ADR § column points at the component spec above. Tickets are cut
per-component as work starts — there is no milestone/board.

| Phase | Work (ADR §) | Ticket | Status |
|-------|--------------|--------|--------|
| 0 | Write-direction type-map: `to_native_type` + `WriteTypeMapRule` (§5) | #564 | **Done** |
| 1 | CDK extraction — plumbing + contracts, engine consumes it, no behaviour change (§4) | #127 | **Done** — PR #128 (open; merges to close #127) |
| 2A | `Discoverable` (list_schemas/tables/columns) + standalone `create_table` (§6) | #131 | **Done** — PR #131 (merged). CDK `cdk/cdk/sql/` building blocks; engine not yet a caller. |
| 2B ("Engine B") | Dynamic `HandlerRegistry` / `analitiq.connectors` entry-point loader (§7) + generic SQL base (§8) + convert postgres to a thin module | — | **In progress**. Decisions: registry keyed by `kind` and drives **both** source connectors and destination handlers; the generic base is **one** `GenericSQLConnector` implementing Readable+Writable+Discoverable+TableCreator (source read + destination write unified). |
| 2 (infra) | `S3SecretsResolver` against the CDK `SecretsResolver` protocol (§9) | infra repo | infra-side; startable once Phase 1 lands |
| 3 | Pilot rebase (postgres) + go/no-go: parity + latency + isolation | infra repo | Gated on 1–2 |
| 4 | Roll out remaining connectors (incl. MSSQL first-class); wire REST API | infra repo | Gated on 3 |
| 5 | Cleanup: delete `k2m.db` + dead ETL-staging helpers | infra repo | Gated on 4 |

**Follow-up cleanups (refinements of a shipped phase, not phases themselves):**

| Ticket | Refines | What |
|--------|---------|------|
| #125 | Phase 0 (§5) | Normalize canonical Arrow type-string vocabulary before type-map lookup |
| #126 | Phase 0 (§5) | Decide connection-vs-connector type-map composition (both directions) |
| #129 | Phase 1 (§4) | Harden `cdk.types.BatchWriteResult` cross-field invariants (cursor-on-failure guard, frozen `failed_record_ids`, IntEnum note) |
| #130 | Phase 1 (§4) | Remove dead `require_port` parameter in `ConnectionRuntime.materialize` (moved verbatim during extraction) |

## 11. Acceptance (overall)

- One driver codebase: infra's `db-utils` fleet runs on the CDK; `k2m.db` is
  deletable (infra breakdown §3.7).
- A new database ships end-to-end from the connector-builder plugin (data +
  driver + thin/thick `connector.py`) on an **unchanged** CDK — engineers are
  involved only for a brand-new *transport family* or a contract change.
- Engine streaming behaviour unchanged; engine remains a self-contained OSS
  Docker deployable.

## 12. Decisions to confirm (collected)

- **CONFIRM-1** `list_columns` return shape + `ColumnDef.canonical_type`.
- **CONFIRM-2 (revised)** keep current `Readable`/`Writable` method shapes but
  swap their types to CDK-native (`SchemaSpec`/`Cursor`/`AckStatus`/
  `BatchWriteResult`); gRPC messages stay engine-side, translated in `server.py`.
- **CONFIRM-3** CDK package name + layout + how infra pins/installs it.
- **CONFIRM-4** write-map as a sibling file vs an extended object.
- **CONFIRM-5** entry-point group name + registration key.
- **CONFIRM-6** the read-path checkpoint seam. There is **no** existing
  `StateManagerProtocol` to lift — the CDK **authors** `CheckpointStore` from the
  concrete `StateManager` (`src/state/state_manager.py`) surface: `get_cursor` /
  `save_cursor` (the DB source path). **Open question:** the API source path also
  calls `save_stream_checkpoint(...)` (hwm / page_state / conditionals), so the
  seam needs a richer method (or a unified `checkpoint(...)`) **if** the API
  source connector becomes CDK-bound. Settle the final method set when the
  read/source path is actually extracted (a later phase); for now `Readable`
  declares `checkpoint: CheckpointStore` so the contract is boundary-clean.

## 13. Out of scope (infra repo)

Lambda fleet rebase, container-image packaging/build, `S3SecretsResolver`, the
published write-map JSON schema, the REST API surface, and `k2m.db` deletion —
all in [connector-modules-infra-work-breakdown.md](./connector-modules-infra-work-breakdown.md).
