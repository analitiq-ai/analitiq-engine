# SQL Write Path: Stage-Then-Merge Across Transports

**Scope:** This document defines the destination SQL write primitive — how every SQL
write lands and commits, on both transports — and everything that hangs off it:
the facade/backend split inside the CDK, the sanctioned extension surface for
thick connectors, the dialect-capability block in `connector.json`, stage-table
lifecycle, transaction boundaries, and engine-side batch coalescing. The
conformance kit enforces this document: a connector that diverges from it fails
CI. Section 8 is the exception — engine-side coalescing and the `write_unit`
declaration it consumes are specified here but not yet built, so the engine
does not coalesce and a `write_unit` declaration currently has no consumer.
The read path, discovery, the mapping layer, and the gRPC ack protocol are
unchanged except where this document names them.

Related docs: transport strategy rationale in
[pyarrow-and-destinations.md](pyarrow-and-destinations.md), wire protocol in
[grpc-streaming-architecture.md](grpc-streaming-architecture.md), CDK packaging
and the connector contract in
[connector-module-architecture.md](connector-module-architecture.md).

## 1. What this prevents

The failure mode this design exists to prevent is **the same author intent
taking a different primitive per transport**. When `upsert` means a direct
dialect statement on one transport and a stage table plus `MERGE` on another,
every write-path rule has to be stated twice, the two copies drift, and each
new system multiplies the divergence rather than adding to it. Per-dialect
divergence was the fastest-growing defect class in this codebase, and guessed
base-class defaults — behavior right for one database family and silently
wrong for the next — were its mechanism.

Three properties follow, and the rest of this document is their consequence:

- **One primitive.** Every SQL write on every transport is stage-then-merge
  (§2), so no rule has a transport-specific copy to drift from.
- **A sanctioned thick-connector surface.** A system with a native bulk
  protocol reaches it through the declared `bulk_land` hook (§4), never by
  overriding CDK internals. Contract-less coupling to private methods breaks
  silently on any refactor, which is why the conformance kit fails it (§10).
- **Declared facts, not guesses.** What a system can do is validated data in
  `connector.json` (§5); how to render it is a small dialect class. A
  needed-but-undeclared capability refuses loudly instead of defaulting.

Engine-side coalescing (§8) exists for a narrower reason: the wire protocol is
strictly one batch → one ack → cursor persisted, so a destination can never
hold more than one unacked batch and has nothing to coalesce on its own. One
small load job per source batch walks into per-table load-job quotas.

## 2. The write primitive

**Every SQL write is stage-then-merge: land the batch in a stage table, then
run exactly one mode statement from stage to target.** Both backends, all
three modes, every dialect.

Per mode, the single statement from stage to target is:

- **`upsert`** — the dialect's declared merge form on the stream's
  `conflict_keys`: `MERGE INTO`, `INSERT … ON CONFLICT DO UPDATE`, or
  `INSERT … ON DUPLICATE KEY UPDATE` (§5). Empty `conflict_keys` refuses
  loudly and never downgrades to insert.
- **`insert`** — one set-based anti-join
  `INSERT INTO target SELECT … FROM stage WHERE NOT EXISTS (…identity match…)`.
  Identity is the contract primary key, or the synthetic `_record_hash` column
  for a keyless stream. The statement is plain ANSI and runs on both backends,
  which is what makes insert exactly-once wherever the system enforces the
  identity constraint (§9) regardless of transport — there is no per-transport
  insert mechanism and so no parity gap to maintain.
- **`truncate_insert`** — empty the target once on the read's first batch
  (`batch_seq == 1`), then a plain
  `INSERT INTO target SELECT … FROM stage` append. The emptying statement is
  rendered by the dialect's `empty_table_sql` (§4) — base ANSI
  `DELETE FROM target`, overridable for systems whose DELETE grammar deviates
  (BigQuery requires a `WHERE` clause) — and never the `TRUNCATE` statement,
  which implicitly commits on Redshift and others and would break the §7
  single-transaction promise. No identity dedup: deduping a full refresh would
  collapse legitimate duplicate rows.

### Intra-batch duplicate rules

The stage feeds set-based statements, so duplicate keys inside one batch must
be resolved before the mode statement runs — a merge with two matching source
rows fails on most systems, and an anti-join admits both copies. The rules,
applied in Arrow space before landing:

- **`insert`** — duplicate identities collapse to the **first** occurrence,
  applied where the identity column is attached to the batch.
- **`upsert`** — duplicate `conflict_keys` inside one **source batch** are a
  loud failure (`ON CONFLICT` raises "cannot affect row a second time";
  `MERGE` raises "multiple source rows match"): a single source page
  carrying the same key twice means the source's `conflict_keys` are not
  actually unique, and no collapse rule can be correct. Coalescing (§8)
  never manufactures this case: duplicate keys *across* the source batches
  of one merged unit are collapsed by the engine's coalescer before the
  unit is built, keeping the later batch's row — byte-for-byte the row a
  sequence of per-batch merges would leave in the target. The destination
  never sees a duplicate an uncoalesced run would not produce, so this rule
  adds no contract surface.
- **`truncate_insert`** — no collapsing, as above.

### Landing

Landing into the stage is **executemany `INSERT` by default, bulk-load by
declaration**. A connector whose system has a native bulk protocol declares it
(§5) and implements the `bulk_land` dialect hook (§4); the CDK then uses it.
The hook is a pure speed slot: stage contents are identical either way, the
downstream mode statement is the same statement, and a declined bulk land
falls back to executemany with an INFO-level log (a speed downgrade is
visible, never silent). `adbc_ingest` is not an ADBC-private code path but
that backend's declared bulk mechanism, and a system's own protocol —
`LOAD DATA LOCAL INFILE`, `COPY`, a load-job API — lands here too.

## 3. Facade and backends

`GenericSQLConnector` remains the single **semantic owner**: write modes,
truncate gating, identity and duplicate rules, `conflict_keys` refusal,
statement timeouts, retry verdicts, the exception → `AckStatus` /
`FailureCategory` ladder, and readiness gates all stay in the facade,
defined once. Transport mechanics move behind a backend interface:

```python
@dataclass(frozen=True)
class StageWritePlan:
    """Everything a backend needs to execute one batch write.

    Built by the facade: addresses from the dialect's TableAddress factory,
    SQL text from the dialect's rendering hooks, scope and transaction shape
    from the connector's declared capabilities.
    """
    stage: TableAddress            # deterministic stage address (section 6)
    target: TableAddress
    scope: StageScope              # TEMP or REAL, from the declaration
    transactional: bool            # from the declaration (section 7)
    create_stage_sql: str          # dialect.stage_table_sql(...)
    truncate_sql: str | None       # first truncate_insert batch only:
                                   # dialect.empty_table_sql(target) (section 2)
    mode_sql: str                  # the one mode statement (section 2)
    drop_stage_sql: str
    columns: tuple[str, ...]       # landing column order, identity included


class TransportBackend(ABC):
    """Executes plans; owns connections, cursors, and commit calls.

    Holds no write-mode logic and no step order. SqlAlchemyBackend serves
    both engine flavors (async engine, and sync engine on a worker thread)
    through one shared sync-Connection body, never a per-flavor fork.
    AdbcBackend owns the ADBC connection, its locks, and reopen/poison
    handling.
    """

    async def connect(self, runtime: ConnectionRuntime) -> None: ...
    async def disconnect(self) -> None: ...
    async def run_ddl(self, statements: Sequence[str]) -> None: ...
    async def target_columns(self, target: TableAddress) -> tuple[str, ...]: ...
    async def health_check(self) -> None: ...

    async def execute_write(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Run one stage cycle for the batch under the backend's write lock.

        The body is the shared StageCycle; what a backend adds here is the
        cancellation discipline of its own lock.
        Success is returning without raising. Deliberately returns nothing:
        database rowcounts lie about the batch (an idempotent replay's
        anti-join affects 0 rows; MySQL upserts count 2 per updated row),
        so the facade reports records_written from the batch's own row
        count -- a backend rowcount never reaches the ack."""
```

**The step order is not a backend's either.** `StageCycle` owns it once
above both transports: the pre-flight drop, the stage creation, the
landing and its verification, the optional target-emptying statement, the
mode statement, the drop after success or failure, the poisoning rule
(§6) and the honest orphan log. It is a sync object — the cycle is
already on a worker thread (or inside `AsyncConnection.run_sync`) when it
starts — handed the live connection as a `StageConnection`:

```python
class StageConnection(Protocol):
    def run_statement(self, sql: str, *, commit: bool) -> None: ...
    def land_rows(self, plan: StageWritePlan, batch: pa.RecordBatch) -> int: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def invalidate(self) -> None: ...
```

`land_rows` returns the rows now staged, so land verification is one copy
in the cycle while each transport still lands its own way (executemany,
`adbc_ingest`, a declared bulk mechanism); a landing that ran the
untrusted `bulk_land` hook reports what the stage actually holds, and a
count that is not the batch's is refused before any target mutation. The
session-schema guard (§4) is inside the ADBC transport's `land_rows`, a
precondition of that transport's ingest mode rather than a cycle step.
`commit` and `rollback` are what let one body express both transaction
shapes; `invalidate` is how a transport satisfies the poisoning rule —
discarding a pooled SQLAlchemy connection, closing the cached ADBC handle.

`execute_write` stays on the backend rather than moving into the cycle:
the SQLAlchemy backend shields its thread future and re-awaits on
cancellation to keep holding the *write* lock, while the ADBC backend
shields and attaches a done-callback because its thread finishes or
abandons under the *op* lock. Different locks, different cancellation
contracts, and one discipline cannot cover both. `target_columns` stays
for the mirror-image reason: SQLAlchemy reflects, ADBC reads the
description of a zero-row probe — same intent, two mechanisms — and it is
called after DDL, outside any write cycle.

**A plan is complete for every dialect-specific write statement, and the
backends render no SQL at all.** The generic statements around the write
— the liveness probe, the stage row count, the zero-row column probe, the
qmark landing `INSERT` — are plain ANSI whose only dialect-aware part is
identifier quoting, so they are rendered once above the transport seam
through the dialect and handed down; a backend executes what it is given.

The facade prepares the batch once (type casting, `_record_hash` attachment,
duplicate collapsing — all semantics) and hands the backend Arrow; each
backend converts to its parameter shape internally (dict records for
SQLAlchemy executemany, Arrow straight through for `adbc_ingest`). Registry
resolution, the connector contract, and the handler surface
(`BaseDestinationHandler`) are unchanged; connectors are unaffected by the
split itself.

The split covers the destination write path only; the read path resolves its
transport divergence through the QueryBuilder and is out of scope.

## 4. The dialect surface

`SqlDialect` keeps its role: per-system subclasses in connector packages
override exactly the quirks their system has; the connector class is
`dialect_class = XDialect` and nothing else. The write-path hooks:

**Rendering hooks**

```python
def stage_table_sql(self, stage: TableAddress, target: TableAddress,
                    *, temp: bool) -> str:
    """CREATE [TEMPORARY] TABLE shaped like target, on both backends;
    temp comes from the declared stage scope. Base raises
    UnsupportedDialectOperationError."""

def merge_statement_sql(self, stage: TableAddress, target: TableAddress,
                        conflict_keys: Sequence[str],
                        columns: Sequence[str]) -> str:
    """The upsert statement from stage to target, in the dialect's declared
    merge form. Serves upsert only: insert uses the ANSI anti-join and
    needs no dialect hook. Base raises."""

def bulk_land(self, conn: Any, stage: TableAddress, batch: pa.RecordBatch,
              *, runtime: ConnectionRuntime) -> bool:
    """Native bulk load into the stage. Return True if landed; False
    declines and the backend falls back to executemany (logged INFO).
    Called only when the connector declares a bulk mechanism (section 5);
    conn is the backend's native connection object. runtime is the same
    resolved ConnectionRuntime the backend connected with: a dialect whose
    mechanism runs through the system's own client rather than the
    transport connection (load_job) ignores conn, builds its client from
    runtime inside the call, and discards it when the call returns. No
    client is cached on the dialect or the backend, so there is no
    stale-client lifecycle to manage: a reconnect or credential rotation
    has nothing to invalidate, and per-call construction is noise at
    coalesced-unit granularity (section 8). A dialect holding private
    client state instead is the coupling this hook exists to replace.
    Base returns False."""
```

The anti-join insert and the append statement are plain ANSI rendered by the
CDK through the dialect's quoting (`quote_table`, `quote_ident`) — not hooks,
because no per-system divergence exists to express. The target-emptying
statement has exactly one known divergence (BigQuery's DELETE requires a
`WHERE` clause), so it follows the `current_timestamp_default` pattern: a
base-implemented render, `empty_table_sql(target: TableAddress) -> str`,
returning ANSI `DELETE FROM <target>`, overridable.

**Retained hooks, and the per-connection composition order**

On every new pooled connection, in order: `verify_tls_state` →
`session_init_sql` → the connection is usable. Per batch: the stage cycle
of §2, with the session-schema guard (`adbc_session_schema_sql`, checked
when the declared session-targeting mode is `session_default`) immediately
before the bare-name landing it protects — the only statement of the cycle
that is not fully qualified, since every statement a plan carries is
rendered with its address.
`adbc_ingest_kwargs`, TLS connect-arg hooks, DDL/discovery hooks, and the
identifier hooks are unchanged.

**Not on the class surface:** capability *facts* are declared data (§5), never
dialect methods. The dialect renders only; there are no `supports_*` booleans
to mirror a declaration, and one `stage_table_sql` serves both backends.

## 5. Declared capabilities in `connector.json`

Guessed defaults are the mechanism of per-dialect divergence: base-class
behavior right for one family and silently wrong for the next. SQL-shape
capabilities are facts about the target system — not derivable from protocol
conformance, unlike the operation capabilities the connector-module contract
rightly refuses to declare — so they are declared as data.

A schema-validated block in `connector.json`:

```json
"sql_capabilities": {
  "catalog": "none" | "read" | "full",
  "session_targeting": "per_statement" | "session_default",
  "merge_form": "merge" | "insert_on_conflict" | "insert_on_duplicate_key" | "none",
  "bulk_load": {
    "sqlalchemy": "copy_from" | "load_data_local_infile" | "load_job",
    "adbc": "adbc_ingest" | "copy_from" | "load_data_local_infile" | "load_job"
  },
  "stage": {
    "scope": "temp" | "real",
    "schema": "target" | "dedicated",
    "dedicated_schema": "<name, required iff schema is dedicated>",
    "transactional_ddl": true | false
  },
  "limits": {
    "max_bind_params": 2100,
    "max_identifier_len": 63
  }
}
```

`bulk_load` maps each SQL transport family to the mechanism its
connections land with — the mechanism is a fact about a transport, not
the connector as a whole (`copy_from` needs the driver's wire
connection; `adbc_ingest` needs an ADBC cursor). An absent family lands
via executemany, the declared default; an empty object declares no bulk
anywhere; a mechanism a family cannot run (`adbc_ingest` under
`sqlalchemy`) is unrepresentable — the parse refuses it, so no
downstream consumer ever meets a declared-but-unrunnable mechanism. A
dual-transport connector declares both entries (postgres: ADBC
connections ingest natively, SQLAlchemy connections COPY) instead of
picking one and silently falling back on the other.

and the connector-level (not SQL-specific) declarations:

```json
"write_unit": { "rows": 200000, "bytes": 33554432 },
"concurrency": { "max_connections": 8 },
"error_map": {
  "sqlstate":    { "08": "unreachable", "28000": "auth", "23": "write_rejected" },
  "exception":   { "OperationalError": "transient" },
  "vendor_code": { "1045": "auth" },
  "http":        { "429": "rate_limited", "401": "auth" }
}
```

Properties:

- **Refuse, don't guess.** Every CDK consumer site treats a
  needed-but-undeclared capability as a loud configuration error at config or
  handshake time — a customer-safe `CONFIG_INVALID`-class message naming the
  missing declaration. No base-class default ever fills in a guess. An upsert
  stream against `merge_form: "none"` refuses; a bare `bulk_land` override
  without a declared `bulk_load` mechanism is never called (and fails the
  conformance suite, §10).
- **One source of truth per fact.** The JSON declares *whether* the system
  has a shape; the dialect class renders *how* to write it. The
  `supports_*` class booleans are deleted, not mirrored.
- `"load_job"` names the mechanism of systems whose bulk path is a load-job
  API driven through the system's own client rather than the transport
  connection (BigQuery). It lands into the per-batch stage table like every
  other mechanism — which is what makes §8's quota shape hold.
- **The block reaches the worker in the resolved payload.** The isolated
  worker never reads `connector.json` — its bootstrap carries only resolved
  values (`build_bootstrap` / `ConnectionRuntime.from_resolved_payload`).
  The engine validates the capability block and folds it into that payload,
  the same channel that already delivers type maps and endpoint documents,
  so the facade and backends consume declared capabilities from the runtime
  in the process where writes execute — never a guessed default because the
  definition file was out of reach.
- Validated offline by the published validator like every other contract
  surface, and visible to any consumer of the connector definition.
- `write_unit` sits at the connector level because it is not a SQL fact —
  any destination whose write cost is per-write-operation (file/S3 sinks
  included) may declare it, and the engine consumes it transport-agnostically
  (§8). Absent means "no preference": the engine does not coalesce.
- **`error_map`, `limits` and `concurrency` are additive, not
  refuse-don't-guess.** A missing
  `merge_form` blocks an upsert; a missing limit or error mapping cannot
  block anything — absence means "no declared cap / no declared mapping" and
  current behavior applies. A runtime failure caused by an undeclared cap or
  mapping is a connector defect, fixed by declaring it — never worked around
  in the engine. Declared content is still validated fail-loud
  (`cdk.sql.capabilities` for `limits`, `cdk.declarations` for `error_map`
  and `concurrency`) at config load on the trusted side; `error_map` and
  `limits` re-validate where the resolved payload is parsed (`concurrency`
  has no worker-side consumer — the engine's fan-out is its only reader).
- **`error_map` declares facts, never verdicts.** The value vocabulary is
  engine-owned — `transient | config | auth | unreachable | rate_limited |
  write_rejected` — and the engine alone derives `AckStatus`,
  `FailureCategory`, and `ErrorCode` from it (the per-context verdict
  tables in `cdk.declarations`, the same trust rule as retry semantics,
  §9). Matching happens at the failure's birth site against the immediate
  exception (plus at most its single explicit driver link — SQLAlchemy's
  `orig` or `raise ... from`); the verdict then crosses process
  boundaries as structured signals (the worker's deterministic flag and
  `declared_category` wire field, the ack's failure category) — never
  re-derived downstream from chains or text. The heuristics are demoted
  to last resort, per context: the read path resolves declared verdicts
  (the birth-site category on the typed error, then the map) → sanctioned
  typed errors; the write ack ladder resolves its typed engine errors
  (type-map, dialect, TLS — engine contracts a driver map must not
  re-route) → declared map → class-name heuristic; the ADBC boundary and
  both HTTP sites resolve declared map → built-in heuristic. The
  engine-side classifiers log when a text heuristic decided.
- **`limits` consumption.** The executemany stage landing chunks rows by
  `floor(max_bind_params / column_count)` (`StageWritePlan.rows_per_statement`,
  applied identically by both transport backends); stage-name rendering and
  `CREATE TABLE` DDL validate identifiers against `max_identifier_len`
  instead of assuming the dialect default.
- **`concurrency` consumption.** The engine's stream fan-out paces streams
  sharing a source connection to at most `max_connections` concurrent —
  connector-level because API systems have connection ceilings too.

## 6. Stage lifecycle

**Naming** is a deterministic token, `sha256(run_id|stream_id|batch_seq)[:16]`,
in the grammar `_analitiq_stage_b<sha16>_<target>`: the fixed prefix and hash
come first, and the target-name tail is readability only, truncated to the
dialect's identifier budget. The order matters — with the tail first, a short
identifier budget (Postgres' 63-byte NAMEDATALEN against any target longer
than 29 characters) truncates the *hash*, distinct stages collapse into one
name, and a pre-flight drop can destroy another batch's in-flight stage. The
token alone is unique; the tail may be cut freely. A
retry of the same batch computes the same name, so the pre-flight
`DROP TABLE IF EXISTS` — run for any non-transactional stage, temp scope
included, since a failed batch can leave a session-temp table on the pooled
connection — finds and clears its own leftovers: stage cleanup is
self-healing across retries by construction, never dependent on a cleanup
pass. The pre-flight drop is safe against a still-running prior attempt by
mutual exclusion, not luck: a backend runs at most one stage cycle at a
time per handler — the whole cycle holds the backend's write lock
(`SqlAlchemyBackend.execute_write` takes `self._write_lock`; the ADBC
backend's connection lock serializes all cursor work) — so a sync-transport
attempt that cannot be cancelled in-band finishes or abandons its cycle
before a retry's cycle can begin. The sync flavor runs on a worker thread
that cannot be cancelled in-band at all, so `execute_write` shields the
thread future and holds the lock through a cancellation until the thread is
done. A retry only ever meets a completed or abandoned stage, never a live
one.

**Scope is declared, temp preferred.** `stage.scope` in the capability block:

- **`temp`** — a session-scoped temporary table: invisible to other sessions,
  dropped by the system on disconnect, no DDL in the customer's schema.
  Declared by systems where a session-temp table is visible to the same
  connection's mode statement (Postgres, MySQL, Redshift, Snowflake).
- **`real`** — an ordinary table, for systems without usable session-temp
  semantics (BigQuery). `stage.schema` places it: `"target"` (the target
  table's schema) or `"dedicated"` with a named
  `dedicated_schema`, keeping stage DDL out of customer schemas entirely
  (the pattern Airbyte's internal schema and Fivetran's staging datasets
  follow). Real-scope stages should additionally carry the system's
  expiration mechanism where one exists (BigQuery table expiration), so an
  orphan is time-bounded even after a process crash.

**Cleanup and poisoning.** These rules are the stage cycle's, so they read
the same on every transport, and they govern the **non-transactional**
path (`transactional_ddl:
false`, §7). On the transactional path they do not apply: every step,
the drop included, lives inside the batch transaction, so a failed drop
aborts the transaction and the batch returns a retryable failure — success
is never acked past a failed step, and nothing can be committed or orphaned.

- The stage is dropped after the mode statement, success or failure, in both
  scopes (a long-lived session accumulates temp stages otherwise).
- Failure path: best-effort drop, then the batch fails with its own error.
- Success path: the drop is attempted twice; if both attempts fail the
  connection is poisoned (a failed DROP means a possibly-dead connection —
  the next batch must not inherit it) and the log tells the truth: for real
  scope, the named stage table is orphaned and needs manual cleanup or the
  expiration policy — no false "will be cleaned up on retry" promises.
- **Poisoning is scoped to this path, and to every failed step of it.**
  A step that fails here leaves unknown session state and a stage that may
  have committed, so the connection is discarded and the next batch
  reopens. On the transactional path the rollback *is* the recovery —
  nothing committed, so nothing survives to clean up — and a rejected row
  says nothing about the connection's health. Discarding a pooled
  connection on every constraint violation is what no connection pool
  does: HikariCP evicts on connection-level SQLStates, SQLAlchemy
  invalidates on `is_disconnect`, neither on every DBAPI error.
- The session-schema invariant guard holds throughout: under
  `session_targeting: "session_default"`, the session schema must equal the
  target schema before any bare-name landing runs.

## 7. Transaction boundaries

Declared per system as `stage.transactional_ddl`:

- **`true`** — the backend runs create-stage, land, mode statement, and drop
  in **one transaction**: an interrupted batch leaves nothing, not even a
  stage. Postgres and Redshift support this outright. MySQL does **not**
  qualify even with `temp` scope: `CREATE TEMPORARY TABLE` avoids the
  implicit commit of regular DDL, but temporary-table DDL still cannot be
  rolled back — a failed batch can leave the temp table sitting on the
  pooled session — so MySQL declares `false` and relies on the pre-flight
  drop (§6) like every other non-transactional system.
- **`false`** — systems whose DDL self-commits or whose loads are their own
  commit unit (Snowflake, BigQuery) run the steps with per-step commits.
  Safety then comes from the primitive itself, not atomicity: deterministic
  stage names make retries self-healing (§6), the mode statement is
  idempotent on identity for upsert and insert (§9), and the poisoning
  rules bound connection reuse after a failed step. This is the documented
  semantics, not a degraded mode — it is how every load-job warehouse
  pipeline works. `truncate_insert` is the exception it always was: its
  append phase has no identity dedup, so an append that commits before a
  lost ack duplicates on the retry — the mode's documented at-least-once
  contract (§9), not a stage-cycle defect.

Both shapes are the same `StageCycle` body; what a declaration selects is
whether each step commits as its own unit or the whole cycle commits once
at the end. Both SQLAlchemy flavors reach that body over one sync
`Connection` — entered from the async flavor through `conn.run_sync(...)`
and from the sync flavor on a worker thread — so the split is by
connection acquisition and cancellation semantics only, never a forked
cycle. The ADBC backend reaches the same body over its cached DBAPI
connection.

Statement-timeout policy stays in the facade (`GenericSQLConnector`), not the
backends: one deadline covers the whole batch write, set per stream through
`set_statement_timeout`, with the known limitation that only the async
SQLAlchemy flavor can enforce it in-band.

## 8. Batch coalescing

**Not yet implemented.** This section specifies the design; the engine does not
coalesce today, and a declared `write_unit` has no consumer until it does.

**The engine coalesces source batches before sending; the wire protocol does
not change.** The destination-side alternatives — buffered
batches with deferred or windowed acks, or a flush hook with held cursors —
are rejected: both require the sandboxed, untrusted connector worker to hold
data the engine has already had acked or to participate in cursor durability,
exactly the trust this architecture withholds from connector code
(connector execution is isolated precisely because it is untrusted). Engine-
side coalescing keeps the exactly-once unit "one sent batch = one ack = one
cursor persist" byte-for-byte intact; the sent batch just gets bigger.

Mechanics:

- The coalescer sits in the engine load stage, upstream of everything
  batch-scoped: it accumulates transform-stage output per stream until the
  declared `write_unit` is reached (rows or bytes, whichever first) or the
  read ends, then concatenates the Arrow batches into one. The declaration
  reaches it through config preparation: the engine reads `write_unit` from
  the destination connector's definition while assembling the pipeline
  (`pipeline_config_prep.py`) and threads it into the runtime batching
  config the load stage already receives — the same path that delivers
  `batch_size`. For upsert streams the coalescer collapses duplicate
  `conflict_keys` across the source batches it merges, keeping the later
  batch's row (§2) — it already holds the post-mapping batches in arrival
  order, so the collapse reproduces sequential-merge semantics exactly.
  The keys it collapses on are already parsed engine-side from the stream
  definition and copied into the destination worker bootstrap; the same values
  thread into the runtime batching config alongside `write_unit` — internal
  plumbing, no new contract surface, and the destination config stays
  mode-only. One asymmetry is pinned: the unit's MAX cursor is
  computed over the *pre-collapse* rows — a dropped earlier duplicate may
  carry the unit's only watermark, and a surviving later row with a null
  or lower cursor value must not erase progress that sequential per-batch
  checkpoints would have persisted. The cursor code (`compute_max_cursor`
  over the materialized sent batch) sees only surviving rows, so the
  coalescer computes the unit's MAX cursor itself over every row it
  merges and carries it on the unit; for collapsed units the load stage
  checkpoints the carried value instead of recomputing. Everything else
  the coalescer sits upstream of — `record_ids`, `emitted_at` stamping,
  `batch_seq` assignment, retry stability, DLQ correlation — is the same
  code operating on a bigger batch. A merged batch's `batch_seq` stays
  stable across retries, which is what deterministic load-job-ID schemes
  built on it depend on.
- No timer: pipeline runs are finite reads, so the tail flushes when the
  read ends. Backpressure is unchanged — the coalescer holds at most one
  unit.
- `truncate_insert` is safe by construction: the merged first unit is sent
  as `batch_seq` 1, so truncate-once gating fires exactly once,
  covering every source page inside the unit. The zero-batch case is
  untouched: a read that yields no batches produces nothing for the
  coalescer to hold, and the engine's synthetic empty `batch_seq` 1 —
  which is what truncates the target after a clean zero-row read — is sent
  outside the coalescer's path.
- **The dlq/skip unit is the sent batch — as it always was.** Declaring
  `write_unit` consciously widens that unit: a fatally rejected coalesced
  unit is DLQ'd or skipped wholesale, good rows included, exactly as an
  uncoalesced source batch would be. Nothing is lost under `dlq`: every row of
  the unit is persisted to the dead-letter queue as a source batch's rows are
  (the per-record JSONL the DLQ writes), and recovery is the
  same operator workflow either way — over more rows. A row the upsert
  collapse dropped is represented in that unit by its surviving later
  version — same `conflict_keys` identity, newer payload — so a
  dead-lettered unit still carries one row per identity, and replaying it
  reaches the same final state sequential merges would have. What a
  failure loses relative to sequential sends is the superseded
  *intermediate* version an earlier batch would have briefly committed;
  that is part of the widened-unit cost, controlled the same way — by
  unit size. Unit size is the
  operator's control over rejection granularity: a deployment that needs
  finer DLQ isolation declares a smaller `write_unit`. The engine adds no split-and-retry machinery for this:
  whole-batch rejection without per-record attribution is this engine's
  recorded design stance (untrusted connectors cannot be trusted to blame
  individual rows), and wholesale rejection of a load unit is the norm for
  load-based warehouse pipelines.
- **Server-registered idempotency tokens include content.** Stage *names*
  stay batch-derived (`run_id|stream_id|batch_seq`): a stale stage is
  dropped and rebuilt, so the name only has to be deterministic per
  attempt. A token the *system remembers* — a deterministic load-job ID —
  is different: the system silently dedups the next
  submission under the same ID, and a supported same-`RUN_ID` restart
  resumes from the committed cursor with `batch_seq` starting over, so the
  same identity triple can carry a different payload. Such tokens must
  therefore also include a payload-sensitive component (a content hash of
  the unit) — and be scoped to one stage incarnation: an engine-level retry
  that pre-flight-drops and rebuilds the stage (§6) submits under a fresh
  job identity (an attempt component alongside the hash), because reusing
  the previous attempt's completed job ID would dedupe the submission and
  the merge would then run against the freshly rebuilt, empty stage —
  acking rows that were never applied. Attach-instead-of-resubmit is for
  *within* an attempt (a client polling timeout); across attempts, row
  idempotency already lives in the mode statement, so nothing needs
  job-level dedup. Within one attempt an identical retry still attaches to
  its in-flight job, while
  a restart's different payload gets a fresh identity instead of a silent
  no-op.
- **Size budget.** The hard bound is the gRPC message cap
  (`GRPC_MAX_MESSAGE_SIZE`), 64 MiB by default. The unit budget counts the
  Arrow payload **plus**
  per-row wire overhead — `record_ids` alone add 64 bytes per row — and the
  coalescer targets the declared `write_unit.bytes` capped at a safety
  margin below the message cap. Single-message units in the tens of
  megabytes are deliberately the ceiling: a protobuf message has no
  streaming inside it, so both containers hold ~3-4x the unit size in
  transient memory. Chunked framing (one logical batch as N wire messages
  under one ack) would lift that ceiling and is explicitly out of scope —
  an additive protocol change to revisit only if a workload proves the
  single-message ceiling insufficient.
- Quota shape, recorded so the ceiling is a decision and not an accident:
  under stage-then-merge the target table receives `MERGE` **query** jobs (a
  high-quota class), and each load job lands in its own per-batch stage
  table — the per-table load-job quota never accumulates against any one
  table. What unit count does bound are the project-level load-job quota and
  the per-table/dataset operation-rate limits, all of which scale down
  linearly with coalescing — which is exactly what `write_unit` buys.
- The write-unit fact lives in `connector.json` and nowhere else. The
  `GetCapabilitiesResponse` sizing fields are not a second declaration
  channel: they are removed, with their field numbers and names `reserved` in
  the proto so a future field can never reuse the tags against a
  mixed-version peer.

## 9. Idempotency and retry verdicts

Row identity is content-derived, never a positional ledger, and the primitive
makes the verdict table transport-independent:

| Mode | Verdict | Mechanism |
|------|---------|-----------|
| `upsert` | exactly-once | merge on `conflict_keys` from stage |
| `insert` | exactly-once where the system enforces the identity constraint; at-least-once where it does not | set-based anti-join on identity (contract PK or `_record_hash`) from stage |
| `truncate_insert` | at-least-once | truncate on first read batch, plain append after — by design |

The insert condition is the honest-verdict rule: the anti-join dedups every
sequential replay on its own, but the enforced `PRIMARY KEY` is the
structural backstop against writes that race it. A system that does not
enforce uniqueness (`pk_not_enforced` — BigQuery) has a filter, not a
guarantee, so its insert streams report at-least-once rather than promising
what the system cannot hold.

`retry_semantics` carries no per-transport rows: both backends run the same
mechanism, so a mode's verdict is a property of the mode and the target
system, never of the transport that reached it. The per-handler matrix in
[grpc-streaming-architecture.md](grpc-streaming-architecture.md) states the
same verdicts.

## 10. What the conformance kit asserts about the primitive

The contract tier (no live database) certifies this document's surface:

- **Rendering matches declaration.** The rendered stage DDL carries the
  temp form iff `stage.scope` is `temp` and the declared schema placement;
  the rendered upsert statement matches the declared `merge_form`;
  declared-but-wrong and used-but-undeclared both fail. The stream's
  `conflict_keys` must appear where the declared form states the match
  keys — the `ON` clause of a `MERGE`, the conflict target of an
  `INSERT … ON CONFLICT` — so a renderer that names the key only among
  the inserted columns and matches on something else fails. The
  `insert_on_duplicate_key` form names no keys in the statement at all
  (MySQL reads them from the unique index), and carries no such
  assertion.
- **Refusals fire.** Upsert with empty `conflict_keys`, upsert against
  `merge_form: "none"`, and any needed-but-undeclared capability produce the
  loud config error, not SQL.
- **The override surface is the sanctioned one.** A connector may override
  the §4 hooks plus `session_init_sql`, `verify_tls_state`, and the
  existing DDL/discovery/TLS hooks — and adds nothing public of its own
  (helpers are underscore-private, so a stale attribute cannot ride along
  silently); overriding a private `GenericSQLConnector` or
  backend internal fails the suite. `StageConnection` is implementable but
  not connector surface: the transports that satisfy it are the CDK's, and
  a connector class or dialect that grows its members fails the same check
  as any other public addition — the write primitive is not extended by
  supplying a second cycle to run it on.
- **Landing is semantics-free.** For a connector declaring a bulk mechanism,
  bulk-landed and executemany-landed stages produce identical stage
  contents against the suite's fakes.
- **Duplicate rules hold.** Intra-batch duplicate identities collapse
  first-wins for insert before the mode statement renders; duplicate
  `conflict_keys` inside one batch fail loudly for upsert; a replayed batch
  leaves target state unchanged for the exactly-once modes. (The
  cross-batch collapse inside a coalesced unit is engine code, exercised by
  engine tests, not by the connector kit.)

The live tier exercises the primitive end-to-end (all modes plus
restart/replay) on systems that run as Docker service containers. Cloud
warehouses are contract-tier-only; that is an accepted residual risk.

## 11. Consequences

**Positive**

- One write primitive: "same concept, same semantics" stops being aspiration
  on the write path — the verdict table has no transport column left.
- The thick-connector write surface is a contract (§4 hooks + declarations)
  certified by CI, so a system with a native bulk protocol has a supported
  route to it instead of overriding private internals.
- Insert is exactly-once on both transports and set-based everywhere: one
  anti-join statement per batch, not N round trips.
- Load-job destinations reach their quotas honestly (§8) with zero wire
  protocol change and zero new durability edge cases.
- Dialect divergence gets a declared vocabulary; a new system states its
  facts in JSON and renders its quirks in one small dialect class.

**Costs / risks**

- Connector definitions must carry the capability block; a connector that
  does not declare what it needs refuses loudly rather than guessing.
- Stage-then-merge costs one extra object and one extra statement per batch
  on systems where direct DML was previously enough (small Postgres
  pipelines). Accepted: the batch sizes where this matters are exactly the
  ones coalescing grows, and temp-scope stages make the overhead one
  in-session table.
- Declaring `write_unit` widens the dlq/skip unit to the coalesced batch: a
  fatally rejected unit is rejected wholesale, good rows included. All rows
  land in the DLQ; the mitigation is unit size, not engine machinery.
- The 64 MiB single-message ceiling is a real bound on write-unit size;
  chunked framing is the known, deliberately deferred escape hatch.

## 12. Decisions

1. **Stage-then-merge is the single write primitive on both transports.**
   Every SQL write lands in a stage, then one mode statement applies it.
   *Rationale:* it is the only shape all three modes, both transports, and
   bulk loading share.
2. **Landing is executemany by default, bulk-load by declaration.**
   *Rationale:* a pure speed slot with identical semantics is the only bulk
   hook that cannot fork behavior.
3. **Facade + cycle + backend split.** `GenericSQLConnector` owns
   semantics; `StageCycle` owns the step order and its cleanup and
   poisoning rules; `SqlAlchemyBackend` / `AdbcBackend` own mechanics
   behind `TransportBackend` (§3). *Rationale:* define-once for every rule
   that would otherwise exist twice — a step order held per transport is a
   second copy, and second copies drift.
4. **Dialect capabilities are declared data in `connector.json`**
   (vocabulary in §5). *Rationale:* guessed defaults are the mechanism of
   per-dialect divergence; facts about a system belong in validated data,
   rendering belongs in code.
5. **Stage scope is declared per dialect, temp preferred; real scope gets
   deterministic names, optional dedicated schema, expiration where the
   system has it.** *Rationale:* session-temp is the industry norm where it
   exists (auto-cleanup, invisibility); where it does not, deterministic
   naming plus honest cleanup (§6) is the sound fallback.
6. **Transaction shape is declared: one transaction spanning the stage cycle
   where the system supports it, per-step commits with self-healing retries
   and poisoning where it does not** (§7). *Rationale:* take atomicity where
   it is free; document idempotent-retry semantics where it is not, rather
   than pretending one model fits warehouses whose DDL and loads
   self-commit.
7. **Batch coalescing is engine-side, single-message, preference declared as
   `write_unit` in `connector.json`; the ack protocol is untouched** (§8).
   *Rationale:* the flush-gated and windowed-ack alternatives
   hand unacked data or cursor durability to untrusted connector workers;
   engine-side merging solves the quota problem with no new trust and no
   wire change. The dlq/skip unit remains the sent batch — `write_unit`
   consciously widens it, and unit size is the operator's granularity
   control. Chunked framing is deferred until a workload needs it.
8. **Duplicate keys inside one source batch stay a loud failure; duplicates
   across coalesced source batches are collapsed by the engine's coalescer,
   later batch wins; insert stays first-wins** (§2, §8). *Rationale:*
   sequential per-batch merges are the semantics being preserved, and the
   coalescer reproduces them exactly at the one point where batch order
   still exists — instead of reconstructing recency at the destination
   through new contract surface. Insert is first-wins.
9. **The conformance contract tier certifies rendering-matches-declaration,
   refusals, the sanctioned override surface, landing equivalence, and the
   duplicate rules** (§10).
10. **There is no compatibility path.** No fallback to a pre-primitive write
    shape, and no capability boolean mirroring a declaration.
