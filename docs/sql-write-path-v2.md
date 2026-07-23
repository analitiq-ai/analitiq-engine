# SQL Write Path v2: Stage-Then-Merge Across Transports

**Scope:** This ADR defines the destination SQL write primitive — how every SQL
write lands and commits, on both transports — and everything that hangs off it:
the facade/backend split inside the CDK, the sanctioned extension surface for
thick connectors, the dialect-capability block in `connector.json`, stage-table
lifecycle, transaction boundaries, and engine-side batch coalescing (the
decision for #384). It settles design; implementation is tracked by #388
(SQLAlchemy backend), #389 (ADBC backend), #390 (capability contract), #391
(conformance kit), and #384 (engine coalescer). The read path, discovery, the
mapping layer, and the gRPC ack protocol are unchanged except where this
document names them.

Related docs: transport strategy rationale in
[pyarrow-and-destinations.md](pyarrow-and-destinations.md), wire protocol in
[grpc-streaming-architecture.md](grpc-streaming-architecture.md), CDK packaging
and the connector contract in
[connector-module-architecture.md](connector-module-architecture.md).

## 1. Problem

`cdk/cdk/sql/generic.py` implements two directions, three transport flavors
(async SQLAlchemy, sync SQLAlchemy, ADBC), three write modes, and N dialects in
one ~2,900-line class. The structural triage behind #387 found it is the one
region of the codebase whose defect rate still rises while every other region
decays, and the per-dialect divergence issue class grew 1 → 2 → 2 → 9 per month
as Snowflake/BigQuery/Redshift landed. The mechanism is that the same author
intent takes a different primitive per transport:

| Concern | SQLAlchemy path | ADBC path |
|---------|-----------------|-----------|
| `upsert` | direct dialect statement (`build_sqlalchemy_upsert`, `generic.py:1684`) | stage table + `MERGE` (`_merge_ingest_locked_sync`, `generic.py:2183`) |
| `insert` idempotency | anti-join `INSERT … SELECT … WHERE NOT EXISTS`, executed once per row (`generic.py:1642-1650`) | keyless: stage + `MERGE`; keyed: plain append, at-least-once (`generic.py:1994`) |
| `truncate_insert` | delete + insert inside the batch transaction (`generic.py:1689`) | separate truncate, then append (`generic.py:2089`) |
| Transaction shape | one transaction per batch (`write_batch`, `generic.py:1338-1344`) | three commits per stage cycle (`generic.py:2226,2241,2269`) |
| Bulk load | none — the sanctioned hook does not exist (#382; mysql#29 parked) | `adbc_ingest`, a private code path |
| Stage tables | never | upsert and keyless insert |

Two further gaps compound it:

- **No sanctioned thick-connector surface for writes.** The CDK's intended
  extension surface is `SqlDialect`, and most registry connectors stay on it —
  but both connectors that needed more prove the gap. MySQL's
  `LOAD DATA LOCAL INFILE` (mysql#29) was parked because its only route was
  overriding private internals; the BigQuery connector shipped exactly that
  way, overriding `_adbc_only_ingest_sync` and `_merge_ingest_locked_sync` to
  land batches as load jobs. Contract-less coupling that breaks silently on
  any CDK refactor — parked in one case, live in the other.
- **No way to reach per-table load-job quotas** (#384). The wire protocol is
  strictly one batch → one ack → cursor persisted, so a destination can never
  hold more than one unacked batch and has nothing to coalesce; one small load
  job per source batch walks into BigQuery's 1,500 load-jobs/table/day quota.

Instance fixes do not end this class; the consolidation below does — the same
move that ended the transformer class (#295/#296) and the state/idempotency
class (#282).

## 2. The write primitive

**Every SQL write is stage-then-merge: land the batch in a stage table, then
run exactly one mode statement from stage to target.** Both backends, all
three modes, every dialect. The primitive already exists on the ADBC upsert
path; v2 makes it the only shape.

Per mode, the single statement from stage to target is:

- **`upsert`** — the dialect's declared merge form on the stream's
  `conflict_keys`: `MERGE INTO`, `INSERT … ON CONFLICT DO UPDATE`, or
  `INSERT … ON DUPLICATE KEY UPDATE` (§5). Empty `conflict_keys` refuses
  loudly, never downgrades to insert — unchanged from today
  (`generic.py:1675-1680`).
- **`insert`** — one set-based anti-join
  `INSERT INTO target SELECT … FROM stage WHERE NOT EXISTS (…identity match…)`,
  replacing the per-row execution at `generic.py:1650`. Identity semantics are
  unchanged: the contract primary key, or the synthetic `_record_hash` column
  for a keyless stream (#282). This statement is plain ANSI and runs on both
  backends, which structurally closes the parity gap: the ADBC keyed insert —
  today a plain append, at-least-once (`generic.py:1994-2020`) — becomes
  exactly-once, and the ADBC keyless stage + `MERGE` special case (#285)
  becomes the same anti-join every insert uses.
- **`truncate_insert`** — truncate once on the read's first batch
  (`batch_seq == 1`, #307 semantics unchanged), then a plain
  `INSERT INTO target SELECT … FROM stage` append. No identity dedup: deduping
  a full refresh would collapse legitimate duplicate rows (unchanged contract,
  `generic.py:1616-1619`).

### Intra-batch duplicate rules

The stage feeds set-based statements, so duplicate keys inside one batch must
be resolved before the mode statement runs — a merge with two matching source
rows fails on most systems, and an anti-join admits both copies. The rules,
applied in Arrow space before landing:

- **`insert`** — duplicate identities collapse to the **first** occurrence.
  Unchanged contract (`generic.py:1625-1638`, `_attach_record_hash_to_batch`).
- **`upsert`** — duplicate `conflict_keys` collapse to the **last** occurrence:
  the batch is in source read order, so the last version of a key is the
  newest, and upsert's intent is to reconcile to the newest version. Today
  this case fails loudly (`ON CONFLICT` raises "cannot affect row a second
  time"; `MERGE` raises "multiple source rows match"), so this is newly
  sanctioned behavior, not a silent change — and it is required: coalescing
  (§8) widens the window a batch covers, making a key updated twice within one
  batch a normal event rather than an anomaly.
- **`truncate_insert`** — no collapsing, as above.

### Landing

Landing into the stage is **executemany `INSERT` by default, bulk-load by
declaration**. A connector whose system has a native bulk protocol declares it
(§5) and implements the `bulk_land` dialect hook (§4); the CDK then uses it.
The hook is a pure speed slot: stage contents are identical either way, the
downstream mode statement is the same statement, and a declined bulk land
falls back to executemany with an INFO-level log (a speed downgrade is
visible, never silent). `adbc_ingest` stops being an ADBC-private code path
and becomes this backend's declared bulk mechanism. This is the sanctioned
home for what #382 asked for; mysql#29's `LOAD DATA LOCAL INFILE` lands here.

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
    mode_sql: str                  # the one mode statement (section 2)
    drop_stage_sql: str
    columns: tuple[str, ...]       # landing column order, identity included


class TransportBackend(ABC):
    """Executes plans; owns connections, cursors, and commit calls.

    Holds no write-mode logic. SqlAlchemyBackend serves both engine flavors
    (async engine, and sync engine on a worker thread) through one shared
    sync-Connection body -- the property _apply_write_in_txn gives the
    write path today, and it survives the split. AdbcBackend owns the ADBC
    connection, its locks, and reopen/poison handling.
    """

    async def connect(self, runtime: ConnectionRuntime) -> None: ...
    async def disconnect(self) -> None: ...
    async def run_ddl(self, statements: Sequence[str]) -> None: ...

    async def execute_write(self, plan: StageWritePlan, batch: pa.RecordBatch) -> int:
        """Create the stage, land the batch (declared bulk mechanism first,
        executemany fallback), run plan.mode_sql, drop the stage.

        One transaction spanning all four steps when plan.transactional;
        stepwise commits with the section-6 poisoning rules when not.
        Returns rows affected by the mode statement."""
```

The facade prepares the batch once (type casting, `_record_hash` attachment,
duplicate collapsing — all semantics) and hands the backend Arrow; each
backend converts to its parameter shape internally (dict records for
SQLAlchemy executemany, Arrow straight through for `adbc_ingest`). Registry
resolution, the connector contract, and the handler surface
(`BaseDestinationHandler`) are unchanged; connectors are unaffected by the
split itself.

The split covers the destination write path only. The read path already
resolved its transport divergence through the QueryBuilder (#105) and is out
of scope.

## 4. The dialect surface

`SqlDialect` keeps its role: per-system subclasses in connector packages
override exactly the quirks their system has, and under v2 the connector class
is `dialect_class = XDialect` and nothing else — the BigQuery connector's
private-internal overrides migrate onto the hooks below (§11). The write-path
hooks after v2:

**New / generalized rendering hooks**

```python
def stage_table_sql(self, stage: TableAddress, target: TableAddress,
                    *, temp: bool) -> str:
    """CREATE [TEMPORARY] TABLE shaped like target. Generalizes
    adbc_stage_table_sql to both backends; temp comes from the declared
    stage scope. Base raises UnsupportedDialectOperationError."""

def merge_statement_sql(self, stage: TableAddress, target: TableAddress,
                        conflict_keys: Sequence[str],
                        columns: Sequence[str]) -> str:
    """The upsert statement from stage to target, in the dialect's declared
    merge form. Replaces both build_sqlalchemy_upsert and the inline MERGE
    text at generic.py:2242-2268. Serves upsert only: insert uses the ANSI
    anti-join and needs no dialect hook. Base raises."""

def bulk_land(self, conn: Any, stage: TableAddress,
              batch: pa.RecordBatch) -> bool:
    """Native bulk load into the stage. Return True if landed; False
    declines and the backend falls back to executemany (logged INFO).
    Called only when the connector declares a bulk mechanism (section 5);
    conn is the backend's native connection object. A dialect whose
    mechanism runs through the system's own client rather than the
    transport connection (load_job) may ignore conn. Base returns False."""
```

The anti-join insert, the truncate, and the append statement are plain ANSI
rendered by the CDK through the dialect's quoting (`quote_table`,
`quote_ident`) — they are not hooks, because no per-system divergence exists
to express.

**Retained hooks, and the per-connection composition order**

On every new pooled connection, in order: `verify_tls_state` (#376) →
`session_init_sql` (#385) → the connection is usable. Per batch:
the #377 session-schema guard (`adbc_session_schema_sql`, checked when the
declared session-targeting mode is `session_default`) runs before any
bare-name landing or destructive statement, then the stage cycle of §2.
`adbc_ingest_kwargs`, TLS connect-arg hooks, DDL/discovery hooks, and the
identifier hooks are unchanged.

**Removed from the class surface:** the capability booleans
`supports_upsert_sqlalchemy`, `supports_upsert_adbc` (`dialects.py:129-132`)
— capability facts move to declared data (§5); the dialect keeps only
*rendering*. `supports_catalog_addressing` moves with them.
`adbc_stage_table_sql` is absorbed by `stage_table_sql`.

## 5. Declared capabilities in `connector.json`

Per-dialect SQL divergence is the one issue class still growing, and its
mechanism is guessed defaults: base-class behavior right for one family and
silently wrong for the next (#336–#343, #348, #377, #151). SQL-shape
capabilities are facts about the target system — not derivable from protocol
conformance, unlike the operation capabilities the connector-module ADR
rightly refuses to declare — so they must be declared as data.

A schema-validated block in `connector.json` (contract change tracked in
#390; published-schema version bump coordinated there):

```json
"sql_capabilities": {
  "catalog": "none" | "read" | "full",
  "session_targeting": "per_statement" | "session_default",
  "merge_form": "merge" | "insert_on_conflict" | "insert_on_duplicate_key" | "none",
  "bulk_load": "none" | "copy_from" | "load_data_local_infile" | "adbc_ingest" | "load_job",
  "stage": {
    "scope": "temp" | "real",
    "schema": "target" | "dedicated",
    "dedicated_schema": "<name, required iff schema is dedicated>",
    "transactional_ddl": true | false
  }
}
```

and one connector-level (not SQL-specific) declaration for §8:

```json
"write_unit": { "rows": 200000, "bytes": 33554432 }
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
  other mechanism — which is what makes §8's quota arithmetic hold.
- Validated offline by the published validator like every other contract
  surface, and visible to any consumer of the connector definition.
- `write_unit` sits at the connector level because it is not a SQL fact —
  any destination whose write cost is per-write-operation (file/S3 sinks
  included) may declare it, and the engine consumes it transport-agnostically
  (§8). Absent means "no preference": the engine does not coalesce.

## 6. Stage lifecycle

**Naming** keeps the deterministic grammar the ADBC path proved:
`_analitiq_stage_<target>_b<sha16>` where the token is
`sha256(run_id|stream_id|batch_seq)[:16]` (`generic.py:2152-2154`). A retry of
the same batch computes the same name, so the pre-flight
`DROP TABLE IF EXISTS` (real scope) or `CREATE OR REPLACE`-equivalent finds
and clears its own leftovers — stage cleanup is self-healing across retries
by construction, never dependent on a cleanup pass.

**Scope is declared, temp preferred.** `stage.scope` in the capability block:

- **`temp`** — a session-scoped temporary table: invisible to other sessions,
  dropped by the system on disconnect, no DDL in the customer's schema.
  Declared by systems where a session-temp table is visible to the same
  connection's mode statement (Postgres, MySQL, Redshift, Snowflake).
- **`real`** — an ordinary table, for systems without usable session-temp
  semantics (BigQuery). `stage.schema` places it: `"target"` (the target
  table's schema — today's behavior) or `"dedicated"` with a named
  `dedicated_schema`, keeping stage DDL out of customer schemas entirely
  (the pattern Airbyte's internal schema and Fivetran's staging datasets
  follow). Real-scope stages should additionally carry the system's
  expiration mechanism where one exists (BigQuery table expiration), so an
  orphan is time-bounded even after a process crash.

**Cleanup and poisoning — the #379 rules, generalized to both backends:**

- The stage is dropped after the mode statement, success or failure, in both
  scopes (a long-lived session accumulates temp stages otherwise).
- Failure path: best-effort drop, then the batch fails with its own error.
- Success path: the drop is attempted twice; if both attempts fail the
  connection is poisoned (a failed DROP means a possibly-dead connection —
  the next batch must not inherit it) and the log tells the truth: for real
  scope, the named stage table is orphaned and needs manual cleanup or the
  expiration policy — no false "will be cleaned up on retry" promises
  (`generic.py:2318-2363` is the reference implementation).
- The #377 invariant guard is inherited unchanged: under
  `session_targeting: "session_default"`, the session schema must equal the
  target schema before any bare-name landing or destructive statement runs.

## 7. Transaction boundaries

Declared per system as `stage.transactional_ddl`:

- **`true`** — the backend runs create-stage, land, mode statement, and drop
  in **one transaction**: an interrupted batch leaves nothing, not even a
  stage. Postgres and Redshift support this outright; MySQL qualifies with
  `temp` scope because `CREATE TEMPORARY TABLE` does not implicitly commit
  while its regular `CREATE TABLE` does — the declaration covers the
  combination the system actually has.
- **`false`** — systems whose DDL self-commits or whose loads are their own
  commit unit (Snowflake, BigQuery) run the steps with per-step commits.
  Safety then comes from the primitive itself, not atomicity: deterministic
  stage names make retries self-healing (§6), the mode statement is
  idempotent on identity (§9), and the poisoning rules bound connection
  reuse after a failed step. This is the documented semantics, not a
  degraded mode — it is how every load-job warehouse pipeline works.

Both SQLAlchemy flavors keep sharing one sync-`Connection` transaction body,
as they do today through `_apply_write_in_txn` (`generic.py:1473-1494`,
entered from the async flavor via `run_sync` and from the sync flavor on a
worker thread); the backend split must not fork them. The ADBC backend
replaces today's fixed three-commit cycle (`generic.py:2226,2241,2269`) with
the declared shape.

Statement-timeout policy is unchanged and stays in the facade: one deadline
covers the whole batch write (`generic.py:1290`), with the known limitation
that only the async SQLAlchemy flavor can enforce it in-band
(`generic.py:424-436`).

## 8. Batch coalescing (settles #384)

**Decision: the engine coalesces source batches before sending; the wire
protocol does not change.** The destination-side alternatives — buffered
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
  read ends, then concatenates the Arrow batches into one. Because it runs
  *before* `record_ids`, the MAX-cursor computation, `emitted_at` stamping,
  and `batch_seq` assignment (`stream_processor.py:586-633`), everything
  downstream — retry stability, cursor semantics, DLQ correlation — is
  unchanged code operating on a bigger batch. A merged batch's `batch_seq`
  is stable across retries exactly as today, which also preserves
  deterministic load-job-ID schemes built on it (bigquery#6).
- No timer: pipeline runs are finite reads, so the tail flushes when the
  read ends. Backpressure is unchanged — the coalescer holds at most one
  unit.
- `truncate_insert` is safe by construction: the merged first unit is sent
  as `batch_seq` 1, so truncate-once gating (#307) fires exactly once,
  covering every source page inside the unit.
- **Size budget.** The hard bound is the gRPC message cap
  (`GRPC_MAX_MESSAGE_SIZE`, `src/config/settings.py:103`), raised from 16 MiB
  to 64 MiB by default. The unit budget counts the Arrow payload **plus**
  per-row wire overhead — `record_ids` alone add 64 bytes per row — and the
  coalescer targets the declared `write_unit.bytes` capped at a safety
  margin below the message cap. Single-message units in the tens of
  megabytes are deliberately the ceiling: a protobuf message has no
  streaming inside it, so both containers hold ~3-4x the unit size in
  transient memory. Chunked framing (one logical batch as N wire messages
  under one ack) would lift that ceiling and is explicitly out of scope —
  an additive protocol change to revisit only if a workload proves the
  single-message ceiling insufficient.
- Quota arithmetic, recorded so the ceiling is a decision and not an
  accident: at 64 MiB units, BigQuery's 1,500 load-jobs/table/day binds only
  above ~90 GB/table/day — and under stage-then-merge the target table
  receives `MERGE` **query** jobs (a far higher quota class) while load jobs
  land in per-batch stage tables.
- `GetCapabilitiesResponse.max_batch_size` / `max_batch_bytes`
  (`destination_service.proto:83-84`) are advertised today but consumed by
  nothing on the send path; the write-unit fact now lives in
  `connector.json`, so the implementing PR deletes the two proto fields
  rather than keeping a second, dead declaration channel.

## 9. Idempotency and retry verdicts

The idempotency contract is unchanged — content-derived row identity, no
positional ledger (#282) — but v2 makes the verdict table
transport-independent, which is the point of the primitive:

| Mode | Verdict | Mechanism |
|------|---------|-----------|
| `upsert` | exactly-once | merge on `conflict_keys` from stage |
| `insert` | exactly-once | set-based anti-join on identity (contract PK or `_record_hash`) from stage |
| `truncate_insert` | at-least-once | truncate on first read batch, plain append after — by design (#307) |

`retry_semantics` (`generic.py:559-621`) loses its per-transport rows: the
ADBC keyed-insert at-least-once row and the ADBC keyless special case
disappear because the backends no longer differ in mechanism. The
per-handler matrix in
[grpc-streaming-architecture.md](grpc-streaming-architecture.md) is amended
accordingly by the implementing PRs, including deleting its "ADBC-only
transports do not yet do the keyless insert anti-join" caveat — already stale
for keyless (#285 made it stage + `MERGE`), and moot for keyed under v2.

## 10. What the conformance kit asserts about the primitive

The contract tier (#391, no live database) certifies this ADR's surface:

- **Rendering matches declaration.** The rendered stage DDL carries the
  temp form iff `stage.scope` is `temp` and the declared schema placement;
  the rendered upsert statement matches the declared `merge_form`;
  declared-but-wrong and used-but-undeclared both fail.
- **Refusals fire.** Upsert with empty `conflict_keys`, upsert against
  `merge_form: "none"`, and any needed-but-undeclared capability produce the
  loud config error, not SQL.
- **The override surface is the sanctioned one.** A connector may override
  the §4 hooks plus `session_init_sql`, `verify_tls_state`, and the
  existing DDL/discovery/TLS hooks; overriding a private
  `GenericSQLConnector` or backend internal fails the suite.
- **Landing is semantics-free.** For a connector declaring a bulk mechanism,
  bulk-landed and executemany-landed stages produce identical stage
  contents against the suite's fakes.
- **Duplicate rules hold.** Intra-batch duplicate identities collapse
  first-wins for insert and last-wins for upsert before the mode statement
  renders; a replayed batch leaves target state unchanged for the
  exactly-once modes.

The live tier exercises the primitive end-to-end (all modes plus
restart/replay) on systems that run as Docker service containers; cloud
warehouses stay contract-tier-only, the accepted residual risk recorded in
#391.

## 11. Migration

The implementing PRs delete the replaced paths outright — no fallback, no
compatibility layer, per the engine's no-legacy rule:

- #388: `SqlAlchemyBackend` — stage-then-merge on SQLAlchemy; deletes the
  per-row anti-join execution and the direct `build_sqlalchemy_upsert` call
  path.
- #389: `AdbcBackend` — aligns insert/truncate_insert on the primitive;
  deletes the plain-append insert path; inherits #377/#379 behavior as
  specified in §6-§7. Once it lands, the BigQuery connector's registry repo
  moves its load-job landing from the private `_adbc_only_ingest_sync` /
  `_merge_ingest_locked_sync` overrides onto `bulk_land` with
  `bulk_load: "load_job"` — until then that connector fails the §10 override
  rule by construction.
- #390: the `sql_capabilities` + `write_unit` contract block; deletes the
  `supports_*` dialect booleans and every guessing default. Connector
  definition updates in the registry repos follow per connector, including
  renaming `adbc_stage_table_sql` overrides to `stage_table_sql` and
  removing the dead `MySQLDialect.batch_commits_key_type`.
- #391: the conformance kit asserting §10.
- #384: the engine coalescer, the message-cap default raise, and removal of
  the dead `GetCapabilitiesResponse` sizing fields — #384 remains open as
  the implementation tracker for §8.
- Docs: the affected sections of
  [grpc-streaming-architecture.md](grpc-streaming-architecture.md) (verdict
  matrix, ADBC parity caveat, batch-size prose) and
  [pyarrow-and-destinations.md](pyarrow-and-destinations.md) (the
  "two code paths" strategy becomes the facade/backend shape) are amended in
  the PRs that change the behavior they describe.

## 12. Consequences

**Positive**

- One write primitive: "same concept, same semantics" stops being aspiration
  on the write path — the verdict table has no transport column left.
- The thick-connector write surface is a contract (§4 hooks + declarations),
  certified by CI (#391), ending the private-override coupling that parked
  mysql#29.
- Insert becomes exactly-once on ADBC, set-based everywhere — the per-row
  anti-join's N round trips collapse into one statement.
- Load-job destinations reach their quotas honestly (§8) with zero wire
  protocol change and zero new durability edge cases.
- Dialect divergence gets a declared vocabulary; a new system states its
  facts in JSON and renders its quirks in one small dialect class.

**Costs / risks**

- A schema contract bump (#390) that connector definitions must adopt;
  registry repos need per-connector updates.
- Stage-then-merge costs one extra object and one extra statement per batch
  on systems where direct DML was previously enough (small Postgres
  pipelines). Accepted: the batch sizes where this matters are exactly the
  ones coalescing grows, and temp-scope stages make the overhead one
  in-session table.
- Last-wins upsert collapsing is newly defined behavior where today fails
  loud; pipelines relying on the loud failure to detect upstream duplicate
  keys lose that tripwire (they retain it for insert mode).
- The 64 MiB single-message ceiling is a real bound on write-unit size;
  chunked framing is the known, deliberately deferred escape hatch.

## 13. Decisions

1. **Stage-then-merge is the single write primitive on both transports**
   (settled by #387). Every SQL write lands in a stage, then one mode
   statement applies it. *Rationale:* it is the only shape all three modes,
   both transports, and bulk loading share; the ADBC upsert path already
   proved it.
2. **Landing is executemany by default, bulk-load by declaration** (settled
   by #387). *Rationale:* a pure speed slot with identical semantics is the
   only bulk hook that cannot fork behavior.
3. **Facade + backend split** (settled by #387). `GenericSQLConnector` owns
   semantics; `SqlAlchemyBackend` / `AdbcBackend` own mechanics behind
   `TransportBackend` (§3). *Rationale:* define-once for every rule that
   today exists twice.
4. **Dialect capabilities are declared data in `connector.json`** (settled
   by #387/#390; vocabulary in §5). *Rationale:* guessed defaults are the
   mechanism of the still-growing defect class; facts about a system belong
   in validated data, rendering belongs in code.
5. **Stage scope is declared per dialect, temp preferred; real scope gets
   deterministic names, optional dedicated schema, expiration where the
   system has it.** *Rationale:* session-temp is the industry norm where it
   exists (auto-cleanup, invisibility); where it does not, deterministic
   naming plus honest cleanup (#379 rules, §6) is the sound fallback.
6. **Transaction shape is declared: one transaction spanning the stage cycle
   where the system supports it, per-step commits with self-healing retries
   and poisoning where it does not** (§7). *Rationale:* take atomicity where
   it is free; document idempotent-retry semantics where it is not, rather
   than pretending one model fits warehouses whose DDL and loads
   self-commit.
7. **Batch coalescing is engine-side, single-message, preference declared as
   `write_unit` in `connector.json`; the ack protocol is untouched** (folds
   #384; §8). *Rationale:* the flush-gated and windowed-ack alternatives
   hand unacked data or cursor durability to untrusted connector workers;
   engine-side merging solves the quota problem with no new trust and no
   proto change. Chunked framing is deferred until a workload needs it.
8. **Upsert collapses intra-batch duplicate keys last-wins; insert stays
   first-wins** (§2). *Rationale:* coalescing makes intra-batch key updates
   normal; last version wins matches upsert's reconcile-to-newest intent,
   while insert's first-wins contract predates v2 and is unchanged.
9. **The conformance contract tier certifies rendering-matches-declaration,
   refusals, the sanctioned override surface, landing equivalence, and the
   duplicate rules** (§10).
10. **Old paths are deleted in the implementing PRs** (§11) — no fallback,
    no compatibility path, no mirrored capability booleans.
