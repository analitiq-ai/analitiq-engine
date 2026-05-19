# Engine Schema-Alignment Plan

Living plan for aligning the engine with the published JSON-Schema contracts at
<https://schemas.analitiq.ai/>. Iterate on this document — do not duplicate it.

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[!]` blocked

## 0. Problem statement

The engine consumes pipeline / stream / connection / connector / endpoint JSON
from disk (and from the cloud) shaped by six published schemas:

- `pipeline/latest.json` (v6)
- `stream/latest.json` (v8)
- `connection/latest.json` (v6)
- `connector/latest.json` (v7)
- `api-endpoint/latest.json` (v7)
- `database-endpoint/latest.json` (v7)

Today the engine passes untyped flat dicts between `PipelineConfigPrep`,
`Pipeline`, and the source/destination connectors. Every consumer re-extracts
the same fields, often under different names. Schema drift only surfaces when a
pipeline run hits the offending code path, which is why bugs keep emerging in
production. Fix is a single typed boundary plus integration tests that drive
every pipeline shape end-to-end.

## 1. Audit results

### 1.1 Drift inventory — legacy keys to delete (no fallback)

File:line references are against the `dev` branch.

| Key read | Location on dev | Schema reality |
|---|---|---|
| `replication_key` | `src/source/connectors/database.py:159`; **fallback chain** `src/engine/engine.py:477` (`config.get("cursor_field") or config.get("replication_key")`) | schema only defines `cursor_field` |
| `endpoint` (flat string) | `src/source/connectors/api.py:88`; `src/grpc/client.py:451,477` | API: `operations.read.request.path`; DB: `database_object.{schema,name}` |
| `method` (flat) | `src/source/connectors/api.py:89`; `src/grpc/client.py:452` | `operations.read.request.method` |
| `replication_method` | `src/source/connectors/api.py:265,99`; `src/models/state.py:62,85`; `src/models/engine.py:63,73` | stream `source.replication.method` / endpoint `operations.read.replication.supported_methods` |
| `name` (stream/pipeline) | `src/engine/engine.py:137`; `src/engine/pipeline.py:279`; `src/engine/orchestrator.py:227`; `src/engine/pipeline_config_prep.py:529` | `display_name` |
| `version` (pipeline) | `src/engine/pipeline.py:280`; `src/engine/pipeline_config_prep.py:522,617` | not in schema |
| `primary_key` (singular) | `src/engine/pipeline.py:165`; `src/engine/pipeline_config_prep.py:650`; `src/grpc/client.py:520` | `source.primary_keys` / db-endpoint `primary_keys` |
| `connector_type` | `src/destination/connectors/file.py:86`; `src/engine/pipeline.py:155,206`; `src/shared/connector_utils.py:57`; `src/engine/pipeline_config_prep.py:409,430` | `connector.kind` (only on connector definition) |
| `transformations` | `src/engine/data_transformer.py:603` | `mapping.assignments[].value.{expression,constant}` |
| `batching.supported`, `batching.size` | `src/engine/pipeline_config_prep.py:688-693`; `src/engine/pipeline.py:213-214`; `src/models/stream.py:315-317` | stream `destinations[].execution.batch_size`; api-endpoint `operations.write[mode].batching.max_records` |
| `target_schema_fingerprint`, `source_schema_fingerprint` | `src/engine/pipeline_config_prep.py:657-658,682-683`; `src/models/stream.py:295,326` | `schema_hash` (server-assigned, read-only) |
| `idempotency_key` | `src/engine/pipeline_config_prep.py:686-687`; `src/models/stream.py:300-302` | not in schema |
| `cursor_param`, `limit_param`, `start_page`, `data_field`, `replication_filter_mapping` | `src/source/connectors/api.py:325,371-372,422,522,572` | pagination is discriminated on `type` with nested `{cursor,offset,page,keyset,link}` blocks |
| `transports[name].base_url` | `src/engine/pipeline.py:332`; `src/shared/transport_factory.py:133`; `src/shared/connection_runtime.py:57` | HTTP-only; SqlAlchemy uses `dsn` with `url_template` resolver |

Removed since the original audit (already absent on dev — no work needed):
- `filters[].default` — no reads found on dev.

Tracking:
- [ ] all of the above deleted in the connector / engine refactor (§2.2)
- [ ] no `.get(legacy, new)` fallbacks introduced anywhere
- [ ] kill the existing fallback at `src/engine/engine.py:477` (`cursor_field or replication_key`) — this is exactly the pattern the rule forbids and it is already in tree

### 1.2 Schema fields the engine ignores today

Not strictly drift — but fields that should be wired or explicitly waived:

- `operations.read.replication.cursor_mappings`, `supported_methods` — engine derives cursor from stream-side `replication.cursor_field` only.
- `operations.read.params[*].{controlled_by, default, enum, operators}` — only `default` is partially resolved.
- `operations.read.request.{headers_remove, path_params, body, transport_ref}` — engine flattens path/method/query/headers only.
- `operations.write[mode].{params, response, batching}` — ignored.
- `operations.read.response.{records, metadata, schema}` — engine scans `data` instead of using the declared records ref.
- `source.database_pagination`, `source.tie_breaker_fields` — partial.
- `connection_contract.{required_for_activation, validation.rules}` — no pre-flight validation.
- `connector.auth.test` — no connection-test path.
- `pipeline.engine.{vcpu, memory}` — read but only used by cloud entrypoint.
- `destinations[].execution.{batch_size, max_concurrent_batches}` — engine uses `runtime.batching.*` instead, ignoring per-destination overrides.
- `stream.status`, `stream.tags`, `pipeline.tags` — ignored.

Tracking:
- [ ] each item triaged: wire it up OR document the waiver in `docs/engine-schema-alignment-plan.md`

### 1.3 Duplication

Same field extracted in multiple places — these collapse once the typed
boundary is in place:

1. **EndpointRef parsing** in 5 sites: `src/engine/pipeline.py:113`, `src/engine/pipeline_config_prep.py:469,645,672`, `src/config/endpoint_resolver.py:29`.
2. **Database `{schema, table}` extraction** in `src/source/connectors/database.py:115-117` (parses flat `endpoint` string into `schema/table`) — the equivalent extraction inside `engine/pipeline.py` was removed since the original audit, but the duplication moved into the connector instead of being deleted.
3. **API request flattening**: `src/engine/pipeline.py:189-195` writes flat `endpoint/method/headers/query/pagination` keys; `src/source/connectors/api.py:88-89` reads them back. Round-trip via a flat dict.
4. **Pagination block** parsed in `src/source/connectors/api.py` at lines 107, 323, 370, 418 (one per pagination method), each pulling different sub-keys.
5. **Replication settings** scattered: `src/engine/pipeline.py:142-201`, `src/engine/pipeline_config_prep.py:642-660`, `src/source/connectors/api.py:265`, `src/source/connectors/database.py:159`.
6. **Three orchestrators** still coexist on dev (none have been collapsed):
   - `src/engine/engine.py` — `StreamingEngine` (~874 lines)
   - `src/engine/pipeline.py` — `Pipeline` (~412 lines)
   - `src/engine/orchestrator.py` — `PipelineOrchestrator` (~358 lines)

Tracking:
- [ ] EndpointRef parsed once (in PipelineConfigPrep) and passed as typed object
- [ ] `{schema, table}` lives only on the resolved `DatabaseReadEndpoint`
- [ ] no more flat-dict round-tripping between pipeline.py and connectors
- [ ] pagination parsed once into a `PaginationSpec` instance
- [ ] replication parsed once into a `ResolvedSource.replication`
- [ ] one orchestrator (`StreamingEngine`); `Pipeline` and `orchestrator.py` deleted

### 1.4 On-disk fixture drift (NOT engine work — needs contract-repo fix)

User-owned files. Per `CLAUDE.md` they are out of scope for engine changes.
Validated end-of-audit by running `jsonschema` against the live published
contracts at https://schemas.analitiq.ai/ — 48 of 69 fixtures pass, 21 fail,
with two distinct drift classes.

**Class A: legacy `alias` instead of `endpoint_id` (21 files)** —
the published api-endpoint and database-endpoint schemas forbid `alias` and
require `endpoint_id`.

| Fixture | Drift |
|---|---|
| `connections/8687069d.../definition/endpoints/batch_commits.json` | `alias`, missing `endpoint_id` |
| `connections/bf65826d.../definition/endpoints/{batch_commits,new_table,wise_transfers,wise_transfers2}.json` | same |
| `connectors/sevdesk/definition/endpoints/*.json` (16 files: `accounting_contacts`, `accounting_types`, `categories`, `communication_ways`, `contact_addresses`, `contact_fields`, `layouts`, `order_positions`, `orders`, `parts`, `private_transaction_rules`, `static_countries`, `static_currencies`, `tag_relations`, `tags`, `unities`) | same |

**Class B: unparameterized Arrow types (4 files)** —
the published database-endpoint schema requires full Arrow type parameterization
(`Timestamp(MICROSECOND, UTC)`, `Decimal128(20, 6)`, etc.). Several columns
declare bare `Timestamp` and `Decimal128`, which fail the schema's regex.

| Fixture | Affected columns |
|---|---|
| `connections/bf65826d.../definition/endpoints/new_table.json` | columns 2 & 8 (`Timestamp`), columns 3, 4, 7 (`Decimal128`) |
| `connections/bf65826d.../definition/endpoints/wise_transfers.json` | same column positions |
| `connections/bf65826d.../definition/endpoints/wise_transfers2.json` | same |
| (note) `connections/.../batch_commits.json` files only fail class A |

Tracking:
- [x] in-tree fix applied: 21 `alias`→`endpoint_id` renames + 15 column Arrow-type parameterizations across 3 db-endpoint files (`new_table`, `wise_transfers`, `wise_transfers2`). Completed 2026-05-19.
- [x] schema validation against the live published contracts: 69/69 pass.
- [ ] follow-up: feed the same edits back to the contract repos that own these fixtures so future syncs don't re-introduce the drift.
- [ ] once Layer-B factories exist, the same validation runs as part of the construction smoke test (§3.1).

### 1.5 In-band signalling on config dicts

The engine smuggles non-schema objects into the same dict it passes to
connectors by prefixing keys with an underscore. This is exactly the
dict-passing the typed boundary should eliminate.

| Injection site | Consumed at | What is smuggled |
|---|---|---|
| `src/engine/pipeline_config_prep.py:591-592` | `src/engine/engine.py:248` | `_runtime` (ConnectionRuntime), `_endpoint` (resolved endpoint dict) — source side |
| `src/engine/pipeline_config_prep.py:612-613` | `src/engine/engine.py:735` | same — destination side |
| `src/engine/pipeline.py:100` | (same consumers) | `_runtime` |

Tracking:
- [ ] underscore-prefixed keys removed once connectors take typed `ResolvedSource` / `ResolvedDestination` (the runtime + resolved endpoint become real fields)

### 1.6 Hand-rolled dataclasses in `src/models/` that overlap the resolved-runtime layer

`src/models/stream.py`, `src/models/engine.py`, and `src/models/state.py`
contain partial, hand-maintained versions of what the resolved-runtime layer
unifies. They carry the same legacy field names listed in §1.1 (e.g.
`replication_method`, `primary_key`, `source_schema_fingerprint`,
`idempotency_key`, `batching.supported/size`), which is one reason the drift
keeps coming back.

Tracking:
- [ ] `src/models/stream.py` deleted once `src/engine/resolved/` is in place
- [ ] `src/models/engine.py` triaged — fields merge into resolved-runtime or get deleted
- [ ] `src/models/state.py` triaged — replication state stays, but `replication_method` / `primary_key` fields use the schema-canonical names

## 2. Design

### 2.1 Resolved runtime (`src/engine/resolved/`)

Fusion layer. `PipelineConfigPrep` produces one object graph; everything
downstream consumes typed nodes, never raw dicts.

```python
ResolvedPipeline:
    pipeline_id: str
    display_name: str
    schedule: ScheduleConfig
    runtime: RuntimeConfig
    streams: list[ResolvedStream]

ResolvedStream:
    stream_id: str
    source: ResolvedSource
    destinations: list[ResolvedDestination]
    mapping: StreamMapping | None

ResolvedSource:
    connection: ResolvedConnection
    endpoint: ApiReadEndpoint | DatabaseReadEndpoint
    replication: ReplicationConfig
    filters: list[Filter]
    selected_columns: list[str] | None
    database_pagination: DatabasePagination | None
    primary_keys: list[str]

ResolvedDestination:
    connection: ResolvedConnection
    endpoint: ApiWriteEndpoint | DatabaseWriteEndpoint
    write_mode: str
    conflict_keys: list[list[str]] | None
    execution: ExecutionConfig

ApiReadEndpoint:
    request: HttpRequest
    params: dict[str, ParamSpec]
    pagination: PaginationSpec | None
    replication: Replication
    response: ResponseExtraction

DatabaseReadEndpoint:
    database_object: DatabaseObject
    columns: list[Column]
    primary_keys: list[str]

ApiWriteEndpoint:
    write_mode: str
    request: HttpRequest
    input_schema: dict
    batching: WriteBatching | None
    response: WriteResponseSpec | None

DatabaseWriteEndpoint:
    database_object: DatabaseObject
    columns: list[Column]
    primary_keys: list[str]
```

Source/destination connectors take a `ResolvedSource` / `ResolvedDestination`
directly. The connector signature becomes:

```python
async def read_batches(
    self, source: ResolvedSource, *,
    state_manager: StateManager, stream_id: str, batch_size: int,
) -> AsyncIterator[list[dict]]: ...
```

Tracking:
- [x] `src/engine/resolved/` package with frozen dataclasses (types.py). Completed 2026-05-19.
- [x] factory functions in `build.py`: `build_resolved_pipeline`, `build_resolved_stream`, `build_api_read_endpoint`, `build_db_read_endpoint`, `build_*_write_endpoint`, `build_connector`, `build_connection`. Completed 2026-05-19.
- [x] disk loader in `loader.py`: `load_resolved_pipeline(root, pipeline_id)` walks `pipelines/`, `connections/`, `connectors/`. Completed 2026-05-19.
- [x] unit tests covering build functions + error paths against synthetic specs (`tests/unit/engine/test_resolved_build.py`, 23 tests). Completed 2026-05-19.

### 2.2 Engine glue

- [ ] `engine/pipeline_config_prep.py` becomes the *only* place that touches raw JSON; returns `ResolvedPipeline`
- [ ] delete the flattening code in `engine/pipeline.py`
- [ ] delete `engine/orchestrator.py`; collapse responsibilities into `engine/engine.py:StreamingEngine`
- [ ] gRPC `Schema` message stays a dict on the wire; destination materializes it back into `DatabaseWriteEndpoint` locally for typed handler state
- [ ] `src/models/stream.py` (legacy hand-rolled dataclasses) deleted; callers use `src/engine/resolved/`

### 2.3 Per-stream destination state

In flight already; complete under the typed boundary.

- [ ] `DatabaseDestinationHandler._streams: dict[str, ResolvedDestinationState]` where state bundles `(DatabaseWriteEndpoint, sqlalchemy.Table, schema_contract)`
- [ ] DDL lock (`asyncio.Lock`) serializes schema creation across concurrent streams
- [ ] `DestinationGRPCServicer.configured_streams: set[str]` replaces single `_schema_configured: bool`
- [ ] all `_prepare_records`, `_insert_records`, `_upsert_records`, `_truncate_and_insert` take `state` arg

## 3. Tests

### 3.1 Construction smoke test

Replaces the old Layer-A "fixture conformance" test. Walks every on-disk
artifact under `pipelines/`, `connections/`, `connectors/` and feeds each one
to the matching Layer-B factory (`build_resolved_pipeline` etc.). A missing or
mistyped field surfaces as a clear constructor error naming the field. Same
drift-detection signal as the Pydantic version, no duplicated schemas.

Tracking:
- [x] `tests/integration/test_resolved_construction.py` parametrises over every active pipeline and loads each via `load_resolved_pipeline`. Completed 2026-05-19.
- [x] failures point at `file:$.path: message` via `ResolveError`. Completed 2026-05-19.
- [x] all 5 active pipelines on disk construct cleanly; the §1.4 drift fixes were a prerequisite. Completed 2026-05-19.

### 3.2 Pipeline-shape integration tests

`tests/integration/pipelines/` — synthetic fixtures shaped per the published
contracts, one per pipeline shape. Tests drive `PipelineConfigPrep` →
`StreamingEngine.run()` against fake source/destination, assert no
`KeyError`/`AttributeError`.

| Fixture | Source | Destination | Replication | Asserts |
|---|---|---|---|---|
| `api_to_db_incremental` | API w/ cursor pagination | DB | incremental | request flattened, batches mapped, cursor advances |
| `db_to_db_incremental` | DB, offset pagination | DB | incremental | SQL built, dest table created, batch_commits idempotent |
| `db_to_db_full` | DB | DB | full_refresh | truncate-insert path |
| `api_to_api` | API | API write (insert) | full_refresh | write_op flattened, body templated |
| `multi_stream_one_dest` | one API connection, 3 streams | one DB connection | mixed | concurrent `configure_schema`, per-stream state isolated |

Tracking:
- [ ] synthetic fixtures authored under `tests/integration/pipelines/fixtures/`
- [ ] each fixture loads cleanly via Layer-B factories
- [ ] one test per shape exercising end-to-end load + run with fakes
- [ ] all five tests green before disk-pipeline runs resume

## 4. Execution order

1. [x] Delete dead exploration code: `src/models/spec/`, `tests/contract/test_spec_models.py`, and (if unused) `tests/contract/schemas/`. Completed in branch switch (prior session).
2. [x] Resolved-runtime types (§2.1) + build functions + unit tests. Completed 2026-05-19.
3. [x] Construction smoke test (§3.1) — exercises factories against on-disk fixtures. Completed 2026-05-19.
4. [ ] Pipeline-shape integration fixtures and tests (§3.2)
5. [ ] Engine glue (§2.2): rewrite `PipelineConfigPrep` to emit `ResolvedPipeline`; delete duplicate orchestrators in same commit
6. [ ] Connector refactor: API source, DB source, DB dest, API dest, file dest take typed objects; drift items §1.1 resolved
7. [ ] Re-run all 5 disk pipelines + cloud sim end-to-end

## 5. Out-of-scope (do not work on here)

- Editing user-owned files under `connectors/`, `connections/`, `pipelines/`, or
  endpoint/stream JSON. Drift in §1.4 is fixed in the contract repos.
- Adding fallback / legacy-key reads. Anywhere we find one, delete it.
- Adding cloud SDKs to the engine. Engine stays cloud-agnostic.
- New unit tests for thin coverage; the deliverables are §3 integration tests.
