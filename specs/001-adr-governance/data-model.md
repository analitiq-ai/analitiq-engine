# Data Model: Decision-Record Governance

## Decision record

One file `docs/adr/NNNN-slug.md`, flat. Frontmatter schema is owned by adrkit
(`schema/adr.schema.json`); this feature adapts to it and defines no fields of its own.

| Field | Rule in this repo |
|---|---|
| `id` | quoted 4-digit string, next free number; never reused |
| `title` | the decision as an imperative statement, 3–120 chars |
| `status` | see state transitions below |
| `date` | date the decision was made; for backfilled records, the commit that introduced the rule |
| `deciders` | `["@Analitiq-Bot-Wonka"]` |
| `affects` | non-empty list of `{type: path, pattern: <glob>}`; every pattern matches a real path |
| `supersedes` / `supersededBy` | set as a pair when one record replaces another |
| `relatesTo` | records it depends on or refines |
| `provenance` | `authoredBy: agent` plus `ratifiedBy` for agent-drafted records |

Body sections: Context, Decision, Options considered, Trade-offs, Consequences (including how we
would know it was wrong). No issue numbers, no `file.py:123` coordinates.

### State transitions

```text
draft ──> proposed ──> accepted ──> superseded   (only via a new record that supersedes it)
  │           │            └──────> deprecated
  └───────────┴──> rejected
```

- Automation may create `draft` only (FR-012). `proposed → accepted` is a maintainer action.
- An `accepted` record's body is never edited; a changed decision is a new record.

## Constitution

Gate list in `.specify/memory/constitution.md`. Each principle stating an architecture rule cites
decision-record ids, not doc sections.

## Orientation overview

One prose doc describing how components fit together. States no rule; links to records.

## Citation

A reference from code, docstring, runtime error text or Markdown to a decision record, by id
(e.g. `ADR 0005`). Moves in the same commit as its target.

## Record catalog

### Migrated (existing)

| Id | Decision | Note |
|---|---|---|
| 0001 | Two failure vocabularies | absorbs ErrorCode add-only and classification-priority rules, or they become new records |
| 0002 | One stop rule for every paging scheme | state its API-only scope |
| 0003 | The CDK is a toolbox, not a gatekeeper | |
| 0004 | Capability is derived, never declared | |
| 0005 | Stage-then-merge is the single SQL write primitive | |
| 0006 | Batch coalescing is engine-side | correct the fatal-unit verdict (research R3) |

### New (from the rule inventory)

Source doc = the doc whose migration PR lands the record. `(u)` = path to verify before use.

| # | Decision | Source doc | `affects` |
|---|---|---|---|
| N1 | Persist only the destination-acked cursor and resume inclusively | engine-architecture | `src/engine/stream_processor.py`, `src/engine/batch_policy.py`, `src/grpc/cursor.py`, `src/state/**` |
| N2 | Reach exactly one verdict per batch, in BatchPolicy | grpc-streaming | `src/engine/batch_policy.py`, `src/engine/stream_processor.py`, `src/grpc/client.py` |
| N3 | Keep one batch in flight, acked in order, with an opaque cursor and content-derived row idempotency | grpc-streaming | `src/grpc/**`, `src/destination/server.py`, `cdk/cdk/base_handler.py`, `cdk/cdk/sql/generic.py` |
| N4 | Carry the ack budget on the handshake and derive the statement timeout from it | grpc-streaming | `src/grpc/client.py`, `src/destination/server.py`, `cdk/cdk/sql/generic.py` |
| N5 | Do not set client keepalive on the engine-destination channel | grpc-streaming | `src/grpc/client.py`, `src/destination/server.py` |
| N6 | Keep ErrorCode an add-only published contract; error_detail carries class names only | engine-architecture | `src/state/error_classification.py` |
| N7 | Classify failures where they occur; connectors declare facts, never verdicts | engine-architecture | `cdk/cdk/declarations.py` (u), `cdk/cdk/sql/**`, `src/state/error_classification.py` |
| N8 | Bind SQL capabilities once; refuse undeclared shape facts | sql-write-path | `cdk/cdk/sql/dialects.py`, `cdk/cdk/sql/capabilities.py` |
| N9 | Name stages hash-first, one cycle per backend under the write lock, poison per path | sql-write-path | `cdk/cdk/sql/stage_cycle.py`, `cdk/cdk/sql/*backend.py` |
| N10 | Run exactly two SQL transports and dispatch, never fork | arrow-and-transport | `cdk/cdk/sql/*backend.py` |
| N11 | Load connector code only in the worker; verify declared registry roles on load | engine-architecture | `cdk/cdk/registry.py`, `src/worker/**` (u) |
| N12 | Depend one way, engine to CDK; connectors import no connector or runtime; engine is cloud-agnostic | connector-module | `cdk/cdk/**`, `src/**` |
| N13 | Lazy-import transports by extra and fail naming the missing extra | connector-module | `cdk/cdk/registry.py`, `cdk/cdk/_extras.py` (u) |
| N14 | Resolve transports and secrets engine-side; keep each operation within its transport's origin | source-config | `cdk/cdk/connection_runtime.py` (u), `cdk/cdk/secrets/**` (u), `cdk/cdk/api/**` |
| N15 | Enforce param keywords with the reference JSON Schema implementation; never render a refused value | source-config | `cdk/cdk/api/param_rules.py` |
| N16 | Decide every type conversion from one published matrix generated from ARROW_FAMILIES | arrow-and-transport | `cdk/cdk/type_map/**`, `packages/conversion-matrix/**` |
| N17 | Compile mappings once; reject a batch wholesale, strictest strategy wins | mapping-and-transformations | `src/engine/mapping.py` (u), `src/engine/batch_policy.py` |
| N18 | Keep the safety window and runtime defaults in the engine, resolved runtime block > env > default | settings-reference | `src/config/settings.py`, `src/engine/pipeline_config_prep.py` |
| N19 | Fail a conformance run that assesses nothing for the connector's kind | conformance-kit | `cdk/cdk/conformance/**`, `tests/conformance_kit/**` |
| N20 | Commit generated bindings and artifacts and verify them in the built package | connector-module | `proto/**` (u), `src/grpc/generated/**` (u), `tools/contract_consumption.py` |

Remaining inventoried rules (intra-batch duplicates, transactional DDL, handler `land` vs
`write_batch`, credentials never cross gRPC, etc.) are assigned during each doc's migration:
folded into the nearest record above or given their own, decided in that PR.
