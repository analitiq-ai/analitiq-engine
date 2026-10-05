# Analitiq Core Engine Constitution

Constitution for the Analitiq core engine: the runtime that moves data from source to destination
using connectors, connections and pipelines, and the Connector CDK it publishes. Each principle is
a plan-time gate: a plan passes only if it answers the gate question. The source files named are
the source of truth; this document indexes them and never restates their text.

## Core Principles

### Inherited Organisation Principles (Analitiq constitution v1.0.0)

### I. Contract-First, One Source of Truth per Domain

Gate: does the plan introduce a data shape, validator, type-name list or hand-written schema? If so,
it MUST name the owning artifact from the contracts map and be an adapter against it. A new contract
or a second validator over an already-gated shape fails the gate.

Source: Analitiq organisation rule set (contracts map; not vendored in this repo);
`.claude/rules/coding-principles.md` (Structure & consistency: contract-first, define once).

### II. Ownership & Trust Boundaries

Gate: is every change placed in the repo that owns it; is external input validated where it enters;
does no internal, infrastructure or cloud-specific detail reach a public repo; does no public-repo
issue link to a private one; are there no secrets in code?

Source: Analitiq organisation rule set (repo roles; not vendored in this repo);
`.claude/rules/coding-principles.md` (Security & data; ownership);
`.claude/rules/github-workflow.md` (branch rule 7; other GitHub rules).

### III. Fail Loud, No Workarounds, No Backwards Compatibility

Gate: does the design fix the cause rather than mask the symptom, with no fallbacks, swallowed
errors or compatibility branches unless explicitly instructed? If the current layer cannot meet the
spec, the plan MUST say so and name the real fix instead.

Source: `.claude/rules/coding-principles.md` (Correctness & failure handling; No workarounds; Global
constraints); `.claude/rules/pr-review-loop.md` (Triage hard rules, last bullet).

### IV. Test-First & Verified

Gate: does every task start from a test that fails for the expected reason, is logic testable in
isolation, and is "done" defined as executed and verified against the spec?

Source: `.claude/rules/coding-principles.md` (Testing & verification).

### V. Smallest Complete Change

Gate: is there no speculative generality or dead code; is scope defined by what a working deployment
needs; does a fix for a class of defect cover every instance of it in the files touched?

Source: `.claude/rules/coding-principles.md` (Scope discipline).

### VI. Governing Rule Ships Apart from Governed Code

Gate: if the plan changes a rule file, lint, guard or schema check together with the code it
governs, does it split them into separate PRs?

Source: `.claude/rules/coding-principles.md` (Scope discipline: never move a governing rule and the
code it governs in the same PR).

### Repo-Specific Principles

### R1. Engine Owns the Arrow Type System

Gate: does every new or changed Arrow family, conversion rule or type-name list land in the CDK
type-map tables, with the published grammar, conversion matrix and npm package regenerated from them
(never hand-edited) under a bumped version?

Source: `cdk/cdk/type_map/grammar.py` and `cdk/cdk/type_map/conversions.py` (module docstrings);
`.github/workflows/conversion-matrix.yml`; `packages/conversion-matrix/README.md` (Source of truth).

### R2. Connector-Agnostic Engine, CDK as Toolbox

Gate: would this change have to change again when a new database or API is added? If yes, does the
plan put it in the connector rather than in the engine or a generic CDK class?

Source: `docs/adr/0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md`;
`docs/architecture/connector-module-architecture.md` (§7 Anti-monolith principles).

### R3. Published Contracts Lead, the Engine Does Not Adapt to Connectors

Gate: does the plan change the engine to accommodate an authored connector, rather than fixing the
connector in its own package against the published contracts, validator and CDK?

Source: `.claude/CLAUDE.md` (Connector architecture: driver belongs to the connector; Coding Rules
for Engine Repo) — local-only file.

### R4. Isolated Connectors, Engine-Side Secrets

Gate: does connector code stay behind the process boundary, receiving only resolved values, with
secret resolution on the engine side of that boundary?

Source: `.claude/CLAUDE.md` (Connector architecture: isolate connector execution; secrets stay
engine-side) — local-only file; `docs/architecture/connector-module-architecture.md` (secrets seam).

### R5. One Wire Protocol, One Ack per Batch

Gate: does a protocol change edit the `proto/` definitions and regenerate the committed bindings,
and does it keep one batch, one ack, one cursor persist — no early or windowed acks?

Source: `docs/architecture/grpc-streaming-architecture.md` (Wire Protocol; Key Design Decisions);
`docs/adr/0006-batch-coalescing-is-engine-side.md` (the ack shapes it rules out).

### R6. One Binding Grammar Across Transports

Gate: does every transport source values through the single resolver and ref grammar, with identical
author intent behaving identically on every transport and engine-built objects in their own typed
slot?

Source: `.claude/CLAUDE.md` (Engine Design Principles) — local-only file.

### R7. Stage-Then-Merge Is the Only SQL Write

Gate: does every SQL write path land in a stage table and run exactly one mode statement to target,
with native bulk paths only through the declared, conformance-certified hook?

Source: `docs/adr/0005-stage-then-merge-is-the-single-sql-write-primitive.md`;
`docs/data-path/sql-write-path.md`.

### R8. Capability Is Derived, Never Declared

Gate: does the plan decide what a connector can do from Protocol conformance and runtime
authorization, never from a static capability flag?

Source: `docs/adr/0004-capability-is-derived-never-declared.md`.

### R9. One Stop Rule for Paging

Gate: does a paging change go through the single page loop and its stop conditions, rather than a
scheme-specific stop rule?

Source: `docs/adr/0002-one-stop-rule-for-every-paging-scheme.md`.

### R10. Two Failure Vocabularies, One-Way Adapter

Gate: does a failure reported by a peer stay in the wire vocabulary, with customer-facing codes
assigned only by the engine through the one-way adapter?

Source: `docs/adr/0001-two-failure-vocabularies.md`.

### R11. Idempotent, Durable Writes

Gate: is every write and retry in the design deterministic and safe to replay, with progress
persisted durably?

Source: `CONTRIBUTING.md` (Coding Guidelines); `.claude/CLAUDE.md` (Engine Design Principles:
fault-tolerant) — local-only file.

### R12. Contract Consumption Is Pinned and Censused

Gate: does a plan that adds or removes a read on a contract model regenerate the consumption
manifest, and does it consume the contract models and validator at the agreed published pin?

Source: `CONTRIBUTING.md` (Continuous Integration); `tools/contract_consumption.py`;
`tools/check_contract_version_pin.py`; `docs/architecture/connector-module-architecture.md`
(§5 CDK packaging: the manifest is published as versioned JSON per CDK release).

### R13. Document Tests Belong to the Validator

Gate: is any planned test whose verdict depends on the authored documents alone routed to the
validator's repo instead of this one?

Source: `.claude/rules/test-ownership.md`.

### R14. Three Leaks Means One Abstraction

Gate: when three or more findings share one mechanism, does the plan fix the class in one change
rather than the instances?

Source: `CONTRIBUTING.md` (Consolidation Rule).

### R15. Docs State the Present

Gate: does a plan that renames, moves or renumbers a document update every citation, and does it add
no versioned filename or migration narrative?

Source: `.claude/rules/documentation.md`.

## Platform Constraints

Facts a plan can get wrong. Each is owned by the file named; consult it rather than this summary.

- Public repository: no private deployment or cloud references (`.claude/CLAUDE.md`, Coding Rules
  for Engine Repo — local-only file).
- Runtime: one Docker image in two roles, source and destination, joined by gRPC
  (`docs/architecture/grpc-streaming-architecture.md`; `docker/`).
- Engine layout, pipeline lifecycle, registries: `docs/architecture/engine-architecture.md`.
- CDK packaging, extras and connector attachment:
  `docs/architecture/connector-module-architecture.md` (§5–§6); CDK release (PyPI, one per `cdk-v*`
  tag): `docs/architecture/connector-module-architecture.md` (§8 Ownership and distribution),
  `.github/workflows/publish-cdk.yml`.
- Settings and their resolution order: `docs/config/settings-reference.md`.
- Connector conformance suite: `docs/testing/conformance-kit.md`.
- Pipeline, connection, connector and stream JSON files are authored documents, not engine code;
  plans do not edit them (`.claude/CLAUDE.md` — local-only file).

## Delivery Workflow

Not gates — context so `/speckit-tasks` shapes tasks to fit: issue-numbered branch, draft PR, review
rounds, then the required status checks. Owned by `CONTRIBUTING.md` (CI; Merge Requirements),
`.claude/rules/github-workflow.md` and `.claude/rules/pr-review-loop.md`.

## Governance

- Inherits the Analitiq organisation constitution v1.0.0. Principles I–VI restate its rules as gate
  questions and are not weakened here; amendments to them happen upstream first and are then re-synced.
- The source files named in each principle are the source of truth. Where this document and a source
  disagree, the source wins and this document is amended. Under `.claude/`, only
  `rules/documentation.md` and `rules/test-ownership.md` are tracked in this repository; every other
  file cited there (`CLAUDE.md`, `rules/coding-principles.md`, `rules/github-workflow.md`,
  `rules/pr-review-loop.md`) exists only in local working copies.
- Amending a repo principle: change its source first, in its own PR (Principle VI), then amend this
  document and bump its version — MAJOR for removing or redefining a principle, MINOR for adding a
  principle or section, PATCH for wording.
- Compliance: `/speckit-plan` evaluates every gate in its Constitution Check; a violation MUST be
  justified in the plan's Complexity Tracking table or the plan changes.

**Version**: 1.0.2 | **Ratified**: 2026-09-24 | **Last Amended**: 2026-10-05
