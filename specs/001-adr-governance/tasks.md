---

description: "Task list for decision-record governance"
---

# Tasks: Decision-Record Governance for Specs, Plans and Docs

**Input**: Design documents from `specs/001-adr-governance/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/governance-interfaces.md,
quickstart.md

**Tests**: Included where the constitution requires test-first (IV): the hooks registry test and
the lint gate are each proven red before they pass.

**Organization**: Grouped by user story. `PR n` names the PR from plan.md "Delivery sequence";
each PR is its own branch (`<type>/<story-issue>-<slug>`) and worktree, where `<story-issue>` is the
sub-issue for that PR's user story (T000). Each PR
checks off its own tasks in this file, so every branch also changes the spec folder.

**Record rules for every task that writes a record** (data-model.md): file `docs/adr/NNNN-slug.md`;
`id` "quoted 4-digit string, next free number; never reused"; `title` "the decision as an
imperative statement, 3–120 chars"; `deciders: ["@Analitiq-Bot-Wonka"]`; `affects` "non-empty list
of `{type: path, pattern: <glob>}`; every pattern matches a real path"; `provenance.authoredBy:
agent`; `status: draft` (automation creates `draft` only, FR-012); body sections Context,
Decision, Options considered, Trade-offs, Consequences; no issue numbers, no `file.py:123`.

**Citation rule for every migration task**: before deleting text, run
`grep -rnE "<doc-filename>|ADR ?§" --include=*.py --include=*.md --include=*.toml .` (excluding
`specs/`, `CHANGELOG.md`, `.venv`, `connectors/`) and repoint every hit to `ADR NNNN` in the same
commit, runtime strings included.

## Phase 1: Setup (PR 1)

**Purpose**: Governance setup is in git so every clone, the bot and CI see it (FR-001).

- [X] T000 Create three sub-issues of #570 (type Task), one per user story, each with that story's goal and Independent Test from spec.md as acceptance criteria; Setup and Foundational PRs use the US1 sub-issue
- [X] T001 Add `.gitignore` re-includes so `.specify/` survives the global `*.json`, and unanchored `scripts/` rules, and re-include `.claude/skills/speckit-*/` next to the existing `!.claude/rules/`; ignore `.specify/feature.json`, `.specify/extensions/.cache/`, `.specify/extensions/.backup/` in `.gitignore`
- [X] T002 Remove `/.specify/` from `.git/info/exclude` (local only; note it in the PR body so other clones do the same)
- [X] T003 Strip the Sync Impact Report comment from `.specify/memory/constitution.md` and confirm it states no private or cloud detail
- [X] T004 Stage `.specify/extensions.yml`, `.specify/extensions/.registry`, `.specify/extensions/{adrkit,assess,critique,grill}/**`, `.specify/integrations/`, `.specify/scripts/`, `.specify/templates/`, `.specify/memory/`, `.specify/workflows/`, `.specify/integration.json`, `.specify/init-options.json`, `.claude/skills/speckit-*/`, `specs/001-adr-governance/`; verify with `git status --ignored` that nothing listed in T001's ignore set is staged
- [X] T005 Verify in a fresh clone (`git clone` to scratchpad) that `specify extension list` shows adrkit and `.claude/skills/speckit-plan/SKILL.md` exists; open PR 1

---

## Phase 2: Foundational (PR 2)

**Purpose**: The governing rule for records lands alone, before any record is written (FR-005,
Principle VI).

**⚠️ CRITICAL**: No record may be written or migrated until this PR merges.

- [X] T006 Rewrite the "An ADR stops being an ADR the moment it ships" section of `.claude/rules/documentation.md`: records in `docs/adr/` keep Context/Decision/Options/Consequences after shipping, an accepted record's decision text is never edited (fixing a link or citation is allowed), a changed decision is a new record that supersedes it, and behaviour is cited as `ADR NNNN` rather than a doc section number
- [X] T007 Open PR 2 containing `.claude/rules/documentation.md` and this file's T006–T007 ticks

**Checkpoint**: Records can now be written.

---

## Phase 3: User Story 1 - Existing decisions valid and enforced (Priority: P1) 🎯 MVP

**Goal**: Six records pass validation; every PR is linted and gets its governing decisions reported.

**Independent Test**: quickstart.md scenarios 1–3.

### PR 3 — migrate records

- [ ] T008 [US1] Add root `package.json` (private, no scripts) with devDependency `@adrkit/cli` pinned exactly to the version the `mbeacom/adrkit` Action release bundles, plus `package-lock.json`; add `!/package.json` and `!/package-lock.json` re-includes to `.gitignore` (global `*.json` rule); run `npm ci` and confirm `./node_modules/.bin/adr --version` matches; document `npm ci` under `CONTRIBUTING.md` "Setup" — the adrkit extension resolves `./node_modules/.bin/adr` with no env var
- [ ] T009 [US1] Run `./node_modules/.bin/adr migrate --from madr --dir docs/adr` on `docs/adr/0001-*.md` … `docs/adr/0006-*.md`
- [ ] T010 [P] [US1] In each of `docs/adr/0001-*.md` … `0006-*.md` set `date` to the commit that introduced the rule (`git log --reverse -S '<rule phrase>' --format=%ad --date=short` over code and docs; not the file-add date, since 0003–0006 were added together by a docs move), `status: accepted`, `deciders: ["@Analitiq-Bot-Wonka"]` (FR-012: approving PR 3 is the ratification of these six)
- [ ] T011 [P] [US1] Write `affects` for each of the six records from the code each one governs (e.g. 0005 → `cdk/cdk/sql/stage_cycle.py`, `cdk/cdk/sql/*backend.py`, `cdk/cdk/sql/generic.py`; 0002 → the API page loop module) and confirm each with `adr explain <path>`
- [ ] T012 [US1] Correct `docs/adr/0006-batch-coalescing-is-engine-side.md` and `docs/data-path/sql-write-path.md` §8: a fatally rejected coalesced unit fails the stream whatever the strategy (DLQ writes the unit out), while a unit that exhausts its retries is DLQ'd or skipped, matching `BatchPolicy.run` in `src/engine/batch_policy.py`
- [ ] T013 [US1] Scope `docs/adr/0002-one-stop-rule-for-every-paging-scheme.md` explicitly to the API read path (SQL read paths stop on a short page)
- [ ] T014 [US1] Run `./node_modules/.bin/adr lint --dir docs/adr`: exit 0, zero errors; open PR 3

### PR 4 — CI gate

- [ ] T015 [US1] On a scratch branch, break one record (remove `date`) and run `./node_modules/.bin/adr lint`: confirm exit 1 for the expected `required-field` reason; discard
- [ ] T016 [US1] Create `.github/workflows/adr.yml` with job `name: decision records are valid` (checkout, `actions/setup-node`, `npm ci && ./node_modules/.bin/adr lint --dir docs/adr`) and a job using `mbeacom/adrkit` at the tag matching `package-lock.json` with `dir: docs/adr` and `pull-requests: write`, following the `setup-node` pattern in `.github/workflows/conversion-matrix.yml`
- [ ] T017 [US1] Add `decision records are valid` and the adrkit Action check to `CONTRIBUTING.md` "Continuous Integration" and "Merge Requirements"; in the PR body, ask a maintainer to add both to branch protection on `main`
- [ ] T018 [US1] Open PR 4; confirm the Action posts the governing decisions for a touched governed file (quickstart scenario 2)

**Checkpoint**: US1 complete — SC-001 and SC-006 verifiable.

---

## Phase 4: User Story 2 - Every rule recorded once (Priority: P2)

**Goal**: Rules move from 10 docs into records; hand-maintained docs drop to 2 (SC-002, SC-003).

**Independent Test**: quickstart.md scenario 4 after each PR.

Each doc PR: write its records (data-model.md "Record catalog"), delete the rule text, fold the
descriptive remainder into `docs/architecture/engine-architecture.md` or delete it, repoint
citations, update the constitution Source lines that cite the doc (Complexity Tracking), delete the
doc when empty. A rule already recorded by an earlier PR is only deleted, not re-recorded.

### PR 6 — sql-write-path

- [ ] T019 [US2] Write records N8 and N9, plus records for the remaining §2/§3/§4/§6/§7/§9 rules (refuse empty conflict_keys, intra-batch duplicates, backends render no SQL, hook composition, transactional DDL, verdict table) in `docs/adr/`
- [ ] T020 [US2] Repoint the 5 runtime `Violation` strings and docstrings in `cdk/cdk/conformance/surface.py`, `cdk/cdk/conformance/declaration.py`, `cdk/cdk/conformance/__init__.py`, `cdk/cdk/conformance/tier1/`, `cdk/cdk/conformance/tier2/`, `cdk/cdk/sql/{capabilities,stage_cycle,backend,dialects,generic,adbc_backend}.py`; fix the dangling "ADR §n" references in `cdk/cdk/sql/{generic,adbc_backend,ddl,discovery,__init__}.py` and `cdk/cdk/types.py`; `tests/unit/cdk_tests/test_retry_semantics.py` (§9 → verdict-table record), `tests/unit/cdk_tests/sql/test_write_plan.py` (§3), `tests/unit/cdk_tests/sql/test_adbc_backend.py` (§6–§7, 'ADR §6'), `tests/unit/destination/handlers/test_database_handler_endpoint_refs.py` ('ADR §2')
- [ ] T021 [US2] Run `poetry run pytest cdk/cdk/conformance tests/conformance_kit tests/unit/cdk_tests tests/unit/test_cdk_boundary.py tests/unit/destination/handlers` (runtime strings changed); delete `docs/data-path/sql-write-path.md`; fix links (only links) in `docs/adr/0004-*.md`, `0005-*.md`, `0006-*.md`, and update `docs/testing/conformance-kit.md`, other docs, `.specify/memory/constitution.md` R7

### PR 7 — grpc-streaming-architecture

- [ ] T022 [US2] Write records N2, N3, N4, N5 and records for one-image-two-roles, Arrow-IPC-only payload, emitted_at stamping, SHA-256 record ids, retry_semantics never consulted in `docs/adr/`
- [ ] T023 [US2] Check `src/destination/server.py`'s idempotency-ledger comment against N3 ("no in-run pre-send skip"); correct whichever is wrong
- [ ] T024 [US2] Repoint citations, update `.specify/memory/constitution.md` R5 and Platform Constraints; delete `docs/architecture/grpc-streaming-architecture.md`

### PR 8 — connector-module-architecture

- [ ] T025 [US2] Write records N12, N13, N20 and records for resolve-by-connector_id-then-kind, best-effort entry-point discovery, one class in both entry-point groups, TypeMapper directions in `docs/adr/`
- [ ] T026 [US2] Repoint `cdk/pyproject.toml`, `cdk/cdk/registry.py`, `cdk/cdk/base_handler.py`, `tests/unit/test_cdk_boundary.py` (module docstring and the assertion message at the 'ADR §4.1' line → N12), `docs/adr/0003-*.md`, `docs/adr/0004-*.md` links, `.specify/memory/constitution.md` R2/R4/R12 and Platform Constraints; delete `docs/architecture/connector-module-architecture.md`

### PR 9 — arrow-and-transport-strategy

- [ ] T027 [US2] Write records N10 and N16 (Arrow stops where the destination consumes rows folds into N16 or its own record) in `docs/adr/`
- [ ] T028 [US2] Repoint citations; delete `docs/data-path/arrow-and-transport-strategy.md`

### PR 10 — mapping-and-transformations

- [ ] T029 [US2] Write record N17 and records for no dot-splitting, pure assignments, mapping defects are TransformationError under any strategy in `docs/adr/`
- [ ] T030 [US2] Repoint citations; delete `docs/data-path/mapping-and-transformations.md`

### PR 11 — source-config

- [ ] T031 [US2] Write records N14, N15 and records for connection keyed by directory name, required param fails before first request, incremental stream needs a mapped cursor_field in `docs/adr/`
- [ ] T032 [US2] Repoint citations; delete `docs/config/source-config.md`

### PR 12 — destination-config

- [ ] T033 [US2] Write records for credentials never cross gRPC, GenericAPIConnector varies only by endpoint operations, s3 raises StorageBackendNotBuiltError, land vs write_batch, advertise nothing until the relay has something in `docs/adr/`
- [ ] T034 [US2] Correct the README "file manifest" wording to content-addressed file names; repoint citations; delete `docs/config/destination-config.md`

### PR 13 — settings-reference

- [ ] T035 [US2] Write record N18 (safety window and runtime defaults engine-owned; runtime block > env > default; read on use) in `docs/adr/`
- [ ] T036 [US2] Delete the README "Environment Variables" table, pointing to `src/config/settings.py`; repoint the two README anchors to settings-reference; delete `docs/config/settings-reference.md`

### PR 14 — conformance-kit (kept as how-to)

- [ ] T037 [US2] Write record N19 and records for tier 1 mandatory, round-trip fixed point, guards tested with the guard removed, ANALITIQ_CONFORMANCE_REQUIRE_LIVE fails not skips in `docs/adr/`
- [ ] T038 [US2] Strip rule statements from `docs/testing/conformance-kit.md`, citing records instead; remove "v2 surface" / "post-ADR" wording there and in `tests/conformance_kit/reference_connector.py`

### PR 15 — engine-architecture becomes the overview

- [ ] T039 [US2] Write records N1, N6 (or amend-by-supersede 0001), N7, N11 in `docs/adr/`
- [ ] T040 [US2] Rewrite `docs/architecture/engine-architecture.md` as the orientation overview: components and data flow, links to records, no rule, no file inventory (drop Module Layout); repoint `cdk/cdk/resolver.py` and README citations
- [ ] T041 [US2] Rule audit: for every record in `docs/adr/`, grep its key phrase across `docs/` and `.specify/memory/constitution.md`; zero restatements (SC-002)

**Checkpoint**: US2 complete — 2 hand-maintained docs remain.

---

## Phase 5: User Story 3 - Plans checked before implementation (Priority: P3)

**Goal**: Planning loads and checks governing decisions; constitution gates cite records (FR-010,
FR-011). PR 5 can land any time after PR 4, PR 5b after PR 5; PR 16 lands after PR 15.

**Independent Test**: quickstart.md scenario 5.

### PR 5 — hooks

- [ ] T042 [US3] Edit `.specify/extensions.yml`: flip adrkit's `after_plan` entry to `optional: false` in place; add `before_plan` `speckit.adrkit.context` (extension: adrkit), `before_implement` `speckit.analyze` and `after_implement` `speckit.converge` (extension: project), all `optional: false`; run `/speckit-plan` on a scratch spec and confirm both plan hooks fire (they run without paths until PR 16 adds gate (e)); open PR 5

### PR 5b — hook registry guard

- [ ] T043 [US3] Write `tests/unit/test_speckit_hooks.py` asserting exactly the four entries in contracts/governance-interfaces.md; add `pyyaml` to `[tool.poetry.group.dev.dependencies]` in `pyproject.toml` (the test imports it directly; it is only present today as a transitive dev dependency) and commit the `poetry lock` diff; prove it red by running it against the pre-PR-5 file (`git show <PR5-base>:.specify/extensions.yml` into a temp path the test reads via a parameter), then green on `main`; open PR 5b

### PR 16 — constitution

- [ ] T044 [US3] Amend `.specify/memory/constitution.md` via `/speckit-constitution`: every principle stating an architecture rule cites record ids; add gates (a) a plan contradicts no accepted record governing its paths, (b) superseding requires a new record in the same plan, (c) a plan choosing between real alternatives produces a draft record via `/speckit-adrkit-draft`, (d) flow-forward change model, (e) the planning agent runs `speckit.adrkit.context` on the paths the spec names and `speckit.adrkit.check` on the paths in the plan's "Source Code (repository root)" section; MINOR bump; remove the Sync Impact Report before commit; open PR 16
- [ ] T045 [US3] On a scratch spec touching `cdk/cdk/sql/`, confirm `context` and `check` each run on those paths and name ADR 0005; then run quickstart.md scenario 5: three throwaway specs contradicting ADR 0005, ADR 0006 and N5; `/speckit-plan` flags all three naming the record (SC-005); discard the specs

---

## Phase 6: Polish & Cross-Cutting Concerns

- [ ] T046 Run all quickstart.md scenarios against `main`
- [ ] T047 Ratify: maintainer moves each new record in `docs/adr/` from `draft → proposed → accepted` (FR-012; not automated); `adr lint` stays green
- [ ] T048 Citation resolution (SC-004): `grep -rnoE "ADR ?[0-9]{4}" --include=*.py --include=*.md --include=*.toml .` (excluding `specs/`, `CHANGELOG.md`, `.venv`, `connectors/`, `node_modules/`); every cited id has a file matching `docs/adr/<id>-*.md`; zero misses, and zero remaining `ADR ?§` hits
- [ ] T049 Update the stale FR-009 note in `specs/001-adr-governance/checklists/requirements.md`; close #570

---

## Dependencies & Execution Order

- PR 1 → PR 2 → PR 3 → PR 4 → PR 6 … PR 15 (sequential: each migration may delete text a later doc links to) → PR 16
- PR 5 depends on PR 1 and PR 4; PR 5b depends on PR 5; both can run alongside PRs 6–15
- US1 blocks US2 (records must validate) and US3 (checks need a valid corpus)

## Parallel Opportunities

- T010 and T011 touch six separate files and can be split per record
- PRs 5 and 5b (T042–T043) run in parallel with the US2 migration PRs
- Within a migration PR, writing separate records is parallel; citation repointing is not (shared files)

```text
# PR 3, parallel per record:
Task: "Set date/status/deciders in docs/adr/0001-two-failure-vocabularies.md"
Task: "Set date/status/deciders in docs/adr/0005-stage-then-merge-is-the-single-sql-write-primitive.md"
```

## Implementation Strategy

1. MVP = PR 1–4 (US1): valid corpus, lint gate, governing-decision comments.
2. PR 5 makes planning load and check decisions.
3. PRs 6–15 drain the docs one at a time; each is independently mergeable.
4. PR 16 closes the loop in the constitution.
