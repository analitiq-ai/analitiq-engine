# Implementation Plan: Decision-Record Governance for Specs, Plans and Docs

**Branch**: one branch per PR, `<type>/<story-issue>-<slug>` (one sub-issue of #570 per user story) | **Date**: 2026-10-05 |
**Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-adr-governance/spec.md`

## Summary

Make decision records (adrkit ADRs in `docs/adr/`) the single home of every architecture rule,
cut hand-maintained docs from 10 to 2, and gate future work in two layers: Spec Kit hooks that load
and check governing decisions during planning (agent instructions), and CI (`adr lint` blocking,
adrkit Action reporting governing decisions on every PR). Delivered as a sequence of small PRs,
governing changes separate from governed content.

## Technical Context

**Language/Version**: Markdown + YAML; Python 3.11 for the hooks test; Node 24 for the adrkit Action

**Primary Dependencies**: adrkit CLI and GitHub Action (one pinned release), Spec Kit 1.0.7,
adrkit Spec Kit extension 0.1.4

**Storage**: files in git (`docs/adr/`, `.specify/`, `.claude/skills/speckit-*`)

**Testing**: `adr lint`; pytest test asserting the hook registry (PyYAML, declared dev dependency); quickstart scenarios

**Target Platform**: GitHub Actions CI + contributor/agent workstations

**Project Type**: documentation and governance tooling for an existing library/engine repo

**Performance Goals**: N/A

**Constraints**: public repo — no private or cloud detail in records; one validator per document
(no custom ADR checks); documentation.md rules (no issue numbers, no line coordinates, citations
move with their target)

**Scale/Scope**: 6 migrated + ~20 new records; 10 docs; ~25 code citations (5 runtime strings)

## Constitution Check

*Initial and post-design evaluation (same result).*

| Gate | Result | Note |
|---|---|---|
| I. Contract-first | PASS | Frontmatter is adrkit's schema; no new shape or second validator (empty-`affects` gap accepted, research R2) |
| II. Ownership & trust | PASS | All changes in this repo; records carry no private detail; vendored adrkit extension keeps LICENSE/NOTICE |
| III. Fail loud, no workarounds | PASS | No fallbacks; known tool gaps stated, not shimmed |
| IV. Test-first | PASS | Hooks test written red first; lint gate proven on a broken record before it is required |
| V. Smallest complete change | PASS | Every PR needed for the end state; no generator (FR-009 revised) |
| VI. Governing rule apart from governed code | PASS with note | Rule changes ship alone; citation-only code edits in PRs 6–15 justified in Complexity Tracking |
| R1–R12 | N/A | No engine behaviour changes; records describe existing rules |
| R13. Document tests to validator | PASS | The hooks test checks repo config, not authored connector documents |
| R14. Three leaks, one abstraction | PASS | 12 duplicated rules each collapse to one record |
| R15. Docs state the present | PASS | Citations move in the same commit; no versioned filenames |

## Project Structure

### Documentation (this feature)

```text
specs/001-adr-governance/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/governance-interfaces.md
└── tasks.md             # /speckit-tasks
```

### Repository paths touched

```text
.gitignore                      # re-includes for .specify/, .claude/skills/speckit-*, package.json
package.json, package-lock.json # pins the adrkit CLI (./node_modules/.bin/adr)
.specify/                       # tracked: extensions.yml, extensions/adrkit/, scripts/, templates/, memory/
.claude/skills/speckit-*/       # tracked: render the hooks
.claude/rules/documentation.md  # ADR clause amended
.github/workflows/adr.yml       # lint + adrkit Action
CONTRIBUTING.md                 # Merge Requirements
README.md                       # env-var table removed
docs/adr/                       # 6 migrated + new records
docs/architecture/ docs/data-path/ docs/config/ docs/testing/   # reduced
cdk/cdk/**, src/**              # citation repoints only
tests/                          # hooks registry test
```

**Structure Decision**: `docs/architecture/engine-architecture.md` becomes the orientation overview
(keeps its filename, avoiding citation churn); `docs/testing/conformance-kit.md` stays as the
how-to; the other 8 docs are deleted once their rules move.

## Mechanisms

- Spec Kit planning hooks that load and check governing decisions: R1
- adrkit lint as the blocking CI gate, one pinned adrkit version: R2
- Migration of the six existing records to adrkit frontmatter: R3
- Tracking the Spec Kit setup in git: R4
- Config docs extracted like the other docs, no generator: R5
- Rule-to-record migration, one doc per PR, citations moved with it: R6
- CI workflow layout for the decision-record checks: R7

## Delivery sequence

| PR | Content | Kind |
|---|---|---|
| 1 | Track `.specify/` and `.claude/skills/speckit-*`; `.gitignore` re-includes | setup |
| 2 | Amend documentation.md: ADRs keep alternatives/consequences, superseded not rewritten | governing |
| 3 | Pin one adrkit version; migrate ADRs 0001–0006 (dates, accepted, deciders, `affects`); correct 0006 | governed |
| 4 | CI: `decision records are valid` + adrkit Action; CONTRIBUTING Merge Requirements | governing |
| 5 | Hooks in `.specify/extensions.yml` | governed |
| 5b | Hook registry test `tests/unit/test_speckit_hooks.py` | governing |
| 6–15 | One per doc: new records, delete the doc's rule text, repoint citations, fix that doc's contradictions; settings-reference PR also drops the README env table | governed |
| 16 | Constitution: principles cite record ids; add FR-010 gates; MINOR bump | governing |

PRs 6–15 order: sql-write-path first (most citations, 5 runtime strings), engine-architecture last
(becomes the overview after the others drain into records).

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| PRs 6–15 edit constitution Source lines alongside the docs they migrate | A deleted doc section cited by the constitution would leave a dangling citation (R15) | Deferring all Source edits to PR 16 leaves the constitution citing deleted text for the whole migration; these edits are citation moves, not gate changes |
| PRs 6–15 write new records and edit the code that cites them (docstrings, comments, 5 runtime `Violation` strings) in one PR | A record's id does not exist until the PR that writes it; repointing in a later PR leaves code citing a deleted doc in between (R15) | Splitting into records-first then citations-second leaves every citation dangling for one merge; these edits change citation text only, no behaviour, so the record does not co-evolve with what it grades |
