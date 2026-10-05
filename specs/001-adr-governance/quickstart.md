# Quickstart: Validate Decision-Record Governance

Prerequisites: the pinned adrkit CLI (`adr`), Spec Kit (`specify`), a fresh clone of the repo.

## 1. Records are valid (US1, SC-001)

```shell
adr lint --dir docs/adr
```

Expected: exit 0, zero errors. Then break one record (delete its `date`) on a branch and open a PR:
the `decision records are valid` check fails.

## 2. Governing decisions are reported (US1, FR-004)

```shell
adr explain cdk/cdk/sql/stage_cycle.py
```

Expected: lists ADR 0005. On a PR touching that file, the adrkit Action comment names ADR 0005.

## 3. Fresh clone sees the governance setup (US1, SC-006)

In a fresh clone: `.specify/memory/constitution.md`, `.specify/extensions.yml`,
`.specify/extensions/adrkit/` and `.claude/skills/speckit-plan/` exist; `specify extension list`
shows adrkit. The hooks test passes under `pytest`.

## 4. Rules stated once, citations resolve (US2, SC-002, SC-004)

For a migrated doc, grep its old filename and section references across `*.py`, `*.md` and tests:
zero hits. Every `ADR NNNN` cited in code resolves to a file in `docs/adr/`.

## 5. Contradicting plans are flagged (US3, SC-005)

Create three throwaway feature specs that contradict ADR 0005 (a direct dialect upsert), ADR 0006
(destination-side coalescing) and N5 (client keepalive). Run `/speckit-plan` on each.

Expected: the `after_plan` check, run on each plan's "Repository paths touched", names the
contradicted record for one of those source paths in all three. Discard the specs.
