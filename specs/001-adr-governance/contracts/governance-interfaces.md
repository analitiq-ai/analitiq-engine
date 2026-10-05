# Governance Interfaces

Interfaces this feature exposes to contributors, agents and CI. Record frontmatter is adrkit's
schema and is not restated here (see data-model.md).

## Spec Kit hooks (`.specify/extensions.yml`)

| Event | Command | `extension` label | `optional` |
|---|---|---|---|
| `before_plan` | `speckit.adrkit.context` | `adrkit` | `false` |
| `after_plan` | `speckit.adrkit.check` | `adrkit` (its own entry, flipped in place) | `false` |
| `before_implement` | `speckit.analyze` | `project` | `false` |
| `after_implement` | `speckit.converge` | `project` | `false` |

Both adrkit commands run on repo paths. Hooks pass no arguments, and with none `context` lists
the proposal queue and `check` reads only `plan.md`, so no decision governing `cdk/` or `src/` is
named. The constitution gate therefore has the planning agent pass paths: to `context`, the paths
the spec names; to `check`, the paths in the plan's "Source Code (repository root)" section, which the plan template
requires to hold real paths.

A test asserts exactly these four entries and flags, since `specify extension add adrkit` resets
adrkit's entry to optional and CLI writes drop comments.

## CI checks

| Check name | Runs | Blocks merge when |
|---|---|---|
| `decision records are valid` | `adr lint` on `docs/adr/` | any record has an error finding |
| adrkit Action | `adr check` on the PR's changed files | a changed record is invalid; otherwise it only comments the governing decisions |

PR 4 lists both in CONTRIBUTING.md "Merge Requirements"; a maintainer then makes both required in
branch protection.
CI and local use one pinned adrkit version.

## Citation form

Code, docstrings, runtime error text and Markdown cite a record as `ADR NNNN` (optionally with its
title), linking `docs/adr/NNNN-slug.md` in Markdown. Section numbers inside a record are not
cited; a rule that needs its own citation is its own record.
