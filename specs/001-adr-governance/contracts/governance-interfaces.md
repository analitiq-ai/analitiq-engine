# Governance Interfaces

Interfaces this feature exposes to contributors, agents and CI. Record frontmatter is adrkit's
schema and is not restated here (see data-model.md).

## Spec Kit hooks (`.specify/extensions.yml`, registered by PR 5)

| Event | Command | `extension` label | `optional` |
|---|---|---|---|
| `before_implement` | `speckit.analyze` | `project` | `false` |
| `after_implement` | `speckit.converge` | `project` | `false` |

A test (PR 5b) asserts these two entries and their flags, since CLI writes re-dump the file.

## CI checks

| Check name | Runs | Blocks merge when |
|---|---|---|
| `decision records are valid` | `adr lint` on `docs/adr/` | any record has an error finding |

PR 4 lists it in CONTRIBUTING.md "Merge Requirements"; a maintainer then makes it required in
branch protection.
CI and local use one pinned adrkit version.

## Citation form

Code, docstrings, runtime error text and Markdown cite a record as `ADR NNNN` (optionally with its
title), linking `docs/adr/NNNN-slug.md` in Markdown. Section numbers inside a record are not
cited; a rule that needs its own citation is its own record.
