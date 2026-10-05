---
paths:
  - "docs/**/*.md"
  - "*.md"
  - "**/README.md"
---

# Documentation and ADRs

Applies when writing or editing any Markdown document in this repo.

## A document carries one state: what is true now

- **No migration sections, changelogs, or "what shipped" lists.** Work that
  landed is described as how the system behaves. Work that has not is a plain
  statement about the system — "the engine does not coalesce" — never an open
  ticket.
- **No issue numbers.** If a number carried a reason, write the reason;
  provenance lives in git and the tracker. `CONTRIBUTING.md` and `CHANGELOG.md`
  are the exceptions, being about process and history by definition. Files
  under `specs/` are also exempt: a spec belongs to one issue and names it.
- **No "today", "currently", or "unchanged from before".** The reader has no
  access to the version being compared against, so the comparison is noise that
  becomes a lie.
- **Never cross-link a public repo's docs to a private tracker** (`<private-repo>#123`).
  That leaks internal references into public code.
- **Decision records in `docs/adr/` are exempt from this section.** Each must carry
  a `date` and state what was true and what was decided on it; the current
  state is the set of accepted records that nothing supersedes.

## A decision record keeps its decision after it ships

- **Decision records live in `docs/adr/`** and keep Context, Decision, Options
  considered, and Consequences after the decision ships. They are never
  rewritten into present-tense specs.
- **An accepted record's decision text is never edited.** Fixing a link or a
  citation is allowed. A changed decision is a new record that supersedes the
  old one.
- **Code, docstrings, and runtime error text cite behavior as `ADR NNNN`**,
  never a document section number.
- **Before renaming, superseding, or deleting a record, grep for its id** and
  repoint every citation in the same commit.

## Never version the filename

`sql-write-path-v2.md` is wrong even though no v1 existed. Filename versioning
guarantees either a stale `-v2` describing v3, or two files where one belongs.
The document is the current design; git holds the previous one. A superseded
decision record is not a version: it keeps its own id beside the record that
supersedes it.

## Point only at things that survive

- **No `file.py:123` coordinates.** Cite the function, class, or constant. A
  name survives edits above it, and a wrong name is visible where a wrong line
  number is not.
  Files under `specs/` are exempt: a spec is a working document for one issue.
- **No private helpers** (`_apply_write_in_txn`). Private names get refactored
  without notice; cite the public entry point.
- **No file inventories** that duplicate the directory tree. They rot silently
  as modules come and go.

## Verify every name against the source before writing it

Every symbol, module path, and mechanism a doc names must be **read in the
source first** — not recalled, not inferred from the surrounding prose. The
drift found in this repo was entirely this class:

- `cdk/cdk/sql_types.py` — deleted in a refactor, still cited in five places.
- `build_http_transport`, `_materialize_derived` — never existed under those
  names.
- `partial_result` — a mechanism that is not threaded at all. Every name in the
  sentence was plausible and the claim was still false; only reading the type
  `Callable[[pa.RecordBatch], pa.Array]` disproved it.

A plausible-sounding name is not evidence. Neither is a name appearing
elsewhere in the same document.

## Changing a doc means changing what cites it

Before renaming or moving a document, grep for it across `*.py`, `*.md` and
tests. A rename updates every citation in the same commit — this repo had 29
citations to one document, including runtime error strings.
