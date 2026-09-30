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

## An ADR stops being an ADR the moment it ships

- If code, tests, or runtime error messages cite a document as the authority
  for behavior, it is a **spec**, not a proposal. Rewrite it in the present
  tense and drop the proposal scaffolding — Problem/Migration/Consequences
  framing, future tense, "the implementing PRs will".
- **Section numbers in such a document are an API.** Docstrings and error
  messages cite them by number. Grep the source before renumbering; a shifted
  section silently sends a connector author to the wrong rule.

## Never version the filename

`sql-write-path-v2.md` is wrong even though no v1 existed. Filename versioning
guarantees either a stale `-v2` describing v3, or two files where one belongs.
The document is the current design; git holds the previous one.

## Point only at things that survive

- **No `file.py:123` coordinates.** Cite the function, class, or constant. A
  name survives edits above it, and a wrong name is visible where a wrong line
  number is not.
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
