# Research: Decision-Record Governance

## R1: Hooks on core commands

- **Decision**: Register `before_plan` → `speckit.adrkit.context`, `before_implement` →
  `speckit.analyze`, `after_implement` → `speckit.converge` by hand in `.specify/extensions.yml`,
  all `optional: false`. Label the two core hooks `extension: project` so `specify extension
  add/remove adrkit` does not delete them. Flip adrkit's own `after_plan` entry to
  `optional: false` in place; do not add a second entry.
- **Rationale**: Spec Kit 1.0.7 `HookExecutor` never checks that a hook's command belongs to an
  installed extension; the skills render any id (`speckit.analyze` → `/speckit-analyze`).
  `register_hooks`/`unregister_hooks` only replace entries whose `extension` matches the manifest
  id. The extension user guide documents hand-editing `extensions.yml`.
- **Paths**: hooks pass no arguments, so the planning agent supplies them (the spec's paths to
  `context`, the plan's "Repository paths touched" to `check`); without them `context` lists the
  queue and `check` reads only `plan.md`.
- **Caveats**: every CLI write re-dumps the YAML (comments are lost), and re-adding adrkit resets
  its `after_plan` entry to optional. A test asserts the four hooks and their flags.
- **Alternatives considered**: a preset that overrides command templates — heavier, fights the
  composition model; leaving hooks optional — agents skip them.
- **Existing tools**: Spec Kit's own hook system (`.specify/extensions.yml`, `HookExecutor`) and the adrkit extension's
  registered hooks; a Spec Kit preset was looked at and rejected (see Alternatives).

## R2: adrkit CLI behaviour and version

- **Decision**: `adr lint` is the blocking CI gate. `adr check` runs via the adrkit GitHub Action,
  which comments the governing decisions on the PR. Upgrade the local CLI and pin CI to the same
  adrkit release (the Action's release), one version everywhere. PR 3 adds a root `package.json`
  with the CLI as a devDependency and commits its `package-lock.json` as the pin; the extension
  resolves it as `./node_modules/.bin/adr`. The Action tag is bumped together with it.
- **Rationale**: `adr lint` exits 1 on schema errors, unknown keys, broken
  `supersedes`/`supersededBy`/`relatesTo` links, duplicate ids, accepted-without-decider.
  `adr check` exits 1 only when a changed record is invalid; a governed file never fails it. The
  Action (`action.yml`, Node 24) posts the governing decisions as a PR comment.
- **Known gap**: an empty `affects` is advisory, not a lint error. Record review covers it
  (spec edge case). No second validator is written (one gate per document).
- **Alternatives considered**: a custom script failing on empty `affects` — a second validator
  over adrkit's shape; `adr evaluate` with a snapshot bundle — needs bundle tooling, deferred.
- **Existing tools**: adrkit CLI `lint`/`check`/`explain` and the `mbeacom/adrkit` GitHub Action; no second validator.

## R3: Frontmatter for the six existing records

- **Decision**: `adr migrate --from madr`, then set per record: quoted `id`, `status: accepted`,
  real `date` from the commit that introduced the rule (not the file-add commit: 0003–0006 were
  added together by a docs move), `deciders: ["@Analitiq-Bot-Wonka"]`, and
  `affects` path globs that match real paths.
- **Rationale**: dry run migrated all six with only date/status warnings; schema requires `id`,
  `title`, `status`, `date`; accepted requires `deciders`.
- **Correction folded in**: ADR 0006 says a fatally rejected coalesced unit is "DLQ'd or skipped
  wholesale"; a fatal ack (`ACK_STATUS_FATAL_FAILURE`) fails the stream whatever the strategy (under
  `dlq` the batch is also written to the DLQ first). ADR 0006 and sql-write-path §8 are corrected in the migration PR, before the
  immutability rule lands.
- **Existing tools**: `adr migrate --from madr` (adrkit CLI) and `git log` for dates.

## R4: What to track under `.specify/` and `.claude/`

- **Decision**: track `.specify/extensions.yml`, every installed extension (`extensions/{adrkit,assess,critique,grill}/**`,
  including the `.specify-dev/` folders the symlinked speckit skills point into), `integrations/`, `extensions/.registry`
  (deliberate exception: without it a clone does not see adrkit installed and its local source
  cannot be re-fetched), `scripts/`, `templates/`, `memory/`, `workflows/`, `integration.json`,
  `init-options.json`, and `.claude/skills/speckit-*` (they render the hooks). Ignore
  `feature.json`, `extensions/.cache/`, `extensions/.backup/` (per-extension `local-config.yml` is
  already ignored by Spec Kit's own `.specify/.gitignore`).
- **Rationale**: the root `.gitignore` ignores `*.json` globally, unanchored `scripts/`, and `.claude/*`;
  each needs a re-include. A clone that lists `/.specify/` in its own
  `.git/info/exclude` removes that line.
- **Alternatives considered**: document `specify init` + `specify extension add` as setup — every
  clone re-derives files and may get a different version.
- **Existing tools**: git's own `.gitignore` negation and `specify init`/`specify extension add`; the latter rejected (see
  Alternatives).

## R5: Config docs (FR-009 revision)

- **Decision**: source-config, destination-config and settings-reference go through rule
  extraction like the other docs. No settings generator. The README environment-variable table is
  deleted; the README points to `src/config/settings.py`.
- **Rationale**: none of the three restates schema. They hold about 17 rules and engine behaviour.
  `settings.py` is plain accessor functions; generating a reference would need a settings registry
  refactor in engine code — scope growth with no rule to enforce. The README table is a partial
  second copy: it lists entrypoint inputs (`CONFIG_BUNDLE`, `CONNECTORS_DIR`) that settings-reference
  does not.
- **Existing tools**: None fit. Looked at: `src/config/settings.py` (plain accessors, no registry to generate from) and
  the schemas.analitiq.ai JSON Schemas (the pipeline `runtime` block declares only the
  runtime-tuning overrides, not the env-only settings).

## R6: Rule inventory and citation reach

- **Decision**: 20 new decision records plus the 6 migrated (see data-model.md). One PR per source
  doc; a rule shared by several docs lands with the first doc that states it, and later PRs only
  delete their copy.
- **Rationale**: 12 rules are stated in 2+ docs. `sql-write-path.md` carries 5 runtime-string and
  ~17 docstring citations (`cdk/cdk/conformance/{surface,declaration}.py` Violation messages,
  `cdk/cdk/sql/*`); its section numbers are an API and every one moves in the same commit.
  Several code citations already dangle ("ADR §6", "ADR §7", "ADR §4.1") and are fixed in the PR
  that migrates their real target.
- **Inventory coverage**: rules are complete for all 10 docs; the citation scan was by reading,
  not grep, so each migration PR re-greps before moving anything.
- **Existing tools**: adrkit record format and `supersedes`/`relatesTo` links; grep for citation reach.

## R7: CI layout

- **Decision**: a new `adr` workflow job, `name: decision records are valid` (`adr lint`), plus
  the adrkit Action job for the governing-decisions comment. Added to CONTRIBUTING.md "Merge
  Requirements" and to branch protection (outside the repo, by a maintainer). The hooks test runs
  under the existing `pytest` job.
- **Rationale**: `ci.yml` has no aggregating gate job; each job is its own check, made required by
  branch protection. `conversion-matrix.yml` is the existing `setup-node` pattern.
- **Pre-existing drift noted**: CONTRIBUTING "Merge Requirements" omits two existing `ci.yml`
  checks; not in scope.
- **Existing tools**: GitHub Actions with the existing `setup-node` pattern from the conversion-matrix workflow, and the
  `mbeacom/adrkit` Action.

## R8: Contradictions found during inventory (fixed in the PR that migrates the doc)

- README "file manifest" vs destination-config's content-addressed file names.
- `src/destination/server.py` comment about pruning an idempotency ledger vs grpc "no in-run
  pre-send skip"; verify whether the ledger exists before writing the record.
- ADR 0002's short-page rule is API-only; the SQL read paths stop on a short page. The migrated
  0002 states its scope explicitly.
- documentation.md breaches: file inventory in engine-architecture, "unchanged" in sql-write-path,
  "v2"/"post-ADR" wording in conformance-kit and `tests/conformance_kit/reference_connector.py`.
