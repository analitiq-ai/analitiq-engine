# Research: Decision-Record Governance

## R1: Hooks on core commands

- **Decision**: Register `before_implement` → `speckit.analyze` and `after_implement` →
  `speckit.converge` by hand in `.specify/extensions.yml`, both `optional: false`. Label them
  `extension: project` so `specify extension add/remove adrkit` does not delete them. This feature
  adds no plan-time hook; adrkit's own optional `after_plan` entry stays as the extension installs
  it.
- **Rationale**: Spec Kit 1.0.7 `HookExecutor` never checks that a hook's command belongs to an
  installed extension; the skills render any id (`speckit.analyze` → `/speckit-analyze`).
  `register_hooks`/`unregister_hooks` only replace entries whose `extension` matches the manifest
  id. The extension user guide documents hand-editing `extensions.yml`.
- **Caveats**: every CLI write re-dumps the YAML (comments are lost). A test asserts the two
  hooks and their flags.
- **Alternatives considered**: a preset that overrides command templates — heavier, fights the
  composition model; leaving hooks optional — agents skip them.
- **Existing tools**: Spec Kit's own hook system (`.specify/extensions.yml`, `HookExecutor`) and the adrkit extension's
  registered hooks; a Spec Kit preset was looked at and rejected (see Alternatives).

## R2: adrkit CLI behaviour and version

- **Decision**: `adr lint` is the blocking CI gate. One adrkit CLI version everywhere: PR 3 adds a
  root `package.json` with the CLI as a devDependency and commits its `package-lock.json` as the
  pin; the extension resolves it as `./node_modules/.bin/adr`, and CI runs the same `npm ci`.
  Records set no `affects`, so no tool reports governing decisions on a PR.
- **Rationale**: `adr lint` exits 1 on schema errors, unknown keys, broken
  `supersedes`/`supersededBy`/`relatesTo` links, duplicate ids, accepted-without-decider.
  `affects` is optional in adrkit's schema, and `adr lint` reports nothing when it is empty or
  absent. adrkit matches a decision to a file only through `affects` or an `@adr NNNN` marker in
  the file. Most changes (bug fixes, small improvements) are governed by no decision, and a
  forward-looking decision has no code to point at.
- **Alternatives considered**: `affects` file lists, with the adrkit GitHub Action commenting the
  governing decisions on each PR — a hand-kept file list per decision that drifts as code moves;
  `@adr NNNN` markers in source files — the same list, spread over the files; `adr evaluate` with
  a snapshot bundle — needs bundle tooling, deferred.
- **Existing tools**: adrkit CLI `lint`; no second validator.

## R3: Frontmatter for the six existing records

- **Decision**: `adr migrate --from madr`, then set per record: quoted `id`, `status: accepted`,
  real `date` from the commit that introduced the rule (not the file-add commit: 0003–0006 were
  added together by a docs move), and `deciders: ["@Analitiq-Bot-Wonka"]`; no `affects` (R2).
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

- **Decision**: a new `adr` workflow job, `name: decision records are valid` (`adr lint`).
  Added to CONTRIBUTING.md "Merge
  Requirements" and to branch protection (outside the repo, by a maintainer). The hooks test runs
  under the existing `pytest` job.
- **Rationale**: `ci.yml` has no aggregating gate job; each job is its own check, made required by
  branch protection. `conversion-matrix.yml` is the existing `setup-node` pattern.
- **Pre-existing drift noted**: CONTRIBUTING "Merge Requirements" omits two existing `ci.yml`
  checks; not in scope.
- **Existing tools**: GitHub Actions with the existing `setup-node` pattern from the conversion-matrix workflow.

## R8: Contradictions found during inventory (fixed in the PR that migrates the doc)

- README "file manifest" vs destination-config's content-addressed file names.
- `src/destination/server.py` comment about pruning an idempotency ledger vs grpc "no in-run
  pre-send skip"; verify whether the ledger exists before writing the record.
- ADR 0002's short-page rule is API-only; the SQL read paths stop on a short page. The migrated
  0002 states its scope explicitly.
- documentation.md breaches: file inventory in engine-architecture, "unchanged" in sql-write-path,
  "v2"/"post-ADR" wording in conformance-kit and `tests/conformance_kit/reference_connector.py`.
