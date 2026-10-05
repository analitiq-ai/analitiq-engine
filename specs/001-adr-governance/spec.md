# Feature Specification: Decision-Record Governance for Specs, Plans and Docs

**Feature Branch**: one branch per PR, see plan.md

**Created**: 2026-10-05

**Status**: Draft

**Input**: User description: "create spec for the work documented in #570"

## User Scenarios & Testing *(mandatory)*

Actors: **maintainer** (human who ratifies decisions and amends governance), **plan author** (human
or AI agent producing a spec, plan and task list), **reviewer** (human or bot reviewing a PR),
**CI** (the automated checks every PR must pass).

### User Story 1 - Existing decisions are valid and enforced on every change (Priority: P1)

The six existing architecture decision records are machine-readable, dated, marked accepted, and
each declares which parts of the codebase it governs. Every PR is checked against them
automatically, and the governance setup (constitution, decision tooling configuration) is part of
the repository so every clone, the bot and CI see the same rules.

**Why this priority**: Today the decision checks see an empty set because all six records fail
validation, and the constitution is invisible outside one local working copy. Nothing downstream
works until this does.

**Independent Test**: Open a PR that changes a file governed by an existing decision; CI reports
the governing decision. Open a PR that breaks a record's format; CI fails.

**Acceptance Scenarios**:

1. **Given** the six existing records, **When** decision validation runs, **Then** all six pass
   with zero errors.
2. **Given** a PR touching a path covered by a record's governed-paths list, **When** CI runs,
   **Then** the result names that record.
3. **Given** a PR introducing a malformed record, **When** CI runs, **Then** the required check
   fails and the PR cannot merge.
4. **Given** a fresh clone, **When** a plan author starts planning, **Then** the constitution and
   decision tooling configuration are present without any local setup.

---

### User Story 2 - Every architecture rule is recorded once, as a decision (Priority: P2)

Every architecture rule and invariant now stated in prose documentation is moved into its own
decision record, with the reasoning and rejected alternatives. Prose docs shrink to one short
orientation overview with no rules in it. Every citation from code comments and runtime error
messages to a rule points at the decision record.

**Why this priority**: Rules stated in several places drift apart, and rules left in prose cannot
be checked automatically. This is where the documentation maintenance burden is removed.

**Independent Test**: Pick any architecture rule; it is found in exactly one decision record, and
every citation of it in code and error text resolves to that record.

**Acceptance Scenarios**:

1. **Given** a prose document under migration, **When** its migration lands, **Then** each rule
   it stated exists as one decision record and the document no longer states it.
2. **Given** code or error text citing a migrated rule, **When** the migration lands, **Then** the
   citation points at the decision record in the same change.
3. **Given** the migration is complete, **When** a reader looks for how the system fits together,
   **Then** one overview document answers it without restating any rule.

---

### User Story 3 - Plans are checked against the constitution and decisions before implementation (Priority: P3)

When a plan author plans a feature, the decisions governing the paths it will touch are loaded
first; the finished plan is checked against those decisions and the constitution; a plan that
makes a new architecture choice produces a draft decision record for a maintainer to ratify.
After implementation, the code is compared back to the spec, plan and tasks.

**Why this priority**: This is the forward-looking gate. It depends on Stories 1 and 2 for a
complete, valid decision set to check against.

**Independent Test**: Run the planning workflow on a deliberately contradicting feature (e.g. a
plan adding a second SQL write shape); the contradiction is reported before implementation.

**Acceptance Scenarios**:

1. **Given** a plan that contradicts an accepted decision governing its paths, **When** the plan
   check runs, **Then** the contradiction is reported naming the decision.
2. **Given** a plan that chooses between real alternatives, **When** planning completes, **Then**
   a draft decision record exists with status draft, not accepted.
3. **Given** a plan that supersedes an accepted decision, **When** it is checked, **Then** it
   passes only if it includes a new decision record that supersedes the old one.
4. **Given** the constitution, **When** a principle states an architecture rule, **Then** it cites
   a decision record instead of restating the rule or citing a prose doc section.

---

### Edge Cases

- A record with an empty governed-paths list is never matched to any change. Decision validation
  treats it as advisory, not an error, so review of each record MUST confirm the list is non-empty
  and every pattern matches a real path.
- A superseded record still governs historical context; plans MUST see it as superseded, not as a
  live rule and not as absent.
- A rule in a prose doc that is not a decision (a description of how something works) stays in the
  overview or is deleted; it MUST NOT become a decision record.
- A migration that renumbers or renames a cited section breaks citations in runtime error text;
  every citation MUST move in the same change.
- Two prose docs state the same rule differently; migration MUST resolve to one record and note
  which statement the code actually follows.
- Planning checks are instructions to an agent and can be skipped; CI MUST remain the blocking
  gate.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The governance setup (constitution and decision tooling configuration) MUST be
  version-controlled in the repository.
- **FR-002**: Every decision record MUST carry a decision date taken from repository history, a
  status, a non-empty list of governed paths, and `@Analitiq-Bot-Wonka` as decider.
- **FR-003**: Decision-record validation MUST run as a required check on every PR and fail the PR
  on any invalid record or broken supersede/relation link.
- **FR-004**: Every PR MUST have the decisions governing its changed files reported on it, so the
  reviewer judges the change against them. The tooling only names governing decisions; judging a
  departure is the reviewer's job.
- **FR-005**: The documentation rule MUST be amended so decision records keep the decision,
  alternatives and consequences after shipping, and are superseded rather than rewritten. This
  amendment MUST land in its own PR before any record is migrated.
- **FR-006**: Every architecture rule currently stated in prose documentation MUST be moved into
  exactly one decision record and removed from the prose.
- **FR-007**: Migration MUST be delivered one prose document per PR, each PR repointing every code
  and runtime-error citation of the rules it moves.
- **FR-008**: After migration, hand-maintained architecture prose MUST be one orientation overview
  that states no rule.
- **FR-009**: The config docs (source, destination, settings reference) MUST go through the same
  extraction as the architecture docs: rules become decision records, behaviour description folds
  into the overview or is deleted. No generator is built. The hand-kept environment-variable table
  in the README is deleted in favour of the settings module. The conformance kit stays as the one
  hand-written how-to and states no rule.
- **FR-010**: The constitution MUST cite decision records for every architecture rule, and MUST
  gain gates that (a) a plan contradicts no accepted decision governing its paths, (b) superseding
  requires a new record in the same plan, (c) a plan that chooses between real alternatives
  produces a draft record, (d) changes follow the flow-forward model (a new spec directory per
  change; durable rules live in decision records).
- **FR-011**: The planning workflow MUST load governing decisions before planning, check the
  finished plan against them after planning, check spec/plan/tasks against the constitution before
  implementation, and compare code to spec/plan/tasks after implementation.
- **FR-012**: Draft decision records MUST NOT be marked accepted by automation; ratification is a
  maintainer action. The six existing records are the exception: they record decisions already in
  force, are set to accepted during their migration, and approving that migration PR is their
  ratification.

### Key Entities

- **Decision record**: one architecture decision — context, decision, alternatives considered,
  consequences; carries id, date, status (draft/proposed/accepted/superseded/deprecated/rejected), governed
  paths, and links to records it supersedes or relates to. Immutable once accepted.
- **Constitution**: the list of plan-time gates; cites decision records, never restates them.
- **Orientation overview**: the single prose description of how components fit together; states
  no rule.
- **Citation**: a reference from code, docstring or runtime error text to a decision record.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of decision records pass validation; the required check has blocked every PR
  that introduced an invalid record.
- **SC-002**: Every architecture rule is stated in exactly one place: an audit of all rules finds
  0 restated in prose docs or the constitution.
- **SC-003**: Hand-maintained documentation drops from 10 documents to 2 (the overview and the
  conformance how-to); the rest become decision records or are deleted.
- **SC-004**: 0 broken citations: every reference from code and error text to a rule resolves to
  an existing decision record.
- **SC-005**: Of 3 deliberately contradicting test plans, 3 are flagged before implementation, each
  naming the governing decision.
- **SC-006**: A fresh clone runs the planning checks with no local setup beyond installing the
  documented tools.

## Assumptions

- The decision tooling is the adrkit CLI and its Spec Kit extension; decision records live in
  `docs/adr/`.
- The six existing records are migrated in place, keep their ids, and become accepted.
- Migration covers all of `docs/architecture/`, `docs/data-path/`, `docs/config/` and
  `docs/testing/`. Each user story is one unit of work, tracked as a sub-issue of #570; its PRs
  reference that sub-issue.
- Spec Kit planning checks are agent instructions, not hard blocks; CI is the only enforcing
  layer.
- Hooking core Spec Kit commands (analyze, converge) as extension hooks is unverified and is
  confirmed during planning.
- Spec Kit does not compare one feature's spec with another's; the flow-forward model plus
  decision records is the mitigation, and no further tooling is in scope.
- Governing-rule changes (documentation rule, constitution, CI checks) land in separate PRs from
  the content they govern.
