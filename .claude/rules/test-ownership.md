---
paths:
  - "tests/**"
  - "cdk/**/tests/**"
---

# Test Ownership: Engine vs Validator

Applies when adding or editing any test in the engine test tree or the CDK test tree. Follows from
one-gate-per-document: a document is validated once, by `analitiq-validator`
(backed by `analitiq-contract-models`), both authored in the claude-code-plugins repo.

## The question to ask before writing the test

**Is the test's pass/fail decided by the authored documents alone?** Connector, api/database
endpoint, type-map, connection, stream, pipeline, or a set of them. If yes, it is a document-validity
test and it does not belong in this repo.

## Belongs in the Validator (plugins repo) — never add it here

A test whose verdict needs nothing but the documents:
- Shape: required/unknown fields, types, closed enums, non-empty values, string patterns.
- Constraints inside one document: `default_transport` names a declared transport, a param bound
  twice, `minimum <= maximum`, a pagination block without `stop_when`, a malformed `${…}` token.
- Constraints across documents: a stream's replication method is in its endpoint's
  `supported_methods`, a filter lands on a declared endpoint param, a write mode has a matching
  write operation, a catalog needs the connector's catalog capability, one type-map document per
  direction.
- Any engine test that calls `analitiq.contracts` or `analitiq.validator` directly to assert they
  refuse or accept a document.

## Belongs in the engine

- How a valid document is loaded, resolved, converted, planned, or executed.
- Safety tied to engine libraries or runtime: RE2 compile/subset checks, secret and value
  resolution, driver behaviour, Arrow conversion, gRPC, state, retries, shutdown.
- Wiring: the engine refuses a bad document *through* the validator before running anything. One
  wiring test per entry point, using any refused document — not one test per rule.
- Pins, censuses, and conformance of the engine and CDK themselves.

If a test mixes both, split it: the document verdict goes to the Validator, the runtime assertion
stays.

## What to do when the test belongs in the Validator

1. Do not add the engine test. Search open and closed issues in `analitiq-ai/claude-code-plugins`
   for the rule first; comment on a match instead of opening a duplicate.
2. Otherwise open an issue there (`gh issue create -R analitiq-ai/claude-code-plugins --type Task`,
   or `Bug` if the validator accepts a document it should refuse) with:
   - The rule in one sentence, and its `RULE-*` id if one exists, or "new rule".
   - The document kind(s), and whether one document decides it or several.
   - A minimal valid document and a minimal invalid one.
   - Whether the rule is enforced today (cite the model or check) and only untested, or missing.
3. The plugins repo is public: no infrastructure, cloud, or deployment detail in the issue.
4. If the engine currently enforces the rule itself because the validator does not, the engine
   check and its test stay until the validator release carrying the rule is pinned; then both are
   removed in one engine change. Say so in the issue.
