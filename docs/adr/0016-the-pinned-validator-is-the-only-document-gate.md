---
schemaVersion: 0.2.0
id: "0016"
title: "The pinned validator is the only document gate; the engine consumes typed contract models"
status: accepted
date: 2026-08-30
deciders: ["@Analitiq-Bot-Wonka"]
tags: [contracts, validation, config-loading]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0018"]
affects:
  - type: path
    pattern: "src/config/**"
  - type: path
    pattern: "src/engine/pipeline_config_prep.py"
  - type: path
    pattern: "src/engine/mapping.py"
  - type: path
    pattern: "tools/contract_consumption.py"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/engine-architecture.md
---

# ADR-0016: The pinned validator is the only document gate; the engine consumes typed contract models

## Context

Pipeline, stream, connection, connector, endpoint and type-map documents are authored against contracts the plugins repo publishes as `analitiq-contract-models` and checks with `analitiq-validator`. The engine could grade those documents itself, or trust the published gate. A second engine-side gate over the same shapes gives two verdicts that drift apart. Parsing different bytes from the ones that were graded would let ungraded content run. Documents read as dicts also made "which contract fields does the engine read" impossible to answer: contract fields shipped that no runtime path read. The mapping vocabulary (expression forms, conversion functions) is part of the contract too, and an engine that falls behind a contract release would otherwise fail mid-run on the first batch carrying an unknown form.

## Decision

We refuse every run unless the pinned `analitiq-validator` passes the run's workspace request: `read_run_workspace` collects the manifest and the packages of the pipeline, the connections it names and their connectors, and `gate_run` calls `validate_workspace` on that one request and raises `WorkspaceRejectedError` on a failed verdict. Config loading builds the engine's typed `analitiq.contracts` models from the texts that verdict graded. The mapping compiler checks at import that its expression compilers and function kernels match the contract's expression forms and function names exactly, so a contract release that adds or removes one fails engine startup.

## Options considered

### Option A: The pinned validator is the one gate; typed models from the graded texts (chosen)
- Pro: the engine builds its models from the texts that were graded; static rules live in one place; field use can be checked statically.
- Con: the engine cannot run without a matching validator and contract-models release.

### Option B: Engine-side validation (Pydantic or JSON Schema) in addition to the validator
- Pro: the engine does not depend on the validator's behaviour.
- Con: two gates over one shape disagree; every contract rule has to be implemented twice.

### Option C: Validate against a `latest.json` schema fetched at runtime
- Pro: no package pin to bump.
- Con: the run's verdict depends on network state and an unpinned schema; it cannot be reproduced.

### Option D: Consume documents as plain dicts
- Pro: no model dependency in the CDK or engine.
- Con: field reads are string lookups no tool can list, so unread contract fields go unnoticed.

## Trade-offs

The engine pins `analitiq-contract-models` and `analitiq-validator` exactly, and every contract change reaches the engine as a deliberate pin bump. The CDK depends on `analitiq-contract-models` as a range within one major. Building a model still runs Pydantic parsing, which rejects a document whose shape the model refuses. This is a side effect of building the model, not a second gate. Reads mypy's type map cannot see (a `getattr` over a table, a path walk) have to be registered by hand in the census.

## Consequences

- Easier: tests of document validity belong to the validator, not the engine; a contract upgrade is a pin bump plus the census.
- Harder: an engine that lags a contract release that changes the mapping vocabulary does not start, even for pipelines that use none of the change.
- Known exception: before the gate, the manifest, the pipeline document and the connection documents are read as plain JSON, only to find which packages the request holds. The validator still grades them in the same request, but the census does not see those reads.
- Known exception: the worker bootstrap re-reads each `type-map.json` from disk with `read_raw_type_map` instead of shipping the graded text, and type maps are parsed into rules by `parse_type_map`, not held as contract models.
- Known exception: some blocks are still read as the JSON their author wrote, not field by field through the model: transport blocks, predicates, value expressions, endpoint columns, assignment targets, the declared SQL capabilities, error map and concurrency, and the whole connector as the resolver's `connector` scope. The census lists these as opaque, with the one module that reads each, and cannot list the fields read from them.
- Known exception: the import check covers expression forms and function names only. Validation rule types are not checked at import; a rule type the mapping compiler does not handle fails when a batch reaches it.
- **How we would know this was wrong:** documents the validator passes that the engine then rejects or misreads at run time, or engine-side checks reappearing to close gaps in the validator.
- Revisit if: the validator can no longer run in-process at the engine's pinned version, or the contract stops shipping Pydantic models.
