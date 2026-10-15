---
schemaVersion: 0.2.0
id: "0017"
title: "The engine owns the Arrow type vocabulary and the conversion matrix"
status: accepted
date: 2026-07-01
deciders: ["@Analitiq-Bot-Wonka"]
tags: [arrow-types, conversion-matrix, contracts]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0018"]
affects:
  - type: path
    pattern: "cdk/cdk/type_map/**"
  - type: path
    pattern: "cdk/cdk/schema_contract.py"
  - type: path
    pattern: "packages/conversion-matrix/**"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/data-path/arrow-and-transport-strategy.md
---

# ADR-0017: The engine owns the Arrow type vocabulary and the conversion matrix

## Context

Many surfaces need the Arrow family names and their parameters: the `arrow_type` parser, the conversion policy, the type-map rule normaliser, the conformance probes, the plugins' contract, and the frontend's mapping UI. Each one keeping its own list drifts on the first family added. Conversion rules had the same problem: the transform retype and the destination cast each worked out their own rules. The transform rejected `Int64 -> Utf8` while the destination cast silently accepted it, so the same author intent got opposite outcomes. The engine is the component that has to run each type. Every connector type map, the plugins repo and the frontend consume this vocabulary.

## Decision

We declare every Arrow family once, in the CDK's `ARROW_FAMILIES` table, with its parameter grammar, its conversion kind and its pyarrow bindings. The parser, the conversion policy (`classify_conversion`, which reasons only in conversion kinds) and the two published artifacts are derived from that table. The conformance probe set is built from it too, with hand-written exemplars for the integer-parameterized families. The artifacts are the grammar from `build_arrow_type_grammar` and `conversion_matrix.json` from `build_conversion_matrix`. One conversion matrix decides every Arrow-to-Arrow `source -> target` conversion. The transform retype and `SchemaContract.cast_arrow_batch` both consult it through `classify_arrow_conversion`, and both execute with `pc.cast(safe=True)`, so a bad row fails loudly instead of being truncated. Each artifact states its own version (`GRAMMAR_VERSION`, `CONVERSION_MATRIX_VERSION`) in its bytes. Both ship inside the CDK distribution, and the CDK release is gated on reading them back from the built wheel in a clean environment and on finding them in the sdist. The frontend consumes the matrix as `@analitiq-ai/conversion-matrix`, and the plugins contract vendors a pinned copy of the grammar.

## Options considered

### Option A: The engine's CDK table is the single vocabulary; everything else is generated (chosen)
- Pro: adding a family is one edit; the two boundaries and the frontend agree by construction.
- Con: the engine repo becomes a publisher, with an npm package and versioned artifacts to maintain.

### Option B: The plugins contract owns the vocabulary and the engine mirrors it
- Pro: all authored shapes come from one repo.
- Con: the vocabulary would be owned away from the code that has to build and cast each type.

### Option C: Each surface (parser, matrix, rules, frontend) keeps its own list or rules
- Pro: no shared artifact or release coupling.
- Con: lists drift on the first new family, and boundaries can disagree on the same conversion.

### Option D: Unsafe casts that truncate instead of failing
- Pro: fewer failed batches.
- Con: silent data loss.

## Trade-offs

Changing the matrix's content requires a version bump: the publisher refuses to republish changed content under a version it has already published. Every downstream repo has to vendor or install a pinned copy. `safe=True` does not catch everything: a finite `Float64` beyond `Float32` range still casts to infinity.

## Consequences

- Easier: a new family of an existing conversion kind reaches the parser and the conversion policy from its one `ARROW_FAMILIES` entry.
- Harder: changing the policy is a cross-repo release, not a local fix.
- Known exception: `SchemaContract.from_pylist`, which builds the API and SQLAlchemy source reads from Python values, is outside the matrix. It parses ISO strings into temporal columns, which the matrix forbids as a cast, and a declared `source_format` path can end in `pc.cast(safe=False)`.
- Known exception: only SQL connectors run `SchemaContract.cast_arrow_batch`, on the destination write and the ADBC source read. API, file and stdout destinations write the batch's values without casting them to the endpoint schema, so no matrix check runs there.
- Known exception: the transform encodes a struct or list column for a `Json` target with `json.dumps`, a conversion the matrix lists as forbidden.
- Known exception: `resolve_arrow_type` names the structural families `Object` and `List` and their sub-schema keys itself, not from the table, so a new structural family also needs an edit there.
- **How we would know this was wrong:** a consumer re-declaring family names or conversion rules by hand, or the transform and the destination cast disagreeing on one conversion.
- Revisit if: a non-Arrow runtime has to execute these types, or the contract must define types the engine cannot build.
