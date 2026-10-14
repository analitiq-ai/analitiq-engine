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
  authoredBy: human
  sourceArtifact: docs/data-path/arrow-and-transport-strategy.md
---

# ADR-0017: The engine owns the Arrow type vocabulary and the conversion matrix

## Context

Many surfaces need the Arrow family names and their parameters: the `arrow_type` parser, the conversion policy, the type-map rule normaliser, the conformance probes, the plugins' contract, and the frontend's mapping UI. Each one keeping its own list drifts on the first family added. Conversion rules had the same problem: the transform retype and the destination cast each worked out their own rules. The transform rejected `Int64 -> Utf8` while the destination cast silently accepted it, so the same author intent got opposite outcomes. The engine is the component that has to run each type. Every connector type map, the plugins repo and the frontend consume this vocabulary.

## Decision

We declare every Arrow family once, in the CDK's `ARROW_FAMILIES` table, with its parameter grammar, its conversion kind and its pyarrow bindings. The parser, the conversion policy (`classify_conversion`, which reasons only in conversion kinds) and the two published artifacts are derived from that table. The conformance probe set is built from it too, with hand-written exemplars for the integer-parameterized and structural families. The artifacts are the grammar from `build_arrow_type_grammar` and `conversion_matrix.json` from `build_conversion_matrix`. One conversion matrix decides every `source -> target` conversion. The transform retype and `SchemaContract.cast_arrow_batch` both consult it through `classify_arrow_conversion`, and both execute with `pc.cast(safe=True)`, so a bad row fails loudly instead of being truncated. Each artifact states its own version (`GRAMMAR_VERSION`, `CONVERSION_MATRIX_VERSION`) in its bytes. Both ship inside the CDK distribution, and the CDK release is gated on reading them back from the built wheel in a clean environment and on finding them in the sdist. The frontend consumes the matrix as `@analitiq-ai/conversion-matrix`, and the plugins contract vendors a pinned copy of the grammar.

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

- Easier: a new family reaches the parser, the policy and every consumer from one edit; an integer-parameterized or structural family also needs a probe exemplar, and the conformance kit refuses to run until it has one.
- Harder: changing the policy is a cross-repo release, not a local fix.
- Known exception: `SchemaContract` parses strings with a declared `source_format` from Python values, not Arrow-to-Arrow. That path can end in `pc.cast(safe=False)` and is outside the matrix.
- **How we would know this was wrong:** a consumer re-declaring family names or conversion rules by hand, or the transform and the destination cast disagreeing on one conversion.
- Revisit if: a non-Arrow runtime has to execute these types, or the contract must define types the engine cannot build.
