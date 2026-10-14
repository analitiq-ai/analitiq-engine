---
schemaVersion: 0.2.0
id: "0018"
title: "Mappings compile to vectorized Arrow kernels"
status: accepted
date: 2026-06-04
deciders: ["@Analitiq-Bot-Wonka"]
tags: [mapping, arrow, transform]
scope: component
reversibility: one-way-door
blastRadius: team
relatesTo: ["0011", "0016", "0017"]
affects:
  - type: path
    pattern: "src/engine/mapping.py"
  - type: path
    pattern: "src/engine/stream_processor.py"
provenance:
  authoredBy: human
  sourceArtifact: docs/data-path/mapping-and-transformations.md
---

# ADR-0018: Mappings compile to vectorized Arrow kernels

## Context

The stream mapping (assignments, expressions, conversion functions, validation rules) runs on every batch, and batches already arrive as Arrow. Evaluating it per row in Python means a `to_pylist`/`from_pylist` round trip on every batch and loosely typed intermediate values. Keeping a Python path next to an Arrow path would give two implementations of each function that drift apart. The mapping semantics also had to settle whether a later assignment can read a target field an earlier one built.

## Decision

We compile each stream's contract `StreamMapping` once, with `compile_mapping`, into a `CompiledTransform`. Its `run` applies `pyarrow.compute` kernels to whole columns of each batch. This is the only transform path: every assignment, expression op, catalog function and validation rule is a vectorized column operation. Each assignment compiles to a function of the source batch alone (`Callable[[pa.RecordBatch], pa.Array]`). No in-progress target record is passed between assignments, so an assignment cannot see the ones before it. Retyping goes through the conversion matrix of [ADR 0017](0017-the-engine-owns-the-arrow-type-vocabulary-and-the-conversion-matrix.md). Failures are batch-wide: a build defect raises `TransformationError`, and a failed rule fails the whole batch ([ADR 0011](0011-a-batch-is-accepted-or-rejected-whole.md)).

## Options considered

### Option A: Compile once to vectorized Arrow compute (chosen)
- Pro: fast, typed evaluation that never leaves Arrow, with one implementation per function.
- Con: the semantics are Arrow's, not Python's, where the two differ.

### Option B: Per-row Python evaluation over dicts
- Pro: Python semantics authors may expect (`str(datetime)`, Python regex).
- Con: slow, untyped, and a list/dict round trip on every batch.

### Option C: Vectorized path plus a Python fallback for hard cases
- Pro: covers ops Arrow lacks.
- Con: two paths whose results drift apart.

### Option D: Thread the in-progress target record so later assignments read earlier ones
- Pro: authors can derive one target field from another.
- Con: results depend on assignment order, which makes compiling and reasoning about a mapping harder.

## Trade-offs

We accept the differences that come with typed, vectorized evaluation. A sub-expression cannot change type partway through. `to_string` of a temporal value uses Arrow's ISO format. A validation `pattern` runs on RE2, which has no lookaround. Authors cannot build one target field from another within the same mapping.

## Consequences

- Easier: assignments do not depend on their order; static work happens once per stream; throughput scales with Arrow, not the interpreter.
- Harder: every new expression form or function needs an Arrow kernel, and the import-time catalog check of [ADR 0016](0016-the-pinned-validator-is-the-only-document-gate.md) stops startup until it exists.
- Validation rules run after every column is built and may address any declared target. Rules see the built record, assignments do not.
- Known exception: a `Json` target built from a struct or list column is JSON-encoded per value in Python (`json.dumps`), not by an Arrow kernel.
- **How we would know this was wrong:** a growing set of Python special cases inside the transform, or authors routinely needing a derived-from-target field.
- Revisit if: the contract adds a function no Arrow kernel can express, or batches stop arriving as Arrow.
