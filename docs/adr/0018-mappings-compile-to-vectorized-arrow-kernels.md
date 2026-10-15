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
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/data-path/mapping-and-transformations.md
---

# ADR-0018: Mappings compile to vectorized Arrow kernels

## Context

The stream mapping (assignments, expressions, conversion functions, validation rules) runs on every batch, and batches already arrive as Arrow. Evaluating it per row in Python means a `to_pylist`/`from_pylist` round trip on every batch and loosely typed intermediate values. Keeping a Python path next to an Arrow path would give two implementations of each function that drift apart. The mapping semantics also had to settle whether a later assignment can read a target field an earlier one built.

## Decision

We compile each stream's contract `StreamMapping` once, with `compile_mapping`, into a `CompiledTransform`; a stream with no assignments gets no transform, and its batches pass through unchanged. Its `run` applies `pyarrow.compute` kernels to whole columns of each batch. This is the only transform path. Each assignment compiles to a function of the source batch alone (`Callable[[pa.RecordBatch], pa.Array]`). No in-progress target record is passed between assignments, so an assignment cannot see the ones before it. Retyping an expression's column to a target other than `Json` goes through the conversion matrix of [ADR 0017](0017-the-engine-owns-the-arrow-type-vocabulary-and-the-conversion-matrix.md). Failures are batch-wide: a build defect raises `TransformationError`, and a failed rule rejects the whole batch ([ADR 0011](0011-a-batch-is-accepted-or-rejected-whole.md)).

## Options considered

### Option A: Compile once to vectorized Arrow compute (chosen)
- Pro: fast, typed evaluation over whole columns, with one implementation per function.
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

We accept the differences that come with typed, vectorized evaluation. Every sub-expression yields one typed Arrow column, so a value cannot change type from row to row. `to_string` of a temporal value uses Arrow's string cast (`2024-01-02 03:04:05.000000` for a microsecond timestamp), not Python's `str()`. A validation `pattern` runs on RE2, which has no lookaround. Authors cannot build one target field from another within the same mapping.

## Consequences

- Easier: assignments do not depend on their order; compilation happens once per stream; most column work runs as Arrow kernels rather than in the interpreter.
- Harder: every expression form or function the contract adds needs an engine implementation, and the import-time check of [ADR 0016](0016-the-pinned-validator-is-the-only-document-gate.md) stops startup until one is registered.
- Validation rules run after every column is built and may address any declared target. Rules see the built record, before each column is converted to its target type; assignments do not.
- Known exception: a `Json` target built from a struct or list column is JSON-encoded per value in Python (`json.dumps`), not by an Arrow kernel.
- Known exception: a `constant` is built on every batch as a Python list holding the value once per row, passed to `pa.array` at the target type, so it uses no compute kernel and skips the conversion matrix. A `range` rule likewise seeds its failure mask from a Python list of one `False` per value, and an `in_list` rule rebuilds its value set on every batch.
- Known exception: a `get` whose source column or nested segment is missing yields an all-null column instead of failing; only a non-nullable target turns that into a `TransformationError`.
- **How we would know this was wrong:** a growing set of Python special cases inside the transform, or authors routinely needing a derived-from-target field.
- Revisit if: the contract adds a function no Arrow kernel can express, or batches stop arriving as Arrow.
