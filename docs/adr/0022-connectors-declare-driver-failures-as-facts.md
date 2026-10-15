---
schemaVersion: 0.2.0
id: "0022"
title: "Connectors declare driver failures as facts; fixed CDK and engine tables derive the verdicts"
status: accepted
date: 2026-07-25
deciders: ["@Analitiq-Bot-Wonka"]
tags: [error-classification, connector-contract, failure-handling]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0001", "0008", "0013", "0023"]
affects:
  - type: path
    pattern: "cdk/cdk/declarations.py"
  - type: path
    pattern: "cdk/cdk/sql/**"
  - type: path
    pattern: "cdk/cdk/api/**"
  - type: path
    pattern: "src/worker/**"
  - type: path
    pattern: "proto/analitiq/v1/**"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/engine-architecture.md
---

# ADR-0022: Connectors declare driver failures as facts; fixed CDK and engine tables derive the verdicts

## Context

Every driver names its failures differently: SQLSTATEs, vendor codes, exception classes, HTTP statuses. The engine has to turn a failure into a retry decision, an ack and a customer code without hard-coding any vendor's taxonomy, and without trusting untrusted, AI-authored connector code to choose its own verdicts ([ADR 0001](0001-two-failure-vocabularies.md)). The exception object and its chain do not cross the process boundary between worker and engine, so whatever classifies a failure has to do it where the failure is caught.

## Decision

We let a connector declare its driver's failures as data in its `error_map`: which exception attributes carry a native code (`key_attrs`), a flat map from native code to a contract `ErrorCategory` (`codes`), and an HTTP status map (`http`). The map is matched where the failure is caught: `codes` against the caught exception and its direct `orig` and `__cause__` links (one hop, no deeper) via `ErrorMap.match_exception`, and `http` against the response status at the HTTP call site via `ErrorMap.match_http`. Fixed tables in the CDK and engine, not connector code, turn a category into verdicts (`DECLARED_WRITE_VERDICTS`, `DECLARED_READ_DETERMINISTIC`, `source_code_for_declared_category`). The result crosses processes as structured signals (the worker's deterministic flag and `declared_category` wire field, the ack's failure category), never re-derived from chains or text. A connector may also classify in code by overriding `classify_error` (the base method classifies nothing); a broken hook or an out-of-vocabulary answer classifies as `config`.

## Options considered

### Option A: declared facts matched at birth, verdicts engine-derived (chosen)
- Pro: deterministic classification with no connector Python; verdicts stay engine-owned.
- Con: a signal that is not a flat attribute read needs code anyway.

### Option B: engine-owned per-family tables (fixed sqlstate/exception/vendor_code/http families, regex and phrase matching)
- Pro: connectors declare nothing.
- Con: vendor knowledge in the engine; a closed family list cannot express every driver's error shape.

### Option C: connectors self-declare verdicts (ack status, retryability, error code)
- Pro: the most expressive.
- Con: an untrusted connector decides retries and what the customer is told.

### Option D: re-derive the category downstream from the exception chain or message
- Pro: no birth-site wiring.
- Con: chains do not cross the process boundary, and text matching is guesswork.

## Trade-offs

A connector that wants precise customer codes must declare its failures ([ADR 0001](0001-two-failure-vocabularies.md)). Matching one hop deep means a driver that wraps deeper must use `classify_error`.

Known exceptions in code:
- A connector class that overrides `write_batch` builds its own `BatchWriteResult`, status and failure category included, so it self-declares a write verdict. The conformance override-surface check refuses such an override for `kind: database` only; for other kinds the engine range-checks the category but uses an in-range value as sent.
- For an HTTP response, an API dialect's `classify` hook is consulted before the declared `http` map: code before data, the reverse of the exception path.
- On read paths, a typed error raised with its own `declared_category` outranks the map; it is still a category, validated against the vocabulary, not a verdict.
- On read paths, a failure nothing claims falls to a type ladder, so a connector that raises `ReadError` or `TransientReadError` without a category chooses the fatal or retryable verdict itself.
- The CDK's SQL read wraps a source connect failure in `ReadError`, so the worker matches the map one hop above the driver error: a native code SQLAlchemy keeps on `orig` is two hops away, and unless `classify_error` claims the failure it reads as deterministic.
- The map is consulted only for read and batch-write failures. A destination's connect and schema-handshake failures, DDL included, take the engine's own classification whatever the map declares.

## Consequences

- Easier: a connector changes how its driver's failures are classified by editing JSON, with no code.
- Harder: a new verdict needs an engine table change, not a connector change.
- **How we would know this was wrong:** most connectors shipping `classify_error` or `write_batch` overrides instead of declaring an `error_map`.
- Revisit if: the contract's `ErrorCategory` vocabulary cannot express a failure class connectors need.
