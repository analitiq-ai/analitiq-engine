---
schemaVersion: 0.2.0
id: "0012"
title: "The cursor is opaque to the destination"
status: accepted
date: 2026-06-04
deciders: ["@Analitiq-Bot-Wonka"]
tags: [wire-protocol, cursor, checkpointing]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0010", "0014"]
affects:
  - type: path
    pattern: "proto/analitiq/v1/**"
  - type: path
    pattern: "src/grpc/**"
  - type: path
    pattern: "src/destination/**"
  - type: path
    pattern: "cdk/cdk/**"
provenance:
  authoredBy: human
  sourceArtifact: docs/architecture/grpc-streaming-architecture.md
---

# ADR-0012: The cursor is opaque to the destination

## Context

Each batch carries the cursor that, once the batch commits, becomes the
stream's checkpoint. Cursor shape depends on the source: a timestamp with
a tie-breaker, an LSN, a composite key. If destinations read cursors, every
destination handler would have to understand every source's cursor
typing, and a change to the cursor format would touch every connector.

## Decision

We keep the cursor format engine-owned and opaque on the wire: `Cursor` is
a single `bytes token` whose format the engine's cursor module owns
(`encode_cursor`, `decode_cursor`, `compute_max_cursor`,
`cursor_to_state_dict`). The destination receives the token with the batch
and returns it unread as `committed_cursor` on a `SUCCESS` ack; the engine
persists it. Replay safety is keyed on row identity, not the cursor
([ADR 0014](0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md)).

## Options considered

### Option A: Opaque bytes, engine-owned format (chosen)
- Pro: destinations stay source-agnostic; the cursor format changes in the
  engine alone.
- Con: a destination cannot check or use the cursor.

### Option B: A typed cursor message the destination interprets
- Pro: destinations could validate ordering or partition output by cursor.
- Con: source cursor typing spreads into every destination handler and the
  proto.

## Trade-offs

The destination can verify nothing about the cursor it echoes. The engine
persists the echoed bytes only as
[ADR 0013](0013-prefer-duplicates-over-data-loss.md) allows, and trusts the
destination to echo the token it was sent. All cursor semantics, including comparison across
batches, live in one engine module.

## Consequences

- Easier: adding a source cursor kind touches the engine only; destinations
  and the proto are untouched.
- Harder: any destination feature that would key on progress (for example
  cursor-partitioned output) needs another engine-supplied field, such as
  the per-batch `emitted_at`.
- **How we would know this was wrong:** destinations repeatedly needing
  cursor contents, or connectors decoding the token despite the contract.
- Revisit if: resume moves to the destination side, or a destination must
  report its own commit position independently of the engine.
