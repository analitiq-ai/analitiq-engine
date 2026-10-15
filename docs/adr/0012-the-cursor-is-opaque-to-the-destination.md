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
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/grpc-streaming-architecture.md
---

# ADR-0012: The cursor is opaque to the destination

## Context

A batch of an incremental stream carries a cursor, the batch's highest
non-null cursor value. The engine checkpoints from the cursor a `SUCCESS`
ack echoes back.
Cursor shape depends on the source: the cursor column's type (a
timestamp, an integer, a decimal) and any tie-breaker fields. If
destinations read cursors, every destination handler would have to
understand every source's cursor typing, and a change to the cursor
format would touch every connector.

## Decision

We keep the cursor format engine-owned and opaque on the wire: `Cursor` is
a single `bytes token` whose format the engine's cursor module owns
(`encode_cursor`, `decode_cursor`, `compute_max_cursor`,
`cursor_to_state_dict`). The destination receives the token with the batch;
the CDK's destination handlers return it unread as `committed_cursor` on a
`SUCCESS` ack. The engine
decodes the echoed token with `cursor_to_state_dict` and
`StateManager.save_stream_checkpoint` writes the decoded primary
watermark value to the stream's `CursorStore` file; at a log level of INFO
or below, the state log line carries the decoded cursor state, tie-breakers
included, as hex-encoded JSON. Where a sink dedups a replay, it does not
use the cursor
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
persists the watermark decoded from the echoed token only on a `SUCCESS`
ack ([ADR 0013](0013-prefer-duplicates-over-data-loss.md)), and trusts the
destination to echo the token it was sent. The token format and the
per-batch maximum live in one engine module; the engine does not compare
cursors across batches, so each committed watermark overwrites the last.

Known exceptions in code:
- Tie-breakers ride the token but not the checkpoint: `CursorStore` keeps
  the primary value alone, so a resumed read bounds on that value only.
- A failed `CursorStore` write does not stop the load; the
  file keeps what it held before until a later write succeeds.

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
