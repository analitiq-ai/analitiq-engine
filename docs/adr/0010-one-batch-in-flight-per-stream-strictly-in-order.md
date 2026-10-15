---
schemaVersion: 0.2.0
id: "0010"
title: "One batch in flight per stream, strictly in order"
status: accepted
date: 2026-06-04
deciders: ["@Analitiq-Bot-Wonka"]
tags: [grpc, wire-protocol, checkpointing, ordering]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0006", "0011", "0012"]
affects:
  - type: path
    pattern: "src/grpc/**"
  - type: path
    pattern: "src/engine/**"
  - type: path
    pattern: "src/worker/**"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/grpc-streaming-architecture.md
---

# ADR-0010: One batch in flight per stream, strictly in order

## Context

A batch of an incremental stream carries a cursor, and the engine
checkpoints the watermark it decodes from the cursor a `SUCCESS` ack
returns. If several batches were in flight, acks could arrive out of order
and the engine would have to work out which checkpoint is safe to persist,
or trust the destination to report its commit state across a window of
batches. Destination connectors are untrusted code, so that trust is
exactly what the engine avoids. Against this, round-trip latency per
batch caps throughput, and pipelining is the usual answer to that.

## Decision

We send one batch, block until its ack (or an ACK timeout) arrives, and
only then send the next: `DestinationGRPCClient.send_batch` queues the
batch and waits for the next ack before it returns; an ACK timeout or a
dead stream returns a retryable failure instead. `BatchPolicy.run` drives a batch, retries included, to its final outcome
before the load stage takes the next one; extract and transform run ahead
into bounded queues. The rule is per stream; separate streams run
concurrently, each over its own client. The same client
carries the destination shell's hop to its connector worker, so both hops
keep the rule.

## Options considered

### Option A: One batch in flight, strictly ordered (chosen)
- Pro: checkpoints are written in commit order, one commit at a time; the
  engine never has more than one unacked batch outstanding on a stream.
- Con: per-batch round-trip latency limits throughput.

### Option B: Pipelined or windowed sends, several batches in flight
- Pro: hides round-trip latency.
- Con: needs out-of-order ack handling and a rule for which cursor may be
  persisted; a failure mid-window leaves later batches with unclear
  commit state.

### Option C: Early or windowed acks from the destination
- Pro: same throughput gain without engine-side windowing.
- Con: an ack no longer means "durably committed", so the engine must trust
  an untrusted connector to flush later (rejected in
  [ADR 0006](0006-batch-coalescing-is-engine-side.md)).

## Trade-offs

Throughput per stream is bounded by write latency plus one round trip per
batch. Raising it means larger batches, which is why coalescing small
batches belongs in the engine ([ADR 0006](0006-batch-coalescing-is-engine-side.md))
rather than in deferred acks.

Known exceptions in code:
- The engine does not coalesce. A batch goes on the wire as the source
  read yields it.

## Consequences

- Easier: cursor persistence is one ack, at most one checkpoint; retry and DLQ
  decisions concern exactly one batch.
- Harder: per-stream throughput cannot be raised by concurrency; a slow
  destination write stalls its stream.
- **How we would know this was wrong:** streams whose throughput is
  dominated by idle round-trip time even with coalesced batches at the
  size limit.
- Revisit if: a destination needs multi-batch transactions, or latency
  between engine and destination becomes large relative to write time.
