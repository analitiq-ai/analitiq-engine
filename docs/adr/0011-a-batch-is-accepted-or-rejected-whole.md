---
schemaVersion: 0.2.0
id: "0011"
title: "A batch is accepted or rejected whole"
status: accepted
date: 2026-06-04
deciders: ["@Analitiq-Bot-Wonka"]
tags: [wire-protocol, error-handling, dead-letter-queue]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0006", "0010", "0014"]
affects:
  - type: path
    pattern: "proto/analitiq/v1/**"
  - type: path
    pattern: "src/engine/**"
  - type: path
    pattern: "src/state/**"
  - type: path
    pattern: "cdk/cdk/base_handler.py"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/grpc-streaming-architecture.md
---

# ADR-0011: A batch is accepted or rejected whole

## Context

A batch is already the unit of send, ack and cursor persistence
([ADR 0010](0010-one-batch-in-flight-per-stream-strictly-in-order.md)).
When some of its rows are bad, the engine either rejects the batch or
accepts the good rows and routes the bad ones. Per-row routing needs a
per-row account of what failed, and that account would come from a
destination connector, which is untrusted and often cannot know: a
database error frequently names no row. Per-row acceptance would also need
partial cursor advance and partial rollback in every handler.

## Decision

We make the batch the unit of acceptance and rejection. `AckStatus` has no
partial value. The ack's
`failed_record_ids` is carried across both hops, and no disposition reads
it: the engine disposes of the whole batch regardless of it. A validation rule that fails on any row of a
batch with no mapping defect fails the whole batch with a
`ValidationFailure` carrying `ErrorStrategy.strictest` of the failed rules
(fail over dlq over skip), and `BatchPolicy.reject` disposes of the batch
under it. A mapping defect raises `TransformationError` instead, whatever
the rules say. A batch whose
retries run out is disposed of whole under the stream's strategy, except
the first batch sent on a `truncate_insert` stream, which fails the stream. A
`FATAL_FAILURE` ack fails the stream under every strategy; under `dlq` the
whole batch is passed to the DLQ first. The engine does not split and retry a
batch to isolate bad rows.

The verdict is whole, but the write is not always atomic. An API
destination sends each record, or each chunk under `max_records`, as its
own request, and a SQL destination without transactional DDL commits each
stage-cycle step separately, so a `truncate_insert` first batch can
truncate the target and then fail. What landed stays written, the batch is
still rejected whole, and a replay re-sends all of it.

## Options considered

### Option A: Whole-batch acceptance and rejection (chosen)
- Pro: one disposition per batch, no partial cursor or rollback state, no
  reliance on a connector's per-row blame.
- Con: good rows travel with bad ones into the DLQ, the skip, or the failure.

### Option B: `PARTIAL_SUCCESS` with per-record attribution
- Pro: good rows land; only failed rows are dead-lettered.
- Con: trusts the connector's per-row report, which may be incomplete or
  wrong; needs partial cursor semantics and partial rollback per handler.

### Option C: Split-and-retry to isolate bad rows
- Pro: finds bad rows without trusting the connector.
- Con: multiplies writes and load jobs on failure, and re-sends rows that
  may already be committed.

## Trade-offs

Rejection granularity equals batch size. An operator who wants finer DLQ
isolation lowers batch size.
A batch a validation rule rejects is never retried, because the rule is a
pure function of the batch.

Known exceptions in code:
- A DLQ write is per row. A row that neither the DLQ file nor its fallback
  file takes is logged as lost, and the stream goes on as if the batch
  were dead-lettered.

## Consequences

- Easier: one verdict per batch in `BatchPolicy`; the DLQ records a batch's
  rows with one failure summary; no handler tracks partial rollback.
- Harder: locating the bad row inside a dead-lettered batch is the
  operator's job, aided by the failure summary.
- **How we would know this was wrong:** operators routinely dead-lettering
  large batches for one bad row and cutting batch sizes to compensate,
  at a throughput cost they cannot accept.
- Revisit if: destinations emerge whose per-row error reports are both
  complete and verifiable by the engine.
