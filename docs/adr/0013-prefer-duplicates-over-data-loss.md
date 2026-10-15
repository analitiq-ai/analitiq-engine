---
schemaVersion: 0.2.0
id: "0013"
title: "Prefer duplicates over data loss"
status: accepted
date: 2026-06-23
deciders: ["@Analitiq-Bot-Wonka"]
tags: [fault-tolerance, retries, checkpointing, idempotency]
scope: domain
reversibility: two-way-door
blastRadius: team
relatesTo: ["0011", "0012", "0014"]
affects:
  - type: path
    pattern: "src/engine/**"
  - type: path
    pattern: "src/grpc/**"
  - type: path
    pattern: "src/state/**"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/grpc-streaming-architecture.md
---

# ADR-0013: Prefer duplicates over data loss

## Context

Between the engine sending a batch and recording its checkpoint, many things can fail: the stream is torn down, the peer closes, the ack times out, the process dies. Each leaves the outcome unknown. The source also advances its cursor as it yields batches, ahead of the destination's ack, and cursor columns are often not unique, so rows can arrive at the boundary value between runs. Some sinks cannot dedup a resend at all (an API insert with no idempotency block, the append phase of `truncate_insert`, a file sink across a restart). Every one of these points forces a choice between risking a duplicate and risking a lost row. The same transport event was once fatal on the direct engine path and retryable through the worker proxy, so the choice was being made differently at different hops.

## Decision

We take the duplicate every time:

- A send that reaches no ack is `ACK_STATUS_RETRYABLE_FAILURE`, decided once, in the gRPC client where the condition is detected. Retries stay bounded by `max_retries`.
- The checkpoint never records a cursor the destination has not acked as committed. `Committed` is the only `Disposition` with a cursor; `AlreadyCommitted` carries none. `StateManager.save_cursor` holds the source's pre-ack position in memory only; `save_stream_checkpoint`, run on the ack, is what persists. An ack pairing a cursor with a non-success status is a fatal connector contract violation.
- Incremental SQL reads resume inclusively (`>=`) from that cursor, re-reading the boundary row; incremental API reads move the lower bound back from it by the stream's safety window.
- Each stream's retry-safety verdict (`RetryVerdict`: exactly-once or at-least-once, with a reason) is reported in the schema ack and logged at INFO at stream start. It never gates a retry: `BatchPolicy` takes no semantics argument.

## Options considered

### Option A: At-least-once everywhere, verdict reported (chosen)
- Pro: no row is lost to a missing ack, an early checkpoint, or a non-unique cursor, except in the cases under Known exceptions. Kafka Connect (without idempotence), Airbyte and Singer make the same trade.
- Con: at-least-once sinks can duplicate, so the operator has to know which streams those are.

### Option B: Gate the retry on the stream's declared semantics
- Pro: an at-least-once sink never duplicates.
- Con: a batch on such a sink is failed instead of resent, trading a possible duplicate for a certain loss.

### Option C: At-most-once (missing ack is fatal, exclusive `>` resume, checkpoint the source position)
- Pro: no duplicate is ever written; no destination needs dedup.
- Con: a lost ack drops a batch nobody rejected, an early checkpoint resumes past rows that never landed, and an exclusive resume drops rows that arrive at the boundary value.

## Trade-offs

Every destination must tolerate re-delivery. Sinks without content identity (see [ADR 0014](0014-writes-are-idempotent-on-content-derived-identity-never-batch-position.md)) can duplicate on a resend, and what the operator gets is the ack field and an INFO log line, not a refusal.

Known exceptions in code:

- Under the `dlq` or `skip` error strategy, a batch whose retries run out, including retries of a send that reached no ack, is dropped and the stream goes on; the next committed batch's cursor then moves the checkpoint past it. `dlq` writes the rows to the dead-letter queue (see [ADR 0011](0011-a-batch-is-accepted-or-rejected-whole.md) for a row it cannot write); `skip` writes them nowhere. The exception is a `truncate_insert` stream's first batch, whose failure fails the stream.
- An incremental SQL read pages with `LIMIT`/`OFFSET`, ordered by the cursor column alone. Rows tied on the cursor value across a page boundary can be skipped, and later pages then move the checkpoint past them.
- An incremental API read's lower bound derives from the committed cursor and `safety_window_seconds`. A row below that bound in a batch that never committed (the provider returned rows out of cursor order) is not read again; with a window of 0 and an exclusive `gt` lower bound on a `date-time` or id cursor, neither is a row that later arrives at the cursor value.

## Consequences

- Easier: one verdict per batch, decided at one site; a failed batch cannot advance the checkpoint because the type has nowhere to put a cursor.
- Harder: write modes and handlers must be designed around replays; a sink that cannot dedup is permanently at-least-once.
- **How we would know this was wrong:** operators routinely run at-least-once streams and treat the duplicates as a correctness defect, or loss is reported on a stream whose verdict was exactly-once.
- Revisit if: a sink class appears whose duplicates are more harmful than a failed run (non-idempotent side effects, such as payments) and needs an opt-in fail-instead-of-resend mode.
