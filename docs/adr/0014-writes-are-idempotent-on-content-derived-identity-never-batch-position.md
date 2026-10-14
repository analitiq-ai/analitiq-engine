---
schemaVersion: 0.2.0
id: "0014"
title: "Writes are idempotent on content-derived identity, never batch position"
status: accepted
date: 2026-06-28
deciders: ["@Analitiq-Bot-Wonka"]
tags: [idempotency, row-identity, destinations, retries]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0005", "0013"]
affects:
  - type: path
    pattern: "cdk/cdk/sql/**"
  - type: path
    pattern: "cdk/cdk/api/**"
  - type: path
    pattern: "cdk/cdk/file/**"
  - type: path
    pattern: "cdk/cdk/record_identity.py"
  - type: path
    pattern: "src/grpc/client.py"
provenance:
  authoredBy: human
  sourceArtifact: docs/architecture/grpc-streaming-architecture.md
---

# ADR-0014: Writes are idempotent on content-derived identity, never batch position

## Context

Retries re-deliver batches by design ([ADR 0013](0013-prefer-duplicates-over-data-loss.md)), so each destination needs a key to recognise a replay. The obvious key is the batch's position, `(run_id, stream_id, batch_seq)`, recorded in a commit ledger. It does not hold: a same-run retry resumes from an advanced, inclusive cursor and re-batches the boundary rows, so the same `batch_seq` arrives carrying different rows. A positional ledger then either skips rows it never saw (loss) or misses rows it did (duplicates). The key has to come from what the data is, not where it sat.

## Decision

We make idempotency a protocol property keyed on content-derived identity, per sink:

- **SQL:** `insert` anti-joins from the stage ([ADR 0005](0005-stage-then-merge-is-the-single-sql-write-primitive.md)) on row identity, which is the contract primary key, or, for a keyless stream, a synthetic engine-managed `_record_hash` column (SHA-256 of the row as cast to the destination schema) declared as the table's primary key. `upsert` merges on `conflict_keys`.
- **API:** the engine owns the idempotency key value. `insert` sends the identity-derived `record_id` (first occurrence wins); `upsert` sends `content_idempotency_key`, a full-content hash, so a changed row gets a new key and the update applies.
- **File:** each batch file name carries a hash of its serialized bytes, written to a temp file and renamed into place. Time-partitioned paths resolve from the engine-stamped emit instant (`emitted_at_unix_ms`), stamped once per batch and resent unchanged on retry, never the sink's write-time clock.

Every digest goes through one canonicalisation, `record_digest`; each caller keeps its own basis. `run_id`, `stream_id` and `batch_seq` are routing and ordering fields, never dedup keys. The engine keeps no in-run pre-send skip of already-acked batches.

## Options considered

### Option A: Content-derived identity per sink (chosen)
- Pro: correct under re-batching; no bookkeeping tables; dedup lives in the write itself and is identical on both SQL transports.
- Con: every sink needs an identity; keyless tables carry an extra column; byte-identical keyless rows collapse to one.

### Option B: Positional commit ledger keyed on `(run_id, stream_id, batch_seq)`
- Pro: generic across sinks; no schema change to targets.
- Con: mis-keys a retry from an advanced cursor, dropping or duplicating rows; adds a ledger table and its pruning lifecycle.

### Option C: In-run pre-send skip of batches already acked
- Pro: avoids resending work on a restart.
- Con: positional, so it reintroduces the row drop Option A removes.

### Option D: Per-dialect `INSERT IGNORE` / `ON CONFLICT DO NOTHING`
- Pro: native to each database.
- Con: a different insert mechanism per transport, with a parity gap to maintain.

## Trade-offs

Exactly-once holds only where an identity is enforced: a system that does not enforce primary keys reports at-least-once for `insert`; an API key dedups only inside the provider's replay window; `truncate_insert`'s append phase and file sinks across a restart are at-least-once. Under keyless `insert`, two genuinely identical rows become one, and `insert` cannot tell a re-read from a conflicting key, so a stream that reconciles changes must use `upsert`.

## Consequences

- Easier: replays need no destination-side state; the retry verdict per write mode follows from the write itself.
- Harder: the digest serialisation is a stability contract; changing it re-identifies every row. Keyless destination tables carry `_record_hash` as their primary key.
- **How we would know this was wrong:** duplicates or drops traced to a replay on a sink that reports exactly-once, or users needing to keep legitimate identical keyless rows.
- Revisit if: a sink class appears with no content identity and no way to give it one.

Known exceptions:

- A batch file's name is `batch_seq` plus the content hash, so position is part of the path: identical bytes resent under a different `batch_seq` land in a new file. The content hash is what stops different content at the same `batch_seq` from overwriting.
- The protocol still carries `ACK_STATUS_ALREADY_COMMITTED` and the `BaseDestinationHandler.finalize_run(succeeded=...)` hook, so a handler may keep its own ledger. No built-in handler does.
