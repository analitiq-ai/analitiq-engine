# Batch tracking on the destination — discussion notes

Resume point for a design discussion about how the engine tracks committed batches.

## What the code does today

- **`_batch_commits` table on the destination DB** — idempotency ledger, keyed `(run_id, stream_id, batch_seq)`. `CREATE TABLE IF NOT EXISTS`, permanent. `cdk/cdk/sql/generic.py:255`, DDL `:694-720`.
  - SQLAlchemy path co-commits the marker INSERT in the **same transaction** as the data write -> atomic / exactly-once. `generic.py:990-1018`.
  - ADBC path (Snowflake/BigQuery) commits ingest separately from the marker -> residual gap, surfaced loudly via `AdbcCommitRecordError`. `generic.py:1408-1418`, `148-169`.
  - Queried **only with the current `run_id`** -> it's a within-run retry/resume ledger, not a permanent record of all syncs. `generic.py:954-961`, `1216-1225`.
  - No pruning anywhere -> grows unbounded; rows are never read after their own run completes.
- **`_synced_at` column** added to every destination table, server-defaulted `DEFAULT NOW()`. The one per-row audit column we add. `generic.py:260`, `683-689`.
- **Resume cursor (high-water-mark)** lives in **local filesystem** `state/{pipeline_id}/{stream_id}.json`, NOT in `_batch_commits`. `src/state/store.py:60-96`. Losing it costs a re-scan, never data (idempotent via inclusive `>=` + upsert). `store.py:21-26`.
- **Local mirror** `state/batch_commits/{run_id}.jsonl` (`src/state/batch_commit_tracker.py`) — fast-path skip; lacks atomicity with the remote commit.

## Comparison to Fivetran / Airbyte / Singer

- They stamp ~3 per-row columns (`_fivetran_synced/_deleted/_id`, `_airbyte_raw_id/_extracted_at/_meta`, `_sdc_*`) because they do **at-least-once load + dedup-on-read** (ELT normalization) and have **no transactional batch boundary**.
- Their durable resume state lives in their **control plane**, not in the per-row columns. The columns are provenance/dedup helpers.
- We dedup at write time via **MERGE on the user's real keys**, so we don't need per-row dedup keys. Our resume cursor is engine-side (`state/`), analogous to their control-plane state.

## Key conclusions

1. Dropping `_batch_commits` does **not** lose the resume position (that's local) — premise of "lose all tracking" overstates it. Blast radius: recreated empty; at worst duplicate rows for **append (`insert`) batches mid-flight in a crashed run**. `upsert`/`truncate_insert` replay idempotently.
2. Can't just move the ledger to local state: **dual-write / two-generals**. COMMIT-then-update-local is two non-atomic writes; a crash or lost ack between them is unrecoverable. Only the destination can authoritatively answer "is batch N already here?" The atomicity comes from the marker being **in the same transaction as the data**, not from remembering.
3. In the ephemeral/sandboxed worker model, **local disk is the least durable store**; the destination ledger survives container loss.
4. The only two ways to make **append** exactly-once both write into the destination transaction: co-committed batch marker (current) OR per-row dedup keys (Fivetran). Local substitutes for neither — moving it local pushes toward per-row columns, not away.

## Recommended direction (not yet implemented)

- The ledger only earns its keep for **`insert`** mode. For `upsert`/`truncate_insert` it's dead weight — could be local-only or dropped.
- **Prune `_batch_commits` per run** to kill unbounded growth (never read after its run completes).
- Consider moving within-run idempotency to the control plane / engine-side for `insert`, keeping co-commit semantics.

## Open questions for next session

- Is `state/` persisted across runs in the SaaS deployment, or ephemeral per container? Affects how safe local-only tracking is.
- Worth a per-run pruning step for `_batch_commits` regardless of the larger redesign?
