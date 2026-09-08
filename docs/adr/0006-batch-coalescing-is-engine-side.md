# ADR 0006: Batch coalescing lives in the engine, not the destination

The wire protocol is strictly one batch, one ack, one cursor persist: a
destination can never hold more than one unacked batch, and so has
nothing to coalesce on its own. A source that emits one small batch per
page walks straight into per-table load-job quotas on warehouse
destinations. Where merging happens is therefore a real design choice,
not an implementation detail — and it is specified here (design of
record) even though the engine does not build it yet; see
[`sql-write-path.md`](../data-path/sql-write-path.md) §8 for the current
build status and the full mechanics.

## The alternatives this rules out

Two destination-side shapes would also solve the quota problem, and each
is rejected for its own reason — they are not the same failure mode:

- **Windowed or early acks** — the destination acks a batch before it is
  actually durable, promising a commit it hasn't made yet on the bet that
  it will flush later. This means an acked batch is no longer necessarily
  a durable commit; the engine would have to trust an untrusted connector
  to eventually flush what it claimed was already written. This is the
  trust problem connector isolation exists to prevent.
- **True deferred acks** (accumulate several batches, ack only once they
  are all durably flushed) — not a trust problem the same way, since an
  ack sent under this shape still means "committed." Its obstacle is
  structural: the wire protocol allows exactly one unacked batch per
  stream at a time (this document's opening paragraph), so a destination
  that withholds an ack to accumulate more would simply stall the engine,
  which cannot send the next batch until the current one acks. This shape
  needs a wire-protocol change (multiple batches in flight before an ack),
  not merely a different trust posture — out of scope here.
- **A flush hook with held cursors** — the destination decides when to
  materialize accumulated writes and advance the checkpoint. This hands
  cursor durability, which the engine alone is supposed to own, to
  code a customer supplied and an AI may have authored — the same trust
  problem as windowed acks, on the cursor instead of the ack.

Connector execution is isolated specifically because it is untrusted;
windowed acks and a flush hook both reopen exactly the trust boundary
that isolation exists to hold, and true deferred acks would need a wire
protocol this one isn't. Engine-side coalescing does neither: the wire protocol's
"one sent batch = one ack = one cursor persist" invariant stays intact
byte-for-byte — the sent batch just gets bigger. The destination worker
never learns coalescing happened.

## What this buys, and what it costs

Under stage-then-merge, the target table receives `MERGE` query jobs — a
high-quota class — and each load job lands in its own per-batch stage
table, so the per-table load-job quota never accumulates against any one
table. Coalescing shrinks the *count* of those jobs, which is exactly
what the project-level load-job quota and per-table operation-rate limits
actually bound.

The cost is real and is taken deliberately: declaring a `write_unit`
widens the dlq/skip unit to the coalesced batch, so a fatally rejected
unit is rejected wholesale, good rows included. This is not new
machinery to route around — whole-batch rejection without per-record
attribution is this engine's existing design stance (an untrusted
connector cannot be trusted to blame individual rows), and the operator's
control over the tradeoff is unit size: a deployment that needs finer DLQ
isolation declares a smaller `write_unit`.
