In method `_validate_replication_key_against_schema()` we locate correctly the replication key in schema.

# Composite coursor (Optional)
If the key isn’t unique, pair with a tie-breaker like id.
State stores (cursor_ts, cursor_id).
Query logic: updatedAt > effective_from OR (updatedAt = effective_from AND id > cursor_id) if the API supports compound filters; otherwise filter client-side.

# After Durable Write
Advance the bookmark after a successful batch
Set cursor to the last row’s replication_key (and tie-breaker if used), then commit state atomically.
After durable write to source, update the bookmark in the stream state
last = records[-1]
state.update_bookmark(cursor=last[rk], aux={"last_id": last["id"]})
state.commit()
That’s it: bookmark gives “where,” replication_key gives “by what,” cursor_mode picks the operator, and safety_window_seconds pulls the start back slightly to avoid misses.


# API-Connector Checklist (✅/❌)

Auth (headers/token refresh/OAuth): ❌ (headers only; no refresh/expiry handling)

Pagination: ✅ basic cursor/offset/page; ❌ link/has_more handling (addressed in fixes)

Rate limits: ✅ basic limiter; ❌ monotonic/backoff-aware (fixed)

Backoff/retries with jitter: ❌ (added)

Idempotency keys: ✅ basic; ❌ stable across retries (fixed)

Cursoring/incremental state: ✅ basic; ❌ operator encoding & per-endpoint mapping (improved)

Schema discovery/catalog & filter validation: ❌ (only minimal file-based)

CDC support: ❌

Batching: ✅ writes; ❌ streaming reads for large payloads

Streaming/chunked transfers: ❌

State checkpoints frequency: ❌ (no periodic flushing)

Structured logging/metrics/tracing: ❌

Secrets handling/redaction: ❌ (added URL redaction)

Tests (unit/contract): ❌

Type coverage/strictness: ❌ (no typing for many internals)


# Schema changes
Add source and destination schema hash that are defi and on changes invalidate state on breaking schema changes.



# Add circuit breaker to documentation

Circuit Breaker States & Failure Count Logic

The Problem

The test expects that in CLOSED state, successful calls should NOT reset the failure count. But my current implementation resets it. Here's why this matters:

Example Scenario

Imagine you have a flaky service that sometimes fails:

Calls: SUCCESS, FAIL, SUCCESS, FAIL, FAIL, SUCCESS, FAIL, FAIL, FAIL

Current Implementation (Wrong):

Call 1: SUCCESS → failure_count = 0 (reset)
Call 2: FAIL → failure_count = 1
Call 3: SUCCESS → failure_count = 0 (reset) ❌
Call 4: FAIL → failure_count = 1
Call 5: FAIL → failure_count = 2
Call 6: SUCCESS → failure_count = 0 (reset) ❌
Call 7: FAIL → failure_count = 1
Call 8: FAIL → failure_count = 2
Call 9: FAIL → failure_count = 3
Result: Never reaches threshold (5), circuit never opens!

Correct Implementation (What test expects):

Call 1: SUCCESS → failure_count = 0 (starts at 0)
Call 2: FAIL → failure_count = 1
Call 3: SUCCESS → failure_count = 1 (NO reset) ✅
Call 4: FAIL → failure_count = 2
Call 5: FAIL → failure_count = 3
Call 6: SUCCESS → failure_count = 3 (NO reset) ✅
Call 7: FAIL → failure_count = 4
Call 8: FAIL → failure_count = 5 → CIRCUIT OPENS! 🔴

Why This Matters

Without resetting on success in CLOSED state:
- Circuit breaker properly detects degraded services
- Even if some calls succeed, accumulated failures trigger protection
- Prevents "lucky success" from hiding underlying problems

The failure count should only reset when:
1. Circuit transitions from OPEN → HALF_OPEN → CLOSED (full recovery cycle)
2. Manual reset is called
3. Force close is called

In HALF_OPEN state: Success SHOULD close the circuit because that's the "test if service recovered" phase.

This is a classic circuit breaker pattern - it tracks the general health trend rather than just the last call result.
