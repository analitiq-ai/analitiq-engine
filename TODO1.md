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



# Look for common datetime patterns - be more strict
    return (
        value.endswith('Z') or  # ISO with Z timezone
        ('+' in value[-6:] or '-' in value[-6:]) or  # ISO with timezone offset
        ('T' in value and value.count('-') == 2 and len(value) >= 19)  # ISO datetime format (stricter)
    )


Medium Priority Issues:
4. API Connector: 25/65 failed - REST API connectivity issues
5. Database Connector: 19/43 failed - Database operations partially failing
6. Core Engine: 8/14 failed - Main streaming engine has issues
7. Data Transformer: 11/11 failed - ETL transformation pipeline broken




