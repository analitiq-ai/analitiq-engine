# State File Specification (Local / File-Based Runtime, Non-Partitioned)

**Version:** 1.0 (Aligned)  
**Last Updated:** 2025-12-27  
**Framework:** Analitiq Stream

This document specifies the **file-based state format** for local execution **without partitioning**. Configuration (pipelines/streams/mappings) is stored separately; **state tracks execution progress** (cursor, checkpoints, retries, pagination tokens, etc.).

Key design goals:

- Keep **config** and **state** separate.
- Keep state **stream-scoped** (one state file per stream).
- Represent incremental progress with **cursor + optional tie-breakers**.
- Guard compatibility using **full SHA-256 fingerprints** of source and destination configurations.
- Keep state **versioned** (format evolution via `v1/`).

---

## 1) Directory Layout

```
state/
  {pipeline_id}/
    v1/
      index.json                      # Manifest + lightweight per-stream operational metadata
      streams/
        {stream_id}.json              # Single state file per stream
      locks/
        pipeline.lock.json            # Optional: lease/lock record for multi-process coordination
```

### Naming
- **Stream file:** `streams/{stream_id}.json` where `{stream_id}` is a UUID.
- Do not use display names for identity.

---

## 2) Stream State File: `streams/{stream_id}.json`

Each stream state file contains the execution position for the stream and minimal operational state.

### 2.1 Stream State JSON (recommended shape)

```json
{
  "pipeline_id": "b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15",
  "stream_id": "f1a2b3c4-d5e6-7890-abcd-ef1234567891",

  "status": "idle",

  "config": {
    "config_revision": 17,

    "source": {
      "endpoint_id": "wise.transactions.v3",
      "config_fingerprint": "sha256:3b6d2f8e2a1b4c9f0d9e6a1f7a1d2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c"
    },

    "destinations": [
      {
        "endpoint_id": "sevdesk.checkAccountTransaction.v2",
        "config_fingerprint": "sha256:9a0b1c2d3e4f5061728394a5b6c7d8e9f00112233445566778899aabbccddeef"
      }
    ]
  },

  "replication_state": {
    "method": "incremental",

    "cursor": {
      "field_path": ["created"],
      "mode": "inclusive",
      "value": "2025-12-25T10:30:15Z",
      "tie_breakers": [
        { "field_path": ["id"], "value": "tr_9f3c2a" }
      ]
    },

    "hwm": {
      "value": "2025-12-25T10:31:00Z",
      "purpose": "Highest cursor value observed (optional; useful for safety-window and resume validation)"
    },

    "page_state": {
      "next_token": null,
      "offset": null,
      "page": null,
      "cursor": null
    },

    "http_conditionals": {
      "etag": null,
      "last_modified": null
    }
  },

  "checkpoint": {
    "seq": 42,
    "last_checkpoint_at": "2025-12-25T10:31:00Z",
    "records_processed_since_checkpoint": 500,
    "errors_since_checkpoint": 0
  },

  "retry_state": {
    "consecutive_failures": 0,
    "next_retry_at": null
  },

  "stats": {
    "records_synced_total": 18420,
    "batches_written_total": 215,
    "dlq_total": 12
  },

  "last_run": {
    "run_id": "run_20251225_103000_abc123",
    "started_at": "2025-12-25T10:30:00Z",
    "finished_at": "2025-12-25T10:31:10Z",
    "outcome": "success",
    "error_summary": null
  },

  "last_updated": "2025-12-25T10:31:02Z"
}
```

### 2.2 Field meanings (stream state file)

- `status`: `idle | running | paused | error`.
- `config.config_revision`: stream config revision used when this state was produced (recommended for reproducibility).
- `config.source|destinations.*.config_fingerprint`:
    - full SHA-256 of the *effective immutable* source/destination configuration.
    - used to detect incompatibilities (e.g., config changed but state did not reset/migrate).
- `replication_state.cursor`:
    - `field_path`: token array for cursor field.
    - `mode`: `"inclusive"` or `"exclusive"` (equivalent to `>=` or `>`).
    - `value`: last committed cursor value.
    - `tie_breakers`: optional list to disambiguate ties.
- `replication_state.page_state`: continuation info for APIs (use only one mechanism depending on the connector).
- `replication_state.http_conditionals`: `ETag` / `Last-Modified` for polling sources.
- `checkpoint`: crash-recovery progress markers. Keep semantics consistent with cursor advancement rules.
- `retry_state`: backoff metadata for transient failures.
- `last_run`: compact summary; detailed logs belong elsewhere.
- `last_updated`: last modified timestamp.

**Fingerprint requirements:**
- Use **full SHA-256** (64 hex chars): `sha256:<64-hex>` or `sha256:<base64url>`.
- Fingerprint must be deterministic and based on canonical JSON serialization.

---

## 3) Manifest: `index.json` (Pipeline-level registry + quick introspection)

### Purpose
- Discover streams for a pipeline quickly.
- Provide quick status summary without opening every stream file (optional optimization).
- Track active run metadata and leasing (if used).

### 3.1 Manifest JSON (recommended shape)

```json
{
  "state_format_version": 1,
  "pipeline_id": "b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15",

  "streams": {
    "f1a2b3c4-d5e6-7890-abcd-ef1234567891": {
      "file": "streams/f1a2b3c4-d5e6-7890-abcd-ef1234567891.json",
      "status": "idle",
      "last_run_outcome": "success",
      "last_updated": "2025-12-25T10:31:02Z"
    }
  },

  "run": {
    "run_id": null,
    "lease_owner": null,
    "started_at": null,
    "checkpoint_seq": 0
  },

  "last_updated": "2025-12-25T10:31:02Z"
}
```

---

## 4) Concurrency & Locking (Local / File-Based)

This format supports:
- **Single-process** execution (simplest)
- **Multi-process workers** on a shared filesystem (requires a real lock/lease)

### 4.1 Updates
- Stream state files are independent: `streams/{stream_id}.json`.
- If multiple processes might update the same stream file, you must coordinate (lease/lock or single-writer).

### 4.2 Optional pipeline lease file
If you run multiple processes that might contend, use a lease record:

`state/{pipeline_id}/v1/locks/pipeline.lock.json`
```json
{
  "lease_owner": "worker-7f2a",
  "lease_acquired_at": "2025-12-25T10:30:00Z",
  "lease_expires_at": "2025-12-25T10:30:30Z"
}
```

Rules:
- Only the lease owner may start a run and update stream state for that pipeline.
- Renew the lease periodically.
- If the lease expires, another worker may take over.

---

## 5) Crash Recovery

Recommended recovery sequence:

1. Read `index.json` (optional) to discover stream files.
2. For each stream:
    - load `streams/{stream_id}.json`
    - validate fingerprints against current effective configs
    - resume from `replication_state.cursor`
    - use `page_state` if mid-page continuation is supported
3. Continue processing and checkpoint periodically.

Cursor advancement rules (recommended):
- Advance `cursor.value` only when you have a **durable commit** (e.g., destination batch acknowledged).
- If you overlap via safety window, rely on **upsert/idempotency** to absorb duplicates.

---

## 6) Atomic Write Pattern

Use atomic replace semantics:

```python
import json, os, tempfile

def atomic_write_json(path: str, obj: dict):
    directory = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic on most local filesystems
    finally:
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass
```

---

## 7) Notes on Resets

A reset is functionally a **cursor change** (and optionally clearing `checkpoint`, `page_state`, and `retry_state`).

If you require auditability, record reset events separately (recommended), for example under:

```
state/{pipeline_id}/v1/events/resets/{stream_id}/reset-{timestamp}-{uuid}.json
```

This keeps the stream state minimal and preserves an append-only reset history.

---

## 8) Validation Checklist

A state implementation is considered compliant if it:

- Uses `streams/{stream_id}.json` (UUID identity).
- Stores **full** SHA-256 fingerprints for source and destinations in the stream state.
- Uses token arrays for cursor `field_path` and tie-breaker `field_path`.
- Updates files using atomic replace.
- Coordinates multi-process updates via a lock/lease or single-writer rule.
