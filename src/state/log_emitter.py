"""Structured log emitter used by DLQ, metrics, and state subsystems.

The other state-package modules emit observability records by wrapping a
small payload with a marker and a category so log-collecting infra can
reliably pick them out of stdout. This module is the single seam.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional

from src.shared.run_id import get_run_id

logger = logging.getLogger(__name__)

# Control-plane env var carrying the canonical run identifier; absent locally.
INVOCATION_ID_VAR = "INVOCATION_ID"


# Markers prefix every emitted log line so downstream collectors can
# identify Analitiq-emitted records without parsing free-form messages.
MARKERS: dict[str, str] = {
    "dlq": "ANALITIQ_DLQ",
    "metrics": "ANALITIQ_METRICS",
    "state": "ANALITIQ_STATE",
}

# Process-local monotonic guard for ``emitted_at``. Emissions can come from
# several threads, so the clamp is taken under a lock.
_emit_lock = threading.Lock()
_last_emitted_at: Optional[datetime] = None


def _next_emitted_at() -> str:
    """Return a strictly increasing UTC ISO-8601 emission timestamp.

    Plain ``datetime.now()`` can repeat (sub-microsecond clock resolution) or
    move backward (NTP/VM time correction). The deployment uses ``emitted_at``
    to make its "latest cursor" overwrite conditional on a newer stamp, so a
    backward step within a run could leave durable state stuck on a stale
    cursor. Clamping to ``max(now, last + 1us)`` keeps the value monotonic and
    collision-free for the lifetime of the process (one run).
    """
    global _last_emitted_at
    with _emit_lock:
        now = datetime.now(timezone.utc)
        if _last_emitted_at is not None and now <= _last_emitted_at:
            now = _last_emitted_at + timedelta(microseconds=1)
        _last_emitted_at = now
        return now.isoformat()


def _run_identity() -> dict[str, Any]:
    """Ambient run-identity defaults stamped onto every record.

    ``run_id`` (the engine's per-attempt id, via the central accessor) and
    ``invocation_id`` (the control-plane's canonical run id) are written
    *before* the caller payload, so a caller that legitimately owns one --
    the metrics record's typed ``run_id`` -- still wins. ``invocation_id`` is
    included only when ``INVOCATION_ID`` is set (cloud); absent locally the
    field is omitted with no error. Echoing it lets the run-status processor
    key customer-facing lookups by ``invocation_id`` directly, dropping the
    ``run_id`` -> ``invocation_id`` join.
    """
    identity: dict[str, Any] = {"run_id": get_run_id() or 0}
    invocation_id = os.environ.get(INVOCATION_ID_VAR, "").strip()
    if invocation_id:
        identity["invocation_id"] = invocation_id
    return identity


def emit_log(category: str, data: Mapping[str, Any]) -> None:
    """Emit one observability record at INFO level.

    The record is JSON-serialised so collectors can ingest it directly.
    Unknown categories are still emitted (the marker falls back to the
    category name) but a debug warning is logged so the typo is visible.

    ``org_id`` and ``emitted_at`` are emitter-owned envelope fields, written
    last so a caller payload can never clobber them. ``org_id`` is injected
    from the ``ORG_ID`` environment variable; the downstream
    pipeline-output-processor keys S3 paths and DDB writes by it and drops
    records where it is missing.

    ``run_id`` and ``invocation_id`` are ambient run-identity defaults written
    first (see :func:`_run_identity`), so a record always carries them even
    when the caller does not, while a caller that owns ``run_id`` still wins.

    ``emitted_at`` is stamped on every record at emission time: a sub-second
    UTC ISO-8601 timestamp, the single ordering key collectors use across all
    categories (state/metrics/dlq) without parsing a category-specific field.
    For state lines the deployment uses it to make its "latest cursor"
    overwrite conditional on a newer stamp. It is clamped strictly increasing
    per process (see :func:`_next_emitted_at`) so checkpoints within one run
    never reorder or collide even if the host clock steps backward; across
    separate runs it is wall-clock and subject to inter-host skew.
    """
    marker = MARKERS.get(category)
    if marker is None:
        logger.debug("emit_log: unknown category %r — using as marker", category)
        marker = category.upper()
    org_id = os.environ.get("ORG_ID") or 0
    emitted_at = _next_emitted_at()
    identity = _run_identity()
    enriched: dict[str, Any] = {
        **identity,
        **dict(data),
        "org_id": org_id,
        "emitted_at": emitted_at,
    }
    try:
        payload = json.dumps(enriched, default=str)
    except (TypeError, ValueError) as err:
        logger.error("emit_log: payload not JSON-serialisable: %s", err)
        payload = json.dumps(
            {
                **identity,
                "org_id": org_id,
                "emitted_at": emitted_at,
                "error": str(err),
                "category": category,
            }
        )
    logger.info("%s::%s", marker, payload)
