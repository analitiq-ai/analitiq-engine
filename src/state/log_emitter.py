"""Structured log emitter used by DLQ, metrics, and state subsystems.

The other state-package modules emit observability records by wrapping a
small payload with a marker and a category so log-collecting infra can
reliably pick them out of stdout. This module is the single seam.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Mapping

logger = logging.getLogger(__name__)


# Markers prefix every emitted log line so downstream collectors can
# identify Analitiq-emitted records without parsing free-form messages.
MARKERS: dict[str, str] = {
    "dlq": "ANALITIQ_DLQ",
    "metrics": "ANALITIQ_METRICS",
    "state": "ANALITIQ_STATE",
}


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

    ``emitted_at`` is stamped on every record at emission time: a sub-second
    UTC ISO-8601 wall-clock timestamp. It gives collectors one ordering key
    across all categories (state/metrics/dlq) without parsing a
    category-specific field — for state lines the deployment uses it to make
    its "latest cursor" overwrite conditional on a newer stamp. It is a
    best-effort wall-clock key, not a strict monotonic guarantee (subject to
    clock skew/adjustment); microsecond precision orders closely-spaced
    emissions but does not make the stamp unique.
    """
    marker = MARKERS.get(category)
    if marker is None:
        logger.debug("emit_log: unknown category %r — using as marker", category)
        marker = category.upper()
    org_id = os.environ.get("ORG_ID") or 0
    emitted_at = datetime.now(timezone.utc).isoformat()
    enriched: dict[str, Any] = {
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
                "org_id": org_id,
                "emitted_at": emitted_at,
                "error": str(err),
                "category": category,
            }
        )
    logger.info("%s::%s", marker, payload)
