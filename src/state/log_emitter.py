"""Structured log emitter used by DLQ, metrics, and state subsystems.

The other state-package modules emit observability records by wrapping a
small payload with a marker and a category so log-collecting infra can
reliably pick them out of stdout. This module is the single seam.
"""

from __future__ import annotations

import json
import logging
import os
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

    ``org_id`` is injected from the ``ORG_ID`` environment variable into
    every payload. The downstream pipeline-output-processor keys S3 paths
    and DDB writes by ``org_id`` and drops records where it is missing.
    """
    marker = MARKERS.get(category)
    if marker is None:
        logger.debug("emit_log: unknown category %r — using as marker", category)
        marker = category.upper()
    org_id = os.environ.get("ORG_ID") or 0
    enriched: dict[str, Any] = {"org_id": org_id, **dict(data)}
    try:
        payload = json.dumps(enriched, default=str)
    except (TypeError, ValueError) as err:
        logger.error("emit_log: payload not JSON-serialisable: %s", err)
        payload = json.dumps({"org_id": org_id, "error": str(err), "category": category})
    logger.info("%s::%s", marker, payload)
