"""Structured log emitter for observability markers.

Emits JSON lines to stdout with ANALITIQ_{CATEGORY}:: prefixes so that
log shippers (CloudWatch Logs Insights, Datadog, etc.) can filter and
parse them without touching the payload.

Example output:
    ANALITIQ_DLQ::{"type": "dlq", "pipeline_id": "my-pipe", "added": 3}
    ANALITIQ_METRICS::{"type": "batch", "run_id": "abc", "records_written": 100}
"""

import json
import logging
import sys
from typing import Any, Dict

logger = logging.getLogger(__name__)


def emit_log(category: str, data: Dict[str, Any]) -> None:
    """Write a structured JSON line to stdout with an observability marker.

    Args:
        category: Log category, case-insensitive (e.g. ``"dlq"``, ``"metrics"``).
            Uppercased when constructing the ``ANALITIQ_{CATEGORY.upper()}::`` prefix.
        data: Payload to serialise as JSON. Values must be JSON-serialisable.
    """
    marker = f"ANALITIQ_{category.upper()}::"
    try:
        line = json.dumps(data)
    except (TypeError, ValueError) as exc:
        logger.error(
            "emit_log: failed to serialise %r payload (%s); keys=%s",
            category,
            exc,
            list(data.keys()),
        )
        return
    print(f"{marker}{line}", file=sys.stdout, flush=True)
