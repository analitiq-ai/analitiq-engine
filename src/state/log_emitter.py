"""Structured log emitter for observability markers.

Emits JSON lines to stdout with ANALITIQ_{CATEGORY}:: prefixes so that
log shippers (CloudWatch Logs Insights, Datadog, etc.) can filter and
parse them without touching the payload.
"""

import json
import logging
import sys
from typing import Any, Dict

logger = logging.getLogger(__name__)


def emit_log(category: str, data: Dict[str, Any]) -> None:
    """Write a structured JSON line to stdout with an observability marker."""
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
    try:
        print(f"{marker}{line}", file=sys.stdout, flush=True)
    except OSError as exc:
        logger.error(
            "emit_log: failed to write %r payload to stdout (%s); "
            "log line lost — check container log driver",
            category,
            exc,
        )
