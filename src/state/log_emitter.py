"""Structured log emitter for observability markers.

Emits JSON lines to stdout with ANALITIQ_{CATEGORY}:: prefixes so that
log shippers (CloudWatch Logs Insights, Datadog, etc.) can filter and
parse them without touching the payload.

Example output:
    ANALITIQ_DLQ::{"type": "dlq", "pipeline_id": "my-pipe", "added": 3}
    ANALITIQ_METRICS::{"type": "batch", "run_id": "abc", "records_written": 100}
"""

import json
import sys
from typing import Any, Dict


def emit_log(category: str, data: Dict[str, Any]) -> None:
    """Write a structured JSON line to stdout with an observability marker.

    Args:
        category: Log category (e.g. ``"dlq"``, ``"metrics"``). Used to
            construct the ``ANALITIQ_{CATEGORY.upper()}::`` prefix.
        data: Payload to serialise as JSON. Must be JSON-serialisable.
    """
    marker = f"ANALITIQ_{category.upper()}::"
    print(f"{marker}{json.dumps(data)}", file=sys.stdout, flush=True)
