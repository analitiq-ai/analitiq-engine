"""Structured stdout log emitter for observability markers.

Emits lines of the form ``ANALITIQ_{CATEGORY}::{json}`` to stdout so log
aggregators (CloudWatch Logs Insights, Datadog, etc.) can filter and parse
them with a simple prefix query.  Payloads must never include record-level
data — only counts, IDs, and status strings.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict


def emit_log(category: str, data: Dict[str, Any]) -> None:
    """Write a single structured log line to stdout.

    The line format is ``ANALITIQ_{CATEGORY}::<json>`` where category is
    upper-cased (e.g. ``"dlq"`` → ``ANALITIQ_DLQ::``).
    """
    marker = f"ANALITIQ_{category.upper()}::"
    print(f"{marker}{json.dumps(data, default=str)}", file=sys.stdout, flush=True)
