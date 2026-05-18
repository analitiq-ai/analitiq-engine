"""State checkpoint emission for cross-run observability.

Emits ANALITIQ_STATE:: log lines that log shippers can extract to track
pipeline progress across runs without a database dependency.
"""

import sys
import json


def emit_state_log(
    run_id: str,
    pipeline_id: str,
    stream_id: str,
    cursor_hex: str,
    cursor_value: str,
) -> None:
    """Emit a checkpoint log line to stdout.

    Args:
        run_id: Current pipeline run identifier.
        pipeline_id: Pipeline being executed.
        stream_id: Stream that advanced its cursor.
        cursor_hex: Hex-encoded cursor bytes for replay.
        cursor_value: Human-readable high-water mark value.
    """
    data = {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "stream_id": stream_id,
        "cursor_hex": cursor_hex,
        "cursor_value": cursor_value,
    }
    print(f"ANALITIQ_STATE::{json.dumps(data)}", file=sys.stdout, flush=True)
