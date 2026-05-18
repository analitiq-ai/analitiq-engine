"""State checkpoint emission for cross-run observability.

Emits ANALITIQ_STATE:: log lines that log shippers can extract to track
pipeline progress across runs without a database dependency.
"""

import json
import logging
import sys

logger = logging.getLogger(__name__)


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
    try:
        line = json.dumps(data)
    except (TypeError, ValueError) as exc:
        logger.error(
            "emit_state_log: failed to serialise state payload (%s); stream_id=%s",
            exc,
            stream_id,
        )
        return
    print(f"ANALITIQ_STATE::{line}", file=sys.stdout, flush=True)
