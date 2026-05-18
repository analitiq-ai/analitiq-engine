"""State checkpoint emission for cross-run observability."""

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
    """Emit a checkpoint log line to stdout."""
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
