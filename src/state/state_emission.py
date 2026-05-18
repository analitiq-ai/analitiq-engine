"""State checkpoint emission for cross-run observability.

Writes ANALITIQ_STATE::{json} lines to stdout so log shippers can track
incremental cursor positions across pipeline runs.

cursor_hex  — hex-encoded bytes of the full cursor dict (used for binary cursors)
cursor_value — human-readable high-water mark (timestamp or offset string)

Both failure modes (serialisation and stdout write) are caught and logged;
the caller receives no return value indicating success or failure.
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

    Best-effort: serialisation errors and stdout write failures are logged
    and suppressed. A missed checkpoint means cross-run cursor recovery may
    replay records from an earlier position.
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
    try:
        print(f"ANALITIQ_STATE::{line}", file=sys.stdout, flush=True)
    except OSError as exc:
        logger.critical(
            "emit_state_log: failed to write state checkpoint to stdout (%s); "
            "checkpoint lost for stream_id=%r — cross-run cursor recovery "
            "may replay from an earlier position",
            exc,
            stream_id,
        )
