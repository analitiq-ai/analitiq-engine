"""Structured stdout state log emitter.

Emits ``ANALITIQ_STATE::{json}`` lines to stdout so checkpoint state can be
captured by log aggregators for cross-run observability.

``cursor_hex`` must be hex-encoded by the caller before passing here
(e.g. ``json.dumps(cursor).encode().hex()``).  This module passes it
through unchanged and performs no encoding of its own.
"""

from __future__ import annotations

import logging

from .log_emitter import emit_log

logger = logging.getLogger(__name__)


def emit_state_log(
    run_id: str,
    pipeline_id: str,
    stream_id: str,
    cursor_hex: str,
    cursor_value: str,
) -> None:
    """Emit a checkpoint record to stdout.

    ``cursor_hex`` is the hex-encoded JSON cursor dict (produced by the
    caller).  ``cursor_value`` is the human-readable high-water mark (e.g.
    an ISO timestamp).  Both are emitted for observability; only
    ``cursor_hex`` is machine-parseable.
    """
    if not run_id:
        logger.warning(
            "emit_state_log called with empty run_id for stream %s pipeline %s; "
            "checkpoint will be unqueriable by run",
            stream_id,
            pipeline_id,
        )
    emit_log(
        "state",
        {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "stream_id": stream_id,
            "cursor_hex": cursor_hex,
            "cursor_value": cursor_value,
        },
    )
