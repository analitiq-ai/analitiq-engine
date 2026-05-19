"""Structured stdout state log emitter.

Emits ``ANALITIQ_STATE::{json}`` lines to stdout so checkpoint state can be
captured by log aggregators for cross-run observability.  The cursor payload
is hex-encoded to keep the line safe for line-oriented log shippers.
"""

from __future__ import annotations

from .log_emitter import emit_log


def emit_state_log(
    run_id: str,
    pipeline_id: str,
    stream_id: str,
    cursor_hex: str,
    cursor_value: str,
) -> None:
    """Emit a checkpoint record to stdout."""
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
