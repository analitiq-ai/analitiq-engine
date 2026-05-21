"""State change emission helper.

Thin wrapper around :func:`src.state.log_emitter.emit_log` so the state
manager has a single function to call for every persisted-state change.
The signature accepts arbitrary keyword fields (``run_id``,
``pipeline_id``, ``stream_id``, ``cursor_value``, …) so call sites can
emit structured payloads without coordinating on a single dict shape.
"""

from __future__ import annotations

from typing import Any

from src.state.log_emitter import emit_log


def emit_state_log(**fields: Any) -> None:
    """Emit a state-change observability record.

    All keyword arguments become fields of the emitted record. ``None``
    values are dropped so the on-the-wire payload only contains
    populated fields.
    """
    payload = {k: v for k, v in fields.items() if v is not None}
    emit_log("state", payload)
