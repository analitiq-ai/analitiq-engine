"""Binding a stream's incremental cursor to the param the endpoint declares.

The document is already validated, so the two cursor-mapping shapes are
told apart by the keys they carry: a window mapping declares
start/end params and operators, a single mapping declares one of each.
Only the single form drives an incremental filter today -- half-binding a
window would send a lower bound with no upper one and read a different
range than the author declared.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from ..exceptions import ReadError

__all__ = ["cursor_param_for", "effective_start"]

#: The keys that make a cursor mapping a window rather than a single bound.
_WINDOW_KEYS = ("start_param", "end_param", "start_operator", "end_operator")


def cursor_param_for(
    replication_block: Mapping[str, Any] | None, cursor_field: str
) -> str | None:
    """Return the param the stream's cursor field binds to, or ``None``.

    ``None`` means the endpoint declares no single-param mapping for this
    cursor field, which the caller reports as running full replication --
    loudly, because an incremental stream silently reading everything is
    the failure mode this answer exists to make visible.
    """
    mappings = (replication_block or {}).get("cursor_mappings") or []
    for mapping in mappings:
        if not isinstance(mapping, Mapping):
            continue
        if any(key in mapping for key in _WINDOW_KEYS):
            continue
        if mapping.get("cursor_field") == cursor_field:
            param = mapping.get("param")
            return str(param) if param else None
    return None


def effective_start(cursor: Any, safety_window_seconds: int) -> str:
    """Move a stored cursor back by the safety window, in its own vocabulary.

    A timestamp cursor moves back by seconds; an integer cursor (a
    monotonic id) moves back by that many ids, floored at zero. A cursor
    that is neither is a value this read cannot bound, so it raises rather
    than sending a filter the provider would read as something else.
    """
    # Imported here rather than at module scope: this is the one call in the
    # package that needs it, and a cursor string is provider data -- swapping
    # to the stdlib parser would narrow what parses and start failing
    # cursors that work today.
    from dateutil.parser import isoparse

    cursor_str = str(cursor)
    try:
        cursor_dt: datetime = isoparse(cursor_str)
    except (ValueError, TypeError):
        try:
            cursor_id = int(cursor_str)
        except ValueError as err:
            raise ReadError(
                f"cursor value {cursor!r} is neither an ISO timestamp nor an "
                f"integer; cannot apply the safety window"
            ) from err
        return str(max(0, cursor_id - safety_window_seconds))
    if cursor_dt.tzinfo is None:
        cursor_dt = cursor_dt.replace(tzinfo=timezone.utc)
    effective_dt = cursor_dt - timedelta(seconds=safety_window_seconds)
    return effective_dt.isoformat().replace("+00:00", "Z")
