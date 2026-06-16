"""Low-level ADBC helpers shared by the streaming reader and the control-plane.

This module has no ``pyarrow`` import so it can be loaded on the thin
control-plane path (``cdk.sql`` without the ``arrow`` extra).
"""

from __future__ import annotations

from typing import Any, Sequence


def _adbc_execute(cursor: Any, sql: str, params: Sequence[Any]) -> None:
    """Execute *sql* on *cursor*, forwarding *params* only when non-empty.

    Passing an empty sequence to some ADBC drivers triggers a bind-count
    mismatch; skipping the argument entirely avoids that quirk.
    """
    if params:
        cursor.execute(sql, list(params))
    else:
        cursor.execute(sql)
