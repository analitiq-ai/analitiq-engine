"""Low-level ADBC helpers shared across the sql sub-package.

Extracted here to avoid duplicating the ``params``-forwarding guard across
every call-site (``adbc_reader``, ``execution``). This module carries no
``pyarrow`` import; that property is incidental but convenient — it lets
``execution.py`` (on the eager ``cdk.sql`` import path) pull the helper
without triggering pyarrow's load.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

logger = logging.getLogger(__name__)


def _close_cursor_quietly(cursor: Any) -> None:
    """Close *cursor* best-effort, never letting its close mask a live error.

    Cursor close runs in a ``finally`` after ``execute``/``fetch``. If both the
    body and the close raise, the close exception would otherwise replace the
    original — so the caller sees "cursor close failed" instead of the real
    query failure. Swallowing the close keeps the body's exception in flight;
    the swallowed failure is a server-side resource leak (warehouse session,
    libpq fd, gRPC context) an operator may need to act on, so it is logged at
    WARNING rather than dropped. Mirrors :func:`adbc_reader._close_quietly` for
    connections.
    """
    try:
        cursor.close()
    except Exception:
        logger.warning(
            "ADBC cursor close failed -- potential server-side resource leak",
            exc_info=True,
        )


def _adbc_execute(cursor: Any, sql: str, params: Sequence[Any]) -> None:
    """Execute *sql* on *cursor*, forwarding *params* only when non-empty.

    Some ADBC drivers interpret a second argument of ``[]`` as an intent to
    bind zero parameters, then validate that count against the placeholder
    count in the SQL — raising an error even for parameterless statements.
    Omitting the argument entirely bypasses that driver-side check.
    """
    if params:
        cursor.execute(sql, list(params))
    else:
        cursor.execute(sql)
