"""Low-level ADBC helpers shared across the sql sub-package.

Extracted here to avoid duplicating the ``params``-forwarding guard across
every call-site (``adbc_reader``, ``execution``). This module carries no
``pyarrow`` import; that property is incidental but convenient — it lets
``execution.py`` (on the eager ``cdk.sql`` import path) pull the helper
without triggering pyarrow's load.
"""

from __future__ import annotations

from typing import Any, Sequence


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
