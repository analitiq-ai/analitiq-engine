"""Low-level DBAPI helpers shared across the sql sub-package.

Extracted here to avoid duplicating the ``params``-forwarding guard and the
PEP-249 fatal-error classification across their call sites (``adbc_reader``,
``execution``, the facade's ack ladder in ``generic``, and the transport
backends). This module carries no ``pyarrow`` import; that property is
incidental but convenient — it lets ``execution.py`` (on the eager
``cdk.sql`` import path) pull the helpers without triggering pyarrow's load.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from cdk.adbc_registry import AdbcConfigurationError

logger = logging.getLogger(__name__)


# PEP-249 exception class names that indicate the failure cannot heal
# between retries against an identical request: bad SQL, missing
# objects, permission denials, type mismatches, unsupported operations.
# Driver modules re-export these names per PEP-249; we match on the
# class name so the check works without importing the optional driver.
_FATAL_ADBC_ERROR_NAMES = frozenset(
    {
        "ProgrammingError",
        "NotSupportedError",
        "IntegrityError",
        "DataError",
    }
)


def _is_fatal_adbc_error(exc: BaseException) -> bool:
    """Return ``True`` when *exc* is a failure class retries cannot heal.

    Name-based, so it serves both transports: ADBC drivers re-export the
    PEP-249 names per the spec, and SQLAlchemy's wrapper exceptions
    (``sqlalchemy.exc.IntegrityError`` etc.) carry the same class names.
    """
    return any(cls.__name__ in _FATAL_ADBC_ERROR_NAMES for cls in type(exc).__mro__)


def _reclassify_as_fatal(exc: BaseException) -> AdbcConfigurationError:
    """Wrap a fatal PEP-249 exception in ``AdbcConfigurationError``.

    The wrapped message preserves the original class name so operators
    triaging an opaque ``str(exception)`` in the engine's failure
    summary can still distinguish ProgrammingError (syntax / missing
    object / permission denial) from IntegrityError (PK collision)
    from DataError (type / value mismatch).
    """
    inner_name = type(exc).__name__
    wrapped = AdbcConfigurationError(f"{inner_name}: {exc}")
    wrapped.__cause__ = exc
    return wrapped


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
