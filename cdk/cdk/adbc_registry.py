"""Shared ADBC error type.

The ADBC-only transport (``transport_type: "adbc"``) opens its DBAPI
connections through :func:`ConnectionRuntime.open_adbc_connection`. The
generic SQL connector (:mod:`cdk.sql.generic`) raises
:class:`AdbcConfigurationError` for misconfigurations a retry cannot
heal (missing driver, unsupported dialect, bad credentials). It lives
in ``shared`` because the type is part of the engine's fatal-vs-
retryable classification contract, not handler-private.
"""

from __future__ import annotations


class AdbcConfigurationError(RuntimeError):
    """Deterministic ADBC-side misconfiguration.

    Raised for conditions a retry cannot recover from: missing driver
    package, unsupported dialect, unbuildable connection. Callers
    classify this as a fatal failure so the engine stops re-attempting.
    """
