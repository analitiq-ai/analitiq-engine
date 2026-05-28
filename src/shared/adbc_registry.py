"""Shared ADBC error type.

The ADBC-only transport (``transport_type: "adbc"``) opens its DBAPI
connections through :func:`ConnectionRuntime.open_adbc_connection`; the
source reader (:mod:`src.source.drivers.adbc_reader`) and the
destination handler (:mod:`src.destination.connectors.database`) both
raise :class:`AdbcConfigurationError` for misconfigurations a retry
cannot heal (missing driver, unsupported dialect, bad credentials).
This module exists to hold that shared error type.
"""

from __future__ import annotations


class AdbcConfigurationError(RuntimeError):
    """Deterministic ADBC-side misconfiguration.

    Raised for conditions a retry cannot recover from: missing driver
    package, unsupported dialect, unbuildable connection. Callers
    classify this as a fatal failure so the engine stops re-attempting.
    """
