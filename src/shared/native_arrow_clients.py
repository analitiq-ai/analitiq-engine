"""Native-Arrow client registry for databases without ADBC support.

A small number of databases expose Arrow-native APIs that don't go
through the ADBC standard — notably ClickHouse (`clickhouse-connect`
ships ``query_arrow_stream`` / ``insert_arrow``) and Arrow Flight SQL
targets like Dremio. The engine treats these as a third dispatch arm,
parallel to ADBC and SQLAlchemy.

Dispatch precedence (when each is registered for the dialect):

    native_arrow  >  ADBC  >  SQLAlchemy

This module owns only the registry + lazy import surface. The actual
read / write adapters live next to each integration so per-client
quirks stay co-located with the driver. The same ``ADBC_FAST_PATH``
flag gates this arm — flipping it on opts into both fast paths, which
matches their shared goal of skipping the row-by-row materialization
that SQLAlchemy's parameter binding does.

Empty by default: a database becomes eligible only after both an entry
is added to ``_NATIVE_ARROW_MODULES`` *and* a transport builder is
registered in :mod:`src.shared.transport_factory` that materializes
its client.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, Dict

from src.shared.adbc_registry import adbc_flag_enabled

logger = logging.getLogger(__name__)


# Dialect (matches the base SQLAlchemy dialect string) -> dotted module
# path of the native-Arrow client package. Lazily imported so missing
# packages disable the fast path silently for that dialect.
_NATIVE_ARROW_MODULES: Dict[str, str] = {
    "clickhouse": "clickhouse_connect",
}


# Reusing the ADBC env var: there is no scenario where a user wants the
# ADBC fast path on but the native-Arrow fast path off (or vice versa).
# A separate flag would just be one more knob for the same intent.
def native_arrow_flag_enabled() -> bool:
    return adbc_flag_enabled()


_NATIVE_ARROW_IMPORT_FAILED: Any = object()


def load_native_arrow_module(dialect: str) -> Any:
    """Import the native-Arrow client module for ``dialect``.

    Returns the module on success, or ``_NATIVE_ARROW_IMPORT_FAILED``
    when no client is registered or the package is missing. Callers
    should cache the result per instance.
    """
    if not dialect:
        return _NATIVE_ARROW_IMPORT_FAILED
    module_path = _NATIVE_ARROW_MODULES.get(dialect.lower())
    if not module_path:
        return _NATIVE_ARROW_IMPORT_FAILED
    try:
        return importlib.import_module(module_path)
    except ImportError as exc:
        log_fn = logger.warning if native_arrow_flag_enabled() else logger.debug
        log_fn(
            "Native-Arrow fast path disabled for dialect=%s: %s not "
            "importable (%s). Install the matching extra to enable.",
            dialect,
            module_path,
            exc,
        )
        return _NATIVE_ARROW_IMPORT_FAILED


def native_arrow_supported(dialect: str) -> bool:
    """Whether a native-Arrow client is registered for ``dialect``.

    Does not check whether the package is importable. Combine with
    :func:`load_native_arrow_module` for the full eligibility test.
    """
    return bool(dialect) and dialect.lower() in _NATIVE_ARROW_MODULES
