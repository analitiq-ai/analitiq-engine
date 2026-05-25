"""Shared ADBC fast-path infrastructure.

Used by both the source-side `DatabaseConnector` and the destination-side
`DatabaseDestinationHandler` to decide when an ADBC (Arrow Database
Connectivity) driver is available for a given SQLAlchemy dialect. ADBC
drivers exchange Arrow record batches directly with the database's
binary bulk-load protocol (libpq COPY BINARY, BigQuery Storage Write,
Snowflake internal stages, etc.), skipping the row-by-row coercion that
SQLAlchemy's parameter binding does.

Activation gate: the ``ADBC_FAST_PATH`` env var must be truthy.

Dispatch: ``_ADBC_MODULES`` maps the base SQLAlchemy dialect string
(e.g. ``postgresql``, not ``postgresql+asyncpg``) to the dotted module
path of the matching ADBC dbapi driver. Adding a database means adding
one row here plus the matching ``adbc-driver-*`` package on the install
path -- nothing else in the engine changes.

URI shape varies per driver:

* Postgres / Redshift  -- libpq URI built from SQLAlchemy URL parts.
* SQLite / DuckDB      -- file path (or ``:memory:``).
* Snowflake / BigQuery -- no URI builder yet; their drivers need
  per-connector auth wiring (service-account JSON, OAuth, etc.) that
  doesn't fit a URL string. They are listed in ``_ADBC_MODULES`` so the
  import check passes, but ``build_adbc_uri`` returns ``None`` for them
  until a per-driver builder lands.
"""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Callable, Dict, Optional
from urllib.parse import quote, urlencode

logger = logging.getLogger(__name__)


_ADBC_MODULES: Dict[str, str] = {
    "postgresql": "adbc_driver_postgresql.dbapi",
    "postgres": "adbc_driver_postgresql.dbapi",
    "redshift": "adbc_driver_postgresql.dbapi",
    "sqlite": "adbc_driver_sqlite.dbapi",
    "duckdb": "adbc_driver_duckdb.dbapi",
    "snowflake": "adbc_driver_snowflake.dbapi",
    "bigquery": "adbc_driver_bigquery.dbapi",
}


_ADBC_ENV_VAR = "ADBC_FAST_PATH"


# Distinct from None so callers can ``is``-compare to tell "never tried"
# from "tried and failed" even if a real module overrides ``__bool__``.
_ADBC_IMPORT_FAILED: Any = object()


class AdbcConfigurationError(RuntimeError):
    """Deterministic ADBC-side misconfiguration.

    Raised for conditions a retry cannot recover from: missing driver
    package, unsupported dialect, unbuildable URI. Callers classify
    this as a fatal failure so the engine stops re-attempting.
    """


def adbc_flag_enabled() -> bool:
    value = os.environ.get(_ADBC_ENV_VAR, "").strip().lower()
    return value in ("1", "true", "yes", "on")


def load_adbc_module(dialect: str) -> Any:
    """Import the ADBC dbapi module for ``dialect``.

    Returns the module on success, or ``_ADBC_IMPORT_FAILED`` when the
    dialect has no registered driver or the package is not importable.
    Callers should cache the result per instance -- this function does
    not memoize.
    """
    if not dialect:
        return _ADBC_IMPORT_FAILED
    module_path = _ADBC_MODULES.get(dialect.lower())
    if not module_path:
        return _ADBC_IMPORT_FAILED
    try:
        return importlib.import_module(module_path)
    except ImportError as exc:
        log_fn = logger.warning if adbc_flag_enabled() else logger.debug
        log_fn(
            "ADBC fast path disabled for dialect=%s: %s not importable (%s). "
            "Install the matching `adbc-driver-*` package to enable.",
            dialect,
            module_path,
            exc,
        )
        return _ADBC_IMPORT_FAILED


def _url_backend(url: Any) -> str:
    """Return the SQLAlchemy URL's backend in lowercase (or ``""``)."""
    getter = getattr(url, "get_backend_name", None)
    if not callable(getter):
        return ""
    return (getter() or "").lower()


def _build_pg_uri(url: Any) -> Optional[str]:
    """Render a libpq URI from a SQLAlchemy URL.

    Reassembled from parts so credentials are percent-encoded and any
    non-empty ``url.query`` (e.g. ``sslmode=require``) is forwarded to
    the ADBC driver. Returns ``None`` when the URL backend isn't in the
    PG-wire family or when host/database are missing.
    """
    backend = _url_backend(url)
    if backend and backend not in {"postgresql", "postgres", "redshift"}:
        return None
    if not url.host or not url.database:
        return None
    userinfo = ""
    if url.username:
        userinfo = quote(url.username, safe="")
        if url.password:
            userinfo += ":" + quote(url.password, safe="")
        userinfo += "@"
    port = f":{url.port}" if url.port else ""
    query = getattr(url, "query", None) or {}
    query_string = ""
    if query:
        query_string = "?" + urlencode(list(query.items()), doseq=True)
    return f"postgresql://{userinfo}{url.host}{port}/{url.database}{query_string}"


def _build_file_uri(url: Any) -> Optional[str]:
    """Render the connect argument for in-process file-based ADBC drivers.

    SQLite and DuckDB take a path (or ``:memory:``). SQLAlchemy stores
    that path in ``url.database``; an empty database is the in-memory
    convention. Returns ``None`` when the URL backend isn't sqlite/duckdb.
    """
    backend = _url_backend(url)
    if backend and backend not in {"sqlite", "duckdb"}:
        return None
    return url.database or ":memory:"


_URI_BUILDERS: Dict[str, Callable[[Any], Optional[str]]] = {
    "postgresql": _build_pg_uri,
    "postgres": _build_pg_uri,
    "redshift": _build_pg_uri,
    "sqlite": _build_file_uri,
    "duckdb": _build_file_uri,
}


def build_adbc_uri(dialect: str, engine: Any) -> Optional[str]:
    """Render the per-dialect ADBC ``connect()`` argument from an engine.

    Returns ``None`` when no URI builder is registered for the dialect
    or the engine URL is missing required parts. ``None`` is a
    deliberate signal that the fast path is unavailable -- callers must
    demote to the SQLAlchemy path.
    """
    if not dialect or engine is None:
        return None
    builder = _URI_BUILDERS.get(dialect.lower())
    if builder is None:
        return None
    return builder(engine.url)


def adbc_uri_supported(dialect: str) -> bool:
    """Whether a URI builder is registered for ``dialect``.

    Distinct from ``dialect in _ADBC_MODULES``: Snowflake / BigQuery are
    in the module map (so the driver imports succeed) but have no URI
    builder yet, so the fast path stays demoted for them.
    """
    return bool(dialect) and dialect.lower() in _URI_BUILDERS
