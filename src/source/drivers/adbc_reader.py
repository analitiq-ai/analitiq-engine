"""Source-side ADBC fast path.

The SQLAlchemy source path goes:

    rows = exec_driver_sql(...)
    dicts = [dict(r._mapping) for r in rows]
    batch = schema_contract.from_pylist(dicts)

The ADBC path skips the intermediate Python-row materialization:

    table = cursor.execute(...).fetch_arrow_table()
    batch = schema_contract.cast_arrow_batch(table_batch)

ADBC's DBAPI is synchronous, so each operation runs on a worker thread
via ``asyncio.to_thread``. Connection lifetime is bound to the
:func:`open_session` async context manager so a stream's pages share a
single ADBC connection.

Activation: ``ADBC_FAST_PATH=1`` env var (same gate as the destination
side) and a dialect whose URI builder is registered in
:mod:`src.shared.adbc_registry`.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, List, Optional

import pyarrow as pa
from sqlalchemy.ext.asyncio import AsyncEngine

from src.shared.adbc_registry import (
    _ADBC_IMPORT_FAILED,
    AdbcConfigurationError,
    adbc_flag_enabled,
    adbc_uri_supported,
    build_adbc_uri,
    load_adbc_module,
)

logger = logging.getLogger(__name__)


_demotion_logged: set = set()


def _note_demotion(dialect: str, reason: str) -> None:
    """Log once per ``(dialect, reason)`` why the source-side fast path
    was skipped. Silent when the flag is off — demotion is the default
    then, only worth surfacing when the user actually opted in. Matches
    the destination handler's ``_note_adbc_demotion`` shape so operators
    see symmetric signals on both sides of the pipeline.
    """
    if not adbc_flag_enabled():
        return
    key = (dialect, reason)
    if key in _demotion_logged:
        return
    _demotion_logged.add(key)
    logger.info(
        "ADBC source fast path disabled (dialect=%s): %s",
        dialect or "<empty>",
        reason,
    )


def source_adbc_eligible(
    dialect: str,
    engine: Optional[AsyncEngine],
    *,
    tls_mode: Optional[str] = None,
    tls_ca_bundle_present: bool = False,
) -> bool:
    """Return ``True`` when the source-side ADBC fast path is available.

    Eligibility requires the env-var flag, a registered URI builder
    for the dialect, an importable driver package, and an engine URL
    that actually renders to a usable ADBC connect string -- including
    a TLS configuration the URI can carry. CA-bundle-backed modes
    (verify-ca / verify-full) demote here so the SQLAlchemy path can
    use its materialized SSLContext.
    """
    if not adbc_flag_enabled():
        return False
    if not dialect or engine is None:
        _note_demotion(dialect, "engine not materialized or dialect unknown")
        return False
    if not adbc_uri_supported(dialect):
        _note_demotion(dialect, "no URI builder registered for dialect")
        return False
    if load_adbc_module(dialect) is _ADBC_IMPORT_FAILED:
        _note_demotion(dialect, "ADBC driver package not importable")
        return False
    uri = build_adbc_uri(
        dialect,
        engine,
        tls_mode=tls_mode,
        tls_ca_bundle_present=tls_ca_bundle_present,
    )
    if uri is None:
        if tls_mode and tls_mode.lower() in {"verify-ca", "verify-full"}:
            _note_demotion(
                dialect,
                f"tls.mode={tls_mode!r} requires SQLAlchemy "
                "(ADBC URI cannot carry an inline CA bundle)",
            )
        else:
            _note_demotion(dialect, "engine URL could not be rendered as an ADBC URI")
        return False
    return True


class _AdbcSession:
    """Holds an open ADBC connection. Use via :func:`open_session`."""

    def __init__(self, dialect: str, conn: Any) -> None:
        self.dialect = dialect
        self._conn = conn

    async def fetch_page(
        self,
        sql: str,
        params: Optional[Any] = None,
    ) -> List[pa.RecordBatch]:
        """Execute ``sql`` and return its result as Arrow record batches.

        ``params`` may be a positional sequence (for qmark / numeric
        dialects) or a name->value mapping (for named / pyformat
        dialects); the underlying ADBC cursor accepts both. Empty
        results return ``[]`` so the caller's loop terminates
        without special-casing ``None``.
        """
        return await asyncio.to_thread(self._fetch_sync, sql, params)

    def _fetch_sync(
        self, sql: str, params: Optional[Any]
    ) -> List[pa.RecordBatch]:
        cursor = self._conn.cursor()
        try:
            if isinstance(params, dict):
                # ADBC's DBAPI cursor.execute accepts a parameters
                # mapping for named-paramstyle dialects. Today the
                # registered ADBC URI builders only cover positional
                # dialects (PG / Redshift / SQLite / DuckDB), so
                # ``dict`` here implies a future named dialect got
                # added. Pass it through unchanged.
                cursor.execute(sql, params)
            elif params:
                cursor.execute(sql, list(params))
            else:
                cursor.execute(sql)
            table = cursor.fetch_arrow_table()
        finally:
            cursor.close()
        if table.num_rows == 0:
            return []
        return table.to_batches()


@asynccontextmanager
async def open_session(
    dialect: str,
    engine: AsyncEngine,
    *,
    tls_mode: Optional[str] = None,
    tls_ca_bundle_present: bool = False,
) -> AsyncIterator[_AdbcSession]:
    """Open an ADBC connection for the lifetime of the context manager.

    Raises :class:`AdbcConfigurationError` when the driver module or URI
    cannot be produced -- both are deterministic misconfigurations the
    caller should classify as fatal rather than retry. TLS settings
    flow through to ``build_adbc_uri`` so the ADBC connection matches
    the SQLAlchemy engine's posture.
    """
    module = load_adbc_module(dialect)
    if module is _ADBC_IMPORT_FAILED:
        raise AdbcConfigurationError(
            f"ADBC module for dialect={dialect!r} not available"
        )
    uri = build_adbc_uri(
        dialect,
        engine,
        tls_mode=tls_mode,
        tls_ca_bundle_present=tls_ca_bundle_present,
    )
    if uri is None:
        raise AdbcConfigurationError(
            f"Could not build ADBC URI for dialect={dialect!r}"
        )
    conn = await asyncio.to_thread(module.connect, uri)
    try:
        yield _AdbcSession(dialect, conn)
    finally:
        await asyncio.to_thread(_close_quietly, conn)


def _close_quietly(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        # Best-effort -- the connection is being discarded and the
        # caller already has the original exception (if any). Leave
        # evidence at DEBUG so a libpq-style leak is still traceable.
        logger.debug("ADBC connection close failed", exc_info=True)
