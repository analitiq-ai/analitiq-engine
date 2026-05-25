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
from typing import Any, AsyncIterator, List, Optional, Sequence

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


def source_adbc_eligible(dialect: str, engine: Optional[AsyncEngine]) -> bool:
    """Return ``True`` when the source-side ADBC fast path is available.

    Eligibility requires the env-var flag, a registered URI builder for
    the dialect, an importable driver package, and an engine URL that
    actually renders to a usable ADBC connect string.
    """
    if not adbc_flag_enabled():
        return False
    if not dialect or engine is None:
        return False
    if not adbc_uri_supported(dialect):
        return False
    if load_adbc_module(dialect) is _ADBC_IMPORT_FAILED:
        return False
    if build_adbc_uri(dialect, engine) is None:
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
        params: Optional[Sequence[Any]] = None,
    ) -> List[pa.RecordBatch]:
        """Execute ``sql`` and return its result as Arrow record batches.

        Empty results return ``[]`` so the caller's loop terminates
        without special-casing ``None``.
        """
        return await asyncio.to_thread(self._fetch_sync, sql, params)

    def _fetch_sync(
        self, sql: str, params: Optional[Sequence[Any]]
    ) -> List[pa.RecordBatch]:
        cursor = self._conn.cursor()
        try:
            if params:
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
    dialect: str, engine: AsyncEngine
) -> AsyncIterator[_AdbcSession]:
    """Open an ADBC connection for the lifetime of the context manager.

    Raises :class:`AdbcConfigurationError` when the driver module or URI
    cannot be produced -- both are deterministic misconfigurations the
    caller should classify as fatal rather than retry.
    """
    module = load_adbc_module(dialect)
    if module is _ADBC_IMPORT_FAILED:
        raise AdbcConfigurationError(
            f"ADBC module for dialect={dialect!r} not available"
        )
    uri = build_adbc_uri(dialect, engine)
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
