"""Source-side ADBC-only reader.

Used when the source connector is materialized with ``transport_type:
"adbc"`` (Snowflake / BigQuery, or Postgres-over-ADBC). There is no
SQLAlchemy engine, so reads run directly against an ADBC connection
obtained via :func:`ConnectionRuntime.open_adbc_connection`.

SQL is compiled by the shared :class:`~src.shared.query_builder.QueryBuilder`
in qmark mode (see ``src/source/connectors/database.py``); the reader is
pure execution -- it takes a ``(sql, params)`` pair and returns Arrow
batches. Centralizing rendering in QueryBuilder gives the ADBC path the
same operator coverage (filters) and identifier quoting as the
SQLAlchemy transport.

ADBC's DBAPI is synchronous, so each operation runs on a worker thread
via ``asyncio.to_thread``. Connection lifetime is bound to the
:func:`open_adbc_reader` async context manager so a stream's pages
share a single ADBC connection.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, List, Sequence

import pyarrow as pa

logger = logging.getLogger(__name__)


class AdbcReaderClosedError(RuntimeError):
    """Fetch attempted on an already-closed :class:`AdbcReader`.

    A usage-protocol failure (caller held the reader past its context
    manager scope), not a transient DB error -- retries cannot recover
    from this. Subclasses ``RuntimeError`` so existing catch-alls keep
    working; the specific class lets callers distinguish closed-reader
    bugs from "the database is unreachable" failures.
    """


def _close_quietly(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        # Connection-close failures here are server-side resource
        # leaks (warehouse session, libpq fd, gRPC context) operators
        # may need to act on. WARNING mirrors the destination
        # handler's _poison_adbc_connection treatment of the same
        # condition. The caller's original exception (if any) is not
        # masked -- this is a separate log line.
        logger.warning(
            "ADBC source connection close failed -- potential server-side "
            "resource leak",
            exc_info=True,
        )


class AdbcReader:
    """Source-side reader for ADBC-only mode.

    Owns one DBAPI connection for the lifetime of a stream's read. Each
    page runs ``cursor.execute -> fetch_arrow_table`` on a worker thread
    so the engine's event loop is not blocked. Connection is closed on
    ``close()`` (or on context-manager exit via ``open_adbc_reader``).
    """

    def __init__(self, driver: str, conn: Any) -> None:
        self.driver = driver
        self._conn = conn

    async def fetch_page(
        self, sql: str, params: Sequence[Any] = ()
    ) -> List[pa.RecordBatch]:
        """Run one compiled SELECT and return its Arrow batches.

        ``sql`` carries qmark (``?``) placeholders and ``params`` the
        matching positional values (both produced by ``QueryBuilder`` in
        ADBC mode). An empty ``params`` runs the statement with no binds.
        """
        return await asyncio.to_thread(self._fetch_page_sync, sql, params)

    def _fetch_page_sync(
        self, sql: str, params: Sequence[Any]
    ) -> List[pa.RecordBatch]:
        if self._conn is None:
            raise AdbcReaderClosedError(
                "AdbcReader.fetch_page() called after close(); "
                "reopen via open_adbc_reader()"
            )
        cursor = self._conn.cursor()
        try:
            if params:
                cursor.execute(sql, list(params))
            else:
                cursor.execute(sql)
            # Some ADBC drivers expose ``fetch_record_batch`` (a streaming
            # iterator). Materialising the whole result via
            # ``fetch_arrow_table`` is simpler; per-page paging keeps
            # memory bounded.
            table = cursor.fetch_arrow_table()
        finally:
            try:
                cursor.close()
            except Exception:
                logger.debug("ADBC cursor close failed", exc_info=True)
        if table.num_rows == 0:
            return []
        return table.to_batches()

    async def close(self) -> None:
        conn = self._conn
        if conn is None:
            return
        self._conn = None
        await asyncio.to_thread(_close_quietly, conn)


@asynccontextmanager
async def open_adbc_reader(driver: str, runtime: Any) -> AsyncIterator[AdbcReader]:
    """Open an ADBC-only reader from a ``ConnectionRuntime``.

    ``runtime`` must be materialised with an ADBC transport; the caller
    is responsible for that (the source connector branches on
    ``runtime.is_adbc``). Connection is closed on context-manager exit
    regardless of how the body terminated.
    """
    conn = await asyncio.to_thread(runtime.open_adbc_connection)
    reader = AdbcReader(driver, conn)
    try:
        yield reader
    finally:
        await reader.close()
