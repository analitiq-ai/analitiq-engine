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
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, List, Literal, Optional, Tuple

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


# ---------------------------------------------------------------------------
# ADBC-only mode reader
# ---------------------------------------------------------------------------
#
# When the source connector is materialized with ``transport_type:
# "adbc"``, there is no SQLAlchemy engine to back ``open_session`` and
# no QueryBuilder dialect to compile through. ``AdbcReader`` reads
# directly from an ADBC connection obtained via
# ``runtime.open_adbc_connection()`` and builds SELECT statements using
# a minimal ANSI dialect with positional ``?`` parameters — the
# paramstyle every ADBC driver in the registry accepts.


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier for ADBC dialects.

    Snowflake uppercases unquoted identifiers, so we double-quote
    everything to preserve the engine's lower-case convention exactly.
    BigQuery and Postgres also accept double-quoted identifiers.
    """
    return '"' + name.replace('"', '""') + '"'


def _quote_qualified(schema: Optional[str], name: str) -> str:
    if schema:
        return f"{_quote_ident(schema)}.{_quote_ident(name)}"
    return _quote_ident(name)


@dataclass(frozen=True)
class AdbcReadPlan:
    """Inputs to :meth:`AdbcReader.fetch_page` for one page of a read.

    The plan is fully resolved against the stream's endpoint document
    and cursor state by the caller; the reader does not consult the
    schema contract or state manager. ``cursor_mode`` is validated in
    ``__post_init__`` so a typo (``"inclusiv"`` -> defaults to ``>``)
    cannot silently degrade an incremental read into a full re-scan.
    """

    table_name: str
    columns: List[str]
    schema_name: Optional[str] = None
    cursor_field: Optional[str] = None
    cursor_value: Any = None
    cursor_mode: Literal["inclusive", "exclusive"] = "inclusive"
    limit: Optional[int] = None
    offset: Optional[int] = None

    def __post_init__(self) -> None:
        if self.cursor_mode not in ("inclusive", "exclusive"):
            raise ValueError(
                f"AdbcReadPlan.cursor_mode must be 'inclusive' or 'exclusive', "
                f"got {self.cursor_mode!r}"
            )


def _build_select_sql(plan: AdbcReadPlan) -> Tuple[str, Tuple[Any, ...]]:
    """Render ``plan`` as an ANSI SELECT with positional ``?`` parameters.

    Every supported ADBC driver (postgresql, snowflake, bigquery)
    accepts qmark paramstyle, ``LIMIT n OFFSET n`` clauses, and
    standard double-quoted identifiers. Stream-level filters are NOT
    supported here — the source connector raises ``ReadError`` if a
    stream defines them. Adding filter support means extending this
    helper to render the operators ``QueryBuilder`` already knows.
    """
    if not plan.columns:
        raise ValueError("AdbcReadPlan.columns must not be empty")
    col_list = ", ".join(_quote_ident(c) for c in plan.columns)
    qualified = _quote_qualified(plan.schema_name, plan.table_name)

    where_parts: List[str] = []
    params: List[Any] = []
    if plan.cursor_field and plan.cursor_value is not None:
        op = ">=" if plan.cursor_mode == "inclusive" else ">"
        where_parts.append(f"{_quote_ident(plan.cursor_field)} {op} ?")
        params.append(plan.cursor_value)

    sql = f"SELECT {col_list} FROM {qualified}"
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)
    if plan.cursor_field:
        sql += f" ORDER BY {_quote_ident(plan.cursor_field)} ASC"
    if plan.limit is not None:
        sql += f" LIMIT {int(plan.limit)}"
    if plan.offset is not None:
        sql += f" OFFSET {int(plan.offset)}"

    return sql, tuple(params)


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

    async def fetch_page(self, plan: AdbcReadPlan) -> List[pa.RecordBatch]:
        return await asyncio.to_thread(self._fetch_page_sync, plan)

    def _fetch_page_sync(self, plan: AdbcReadPlan) -> List[pa.RecordBatch]:
        sql, params = _build_select_sql(plan)
        cursor = self._conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            # Some ADBC drivers expose ``fetch_record_batch`` (a streaming
            # iterator). Materialising the whole result via
            # ``fetch_arrow_table`` is simpler and matches the existing
            # session helper; per-page paging keeps memory bounded.
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
