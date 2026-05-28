"""Source-side ADBC-only reader.

Used when the source connector is materialized with ``transport_type:
"adbc"`` (Snowflake / BigQuery, or Postgres-over-ADBC). There is no
SQLAlchemy engine and no QueryBuilder dialect to compile through;
:class:`AdbcReader` reads directly from an ADBC connection obtained via
:func:`ConnectionRuntime.open_adbc_connection` and builds SELECT
statements with a minimal ANSI dialect using positional ``?``
parameters -- the paramstyle every ADBC driver in the registry accepts.

ADBC's DBAPI is synchronous, so each operation runs on a worker thread
via ``asyncio.to_thread``. Connection lifetime is bound to the
:func:`open_adbc_reader` async context manager so a stream's pages
share a single ADBC connection.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, List, Literal, Optional, Tuple

import pyarrow as pa

logger = logging.getLogger(__name__)


class AdbcReaderClosedError(RuntimeError):
    """Fetch attempted on an already-closed :class:`AdbcReader`.

    A usage-protocol failure (caller held the reader past its context
    manager scope), not a transient DB error — retries cannot recover
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
        # masked — this is a separate log line.
        logger.warning(
            "ADBC source connection close failed — potential server-side "
            "resource leak",
            exc_info=True,
        )


def _quote_ident(name: str, driver: str) -> str:
    """Quote a SQL identifier for the active ADBC driver.

    BigQuery GoogleSQL uses backticks (double quotes denote STRING
    literals). Snowflake and Postgres use ANSI double quotes.
    """
    if driver == "bigquery":
        if "`" in name:
            raise ValueError(
                f"BigQuery identifier {name!r} contains a backtick; "
                "BigQuery does not support escaped backticks in names"
            )
        return f"`{name}`"
    return '"' + name.replace('"', '""') + '"'


def _normalize_schema(schema: str, driver: str) -> str:
    """Snowflake-only ``public`` -> ``PUBLIC`` normalization.

    Mirrors :meth:`DatabaseDestinationHandler._normalize_adbc_schema`.
    Snowflake folds unquoted identifiers to upper case; its built-in
    default schema is unquoted ``PUBLIC``. A connector declaring the
    common lowercase ``public`` would otherwise produce a quoted
    ``"public"`` literal that targets a different (usually
    non-existent) schema, so reads from the default schema fail.
    BigQuery and Postgres do not need this — see the destination
    handler's docstring for the per-driver rationale.
    """
    if driver == "snowflake" and schema.lower() == "public":
        return "PUBLIC"
    return schema


def _quote_qualified(schema: Optional[str], name: str, driver: str) -> str:
    if schema:
        return (
            f"{_quote_ident(_normalize_schema(schema, driver), driver)}"
            f".{_quote_ident(name, driver)}"
        )
    return _quote_ident(name, driver)


@dataclass(frozen=True)
class AdbcReadPlan:
    """Inputs to :meth:`AdbcReader.fetch_page` for one page of a read.

    The plan is fully resolved against the stream's endpoint document
    and cursor state by the caller; the reader does not consult the
    schema contract or state manager. ``cursor_mode`` is validated in
    ``__post_init__`` so a typo (``"inclusiv"`` -> defaults to ``>``)
    cannot silently degrade an incremental read into a full re-scan.
    ``columns`` is a tuple so the ``frozen=True`` promise actually
    holds (a List would leave the interior mutable).
    """

    table_name: str
    columns: Tuple[str, ...]
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
        # Coerce list (or any iterable) to tuple so frozen=True actually
        # holds. Without this, ``plan.columns.append(...)`` would slip
        # past the immutability promise.
        if not isinstance(self.columns, tuple):
            object.__setattr__(self, "columns", tuple(self.columns))
        if not self.columns:
            raise ValueError("AdbcReadPlan.columns must not be empty")


def _build_select_sql(
    plan: AdbcReadPlan, driver: str
) -> Tuple[str, Tuple[Any, ...]]:
    """Render ``plan`` as a SELECT with positional ``?`` parameters.

    Every supported ADBC driver (postgresql, snowflake, bigquery)
    accepts qmark paramstyle and ``LIMIT n OFFSET n`` clauses;
    identifier quoting differs per driver (backticks for BigQuery,
    ANSI double quotes for the others) and is handled by
    :func:`_quote_ident`. Stream-level filters are NOT supported here
    — the source connector raises ``ReadError`` if a stream defines
    them. Adding filter support means extending this helper to render
    the operators ``QueryBuilder`` already knows.

    Always emits ``ORDER BY``: when ``cursor_field`` is set we order by
    it (for stable incremental reads); otherwise we order by the first
    selected column. Paging via ``OFFSET`` without a stable order is
    implementation-defined on Postgres / Snowflake / BigQuery (a
    concurrent vacuum, micropartition shuffle, or slot reassignment can
    silently skip or duplicate rows across page boundaries) — falling
    back to the first column gives deterministic paging without
    requiring the caller to declare a cursor.

    **Caveat on the first-column fallback**: if ``columns[0]`` is a
    non-orderable type (Snowflake ``VARIANT`` / ``OBJECT`` / ``ARRAY``,
    BigQuery ``STRUCT`` / ``ARRAY`` / ``JSON`` / ``GEOGRAPHY``, Postgres
    types without an ordering operator), the warehouse will reject the
    query. Callers should set ``cursor_field`` to a scalar primary-key
    column for streams with non-orderable first columns. A WARNING is
    logged once per (table, fallback_column) so the operator sees the
    implicit choice.
    """
    # Non-empty columns is enforced by AdbcReadPlan.__post_init__; the
    # renderer can trust the invariant.
    col_list = ", ".join(_quote_ident(c, driver) for c in plan.columns)
    qualified = _quote_qualified(plan.schema_name, plan.table_name, driver)

    where_parts: List[str] = []
    params: List[Any] = []
    if plan.cursor_field and plan.cursor_value is not None:
        op = ">=" if plan.cursor_mode == "inclusive" else ">"
        where_parts.append(f"{_quote_ident(plan.cursor_field, driver)} {op} ?")
        params.append(plan.cursor_value)

    sql = f"SELECT {col_list} FROM {qualified}"
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)
    if plan.cursor_field:
        order_col = plan.cursor_field
    else:
        order_col = plan.columns[0]
        _note_order_by_fallback(plan.table_name, order_col)
    sql += f" ORDER BY {_quote_ident(order_col, driver)} ASC"
    if plan.limit is not None:
        sql += f" LIMIT {int(plan.limit)}"
    if plan.offset is not None:
        sql += f" OFFSET {int(plan.offset)}"

    return sql, tuple(params)


_order_by_fallback_logged: set = set()


def _note_order_by_fallback(table_name: str, column_name: str) -> None:
    """Log once per (table, column) that ORDER BY fell back to the first column.

    A WARNING (not INFO) because the operator may need to act: a JSON /
    STRUCT / VARIANT first column will fail at query time with an opaque
    "ORDER BY does not support this type" error, and the fix is to set
    ``cursor_field`` on the stream — not something a stack trace points
    at directly.
    """
    key = (table_name, column_name)
    if key in _order_by_fallback_logged:
        return
    _order_by_fallback_logged.add(key)
    logger.warning(
        "ADBC reader: no cursor_field for table %r; defaulting ORDER BY to "
        "first selected column %r. Set cursor_field on the stream if this "
        "column is a non-orderable type (JSON / STRUCT / VARIANT) — the "
        "warehouse will otherwise reject the query.",
        table_name, column_name,
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

    async def fetch_page(self, plan: AdbcReadPlan) -> List[pa.RecordBatch]:
        return await asyncio.to_thread(self._fetch_page_sync, plan)

    def _fetch_page_sync(self, plan: AdbcReadPlan) -> List[pa.RecordBatch]:
        if self._conn is None:
            raise AdbcReaderClosedError(
                "AdbcReader.fetch_page() called after close(); "
                "reopen via open_adbc_reader()"
            )
        sql, params = _build_select_sql(plan, self.driver)
        cursor = self._conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
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
