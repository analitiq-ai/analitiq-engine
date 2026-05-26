"""ADBC-backed reader for database sources without an async SQLAlchemy driver.

Snowflake is the first dialect to use this path. The reader consumes the
cached ADBC DBAPI connection that the source connector holds, executes a
parameterized SELECT, and streams Arrow ``RecordBatch`` instances out
via ``cursor.fetch_record_batch()``. All blocking DBAPI calls are
wrapped in ``asyncio.to_thread`` so the engine's event loop stays
responsive.

This module deliberately uses its own minimal query builder rather than
``src.shared.query_builder.QueryBuilder``: that builder targets specific
SQLAlchemy dialects (postgres/mysql) and emits driver-specific
parameter syntax. ADBC uses standard DBAPI positional ``?`` placeholders
which neither of those dialects produce.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, List, Literal, Optional, Protocol, Tuple

import pyarrow as pa


logger = logging.getLogger(__name__)


class _AdbcCursor(Protocol):
    """Subset of DBAPI 2.0 the reader needs from an ADBC cursor."""

    def execute(self, sql: str, params: Tuple[Any, ...] = ...) -> None: ...

    def fetch_record_batch(self) -> Any: ...

    def close(self) -> None: ...


class _AdbcConnection(Protocol):
    """Subset of DBAPI 2.0 the reader needs from an ADBC connection."""

    def cursor(self) -> _AdbcCursor: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class AdbcReadPlan:
    """Inputs needed to issue a single SELECT against an ADBC source.

    ``cursor_field`` and ``cursor_value`` co-vary: the cursor predicate is
    emitted only when both are set. ``cursor_mode`` is constrained to the
    two inclusive/exclusive choices so a typo cannot silently fall back to
    ``>``.
    """

    schema_name: Optional[str]
    table_name: str
    columns: List[str] = field(default_factory=list)
    cursor_field: Optional[str] = None
    cursor_value: Any = None
    cursor_mode: Literal["inclusive", "exclusive"] = "inclusive"

    def __post_init__(self) -> None:
        if self.cursor_mode not in ("inclusive", "exclusive"):
            raise ValueError(
                f"AdbcReadPlan.cursor_mode must be 'inclusive' or "
                f"'exclusive'; got {self.cursor_mode!r}"
            )


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_qualified(schema: Optional[str], name: str) -> str:
    if schema:
        return f"{_quote_ident(schema)}.{_quote_ident(name)}"
    return _quote_ident(name)


def _build_select_sql(
    plan: AdbcReadPlan, *, batch_size: int, offset: int
) -> Tuple[str, Tuple[Any, ...]]:
    """Return ``(sql, params)`` for a single paged SELECT.

    Identifier interpolation is limited to the table reference, the
    column list, and the cursor predicate's field name; user-supplied
    scalar values always flow through positional ``?`` placeholders.
    """
    if plan.columns:
        col_list = ", ".join(_quote_ident(c) for c in plan.columns)
    else:
        col_list = "*"
    qualified = _quote_qualified(plan.schema_name, plan.table_name)
    where_parts: List[str] = []
    params: List[Any] = []
    if plan.cursor_field and plan.cursor_value is not None:
        op = ">=" if plan.cursor_mode == "inclusive" else ">"
        where_parts.append(f"{_quote_ident(plan.cursor_field)} {op} ?")
        params.append(plan.cursor_value)
    where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    order_clause = (
        f" ORDER BY {_quote_ident(plan.cursor_field)} ASC"
        if plan.cursor_field
        else ""
    )
    sql = (
        f"SELECT {col_list} FROM {qualified}{where_clause}{order_clause} "
        f"LIMIT {int(batch_size)} OFFSET {int(offset)}"
    )
    return sql, tuple(params)


class AdbcReader:
    """Stream Arrow record batches from an ADBC DBAPI connection.

    The reader does not own the connection — the caller (source
    connector) keeps the single cached handle for its lifetime and
    passes it in here. On any cursor error, the reader closes the
    cursor and the exception propagates; no connection-level recovery
    happens at this layer.
    """

    def __init__(self, connection: _AdbcConnection) -> None:
        if connection is None:
            raise ValueError("AdbcReader requires an open ADBC connection")
        self._connection = connection

    async def read_batches(
        self, plan: AdbcReadPlan, *, batch_size: int
    ) -> AsyncIterator[pa.RecordBatch]:
        """Yield Arrow record batches for *plan*, paginated by ``batch_size``.

        Each page issues a fresh ``cursor.execute`` with the next
        ``OFFSET`` value, so pagination is repeated independent SELECTs
        rather than a server-side scroll cursor.
        ``cursor.fetch_record_batch()`` returns an Arrow record-batch
        reader for the current page; iteration over that reader runs on
        a worker thread.
        """
        offset = 0
        while True:
            sql, params = _build_select_sql(
                plan, batch_size=batch_size, offset=offset
            )
            page_batches = await asyncio.to_thread(
                self._fetch_page_sync, sql, params
            )
            if not page_batches:
                return
            yielded = 0
            for batch in page_batches:
                yielded += batch.num_rows
                yield batch
            if yielded < batch_size:
                return
            offset += yielded

    def _fetch_page_sync(
        self, sql: str, params: Tuple[Any, ...]
    ) -> List[pa.RecordBatch]:
        """Execute one page and materialize its batches.

        We materialize inside the worker thread (rather than handing
        the live ``RecordBatchReader`` back to the event loop) because
        ADBC readers are bound to the cursor's lifetime — yielding
        across an ``await`` would risk fetching after the cursor closes.
        """
        cursor = self._connection.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            reader = cursor.fetch_record_batch()
            # ``RecordBatchReader`` is iterable per the documented Arrow
            # API; iterating yields batches until exhaustion. Avoids
            # relying on ``read_next_batch`` raising ``StopIteration``
            # which is a less-stable sentinel.
            return list(reader)
        finally:
            try:
                cursor.close()
            except Exception:
                logger.debug("ADBC cursor close failed", exc_info=True)
