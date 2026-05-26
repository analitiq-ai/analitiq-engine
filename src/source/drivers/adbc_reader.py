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
from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterable, List, Optional, Tuple

import pyarrow as pa


logger = logging.getLogger(__name__)


@dataclass
class AdbcReadPlan:
    """Inputs needed to issue a single SELECT against an ADBC source."""

    schema_name: str
    table_name: str
    columns: List[str]
    cursor_field: Optional[str] = None
    cursor_value: Any = None
    cursor_mode: str = "inclusive"
    extra_where: Optional[str] = None
    extra_params: Tuple[Any, ...] = ()


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_qualified(schema: str, name: str) -> str:
    if schema:
        return f"{_quote_ident(schema)}.{_quote_ident(name)}"
    return _quote_ident(name)


def _build_select_sql(
    plan: AdbcReadPlan, *, batch_size: int, offset: int
) -> Tuple[str, Tuple[Any, ...]]:
    """Return ``(sql, params)`` for a single paged SELECT.

    Column quoting and the cursor predicate are the only places where
    we generate identifiers; user-supplied values always flow through
    positional ``?`` placeholders.
    """
    if plan.columns:
        col_list = ", ".join(_quote_ident(c) for c in plan.columns)
    else:
        col_list = "*"
    qualified = _quote_qualified(plan.schema_name, plan.table_name)
    where_parts: List[str] = []
    params: List[Any] = []
    if plan.extra_where:
        where_parts.append(plan.extra_where)
        params.extend(plan.extra_params)
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
    cursor but leaves the connection alone; the caller is responsible
    for poisoning + re-opening on connection-level failures.
    """

    def __init__(self, connection: Any) -> None:
        if connection is None:
            raise ValueError("AdbcReader requires an open ADBC connection")
        self._connection = connection

    async def read_batches(
        self, plan: AdbcReadPlan, *, batch_size: int
    ) -> AsyncIterator[pa.RecordBatch]:
        """Yield Arrow record batches for *plan*, paginated by ``batch_size``.

        Each page issues a fresh ``cursor.execute`` so the offset
        advances on the server. ``cursor.fetch_record_batch()`` returns
        a ``RecordBatchReader`` for the current page; iteration over
        that reader runs on a worker thread.
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
        across an `await` would risk fetching after the cursor closes.
        """
        cursor = self._connection.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            reader = cursor.fetch_record_batch()
            return list(_iter_record_batches(reader))
        finally:
            cursor.close()


def _iter_record_batches(reader: Any) -> Iterable[pa.RecordBatch]:
    """Iterate Arrow batches from an ADBC ``RecordBatchReader``."""
    while True:
        try:
            yield reader.read_next_batch()
        except StopIteration:
            return
