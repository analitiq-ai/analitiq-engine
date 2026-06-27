"""Transport-agnostic SQL execution for the control-plane helpers.

Discovery and ``create_table`` both need to run SQL against a materialized
:class:`~cdk.connection_runtime.ConnectionRuntime` without caring whether the
transport is SQLAlchemy or ADBC. This module is that single seam:

* ``fetch_rows`` runs a ``SELECT`` and returns plain ``list[dict]`` rows.
* ``execute_ddl`` runs one or more DDL statements.

Queries are authored once with ``?`` (qmark) placeholders. On the ADBC path the
DBAPI binds qmark positionally as-is; on the SQLAlchemy path the placeholders are
rewritten to named binds so SQLAlchemy compiles them to whatever paramstyle the
driver wants (asyncpg ``$1``, aiomysql ``%s``, …). The ADBC DBAPI and the sync
SQLAlchemy engine (sync-only drivers, e.g. Redshift's ``redshift_connector``)
are synchronous, so their calls are off-loaded with ``asyncio.to_thread`` —
matching how the streaming ADBC reader/handler already drive their cursors.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any

from sqlalchemy import text

from ..database_utils import acquire_connection
from ._adbc_utils import _adbc_execute, _close_cursor_quietly
from .exceptions import CreateTableError, DiscoveryError, SqlIntrospectionError

logger = logging.getLogger(__name__)

Row = dict[str, Any]


def _one_line(sql: str) -> str:
    """First non-blank line of *sql*, trimmed — for error messages."""
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:120]
    return sql[:120]


def _qmark_to_named(sql: str, params: Sequence[Any]) -> tuple[str, dict[str, Any]]:
    """Rewrite qmark placeholders to SQLAlchemy named binds.

    ``"... WHERE a = ? AND b = ?"`` -> ``"... WHERE a = :_p0 AND b = :_p1"`` with
    ``{"_p0": ..., "_p1": ...}``. The CDK's discovery SQL never embeds a literal
    ``?`` (no string literals contain one), so a positional split is safe.
    """
    segments = sql.split("?")
    expected = len(segments) - 1
    if expected != len(params):
        raise DiscoveryError(
            f"placeholder/parameter count mismatch: {expected} placeholders, "
            f"{len(params)} parameters"
        )
    rendered: list[str] = []
    binds: dict[str, Any] = {}
    for index, segment in enumerate(segments[:-1]):
        key = f"_p{index}"
        rendered.append(segment)
        rendered.append(f":{key}")
        binds[key] = params[index]
    rendered.append(segments[-1])
    return "".join(rendered), binds


async def fetch_rows(runtime: Any, sql: str, params: Sequence[Any]) -> list[Row]:
    """Run a ``SELECT`` over either transport and return its rows as dicts.

    A transport/driver failure (bad credentials, missing catalog view, syntax
    error, dropped connection) is re-raised as :class:`DiscoveryError` so the
    control-plane sees one error surface; the original driver exception is
    chained. A :class:`SqlIntrospectionError` (e.g. a placeholder mismatch from
    the bind rewrite) passes through unwrapped.
    """
    try:
        if runtime.is_adbc:
            return await asyncio.to_thread(_fetch_rows_adbc_sync, runtime, sql, params)
        if runtime.is_sync_sqlalchemy:
            return await asyncio.to_thread(
                _fetch_rows_sync_engine, runtime, sql, params
            )
        return await _fetch_rows_sqlalchemy(runtime, sql, params)
    except SqlIntrospectionError:
        raise
    except Exception as err:
        raise DiscoveryError(
            f"discovery query failed [{_one_line(sql)}]: {err}"
        ) from err


async def _fetch_rows_sqlalchemy(
    runtime: Any, sql: str, params: Sequence[Any]
) -> list[Row]:
    text_sql, binds = _qmark_to_named(sql, params)
    async with acquire_connection(runtime.engine) as conn:
        result = await conn.execute(text(text_sql), binds)
        return [dict(row) for row in result.mappings().all()]


def _fetch_rows_sync_engine(runtime: Any, sql: str, params: Sequence[Any]) -> list[Row]:
    text_sql, binds = _qmark_to_named(sql, params)
    with runtime.sync_engine.connect() as conn:
        result = conn.execute(text(text_sql), binds)
        return [dict(row) for row in result.mappings().all()]


def _fetch_rows_adbc_sync(runtime: Any, sql: str, params: Sequence[Any]) -> list[Row]:
    conn = runtime.open_adbc_connection()
    try:
        cursor = conn.cursor()
        try:
            _adbc_execute(cursor, sql, params)
            table = cursor.fetch_arrow_table()
        finally:
            _close_cursor_quietly(cursor)
        rows: list[Row] = table.to_pylist()
        return rows
    finally:
        conn.close()


async def execute_ddl(runtime: Any, statements: str | Sequence[str]) -> None:
    """Run one DDL string or a sequence of them, committed together (atomically).

    The SQLAlchemy path wraps the batch in ``engine.begin()`` (one transaction);
    the ADBC path commits once after the last statement and rolls back on
    failure (see :func:`_execute_ddl_adbc_sync`), so both transports give
    all-or-nothing semantics. A transport/driver failure is re-raised as
    :class:`CreateTableError` with the original chained.
    """
    stmts = [statements] if isinstance(statements, str) else list(statements)
    if not stmts:
        return
    try:
        if runtime.is_adbc:
            await asyncio.to_thread(_execute_ddl_adbc_sync, runtime, stmts)
            return
        if runtime.is_sync_sqlalchemy:
            await asyncio.to_thread(_execute_ddl_sync_engine, runtime, stmts)
            return
        async with runtime.engine.begin() as conn:
            for ddl in stmts:
                await conn.exec_driver_sql(ddl)
    except SqlIntrospectionError:
        raise
    except Exception as err:
        raise CreateTableError(f"DDL execution failed: {err}") from err


def _execute_ddl_sync_engine(runtime: Any, statements: Sequence[str]) -> None:
    """Run the DDL batch on the sync engine in one transaction (worker thread)."""
    with runtime.sync_engine.begin() as conn:
        for ddl in statements:
            conn.exec_driver_sql(ddl)


def _execute_ddl_adbc_sync(runtime: Any, statements: Sequence[str]) -> None:
    conn = runtime.open_adbc_connection()
    try:
        try:
            cursor = conn.cursor()
            try:
                for ddl in statements:
                    cursor.execute(ddl)
            finally:
                _close_cursor_quietly(cursor)
            conn.commit()
        except Exception:
            # Roll back the partially-applied batch so "committed together"
            # holds even on a driver whose default is not autocommit. Best
            # effort: a rollback failure must not mask the original error, but
            # is logged so a connection left mid-transaction is diagnosable.
            try:
                conn.rollback()
            except Exception:
                logger.warning(
                    "ADBC DDL rollback failed after a batch error; the original "
                    "error is preserved",
                    exc_info=True,
                )
            raise
    finally:
        conn.close()
