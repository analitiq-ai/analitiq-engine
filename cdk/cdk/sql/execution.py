"""Transport-agnostic SQL execution for the control-plane helpers.

Discovery and ``create_table`` both need to run SQL against a materialized
:class:`~cdk.connection_runtime.ConnectionRuntime` without caring whether the
transport is SQLAlchemy or ADBC. This module is that single seam:

* ``fetch_rows`` runs a ``SELECT`` and returns plain ``list[dict]`` rows.
* ``execute_ddl`` runs one or more DDL statements.

Queries are authored once with ``?`` (qmark) placeholders. On the ADBC path the
DBAPI binds qmark positionally as-is; on the SQLAlchemy path the placeholders are
rewritten to named binds so SQLAlchemy compiles them to whatever paramstyle the
async driver wants (asyncpg ``$1``, aiomysql ``%s``, …). The ADBC DBAPI is
synchronous, so its calls are off-loaded with ``asyncio.to_thread`` — matching
how the streaming ADBC reader/handler already drive their cursors.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Sequence, Tuple, Union

from sqlalchemy import text

from ..database_utils import acquire_connection
from .exceptions import DiscoveryError

Row = Dict[str, Any]


def _qmark_to_named(sql: str, params: Sequence[Any]) -> Tuple[str, Dict[str, Any]]:
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
    rendered: List[str] = []
    binds: Dict[str, Any] = {}
    for index, segment in enumerate(segments[:-1]):
        key = f"_p{index}"
        rendered.append(segment)
        rendered.append(f":{key}")
        binds[key] = params[index]
    rendered.append(segments[-1])
    return "".join(rendered), binds


async def fetch_rows(runtime: Any, sql: str, params: Sequence[Any]) -> List[Row]:
    """Run a ``SELECT`` over either transport and return its rows as dicts."""
    if runtime.is_adbc:
        return await asyncio.to_thread(_fetch_rows_adbc_sync, runtime, sql, params)
    return await _fetch_rows_sqlalchemy(runtime, sql, params)


async def _fetch_rows_sqlalchemy(
    runtime: Any, sql: str, params: Sequence[Any]
) -> List[Row]:
    text_sql, binds = _qmark_to_named(sql, params)
    async with acquire_connection(runtime.engine) as conn:
        result = await conn.execute(text(text_sql), binds)
        return [dict(row) for row in result.mappings().all()]


def _fetch_rows_adbc_sync(runtime: Any, sql: str, params: Sequence[Any]) -> List[Row]:
    conn = runtime.open_adbc_connection()
    try:
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(sql, list(params))
            else:
                cursor.execute(sql)
            table = cursor.fetch_arrow_table()
        finally:
            cursor.close()
        return table.to_pylist()
    finally:
        conn.close()


async def execute_ddl(runtime: Any, statements: Union[str, Sequence[str]]) -> None:
    """Run one DDL string or a sequence of them, committed together."""
    stmts = [statements] if isinstance(statements, str) else list(statements)
    if not stmts:
        return
    if runtime.is_adbc:
        await asyncio.to_thread(_execute_ddl_adbc_sync, runtime, stmts)
        return
    async with runtime.engine.begin() as conn:
        for ddl in stmts:
            await conn.exec_driver_sql(ddl)


def _execute_ddl_adbc_sync(runtime: Any, statements: Sequence[str]) -> None:
    conn = runtime.open_adbc_connection()
    try:
        cursor = conn.cursor()
        try:
            for ddl in statements:
                cursor.execute(ddl)
        finally:
            cursor.close()
        conn.commit()
    finally:
        conn.close()
