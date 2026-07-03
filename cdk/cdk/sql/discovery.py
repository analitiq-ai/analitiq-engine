"""Control-plane discovery: list schemas / tables / columns.

These implement the ``Discoverable`` contract (``cdk.contract``) as plain
functions over a materialized ``ConnectionRuntime``. They run the dialect's
``INFORMATION_SCHEMA`` queries (``cdk.sql.dialects``) through the
transport-agnostic executor (``cdk.sql.execution``) and map each native column
type to its canonical Arrow string via the connector's **read** type-map — so a
``ColumnDef`` carries the canonical type ``create_table`` consumes downstream
(ADR §6, CONFIRM-1), not the raw native string.

No gRPC server, engine state, or orchestration is involved: the control-plane
calls these directly.
"""

from __future__ import annotations

from typing import Any

from ..contract import ColumnDef
from ..type_map.exceptions import UnmappedTypeError
from .dialects import SqlDialect
from .exceptions import DiscoveryError
from .execution import Row, fetch_rows

_TRUE_STRINGS = frozenset({"YES", "TRUE", "1", "T", "Y"})


def _col(row: Row, name: str) -> Any:
    """Read *name* from a result row, tolerating dialect column-name casing.

    Snowflake/BigQuery fold unquoted output column names to upper case, so a
    ``SELECT schema_name`` comes back keyed ``SCHEMA_NAME``; postgres keeps it
    lower. Look up exact first, then case-insensitively (both sides folded, so
    the lookup does not silently assume *name* is already lowercase).
    """
    if name in row:
        return row[name]
    target = name.lower()
    for key, value in row.items():
        if key.lower() == target:
            return value
    raise DiscoveryError(
        f"expected column {name!r} not in result row (keys: {sorted(row)})"
    )


async def list_schemas(runtime: Any, *, dialect: SqlDialect) -> list[str]:
    """List the non-system schemas visible to *runtime*.

    *dialect* is the connector's dialect strategy — supplied by the
    connector class (``GenericSQLConnector`` subclasses carry it), since
    per-system dialects live in the connector packages.
    """
    sql, params = dialect.schemas_query()
    rows = await fetch_rows(runtime, sql, params)
    return [_col(row, "schema_name") for row in rows]


async def list_tables(runtime: Any, schema: str, *, dialect: SqlDialect) -> list[str]:
    """List the tables (and views) in *schema*."""
    sql, params = dialect.tables_query(schema)
    rows = await fetch_rows(runtime, sql, params)
    return [_col(row, "table_name") for row in rows]


async def list_columns(
    runtime: Any, schema: str, table: str, *, dialect: SqlDialect
) -> tuple[list[ColumnDef], list[str]]:
    """Describe *table*: its columns (canonical types) and its primary keys.

    Returns ``(columns, primary_keys)``. Each column's native type is mapped to
    its canonical Arrow string through the connector's read type-map; an
    unmapped native type raises :class:`DiscoveryError` naming the offending
    column (the underlying ``UnmappedTypeError`` is chained).
    """
    type_mapper = runtime.connector_type_mapper

    pk_sql, pk_params = dialect.primary_keys_query(schema, table)
    pk_rows = await fetch_rows(runtime, pk_sql, pk_params)
    primary_keys = [_col(row, "column_name") for row in pk_rows]
    pk_set = set(primary_keys)

    col_sql, col_params = dialect.columns_query(schema, table)
    col_rows = await fetch_rows(runtime, col_sql, col_params)

    columns: list[ColumnDef] = []
    for row in col_rows:
        name = _col(row, "column_name")
        native = _col(row, "data_type")
        try:
            canonical = type_mapper.to_arrow_type(native)
        except UnmappedTypeError as err:
            raise DiscoveryError(
                f"{schema}.{table}.{name}: native type {native!r} has no "
                f"type-map rule"
            ) from err
        nullable = str(_col(row, "is_nullable")).strip().upper() in _TRUE_STRINGS
        columns.append(
            ColumnDef(
                name=name,
                canonical_type=canonical,
                nullable=nullable,
                primary_key=name in pk_set,
            )
        )
    return columns, primary_keys
