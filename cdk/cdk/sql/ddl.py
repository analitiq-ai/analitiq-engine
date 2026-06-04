"""Standalone ``create_table`` — the ``TableCreator`` contract (ADR §6).

Builds and runs ``CREATE TABLE`` DDL from a list of ``ColumnDef`` (canonical
Arrow types) using the connector's **write** type-map (``to_native_type``,
shipped in Phase 0) to render each native column type, and the dialect strategy
for quoting and the PRIMARY KEY clause. Callable with no gRPC server running:
the control-plane invokes it directly to pre-create a destination table.

Deliberately minimal versus the streaming handler's table creation: it emits
exactly the columns it is given plus the PK clause. It does **not** append the
``_synced_at`` audit column — that is a streaming-write concern, not a
control-plane "create the table I described" one.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

from ..contract import ColumnDef
from ..type_map.exceptions import InvalidTypeMapError, UnmappedTypeError
from .dialects import SqlDialect
from .exceptions import CreateTableError
from .execution import execute_ddl


def build_create_table_sql(
    dialect: SqlDialect,
    type_mapper: Any,
    schema: str,
    table: str,
    columns: Sequence[ColumnDef],
    primary_keys: Sequence[str],
    *,
    if_not_exists: bool = True,
) -> str:
    """Render ``CREATE TABLE`` DDL for *table* (no execution).

    Each column's canonical Arrow type is rendered to native DDL via
    ``type_mapper.to_native_type``. A primary-key column is emitted ``NOT NULL``
    regardless of its declared nullability (matching the streaming handler), and
    the table-level PRIMARY KEY clause is appended when *primary_keys* is
    non-empty.
    """
    if not columns:
        raise CreateTableError(
            f"create_table for {table!r} requires at least one column"
        )

    declared = {col.name for col in columns}
    unknown_pks = [pk for pk in primary_keys if pk not in declared]
    if unknown_pks:
        raise CreateTableError(
            f"create_table for {table!r}: primary key column(s) "
            f"{unknown_pks} are not in the column list"
        )

    pk_set = set(primary_keys)
    column_defs: List[str] = []
    for col in columns:
        try:
            native = dialect.render_column_type(col.canonical_type, type_mapper)
        except (UnmappedTypeError, InvalidTypeMapError) as err:
            raise CreateTableError(
                f"create_table for {table!r}: column {col.name!r} canonical "
                f"type {col.canonical_type!r} has no type-map-write rule"
            ) from err
        parts = [dialect.quote_ident(col.name), native]
        # A PK column is NOT NULL even if the source declared it nullable.
        effective_nullable = col.nullable and col.name not in pk_set
        if not effective_nullable:
            parts.append("NOT NULL")
        # SQL DEFAULT expressions pass through verbatim (server-side
        # defaults like now(); never Python values).
        if col.default:
            parts.append(f"DEFAULT {col.default}")
        column_defs.append(" ".join(parts))

    if primary_keys:
        column_defs.append(dialect.pk_clause(list(primary_keys)))

    qualified = dialect.quote_qualified(schema, table)
    prefix = "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
    return f"{prefix} {qualified} (\n  " + ",\n  ".join(column_defs) + "\n)"


async def create_table(
    runtime: Any,
    schema: str,
    table: str,
    columns: Sequence[ColumnDef],
    primary_keys: Sequence[str],
    *,
    dialect: SqlDialect,
    if_not_exists: bool = True,
    type_mapper: Optional[Any] = None,
) -> None:
    """Build and execute ``CREATE TABLE`` DDL over *runtime*'s transport.

    *dialect* is the connector's dialect strategy (per-system dialects live
    in the connector packages). Uses the connector's write type-map
    (``runtime.connector_type_mapper``) unless an explicit *type_mapper* is
    supplied.
    """
    mapper = type_mapper if type_mapper is not None else runtime.connector_type_mapper
    ddl = build_create_table_sql(
        dialect,
        mapper,
        schema,
        table,
        columns,
        primary_keys,
        if_not_exists=if_not_exists,
    )
    await execute_ddl(runtime, ddl)
