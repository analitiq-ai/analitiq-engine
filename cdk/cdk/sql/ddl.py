"""Standalone ``create_table`` — the ``TableCreator`` contract (ADR §6).

Builds and runs ``CREATE TABLE`` DDL from a list of ``ColumnDef`` (canonical
Arrow types) using the connection-scoped **write** type-map
(``to_native_type``; connection rules first, connector rules on a miss —
issue #368) to render each native column type, and the dialect strategy for
quoting and the PRIMARY KEY clause. Callable with no gRPC server running: the
control-plane invokes it directly to pre-create a destination table — a table
in the connection's own database, so the connection scope is inherent, exactly
as in ``cdk.sql.discovery``.

Deliberately minimal versus the streaming handler's table creation: it emits
exactly the columns it is given plus the PK clause. It does **not** append the
``_synced_at`` audit column — that is a streaming-write concern, not a
control-plane "create the table I described" one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ..contract import ColumnDef
from ..type_map.exceptions import InvalidTypeMapError, UnmappedTypeError
from ..types import EndpointScope
from .capabilities import bind_dialect_capabilities
from .dialects import SqlDialect, TableAddress
from .exceptions import CatalogAddressingError, CreateTableError
from .execution import execute_ddl


def build_create_table_sql(
    dialect: SqlDialect,
    type_mapper: Any,
    address: TableAddress,
    columns: Sequence[ColumnDef],
    primary_keys: Sequence[str],
    *,
    if_not_exists: bool = True,
) -> str:
    """Render ``CREATE TABLE`` DDL for *address* (no execution).

    Each column's canonical Arrow type is rendered to native DDL via
    ``type_mapper.to_native_type``. A primary-key column is emitted ``NOT NULL``
    regardless of its declared nullability (matching the streaming handler), and
    the table-level PRIMARY KEY clause is appended when *primary_keys* is
    non-empty. *address* is the dialect-built :class:`TableAddress`, so the
    emitted name is quoted up to ``catalog.schema.table``.
    """
    table = address.table
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
    column_defs: list[str] = []
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

    qualified = dialect.quote_table(address)
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
    catalog: str = "",
    if_not_exists: bool = True,
    type_mapper: Any | None = None,
) -> None:
    """Build and execute ``CREATE TABLE`` DDL over *runtime*'s transport.

    *dialect* is the connector's dialect strategy (per-system dialects live
    in the connector packages). Uses the connection-scoped write type-map
    (``runtime.type_mapper_for(scope=CONNECTION)`` — connection write rules
    over the connector's) unless an explicit *type_mapper* is supplied. The
    ``catalog``/``schema``/``table`` intent resolves through
    ``dialect.table_address`` — a catalog the system cannot address fails
    loud there (:class:`~cdk.sql.exceptions.CatalogAddressingError`) before
    any DDL is composed, and DDL against a catalog additionally requires
    the declared ``sql_capabilities.catalog`` to be ``full`` (``read``
    covers discovery and reads only).
    """
    # Standalone entry point: bind the runtime's declared capabilities to
    # the dialect (the same rule the facade applies), so a declaring
    # connector's catalog gate behaves identically however the CDK is
    # driven.
    bind_dialect_capabilities(dialect, runtime)
    mapper = (
        type_mapper
        if type_mapper is not None
        else runtime.type_mapper_for(scope=EndpointScope.CONNECTION)
    )
    address = dialect.table_address(table, schema=schema, catalog=catalog)
    if address.catalog:
        caps = dialect.capabilities
        if caps is not None and caps.catalog != "full":
            raise CatalogAddressingError(
                f"CREATE TABLE targets catalog {address.catalog!r}, but the "
                f"connector declares sql_capabilities.catalog "
                f"{caps.catalog!r} — DDL across catalogs requires 'full'. "
                f"Use a connection whose default catalog is "
                f"{address.catalog!r}."
            )
    ddl = build_create_table_sql(
        dialect,
        mapper,
        address,
        columns,
        primary_keys,
        if_not_exists=if_not_exists,
    )
    await execute_ddl(runtime, ddl)
