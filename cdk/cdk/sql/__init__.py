"""CDK SQL control-plane helpers: discovery + standalone ``create_table``.

Vendor-neutral building blocks for the ``Discoverable`` / ``TableCreator``
contracts (``cdk.contract``), driven directly by the control-plane (no gRPC
server, no engine orchestration). The generic SQL base (ADR §8) will implement
those Protocol methods by delegating to these functions.

Public surface:

* ``list_schemas`` / ``list_tables`` / ``list_columns`` — INFORMATION_SCHEMA
  discovery, native types canonicalized via the connector read type-map.
* ``create_table`` / ``build_create_table_sql`` — DDL from ``ColumnDef``s via
  the write type-map + dialect strategy.
* ``get_dialect`` / ``SqlDialect`` / ``SUPPORTED_DIALECTS`` — the dialect layer.
* error types: ``SqlIntrospectionError`` and its subclasses.
"""

from __future__ import annotations

from .ddl import build_create_table_sql, create_table
from .dialects import SUPPORTED_DIALECTS, SqlDialect, get_dialect
from .discovery import list_columns, list_schemas, list_tables
from .exceptions import (
    CreateTableError,
    DiscoveryError,
    SqlIntrospectionError,
    UnsupportedDialectError,
)
from .execution import execute_ddl, fetch_rows

__all__ = [
    "list_schemas",
    "list_tables",
    "list_columns",
    "create_table",
    "build_create_table_sql",
    "get_dialect",
    "SqlDialect",
    "SUPPORTED_DIALECTS",
    "fetch_rows",
    "execute_ddl",
    "SqlIntrospectionError",
    "UnsupportedDialectError",
    "DiscoveryError",
    "CreateTableError",
]
