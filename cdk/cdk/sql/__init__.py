"""CDK SQL control-plane helpers: discovery + standalone ``create_table``.

Vendor-neutral building blocks for the ``Discoverable`` / ``TableCreator``
contracts (``cdk.contract``), driven directly by the control-plane (no gRPC
server, no engine orchestration). The generic SQL base (ADR §8) will implement
those Protocol methods by delegating to these functions.

Public surface:

* ``list_schemas`` / ``list_tables`` / ``list_columns`` — INFORMATION_SCHEMA
  discovery, native types canonicalized via the connection-scoped read
  type-map (connection rules over connector rules).
* ``create_table`` / ``build_create_table_sql`` — DDL from ``ColumnDef``s via
  the connection-scoped write type-map + dialect strategy.
* ``SqlDialect`` / ``TableAddress`` — the ANSI-neutral dialect base and its
  once-normalized ``catalog.schema.table`` address (per-system dialects live
  in connector packages).
* error types: ``SqlIntrospectionError`` and its subclasses.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .._extras import reraise_for_missing_extra
from .ddl import build_create_table_sql, create_table
from .dialects import SqlDialect, TableAddress
from .discovery import list_columns, list_schemas, list_tables
from .exceptions import (
    CatalogAddressingError,
    CreateTableError,
    DiscoveryError,
    ReadError,
    SchemaConfigurationError,
    SqlIntrospectionError,
    TlsVerificationError,
    UnsupportedDialectOperationError,
)
from .execution import execute_ddl, fetch_rows

# The ADBC reader is the Arrow streaming read path; it imports ``pyarrow``.
# Keep it out of the eager import graph so the SQL control-plane surface
# (discovery + standalone ``create_table``) stays importable without the
# ``arrow`` extra. ``cdk.sql.AdbcReader`` & friends resolve lazily on first
# access (PEP 562); they require ``analitiq-cdk[arrow]``.
_LAZY_ARROW = frozenset({"AdbcReader", "AdbcReaderClosedError", "open_adbc_reader"})

if TYPE_CHECKING:
    from .adbc_reader import (  # noqa: F401
        AdbcReader,
        AdbcReaderClosedError,
        open_adbc_reader,
    )


def __getattr__(name: str) -> Any:
    if name in _LAZY_ARROW:
        try:
            from . import adbc_reader
        except ImportError as exc:
            reraise_for_missing_extra(
                exc, feature=f"cdk.sql.{name}", extra="arrow", modules=("pyarrow",)
            )
        return getattr(adbc_reader, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "list_schemas",
    "list_tables",
    "list_columns",
    "create_table",
    "build_create_table_sql",
    "SqlDialect",
    "TableAddress",
    "fetch_rows",
    "execute_ddl",
    "AdbcReader",
    "AdbcReaderClosedError",
    "open_adbc_reader",
    "SqlIntrospectionError",
    "UnsupportedDialectOperationError",
    "CatalogAddressingError",
    "DiscoveryError",
    "CreateTableError",
    "ReadError",
    "SchemaConfigurationError",
    "TlsVerificationError",
]
