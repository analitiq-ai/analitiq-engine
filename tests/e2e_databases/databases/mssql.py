"""Microsoft SQL Server — matrix stub.

SQL Server runs locally in a container (``mcr.microsoft.com/mssql/server``)
but has no DIP connector yet. Bringing it into the matrix needs:

1. A DIP connector folder at ``connectors/mssql/`` with
   ``transport_type: "sqlalchemy"`` and the ``pyodbc`` driver (the bulk
   path is ``fast_executemany=True`` on the cursor, per ``CLAUDE.md``).
2. A compose service for the source and destination instances.

Until the connector ships, this spec marks itself unavailable so the
matrix skips every pair involving SQL Server.
"""
from __future__ import annotations

from typing import Iterable, List

from tests.e2e_databases.databases._base import (
    ColumnSpec,
    ConnectionDescriptor,
    DatabaseSpec,
    Role,
)
from tests.e2e_databases.seeds import SeedRow


class MSSQLSpec(DatabaseSpec):
    slug = "mssql"
    dip_connector_id = None  # not in DIP yet
    is_cloud = False

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "INT", "Int32", nullable=False),
            ColumnSpec("name", "NVARCHAR(100)", "Utf8", nullable=False),
            ColumnSpec("email", "NVARCHAR(255)", "Utf8", nullable=False),
            ColumnSpec("score", "INT", "Int32", nullable=True),
            ColumnSpec(
                "created_at", "DATETIME2", "Timestamp(MICROSECOND)", nullable=False
            ),
            ColumnSpec(
                "updated_at", "DATETIME2", "Timestamp(MICROSECOND)", nullable=False
            ),
        ]

    def connection(self, role: Role) -> ConnectionDescriptor:
        raise NotImplementedError(
            "SQL Server pairs are skipped until DIP ships a connector"
        )

    def upsert_rows(self, role: Role, rows: Iterable[SeedRow]) -> None:
        raise NotImplementedError

    def up(self, role: Role) -> None:
        pass

    def down(self, role: Role) -> None:
        pass

    def seed(self, role: Role, rows: Iterable[SeedRow]) -> None:
        raise NotImplementedError

    def prepare_destination(self) -> None:
        raise NotImplementedError

    def read_destination(self) -> List[SeedRow]:
        raise NotImplementedError


SPEC = MSSQLSpec()
