"""DuckDB (embedded, ADBC) — matrix stub.

Embedded, ADBC-driven. The engine already ships ``adbc-driver-duckdb``
(see pyproject ``[duckdb]`` extra). What this DB needs to participate
in the matrix:

1. A DIP connector folder at ``connectors/duckdb/`` with
   ``transport_type: "adbc"`` and ``driver: "duckdb"``.
2. A native-type renderer in ``src/destination/sql_types.py`` and the
   matching DDL/temp-table dispatch wired in
   ``src/destination/connectors/database.py`` (per the ADBC checklist
   in ``CLAUDE.md``).

Until both land, this spec marks itself unavailable so the matrix skips
every pair involving DuckDB.
"""
from __future__ import annotations

from typing import Iterable, List

from tests.e2e_databases.databases._base import (
    ColumnSpec,
    ConnectionDescriptor,
    DatabaseSpec,
)
from tests.e2e_databases.seeds import SeedRow


class DuckDBSpec(DatabaseSpec):
    slug = "duckdb"
    dip_connector_id = None  # not in DIP yet
    is_cloud = False

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "INTEGER", "Int32", nullable=False),
            ColumnSpec("name", "VARCHAR", "Utf8", nullable=False),
            ColumnSpec("email", "VARCHAR", "Utf8", nullable=False),
            ColumnSpec("score", "INTEGER", "Int32", nullable=True),
            ColumnSpec(
                "created_at", "TIMESTAMP", "Timestamp(MICROSECOND)", nullable=False
            ),
            ColumnSpec(
                "updated_at", "TIMESTAMP", "Timestamp(MICROSECOND)", nullable=False
            ),
        ]

    def connection(self, role: str) -> ConnectionDescriptor:
        raise NotImplementedError(
            "DuckDB pairs are skipped until DIP ships a connector"
        )

    def native_compose_services(self) -> List[str]:
        return []

    def up(self, role: str) -> None:
        pass

    def down(self, role: str) -> None:
        pass

    def seed(self, role: str, rows: Iterable[SeedRow]) -> None:
        raise NotImplementedError

    def prepare_destination(self) -> None:
        raise NotImplementedError

    def read_destination(self) -> List[SeedRow]:
        raise NotImplementedError


SPEC = DuckDBSpec()
