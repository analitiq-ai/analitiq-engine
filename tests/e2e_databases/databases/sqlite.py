"""SQLite (embedded, ADBC) — matrix stub.

Embedded, ADBC-driven via ``adbc-driver-sqlite``. Needs a
``connectors/sqlite/`` DIP folder plus SQL renderer support before
pairs involving SQLite can run.
"""
from __future__ import annotations

from typing import Iterable, List

from tests.e2e_databases.databases._base import (
    ColumnSpec,
    ConnectionDescriptor,
    DatabaseSpec,
)
from tests.e2e_databases.seeds import SeedRow


class SQLiteSpec(DatabaseSpec):
    slug = "sqlite"
    dip_connector_id = None
    is_cloud = False

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "INTEGER", "Int32", nullable=False),
            ColumnSpec("name", "TEXT", "Utf8", nullable=False),
            ColumnSpec("email", "TEXT", "Utf8", nullable=False),
            ColumnSpec("score", "INTEGER", "Int32", nullable=True),
            ColumnSpec("created_at", "TEXT", "Timestamp(MICROSECOND)", nullable=False),
            ColumnSpec("updated_at", "TEXT", "Timestamp(MICROSECOND)", nullable=False),
        ]

    def connection(self, role: str) -> ConnectionDescriptor:
        raise NotImplementedError(
            "SQLite pairs are skipped until DIP ships a connector"
        )

    def upsert_rows(self, role: str, rows: Iterable[SeedRow]) -> None:
        raise NotImplementedError

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


SPEC = SQLiteSpec()
