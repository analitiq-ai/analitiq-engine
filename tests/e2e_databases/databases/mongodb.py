"""MongoDB — matrix stub.

MongoDB is NoSQL; the engine currently models database connectors as
relational (the ``kind`` enum in the connector schema is
``database/api/file/stdout``). Bringing MongoDB into the matrix needs:

1. Engine support for a non-relational ``database``-kind connector,
   or a new ``document`` connector kind, with mapping rules for the
   canonical seed shape.
2. A DIP connector folder.

Spec sits here so that when both arrive, the matrix picks it up
without further wiring.
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


class MongoDBSpec(DatabaseSpec):
    slug = "mongodb"
    dip_connector_id = None
    is_cloud = False

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "Int32", "Int32", nullable=False),
            ColumnSpec("name", "String", "Utf8", nullable=False),
            ColumnSpec("email", "String", "Utf8", nullable=False),
            ColumnSpec("score", "Int32", "Int32", nullable=True),
            ColumnSpec("created_at", "Date", "Timestamp(MICROSECOND)", nullable=False),
            ColumnSpec("updated_at", "Date", "Timestamp(MICROSECOND)", nullable=False),
        ]

    def connection(self, role: Role) -> ConnectionDescriptor:
        raise NotImplementedError(
            "MongoDB pairs are skipped until DIP ships a connector"
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


SPEC = MongoDBSpec()
