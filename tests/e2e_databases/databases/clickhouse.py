"""ClickHouse — matrix stub.

ClickHouse has first-class Arrow ingest via ``clickhouse-connect``
(see ``CLAUDE.md`` §native bulk-load protocols), but no DIP connector
exists yet. ``connectors/clickhouse/`` needs to land with
``transport_type: "clickhouse"`` (or whatever the engine settles on
for the non-ADBC native-Arrow path) before this spec becomes usable.
"""
from __future__ import annotations

from typing import Iterable, List

from tests.e2e_databases.databases._base import (
    ColumnSpec,
    ConnectionDescriptor,
    DatabaseSpec,
)
from tests.e2e_databases.seeds import SeedRow


class ClickHouseSpec(DatabaseSpec):
    slug = "clickhouse"
    dip_connector_id = None
    # ClickHouse can run locally in Docker; the cloud option is left to
    # ``.env`` (``E2E_CLICKHOUSE_HOST``). For matrix purposes either is fine.
    is_cloud = False

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "Int32", "Int32", nullable=False),
            ColumnSpec("name", "String", "Utf8", nullable=False),
            ColumnSpec("email", "String", "Utf8", nullable=False),
            ColumnSpec("score", "Nullable(Int32)", "Int32", nullable=True),
            ColumnSpec(
                "created_at", "DateTime64(6)", "Timestamp(MICROSECOND)", nullable=False
            ),
            ColumnSpec(
                "updated_at", "DateTime64(6)", "Timestamp(MICROSECOND)", nullable=False
            ),
        ]

    def connection(self, role: str) -> ConnectionDescriptor:
        raise NotImplementedError(
            "ClickHouse pairs are skipped until DIP ships a connector"
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


SPEC = ClickHouseSpec()
