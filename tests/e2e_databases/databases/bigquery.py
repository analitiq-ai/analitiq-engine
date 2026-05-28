"""BigQuery (cloud, ADBC) — matrix stub.

Engine ADBC support exists; DIP connector folder
``connectors/bigquery/`` does not yet. Credentials come from
``.env`` (``E2E_BIGQUERY_*``).
"""
from __future__ import annotations

import os
from typing import Iterable, List, Optional

from tests.e2e_databases.databases._base import (
    ColumnSpec,
    ConnectionDescriptor,
    DatabaseSpec,
)
from tests.e2e_databases.seeds import SeedRow

_REQUIRED_ENV = (
    "E2E_BIGQUERY_PROJECT",
    "E2E_BIGQUERY_DATASET",
    "E2E_BIGQUERY_CREDENTIALS_PATH",
)


class BigQuerySpec(DatabaseSpec):
    slug = "bigquery"
    dip_connector_id = None
    is_cloud = True

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "INT64", "Int32", nullable=False),
            ColumnSpec("name", "STRING", "Utf8", nullable=False),
            ColumnSpec("email", "STRING", "Utf8", nullable=False),
            ColumnSpec("score", "INT64", "Int32", nullable=True),
            ColumnSpec(
                "created_at", "TIMESTAMP", "Timestamp(MICROSECOND)", nullable=False
            ),
            ColumnSpec(
                "updated_at", "TIMESTAMP", "Timestamp(MICROSECOND)", nullable=False
            ),
        ]

    @property
    def available(self) -> Optional[str]:
        upstream = super().available
        if upstream is not None:
            return upstream
        missing = [k for k in _REQUIRED_ENV if not os.environ.get(k)]
        if missing:
            return f"missing BigQuery env vars: {', '.join(missing)}"
        return None

    def connection(self, role: str) -> ConnectionDescriptor:
        raise NotImplementedError("BigQuery pairs skipped until DIP ships a connector")

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


SPEC = BigQuerySpec()
