"""Redshift (cloud, Postgres-protocol) — matrix stub.

Redshift speaks libpq; the engine policy (see CLAUDE.md) is to use
``adbc-driver-postgresql`` against Redshift. A DIP
``connectors/redshift/`` folder is the missing piece; once it lands,
the matrix picks Redshift up automatically.
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
    "E2E_REDSHIFT_HOST",
    "E2E_REDSHIFT_DATABASE",
    "E2E_REDSHIFT_USER",
    "E2E_REDSHIFT_PASSWORD",
)


class RedshiftSpec(DatabaseSpec):
    slug = "redshift"
    dip_connector_id = None
    is_cloud = True

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "INTEGER", "Int32", nullable=False),
            ColumnSpec("name", "VARCHAR(100)", "Utf8", nullable=False),
            ColumnSpec("email", "VARCHAR(255)", "Utf8", nullable=False),
            ColumnSpec("score", "INTEGER", "Int32", nullable=True),
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
            return f"missing Redshift env vars: {', '.join(missing)}"
        return None

    def connection(self, role: str) -> ConnectionDescriptor:
        raise NotImplementedError("Redshift pairs skipped until DIP ships a connector")

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


SPEC = RedshiftSpec()
