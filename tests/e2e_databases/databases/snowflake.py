"""Snowflake (cloud, ADBC) — matrix stub.

Engine support for Snowflake ADBC is already in (see git log:
"feat: add Snowflake support via ADBC transport"). What's missing is
the DIP connector folder ``connectors/snowflake/``. Until that lands
the matrix skips every Snowflake pair.

Credentials come from ``.env`` (``E2E_SNOWFLAKE_*``); missing creds
are reported as the skip reason once the connector is published.
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
    "E2E_SNOWFLAKE_ACCOUNT",
    "E2E_SNOWFLAKE_USER",
    "E2E_SNOWFLAKE_PASSWORD",
    "E2E_SNOWFLAKE_DATABASE",
    "E2E_SNOWFLAKE_SCHEMA",
    "E2E_SNOWFLAKE_WAREHOUSE",
)


class SnowflakeSpec(DatabaseSpec):
    slug = "snowflake"
    dip_connector_id = None  # waiting on DIP folder
    is_cloud = True

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "NUMBER(10,0)", "Int32", nullable=False),
            ColumnSpec("name", "VARCHAR(100)", "Utf8", nullable=False),
            ColumnSpec("email", "VARCHAR(255)", "Utf8", nullable=False),
            ColumnSpec("score", "NUMBER(10,0)", "Int32", nullable=True),
            ColumnSpec(
                "created_at",
                "TIMESTAMP_NTZ(6)",
                "Timestamp(MICROSECOND)",
                nullable=False,
            ),
            ColumnSpec(
                "updated_at",
                "TIMESTAMP_NTZ(6)",
                "Timestamp(MICROSECOND)",
                nullable=False,
            ),
        ]

    @property
    def available(self) -> Optional[str]:
        upstream = super().available
        if upstream is not None:
            return upstream
        missing = [k for k in _REQUIRED_ENV if not os.environ.get(k)]
        if missing:
            return f"missing Snowflake env vars: {', '.join(missing)}"
        return None

    def connection(self, role: str) -> ConnectionDescriptor:
        raise NotImplementedError("Snowflake pairs skipped until DIP ships a connector")

    def native_compose_services(self) -> List[str]:
        return []

    def up(self, role: str) -> None:
        ...

    def down(self, role: str) -> None:
        ...

    def seed(self, role: str, rows: Iterable[SeedRow]) -> None:
        raise NotImplementedError

    def prepare_destination(self) -> None:
        raise NotImplementedError

    def read_destination(self) -> List[SeedRow]:
        raise NotImplementedError


SPEC = SnowflakeSpec()
