"""PostgreSQL database spec for the E2E matrix.

Local: ``docker compose`` brings up ``e2e-pg-source`` and ``e2e-pg-dest``
containers. The engine reaches them via those hostnames on the shared
network; pytest reaches them via ``localhost:5433`` / ``localhost:5434``.

DIP connector: ``connectors/postgres`` (slug-named, ships in the repo).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Iterable, List

import asyncpg

from tests.e2e_databases.databases._base import (
    ColumnSpec,
    ConnectionDescriptor,
    DatabaseSpec,
)
from tests.e2e_databases.databases._docker import compose_down, compose_up
from tests.e2e_databases.seeds import SEED_TABLE_NAME, SeedRow

logger = logging.getLogger(__name__)


_USER = "e2e_user"
_PASSWORD = "e2e_password"
_DATABASE = "e2e_db"


class PostgresSpec(DatabaseSpec):
    slug = "postgres"
    dip_connector_id = "postgres"
    is_cloud = False

    # role -> (compose service, host port)
    _ROLES = {
        "source": ("e2e-pg-source", 5433),
        "destination": ("e2e-pg-dest", 5434),
    }

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

    def connection(self, role: str) -> ConnectionDescriptor:
        service, host_port = self._ROLES[role]
        return ConnectionDescriptor(
            engine_host=service,
            engine_port=5432,
            host_address="127.0.0.1",
            host_port=host_port,
            database=_DATABASE,
            username=_USER,
            password=_PASSWORD,
            schema="public",
            extra_parameters={"ssl_mode": "disable"},
        )

    def native_compose_services(self) -> List[str]:
        return [service for service, _ in self._ROLES.values()]

    def up(self, role: str) -> None:
        service, _ = self._ROLES[role]
        compose_up(service)
        self._wait_until_ready(role)

    def down(self, role: str) -> None:
        service, _ = self._ROLES[role]
        compose_down(service)

    def seed(self, role: str, rows: Iterable[SeedRow]) -> None:
        asyncio.run(self._seed_async(role, list(rows)))

    def prepare_destination(self) -> None:
        asyncio.run(self._drop_table("destination"))

    def read_destination(self) -> List[SeedRow]:
        return asyncio.run(self._read_async("destination"))

    # ---- internals ------------------------------------------------------

    def _dsn_for_host(self, role: str) -> str:
        desc = self.connection(role)
        return (
            f"postgres://{desc.username}:{desc.password}"
            f"@{desc.host_address}:{desc.host_port}/{desc.database}"
        )

    async def _seed_async(self, role: str, rows: List[SeedRow]) -> None:
        conn = await asyncpg.connect(self._dsn_for_host(role))
        try:
            await conn.execute(f'DROP TABLE IF EXISTS "{SEED_TABLE_NAME}"')
            await conn.execute(
                f"""
                CREATE TABLE "{SEED_TABLE_NAME}" (
                    id          INTEGER     PRIMARY KEY,
                    name        VARCHAR(100) NOT NULL,
                    email       VARCHAR(255) NOT NULL,
                    score       INTEGER,
                    created_at  TIMESTAMP   NOT NULL,
                    updated_at  TIMESTAMP   NOT NULL
                )
                """
            )
            await conn.executemany(
                f'INSERT INTO "{SEED_TABLE_NAME}" '
                f"(id, name, email, score, created_at, updated_at) "
                f"VALUES ($1, $2, $3, $4, $5, $6)",
                [
                    (r.id, r.name, r.email, r.score, r.created_at, r.updated_at)
                    for r in rows
                ],
            )
        finally:
            await conn.close()

    async def _drop_table(self, role: str) -> None:
        conn = await asyncpg.connect(self._dsn_for_host(role))
        try:
            await conn.execute(f'DROP TABLE IF EXISTS "{SEED_TABLE_NAME}"')
        finally:
            await conn.close()

    async def _read_async(self, role: str) -> List[SeedRow]:
        conn = await asyncpg.connect(self._dsn_for_host(role))
        try:
            records = await conn.fetch(
                f"SELECT id, name, email, score, created_at, updated_at "
                f'FROM "{SEED_TABLE_NAME}" ORDER BY id'
            )
            return [
                SeedRow(
                    id=r["id"],
                    name=r["name"],
                    email=r["email"],
                    score=r["score"],
                    created_at=r["created_at"],
                    updated_at=r["updated_at"],
                )
                for r in records
            ]
        finally:
            await conn.close()

    def _wait_until_ready(self, role: str, timeout_seconds: int = 60) -> None:
        async def _probe() -> None:
            deadline = asyncio.get_event_loop().time() + timeout_seconds
            last_err: Exception | None = None
            while asyncio.get_event_loop().time() < deadline:
                try:
                    conn = await asyncpg.connect(self._dsn_for_host(role))
                    await conn.close()
                    return
                except (OSError, asyncpg.PostgresError) as err:
                    last_err = err
                    await asyncio.sleep(1)
            raise RuntimeError(
                f"Postgres {role} ({self._ROLES[role][0]}) did not accept "
                f"connections within {timeout_seconds}s: {last_err}"
            )

        asyncio.run(_probe())


SPEC = PostgresSpec()
