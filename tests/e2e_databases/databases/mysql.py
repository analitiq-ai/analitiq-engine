"""MySQL database spec for the E2E matrix.

Local: ``docker compose`` brings up ``e2e-mysql-source`` /
``e2e-mysql-dest``. The engine reaches them by service name; pytest
reaches them via host-mapped ports.

DIP connector: ``connectors/mysql``.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Iterable, List

import aiomysql

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
_ROOT_PASSWORD = "e2e_root"


class MySQLSpec(DatabaseSpec):
    slug = "mysql"
    dip_connector_id = "mysql"
    is_cloud = False

    _ROLES = {
        "source": ("e2e-mysql-source", 13306),
        "destination": ("e2e-mysql-dest", 13307),
    }

    def columns(self) -> List[ColumnSpec]:
        return [
            ColumnSpec("id", "INT", "Int32", nullable=False),
            ColumnSpec("name", "VARCHAR(100)", "Utf8", nullable=False),
            ColumnSpec("email", "VARCHAR(255)", "Utf8", nullable=False),
            ColumnSpec("score", "INT", "Int32", nullable=True),
            ColumnSpec(
                "created_at", "DATETIME", "Timestamp(MICROSECOND)", nullable=False
            ),
            ColumnSpec(
                "updated_at", "DATETIME", "Timestamp(MICROSECOND)", nullable=False
            ),
        ]

    def connection(self, role: str) -> ConnectionDescriptor:
        service, host_port = self._ROLES[role]
        return ConnectionDescriptor(
            engine_host=service,
            engine_port=3306,
            host_address="127.0.0.1",
            host_port=host_port,
            database=_DATABASE,
            username=_USER,
            password=_PASSWORD,
            schema=_DATABASE,
            extra_parameters={"ssl_mode": "DISABLED"},
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

    async def _connect(self, role: str):
        desc = self.connection(role)
        return await aiomysql.connect(
            host=desc.host_address,
            port=desc.host_port,
            user=desc.username,
            password=desc.password,
            db=desc.database,
            autocommit=True,
        )

    async def _seed_async(self, role: str, rows: List[SeedRow]) -> None:
        conn = await self._connect(role)
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"DROP TABLE IF EXISTS `{SEED_TABLE_NAME}`")
                await cur.execute(
                    f"""
                    CREATE TABLE `{SEED_TABLE_NAME}` (
                        id          INT          NOT NULL PRIMARY KEY,
                        name        VARCHAR(100) NOT NULL,
                        email       VARCHAR(255) NOT NULL,
                        score       INT,
                        created_at  DATETIME     NOT NULL,
                        updated_at  DATETIME     NOT NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                await cur.executemany(
                    f"INSERT INTO `{SEED_TABLE_NAME}` "
                    f"(id, name, email, score, created_at, updated_at) "
                    f"VALUES (%s, %s, %s, %s, %s, %s)",
                    [
                        (r.id, r.name, r.email, r.score, r.created_at, r.updated_at)
                        for r in rows
                    ],
                )
        finally:
            conn.close()

    async def _drop_table(self, role: str) -> None:
        conn = await self._connect(role)
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"DROP TABLE IF EXISTS `{SEED_TABLE_NAME}`")
        finally:
            conn.close()

    async def _read_async(self, role: str) -> List[SeedRow]:
        conn = await self._connect(role)
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT id, name, email, score, created_at, updated_at "
                    f"FROM `{SEED_TABLE_NAME}` ORDER BY id"
                )
                rows = await cur.fetchall()
            return [
                SeedRow(
                    id=row[0],
                    name=row[1],
                    email=row[2],
                    score=row[3],
                    created_at=row[4],
                    updated_at=row[5],
                )
                for row in rows
            ]
        finally:
            conn.close()

    def _wait_until_ready(self, role: str, timeout_seconds: int = 90) -> None:
        async def _probe() -> None:
            deadline = asyncio.get_event_loop().time() + timeout_seconds
            last_err: Exception | None = None
            while asyncio.get_event_loop().time() < deadline:
                try:
                    conn = await self._connect(role)
                    conn.close()
                    return
                except (OSError, aiomysql.Error) as err:
                    last_err = err
                    await asyncio.sleep(1)
            raise RuntimeError(
                f"MySQL {role} ({self._ROLES[role][0]}) did not accept "
                f"connections within {timeout_seconds}s: {last_err}"
            )

        asyncio.run(_probe())


SPEC = MySQLSpec()


# Module-level constants the compose template needs.
USER = _USER
PASSWORD = _PASSWORD
DATABASE = _DATABASE
ROOT_PASSWORD = _ROOT_PASSWORD
