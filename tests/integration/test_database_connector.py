"""Integration tests for the SQL source read path against real PostgreSQL.

The unified :class:`~cdk.sql.generic.GenericSQLConnector` is a pure reader on
the source side: ``read_batches`` is handed a :class:`ConnectionRuntime`,
materializes it, pages the table, and releases it on exit — there is no
persistent ``connect()``/``disconnect()`` lifecycle to introspect. These tests
build their own short-lived runtimes for table setup/teardown and drive the
connector with the contract endpoint document.
"""

import os
import uuid
from unittest.mock import AsyncMock

import pytest

from cdk.connection_runtime import ConnectionRuntime, materialize_runtime
from cdk.database_utils import acquire_connection
from cdk.secrets.resolvers.memory import InMemorySecretsResolver
from cdk.sql.generic import GenericSQLConnector


def _postgres_available() -> bool:
    """Check if PostgreSQL is reachable for testing."""
    import socket

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_available(), reason="PostgreSQL not reachable"
)


@pytest.fixture
def database_config():
    """Connection configuration for real database tests."""
    return {
        "driver": "postgresql",
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "parameters": {
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "analitiq_test"),
            "username": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
            "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
        },
    }


def _make_runtime(database_config) -> ConnectionRuntime:
    """A fresh runtime for one read or one setup/teardown phase."""
    return ConnectionRuntime(
        raw_config=database_config,
        connection_id="test-conn",
        connector_type="database",
        driver="postgresql",
        resolver=InMemorySecretsResolver({}),
    )


@pytest.fixture
def unique_table_name():
    """Generate unique table name for test isolation."""
    return f"test_connector_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def mock_checkpoint():
    """Minimal CheckpointStore: no prior cursor, records saves."""
    checkpoint = AsyncMock()
    checkpoint.get_cursor = AsyncMock(return_value=None)
    checkpoint.save_cursor = AsyncMock()
    return checkpoint


def _read_config(table_name: str):
    """Contract source config the connector reads directly."""
    return {
        "endpoint_document": {
            "database_object": {"name": table_name, "schema": "public"},
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "name", "arrow_type": "Utf8"},
                {"name": "email", "arrow_type": "Utf8"},
            ],
        },
        "stream_source": {
            "filters": [],
            "replication": {"method": "incremental", "cursor_field": ["id"]},
        },
    }


class TestGenericSQLSourceRealIntegration:
    """Integration tests using a real PostgreSQL database."""

    @pytest.mark.asyncio
    async def test_read_batches_pagination(
        self, database_config, unique_table_name, mock_checkpoint
    ):
        """read_batches paginates through a larger dataset and advances cursor."""
        # --- Setup: create + populate the table via a short-lived runtime. ---
        setup_runtime = _make_runtime(database_config)
        await materialize_runtime(setup_runtime)
        try:
            async with acquire_connection(setup_runtime.engine) as conn:
                await conn.exec_driver_sql(
                    f"CREATE TABLE public.{unique_table_name} ("
                    f"  id INTEGER PRIMARY KEY,"
                    f"  name VARCHAR(255),"
                    f"  email VARCHAR(255)"
                    f")"
                )
                values = ", ".join(
                    f"({i}, 'User{i}', 'user{i}@test.com')" for i in range(1, 26)
                )
                await conn.exec_driver_sql(
                    f"INSERT INTO public.{unique_table_name} "
                    f"(id, name, email) VALUES {values}"
                )
        finally:
            await setup_runtime.close()

        try:
            # --- Read: the connector owns its own runtime for the read. ---
            connector = GenericSQLConnector()
            read_runtime = _make_runtime(database_config)

            all_records = []
            batch_count = 0
            async for batch in connector.read_batches(
                read_runtime,
                _read_config(unique_table_name),
                checkpoint=mock_checkpoint,
                stream_name="test_stream",
                batch_size=10,
            ):
                all_records.extend(batch.to_pylist())
                batch_count += 1

            assert len(all_records) == 25
            assert batch_count >= 2  # Small batch size -> multiple pages.
            ids = {r["id"] for r in all_records}
            assert ids == set(range(1, 26))
            # The cursor advanced to the last id seen.
            saved = [
                c.args[2]["cursor"] for c in mock_checkpoint.save_cursor.call_args_list
            ]
            assert saved[-1] == 25
        finally:
            # --- Teardown: drop the table via a fresh runtime. ---
            teardown_runtime = _make_runtime(database_config)
            await materialize_runtime(teardown_runtime)
            try:
                async with acquire_connection(teardown_runtime.engine) as conn:
                    await conn.exec_driver_sql(
                        f"DROP TABLE IF EXISTS public.{unique_table_name}"
                    )
            finally:
                await teardown_runtime.close()
