"""Tests for the ADBC transport in :mod:`src.shared.transport_factory`.

The driver-import branch is covered by patching ``importlib.import_module``
so the tests stay hermetic — no Snowflake account or libpq required.
The field name (``driver``) and the ``anyOf(dsn, db_kwargs)`` shape
mirror the published connector contract
(``schemas.analitiq.ai/connector/latest.json::AdbcTransport``).
"""

from __future__ import annotations

import importlib
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from src.engine.resolver import ResolutionContext, Resolver
from src.engine.derived_functions import DEFAULT_FUNCTIONS
from src.shared.transport_factory import (
    AdbcTransport,
    _ADBC_DRIVER_MODULES,
    _resolve_db_kwargs,
    build_adbc_transport,
    registered_transport_kinds,
)


def _resolver(**connection_params: Any) -> Resolver:
    context = ResolutionContext(
        connector={},
        connection={"parameters": dict(connection_params)},
        secrets={"password": "shh"},
        auth={},
        runtime={"connection_id": "test"},
    )
    return Resolver(context, functions=DEFAULT_FUNCTIONS)


class TestRegistry:
    def test_adbc_kind_is_registered(self):
        assert "adbc" in registered_transport_kinds()

    def test_snowflake_driver_is_known(self):
        assert _ADBC_DRIVER_MODULES["snowflake"] == "adbc_driver_snowflake.dbapi"

    def test_postgresql_driver_is_known(self):
        assert _ADBC_DRIVER_MODULES["postgresql"] == "adbc_driver_postgresql.dbapi"

    def test_postgres_alias_not_in_registry(self):
        # Schema enum only allows ``postgresql``; the ``postgres`` alias
        # the engine previously carried is unreachable post-schema.
        assert "postgres" not in _ADBC_DRIVER_MODULES


class TestResolveDbKwargs:
    def test_returns_empty_dict_when_absent(self):
        assert _resolve_db_kwargs(None, _resolver()) == {}

    def test_resolves_each_value_through_resolver(self):
        resolver = _resolver()
        out = _resolve_db_kwargs(
            {
                "literal": "abc",
                "from_secret": {"ref": "secrets.password"},
            },
            resolver,
        )
        assert out == {"literal": "abc", "from_secret": "shh"}

    def test_drops_none_resolved_values(self):
        resolver = _resolver()
        # ``parameters.warehouse`` is not present, so ref resolution
        # yields None; the key should not appear in the output.
        out = _resolve_db_kwargs(
            {"warehouse": {"ref": "connection.parameters.warehouse"}},
            resolver,
        )
        assert "warehouse" not in out

    def test_rejects_non_mapping(self):
        with pytest.raises(TypeError, match="db_kwargs"):
            _resolve_db_kwargs(["not", "a", "mapping"], _resolver())


class TestBuildAdbcTransport:
    @pytest.fixture
    def postgres_spec(self) -> Dict[str, Any]:
        """Canonical Postgres ADBC connector — uses DSN (libpq URI)."""
        return {
            "transport_type": "adbc",
            "driver": "postgresql",
            "dsn": {
                "kind": "url_template",
                "template": "postgresql://{user}:{password}@host/db",
                "bindings": {
                    "user": {
                        "value": {"ref": "connection.parameters.username"},
                        "encoding": "url_userinfo",
                    },
                    "password": {
                        "value": {"ref": "secrets.password"},
                        "encoding": "url_userinfo",
                    },
                },
            },
        }

    @pytest.fixture
    def snowflake_spec(self) -> Dict[str, Any]:
        """Canonical Snowflake ADBC connector — db_kwargs only, no DSN.

        Matches the example in
        ``docs/schema-contracts/connectors/database-connector-schema-parameterization.md``.
        """
        return {
            "transport_type": "adbc",
            "driver": "snowflake",
            "db_kwargs": {
                "adbc.snowflake.sql.account": {
                    "ref": "connection.parameters.account"
                },
                "adbc.snowflake.sql.warehouse": {
                    "ref": "connection.parameters.warehouse"
                },
                "username": {"ref": "connection.parameters.username"},
                "password": {"ref": "secrets.password"},
            },
        }

    @pytest.mark.asyncio
    async def test_unknown_driver_rejected(self, postgres_spec):
        postgres_spec["driver"] = "duckdb"
        with pytest.raises(ValueError, match="driver 'duckdb' is not registered"):
            await build_adbc_transport(
                postgres_spec, resolver=_resolver(username="kirill"),
            )

    @pytest.mark.asyncio
    async def test_missing_driver_rejected(self, postgres_spec):
        del postgres_spec["driver"]
        with pytest.raises(ValueError, match="requires `driver`"):
            await build_adbc_transport(
                postgres_spec, resolver=_resolver(username="kirill"),
            )

    @pytest.mark.asyncio
    async def test_missing_driver_package_surfaces_runtime_error(self, postgres_spec):
        with patch.object(
            importlib, "import_module", side_effect=ImportError("nope")
        ):
            with pytest.raises(RuntimeError, match="adbc_driver_postgresql"):
                await build_adbc_transport(
                    postgres_spec, resolver=_resolver(username="kirill"),
                )

    @pytest.mark.asyncio
    async def test_neither_dsn_nor_db_kwargs_rejected(self):
        spec = {"transport_type": "adbc", "driver": "snowflake"}
        fake_module = MagicMock()
        with patch.object(importlib, "import_module", return_value=fake_module):
            with pytest.raises(ValueError, match="neither `dsn` nor `db_kwargs`"):
                await build_adbc_transport(spec, resolver=_resolver())
        # The probe connect must not run — the validation gate is
        # before any driver call, so a misconfigured connector never
        # touches the network.
        fake_module.connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_postgres_dsn_only_uses_positional_connect(self, postgres_spec):
        fake_module = MagicMock()
        fake_cursor = MagicMock()
        fake_cursor.fetchone.return_value = (1,)
        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        fake_module.connect.return_value = fake_connection

        with patch.object(importlib, "import_module", return_value=fake_module):
            transport = await build_adbc_transport(
                postgres_spec, resolver=_resolver(username="kirill"),
            )

        assert isinstance(transport, AdbcTransport)
        assert transport.driver == "postgresql"
        assert transport.driver_module_path == "adbc_driver_postgresql.dbapi"
        # No db_kwargs declared → connect takes positional URI only.
        fake_module.connect.assert_called_once_with(
            "postgresql://kirill:shh@host/db"
        )
        fake_cursor.execute.assert_called_once_with("SELECT 1")

        # Connect factory replays the same call shape.
        fake_module.connect.reset_mock()
        transport.connect()
        fake_module.connect.assert_called_once_with(
            "postgresql://kirill:shh@host/db"
        )

    @pytest.mark.asyncio
    async def test_snowflake_db_kwargs_only_uses_kwarg_connect(self, snowflake_spec):
        fake_module = MagicMock()
        fake_cursor = MagicMock()
        fake_cursor.fetchone.return_value = (1,)
        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        fake_module.connect.return_value = fake_connection

        resolver = _resolver(
            account="acct1", warehouse="wh1", username="kirill",
        )
        with patch.object(importlib, "import_module", return_value=fake_module):
            transport = await build_adbc_transport(
                snowflake_spec, resolver=resolver,
            )

        assert transport.driver == "snowflake"
        # db_kwargs only → connect called WITHOUT a positional URI.
        fake_module.connect.assert_called_once_with(
            db_kwargs={
                "adbc.snowflake.sql.account": "acct1",
                "adbc.snowflake.sql.warehouse": "wh1",
                "username": "kirill",
                "password": "shh",
            }
        )

    @pytest.mark.asyncio
    async def test_both_dsn_and_db_kwargs_combine(self, postgres_spec):
        postgres_spec["db_kwargs"] = {
            "application_name": "analitiq",
        }
        fake_module = MagicMock()
        fake_cursor = MagicMock()
        fake_cursor.fetchone.return_value = (1,)
        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        fake_module.connect.return_value = fake_connection

        with patch.object(importlib, "import_module", return_value=fake_module):
            transport = await build_adbc_transport(
                postgres_spec, resolver=_resolver(username="kirill"),
            )

        # Both → uri positional + db_kwargs keyword.
        fake_module.connect.assert_called_once_with(
            "postgresql://kirill:shh@host/db",
            db_kwargs={"application_name": "analitiq"},
        )
        # Same shape on reconnect through the factory.
        fake_module.connect.reset_mock()
        transport.connect()
        fake_module.connect.assert_called_once_with(
            "postgresql://kirill:shh@host/db",
            db_kwargs={"application_name": "analitiq"},
        )

    @pytest.mark.asyncio
    async def test_dsn_present_but_not_mapping_rejected(self, postgres_spec):
        postgres_spec["dsn"] = "postgresql://literal-string-bad"
        with pytest.raises(TypeError, match="dsn`, when present, must"):
            await build_adbc_transport(
                postgres_spec, resolver=_resolver(username="kirill"),
            )
