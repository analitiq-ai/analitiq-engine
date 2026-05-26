"""Tests for the ADBC transport in :mod:`src.shared.transport_factory`.

The driver-import branch is covered by patching ``importlib.import_module``
so the tests stay hermetic — no Snowflake account or libpq required.
"""

from __future__ import annotations

import asyncio
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

    def test_snowflake_dialect_is_known(self):
        assert _ADBC_DRIVER_MODULES["snowflake"] == "adbc_driver_snowflake.dbapi"


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
    def base_spec(self) -> Dict[str, Any]:
        return {
            "transport_type": "adbc",
            "dialect": "snowflake",
            "dsn": {
                "kind": "url_template",
                "template": "{user}:{password}@acct/db/schema",
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

    @pytest.mark.asyncio
    async def test_unknown_dialect_rejected(self, base_spec):
        base_spec["dialect"] = "duckdb"
        with pytest.raises(ValueError, match="dialect 'duckdb' is not registered"):
            await build_adbc_transport(
                base_spec, resolver=_resolver(username="kirill")
            )

    @pytest.mark.asyncio
    async def test_missing_dialect_rejected(self, base_spec):
        del base_spec["dialect"]
        with pytest.raises(ValueError, match="requires `dialect`"):
            await build_adbc_transport(
                base_spec, resolver=_resolver(username="kirill")
            )

    @pytest.mark.asyncio
    async def test_missing_driver_package_surfaces_runtime_error(self, base_spec):
        with patch.object(
            importlib, "import_module", side_effect=ImportError("nope")
        ):
            with pytest.raises(RuntimeError, match="adbc_driver_snowflake"):
                await build_adbc_transport(
                    base_spec, resolver=_resolver(username="kirill")
                )

    @pytest.mark.asyncio
    async def test_successful_build_renders_uri_and_caches_factory(self, base_spec):
        fake_module = MagicMock()
        fake_cursor = MagicMock()
        fake_cursor.fetchone.return_value = (1,)
        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        fake_module.connect.return_value = fake_connection

        with patch.object(importlib, "import_module", return_value=fake_module):
            transport = await build_adbc_transport(
                base_spec, resolver=_resolver(username="kirill")
            )

        assert isinstance(transport, AdbcTransport)
        assert transport.dialect == "snowflake"
        # Probe connect happened once during build.
        fake_module.connect.assert_called_once_with(
            "kirill:shh@acct/db/schema"
        )
        fake_cursor.execute.assert_called_once_with("SELECT 1")

        # The connect factory invokes the module fresh each call.
        fake_module.connect.reset_mock()
        transport.connect()
        fake_module.connect.assert_called_once_with(
            "kirill:shh@acct/db/schema"
        )

    @pytest.mark.asyncio
    async def test_db_kwargs_are_forwarded(self, base_spec):
        base_spec["db_kwargs"] = {
            "adbc.snowflake.sql.warehouse": "ANALYTICS_WH",
        }
        fake_module = MagicMock()
        fake_cursor = MagicMock()
        fake_cursor.fetchone.return_value = (1,)
        fake_connection = MagicMock()
        fake_connection.cursor.return_value = fake_cursor
        fake_module.connect.return_value = fake_connection

        with patch.object(importlib, "import_module", return_value=fake_module):
            transport = await build_adbc_transport(
                base_spec, resolver=_resolver(username="kirill")
            )

        fake_module.connect.assert_called_with(
            "kirill:shh@acct/db/schema",
            db_kwargs={"adbc.snowflake.sql.warehouse": "ANALYTICS_WH"},
        )
        # Re-invoking through the factory keeps the same kwargs.
        fake_module.connect.reset_mock()
        transport.connect()
        fake_module.connect.assert_called_once_with(
            "kirill:shh@acct/db/schema",
            db_kwargs={"adbc.snowflake.sql.warehouse": "ANALYTICS_WH"},
        )
