"""ADBC transport across the resolve/build split.

* ``resolve_adbc_spec`` produces the JSON-safe worker payload and enforces
  the schema's shape constraints (driver required, anyOf(dsn, db_kwargs),
  structured dsn). The driver *values* are validated by the published
  connector schema's ``AdbcTransport.driver`` enum — the engine derives the
  dbapi module by the upstream packaging convention instead of keeping a
  table.
* ``_resolve_db_kwargs`` strips None values (matches the schema's optional
  credential treatment) and rejects non-mapping inputs.
* ``build_adbc_from_spec`` fails loudly when the connector's driver wheel
  is not installed.
"""

from __future__ import annotations

import pytest

from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.resolver import ResolutionContext, Resolver
from cdk.transport_factory import (
    _adbc_dbapi_module_path,
    _resolve_db_kwargs,
    build_adbc_from_spec,
    resolve_adbc_spec,
    registered_transport_kinds,
)


def _resolver() -> Resolver:
    ctx = ResolutionContext(
        connector={}, connection={}, secrets={}, auth={}, runtime={},
    )
    return Resolver(ctx, functions=DEFAULT_FUNCTIONS)


class TestModuleConvention:
    @pytest.mark.parametrize(
        "driver,module",
        [
            ("postgresql", "adbc_driver_postgresql.dbapi"),
            ("snowflake", "adbc_driver_snowflake.dbapi"),
            ("bigquery", "adbc_driver_bigquery.dbapi"),
            ("DuckDB", "adbc_driver_duckdb.dbapi"),  # case-folds
        ],
    )
    def test_dbapi_module_follows_packaging_convention(self, driver, module):
        assert _adbc_dbapi_module_path(driver) == module

    def test_adbc_kind_registered(self):
        assert "adbc" in registered_transport_kinds()


class TestResolveDbKwargs:
    def test_none_returns_empty(self):
        assert _resolve_db_kwargs(None, _resolver()) == {}

    def test_non_mapping_rejected(self):
        with pytest.raises(TypeError, match="db_kwargs"):
            _resolve_db_kwargs(["not", "a", "mapping"], _resolver())

    def test_literals_pass_through(self):
        out = _resolve_db_kwargs(
            {"account": "abc", "warehouse": "wh", "port": 443},
            _resolver(),
        )
        assert out == {"account": "abc", "warehouse": "wh", "port": 443}

    def test_none_values_dropped(self):
        # Matches the schema's treatment of optional credential fields
        # — a binding that resolved to None should not be passed to the
        # driver (would override the driver's own default).
        out = _resolve_db_kwargs(
            {"account": "abc", "role": None},
            _resolver(),
        )
        assert out == {"account": "abc"}


class TestResolveAdbcSpec:
    def test_missing_driver_raises(self):
        with pytest.raises(ValueError, match="`driver`"):
            resolve_adbc_spec(
                {"transport_type": "adbc", "db_kwargs": {"a": "b"}},
                resolver=_resolver(),
            )

    def test_neither_dsn_nor_db_kwargs_raises(self):
        with pytest.raises(ValueError, match="at least one of"):
            resolve_adbc_spec(
                {"transport_type": "adbc", "driver": "snowflake"},
                resolver=_resolver(),
            )

    def test_dsn_non_mapping_raises(self):
        with pytest.raises(TypeError, match="dsn"):
            resolve_adbc_spec(
                {
                    "transport_type": "adbc",
                    "driver": "postgresql",
                    "dsn": "postgresql://host/db",  # not the structured shape
                },
                resolver=_resolver(),
            )

    def test_output_is_json_safe_payload(self):
        import json

        out = resolve_adbc_spec(
            {
                "transport_type": "adbc",
                "driver": "Snowflake",
                "db_kwargs": {"account": "abc", "port": 443},
            },
            resolver=_resolver(),
        )
        assert out == {
            "transport_type": "adbc",
            "driver": "snowflake",  # folded
            "uri": None,
            "db_kwargs": {"account": "abc", "port": 443},
        }
        json.dumps(out)  # must round-trip into a worker bootstrap


class TestBuildAdbcFromSpec:
    @pytest.mark.asyncio
    async def test_uninstalled_driver_wheel_fails_loudly(self):
        with pytest.raises(RuntimeError, match="not importable"):
            await build_adbc_from_spec(
                {
                    "transport_type": "adbc",
                    "driver": "no-such-driver",
                    "uri": None,
                    "db_kwargs": {"a": "b"},
                }
            )
