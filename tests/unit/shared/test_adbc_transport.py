"""ADBC transport across the resolve/build split.

* ``resolve_adbc_spec`` produces the JSON-safe worker payload and enforces
  the schema's shape constraints (driver required, anyOf(dsn, db_kwargs),
  structured dsn). The driver *values* are validated by the published
  connector schema's ``AdbcTransport.driver`` enum — the engine derives the
  dbapi module by the upstream packaging convention instead of keeping a
  table.
* ``_resolve_db_kwargs`` renders each value to its ADBC option string,
  drops entries with no value (an explicit None, or a ref to a connection
  input the user did not supply), and rejects non-mapping inputs and
  non-scalar values.
* ``build_adbc_from_spec`` fails loudly when the connector's driver wheel
  is not installed.
"""

from __future__ import annotations

import pytest

from cdk.exceptions import TransportSpecError
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
        with pytest.raises(TransportSpecError, match="db_kwargs"):
            _resolve_db_kwargs(["not", "a", "mapping"], _resolver())

    def test_scalars_render_to_adbc_option_strings(self):
        # ADBC database/connection options are string-valued; a typed input
        # (an integer port) must reach the driver as a string, the same form
        # the DSN path produces. Passing a native int would route to the
        # SetOptionInt extension a driver may not implement (Snowflake).
        out = _resolve_db_kwargs(
            {"account": "abc", "warehouse": "wh", "port": 443},
            _resolver(),
        )
        assert out == {"account": "abc", "warehouse": "wh", "port": "443"}

    def test_bool_renders_lowercase(self):
        out = _resolve_db_kwargs({"use_high_precision": True}, _resolver())
        assert out == {"use_high_precision": "true"}

    def test_float_renders_to_string(self):
        out = _resolve_db_kwargs({"login_timeout": 1.5}, _resolver())
        assert out == {"login_timeout": "1.5"}

    def test_none_values_dropped(self):
        # Matches the schema's treatment of optional credential fields
        # — a binding that resolved to None should not be passed to the
        # driver (would override the driver's own default).
        out = _resolve_db_kwargs(
            {"account": "abc", "role": None},
            _resolver(),
        )
        assert out == {"account": "abc"}

    def test_missing_optional_ref_omitted(self):
        # A ref to a connection input the user did not supply resolves to
        # missing data (UnresolvedValueError); the entry is dropped so the
        # driver applies its default. Required inputs are enforced earlier,
        # at the connection-contract boundary, so this is always optional.
        out = _resolve_db_kwargs(
            {
                "account": "abc",
                "role": {"ref": "connection.parameters.role"},
            },
            _resolver(),
        )
        assert out == {"account": "abc"}

    def test_non_scalar_value_rejected(self):
        with pytest.raises(TransportSpecError, match="must be scalars"):
            _resolve_db_kwargs({"opts": {"literal": ["a", "b"]}}, _resolver())


class TestResolveAdbcSpec:
    def test_missing_driver_raises(self):
        with pytest.raises(TransportSpecError, match="`driver`"):
            resolve_adbc_spec(
                {"transport_type": "adbc", "db_kwargs": {"a": "b"}},
                resolver=_resolver(),
            )

    def test_neither_dsn_nor_db_kwargs_raises(self):
        with pytest.raises(TransportSpecError, match="at least one of"):
            resolve_adbc_spec(
                {"transport_type": "adbc", "driver": "snowflake"},
                resolver=_resolver(),
            )

    def test_dsn_non_mapping_raises(self):
        with pytest.raises(TransportSpecError, match="dsn"):
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
            # port renders to its ADBC option string (ADBC options are strings)
            "db_kwargs": {"account": "abc", "port": "443"},
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
