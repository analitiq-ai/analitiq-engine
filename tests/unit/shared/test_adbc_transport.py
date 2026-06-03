"""ADBC transport builder.

The builder is the engine's only entry into ``transport_type: "adbc"``
connectors. Tests cover:

* the closed driver enum (must match the published connector schema's
  ``AdbcTransport.driver`` enum exactly — extending one without the
  other yields connectors that either don't validate or don't run).
* ``_resolve_db_kwargs`` strips None values (matches schema's optional
  credential treatment) and rejects non-mapping inputs.
* the anyOf(dsn, db_kwargs) constraint is enforced at build time.
* ``register_transport_kind("adbc", …)`` survives module re-import
  (the registration lives at module top-level).
"""

from __future__ import annotations

import pytest

from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.resolver import ResolutionContext, Resolver
from cdk.transport_factory import (
    _ADBC_DRIVER_MODULES,
    _resolve_db_kwargs,
    build_adbc_transport,
    registered_transport_kinds,
)


def _resolver() -> Resolver:
    ctx = ResolutionContext(
        connector={}, connection={}, secrets={}, auth={}, runtime={},
    )
    return Resolver(ctx, functions=DEFAULT_FUNCTIONS)


class TestAdbcDriverRegistry:
    def test_enum_matches_schema(self):
        """Closed enum must mirror schemas.analitiq.ai AdbcTransport.driver."""
        assert set(_ADBC_DRIVER_MODULES) == {
            "postgresql", "snowflake", "bigquery",
        }

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


class TestBuildAdbcTransportRejections:
    @pytest.mark.asyncio
    async def test_missing_driver_raises(self):
        with pytest.raises(ValueError, match="requires `driver`"):
            await build_adbc_transport(
                {"transport_type": "adbc", "db_kwargs": {"a": "b"}},
                resolver=_resolver(),
            )

    @pytest.mark.asyncio
    async def test_unknown_driver_raises(self):
        with pytest.raises(ValueError, match="not registered"):
            await build_adbc_transport(
                {
                    "transport_type": "adbc",
                    "driver": "mysql",  # not in the enum
                    "db_kwargs": {"a": "b"},
                },
                resolver=_resolver(),
            )

    @pytest.mark.asyncio
    async def test_neither_dsn_nor_db_kwargs_raises(self):
        with pytest.raises(ValueError, match="at least one of"):
            await build_adbc_transport(
                {"transport_type": "adbc", "driver": "snowflake"},
                resolver=_resolver(),
            )

    @pytest.mark.asyncio
    async def test_dsn_non_mapping_raises(self):
        with pytest.raises(TypeError, match="dsn"):
            await build_adbc_transport(
                {
                    "transport_type": "adbc",
                    "driver": "postgresql",
                    "dsn": "postgresql://host/db",  # not the structured shape
                },
                resolver=_resolver(),
            )
