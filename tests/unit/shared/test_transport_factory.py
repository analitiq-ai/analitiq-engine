"""Tests for :mod:`cdk.transport_factory`.

These cover transport selection + spec resolution
(:func:`resolve_transport_spec`, which now dispatches to the per-kind
resolve phase and returns the JSON-safe worker payload) and the kind
registry (register / unregister / dispatch via :func:`build_transport`,
where each kind is a resolve/build pair). The live engine/session probes
that require a database / HTTP endpoint are not exercised here.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cdk.exceptions import TransportSpecError
from cdk.resolver import ResolutionContext
from cdk.transport_factory import (
    build_transport,
    register_transport_kind,
    registered_transport_kinds,
    resolve_adbc_spec,
    resolve_http_spec,
    resolve_sqlalchemy_spec,
    resolve_transport_spec,
    unregister_transport_kind,
)

# ---------------------------------------------------------------------------
# resolve_transport_spec — selection + per-kind resolution
# ---------------------------------------------------------------------------


class TestResolveTransportSpec:
    def test_default_transport_used_when_ref_not_given(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {
                "api": {
                    "transport_type": "http",
                    "base_url": "https://api.example.com",
                }
            },
        }
        ctx = ResolutionContext(connector=connector)
        spec = resolve_transport_spec(connector, context=ctx)
        assert spec["transport_type"] == "http"
        assert spec["base_url"] == "https://api.example.com"

    def test_transport_defaults_merged_into_named_transport(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transport_defaults": {
                "transport_type": "http",
                "headers": {"Accept": "application/json"},
            },
            "transports": {
                "api": {
                    "base_url": "https://api.example.com",
                    "headers": {"Authorization": "Bearer x"},
                }
            },
        }
        ctx = ResolutionContext(connector=connector)
        spec = resolve_transport_spec(connector, context=ctx)
        assert spec["transport_type"] == "http"
        assert spec["headers"] == {
            "Accept": "application/json",
            "Authorization": "Bearer x",
        }

    def test_transport_specific_value_overrides_default(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transport_defaults": {
                "headers": {"Authorization": "Bearer default"},
            },
            "transports": {
                "api": {
                    "transport_type": "http",
                    "base_url": "https://api.example.com",
                    "headers": {"Authorization": "Basic specific"},
                }
            },
        }
        ctx = ResolutionContext(connector=connector)
        spec = resolve_transport_spec(connector, context=ctx)
        assert spec["headers"]["Authorization"] == "Basic specific"

    def test_unknown_transport_ref_rejected(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {"api": {"transport_type": "http", "base_url": "https://x"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(KeyError, match="not in declared transports"):
            resolve_transport_spec(connector, transport_ref="other", context=ctx)

    def test_no_transports_block_rejected(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="has no `transports` block"):
            resolve_transport_spec({"connector_id": "demo"}, context=ctx)

    def test_no_default_transport_rejected(self):
        connector = {
            "connector_id": "demo",
            "transports": {"api": {"transport_type": "http", "base_url": "https://x"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(TransportSpecError, match="default_transport not declared"):
            resolve_transport_spec(connector, context=ctx)

    def test_missing_transport_type_rejected(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {"api": {"base_url": "https://x"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(TransportSpecError, match="transport_type"):
            resolve_transport_spec(connector, context=ctx)


# Transport kind registry (register / build / unregister lifecycle)
# ---------------------------------------------------------------------------


class TestTransportKindRegistry:
    @pytest.fixture(autouse=True)
    def _restore_registry(self):
        # The registry is module-level; a test that fails mid-flight could
        # leave state behind and corrupt sibling tests. Snapshot before,
        # restore after — irrespective of what the test body did.
        from cdk.transport_factory import _TRANSPORT_KINDS

        snapshot = dict(_TRANSPORT_KINDS)
        try:
            yield
        finally:
            _TRANSPORT_KINDS.clear()
            _TRANSPORT_KINDS.update(snapshot)

    def test_built_in_kinds_registered_at_import_time(self):
        kinds = registered_transport_kinds()
        assert "sqlalchemy" in kinds
        assert "adbc" in kinds
        assert "http" in kinds

    def test_register_rejects_empty_kind(self):
        with pytest.raises(ValueError, match="non-empty string"):
            register_transport_kind(
                "", resolve_spec=MagicMock(), build_from_spec=AsyncMock()
            )

    def test_register_rejects_non_string_kind(self):
        with pytest.raises(ValueError, match="non-empty string"):
            register_transport_kind(
                42,  # type: ignore[arg-type]
                resolve_spec=MagicMock(),
                build_from_spec=AsyncMock(),
            )

    def test_register_rejects_non_callable_phases(self):
        # Non-callable phases fail loudly at registration so the bug is
        # near the registration site, not deep in build_transport.
        with pytest.raises(TypeError, match="callable"):
            register_transport_kind(
                "test_bad",
                resolve_spec=None,  # type: ignore[arg-type]
                build_from_spec=AsyncMock(),
            )
        with pytest.raises(TypeError, match="callable"):
            register_transport_kind(
                "test_bad",
                resolve_spec=MagicMock(),
                build_from_spec="nope",  # type: ignore[arg-type]
            )

    def test_re_registering_existing_kind_rejected(self):
        # Use a throwaway kind rather than "http" — if the assertion regex
        # ever stops matching, registering against a built-in would silently
        # overwrite it for the rest of the suite.
        register_transport_kind(
            "test_dup_kind", resolve_spec=MagicMock(), build_from_spec=AsyncMock()
        )
        with pytest.raises(ValueError, match="already registered"):
            register_transport_kind(
                "test_dup_kind",
                resolve_spec=MagicMock(),
                build_from_spec=AsyncMock(),
            )

    def test_unregister_unknown_kind_raises(self):
        with pytest.raises(KeyError, match="not registered"):
            unregister_transport_kind("nope-not-a-kind")

    @pytest.mark.asyncio
    async def test_register_build_unregister_cycle(self):
        # Full lifecycle: a plugin-style registration teaches the engine a
        # new kind; build_transport runs its resolve phase then its build
        # phase; unregister cleans up; afterwards the kind is rejected.
        sentinel = object()
        resolve_phase = MagicMock(
            return_value={"transport_type": "test_kind", "marker": "value"}
        )
        build_phase = AsyncMock(return_value=sentinel)
        register_transport_kind(
            "test_kind", resolve_spec=resolve_phase, build_from_spec=build_phase
        )
        try:
            assert "test_kind" in registered_transport_kinds()

            connector = {
                "connector_id": "demo",
                "default_transport": "api",
                "transports": {
                    "api": {"transport_type": "test_kind", "marker": "value"}
                },
            }
            ctx = ResolutionContext(connector=connector)
            result = await build_transport(connector, context=ctx)
            assert result is sentinel
            resolve_phase.assert_called_once()
            build_phase.assert_awaited_once()
            (resolved,) = build_phase.await_args.args
            assert resolved["transport_type"] == "test_kind"
            assert resolved["marker"] == "value"
        finally:
            unregister_transport_kind("test_kind")

        assert "test_kind" not in registered_transport_kinds()
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {"api": {"transport_type": "test_kind"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(NotImplementedError, match="Unsupported transport_type"):
            await build_transport(connector, context=ctx)


# ---------------------------------------------------------------------------
# TransportSpecError raised by per-kind resolve phases
# ---------------------------------------------------------------------------


def _resolver(ctx: ResolutionContext = None):
    from cdk.derived_functions import DEFAULT_FUNCTIONS
    from cdk.resolver import Resolver

    return Resolver(ctx or ResolutionContext(), functions=DEFAULT_FUNCTIONS)


class TestSQLAlchemySpecValidation:
    def test_missing_driver_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match="driver"):
            resolve_sqlalchemy_spec(
                {"dsn": {"kind": "url_template", "template": "x://h", "bindings": {}}},
                resolver=_resolver(),
            )

    @pytest.mark.parametrize("dsn", [None, "postgresql+asyncpg://user:pw@host/db"])
    def test_non_structured_dsn_raises_transport_spec_error(self, dsn):
        # A missing dsn and a legacy flat-string dsn hit the same branch:
        # the contract requires the structured object.
        spec = {"driver": "postgresql+asyncpg"}
        if dsn is not None:
            spec["dsn"] = dsn
        with pytest.raises(TransportSpecError, match="must be the structured"):
            resolve_sqlalchemy_spec(spec, resolver=_resolver())

    def test_non_object_options_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match=r"`options` must be an object"):
            resolve_sqlalchemy_spec(
                {
                    "driver": "postgresql+asyncpg",
                    "dsn": {
                        "kind": "url_template",
                        "template": "postgresql+asyncpg://h/db",
                        "bindings": {},
                    },
                    "options": "pool_size=5",
                },
                resolver=_resolver(),
            )

    def test_resolved_payload_renders_dsn_and_engine_kwargs(self):
        # Pin the JSON-safe worker payload: bindings rendered through the
        # resolver with their declared encodings, options translated to
        # engine kwargs with the pool_pre_ping default applied.
        ctx = ResolutionContext(
            connection={
                "parameters": {"host": "db.example.test", "password": "p@ss/word"}
            }
        )
        resolved = resolve_sqlalchemy_spec(
            {
                "driver": "postgresql+asyncpg",
                "dsn": {
                    "kind": "url_template",
                    "template": "postgresql+asyncpg://user:{password}@{host}:5432/app",
                    "bindings": {
                        "host": {
                            "value": {"ref": "connection.parameters.host"},
                            "encoding": "host",
                        },
                        "password": {
                            "value": {"ref": "connection.parameters.password"},
                            "encoding": "url_userinfo",
                        },
                    },
                },
                "options": {"pool_size": 5, "max_overflow": 2},
            },
            resolver=_resolver(ctx),
        )
        assert resolved == {
            "transport_type": "sqlalchemy",
            "driver": "postgresql+asyncpg",
            "dsn": (
                "postgresql+asyncpg://user:p%40ss%2Fword" "@db.example.test:5432/app"
            ),
            "tls": None,
            "engine_kwargs": {
                "pool_size": 5,
                "max_overflow": 2,
                "pool_pre_ping": True,
            },
        }

    def test_unsupported_dsn_kind_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match="Unsupported dsn.kind"):
            resolve_sqlalchemy_spec(
                {"driver": "postgresql+asyncpg", "dsn": {"kind": "unknown"}},
                resolver=_resolver(),
            )

    def test_empty_dsn_template_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match="dsn.template"):
            resolve_sqlalchemy_spec(
                {
                    "driver": "postgresql+asyncpg",
                    "dsn": {"kind": "url_template", "template": ""},
                },
                resolver=_resolver(),
            )

    def test_binding_missing_encoding_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match="requires both"):
            resolve_sqlalchemy_spec(
                {
                    "driver": "postgresql+asyncpg",
                    "dsn": {
                        "kind": "url_template",
                        "template": "{host}",
                        "bindings": {"host": {"value": "localhost"}},
                    },
                },
                resolver=_resolver(),
            )

    def test_unknown_binding_encoding_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match="unknown encoding"):
            resolve_sqlalchemy_spec(
                {
                    "driver": "postgresql+asyncpg",
                    "dsn": {
                        "kind": "url_template",
                        "template": "{host}",
                        "bindings": {"host": {"value": "localhost", "encoding": "bad"}},
                    },
                },
                resolver=_resolver(),
            )

    def test_binding_resolved_to_none_raises_transport_spec_error(self):
        ctx = ResolutionContext(connection={"parameters": {"host": None}})
        with pytest.raises(TransportSpecError, match="resolved value is None"):
            resolve_sqlalchemy_spec(
                {
                    "driver": "postgresql+asyncpg",
                    "dsn": {
                        "kind": "url_template",
                        "template": "{host}",
                        "bindings": {
                            "host": {
                                "value": {"ref": "connection.parameters.host"},
                                "encoding": "host",
                            }
                        },
                    },
                },
                resolver=_resolver(ctx),
            )


class TestAdbcSpecValidation:
    def test_missing_driver_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match="driver"):
            resolve_adbc_spec({}, resolver=_resolver())

    def test_no_dsn_or_db_kwargs_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match="at least one"):
            resolve_adbc_spec({"driver": "postgresql"}, resolver=_resolver())


class TestHttpSpecValidation:
    def test_missing_base_url_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match="base_url"):
            resolve_http_spec({}, resolver=_resolver())

    def test_empty_base_url_raises_transport_spec_error(self):
        ctx = ResolutionContext(connection={"parameters": {"url": ""}})
        with pytest.raises(TransportSpecError, match="non-empty string"):
            resolve_http_spec(
                {"base_url": {"ref": "connection.parameters.url"}},
                resolver=_resolver(ctx),
            )

    @pytest.mark.parametrize(
        "rate_limit",
        [{"max_requests": 10}, {"time_window_seconds": 60}],
        ids=["max_requests_only", "time_window_only"],
    )
    def test_rate_limit_missing_one_field_raises_transport_spec_error(self, rate_limit):
        with pytest.raises(TransportSpecError, match="both"):
            resolve_http_spec(
                {
                    "base_url": "https://api.example.com",
                    "rate_limit": rate_limit,
                },
                resolver=_resolver(),
            )

    def test_non_object_headers_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match=r"`headers` must be an object"):
            resolve_http_spec(
                {
                    "base_url": "https://api.example.com",
                    "headers": ["Authorization: Bearer x"],
                },
                resolver=_resolver(),
            )

    def test_non_object_rate_limit_raises_transport_spec_error(self):
        with pytest.raises(TransportSpecError, match=r"`rate_limit` must be an object"):
            resolve_http_spec(
                {
                    "base_url": "https://api.example.com",
                    "rate_limit": [10, 60],
                },
                resolver=_resolver(),
            )

    def test_resolved_payload_pins_http_contract(self):
        # Pin the JSON-safe worker payload: base_url resolved through the
        # resolver and trailing-slash-stripped, header values resolved
        # with None-valued entries dropped, the timeout default applied,
        # and rate_limit normalized to ints.
        ctx = ResolutionContext(
            connection={
                "parameters": {
                    "url": "https://api.example.test/",
                    "token": "tok-1",
                    "optional": None,
                }
            }
        )
        resolved = resolve_http_spec(
            {
                "base_url": {"ref": "connection.parameters.url"},
                "headers": {
                    "Authorization": {"ref": "connection.parameters.token"},
                    "X-Optional": {"ref": "connection.parameters.optional"},
                    "Accept": "application/json",
                },
                "rate_limit": {"max_requests": 10, "time_window_seconds": 60},
            },
            resolver=_resolver(ctx),
        )
        assert resolved == {
            "transport_type": "http",
            "base_url": "https://api.example.test",
            "headers": {"Authorization": "tok-1", "Accept": "application/json"},
            "timeout_seconds": 30.0,
            "rate_limit": {"max_requests": 10, "time_window_seconds": 60},
        }


# ---------------------------------------------------------------------------
# SQLAlchemy engine flavour — selected by the dialect's own async capability
# ---------------------------------------------------------------------------


class TestSqlAlchemyEngineFlavour:
    def test_dialect_capability_detection(self):
        from cdk.transport_factory import _dialect_is_async

        # The dialect class declares the capability — no hardcoded list.
        assert _dialect_is_async("postgresql+asyncpg://u:p@h:5432/db") is True
        assert _dialect_is_async("sqlite://") is False

    @pytest.mark.asyncio
    async def test_sync_only_dialect_builds_sync_engine(self):
        import asyncio

        from sqlalchemy.engine import Engine

        from cdk.transport_factory import build_sqlalchemy_from_spec

        # SQLite's stdlib driver is the in-process stand-in for a
        # sync-only dialect (the production case is Redshift's
        # redshift+redshift_connector). The SELECT 1 probe runs at
        # build time on a worker thread.
        transport = await build_sqlalchemy_from_spec(
            {
                "transport_type": "sqlalchemy",
                "driver": "sqlite+pysqlite",
                "dsn": "sqlite://",
                "tls": None,
                "engine_kwargs": {},
            }
        )
        try:
            assert transport.is_async is False
            assert isinstance(transport.engine, Engine)
            assert transport.dialect == "sqlite"
            assert transport.driver == "sqlite+pysqlite"
        finally:
            await asyncio.to_thread(transport.engine.dispose)

    @pytest.mark.asyncio
    async def test_sync_probe_failure_disposes_engine(self):
        from unittest.mock import patch

        from cdk.transport_factory import build_sqlalchemy_from_spec

        engine = MagicMock()
        engine.connect = MagicMock(side_effect=RuntimeError("probe failed"))
        with patch("cdk.transport_factory.create_engine", return_value=engine):
            with pytest.raises(RuntimeError, match="probe failed"):
                await build_sqlalchemy_from_spec(
                    {
                        "transport_type": "sqlalchemy",
                        "driver": "sqlite+pysqlite",
                        "dsn": "sqlite://",
                        "tls": None,
                        "engine_kwargs": {},
                    }
                )
        engine.dispose.assert_called_once()
