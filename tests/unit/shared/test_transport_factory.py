"""Tests for :mod:`src.shared.transport_factory`.

These cover the spec-resolution surface — derived-value materialisation,
SSL dict to context conversion, transport spec resolution, and the
synchronous validation paths in :func:`build_sqlalchemy_transport` and
:func:`build_http_transport`. The actual engine/session probes are not
exercised here (they would require a live database / HTTP endpoint);
they are covered by the integration fixtures in ``tests/fixtures/``.
"""

from __future__ import annotations

import ssl
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engine.resolver import ResolutionContext
from src.shared.transport_factory import (
    HttpTransport,
    SqlAlchemyTransport,
    _materialize_derived,
    _materialize_ssl_arg,
    _ssl_dict_to_context,
    build_http_transport,
    build_sqlalchemy_transport,
    build_transport,
    resolve_transport_spec,
)


# ---------------------------------------------------------------------------
# _ssl_dict_to_context
# ---------------------------------------------------------------------------


class TestSSLDictToContext:
    def test_cert_none_with_no_hostname_check(self):
        ctx = _ssl_dict_to_context(
            {"verify_mode": "CERT_NONE", "check_hostname": False}
        )
        assert ctx.verify_mode == ssl.CERT_NONE
        assert ctx.check_hostname is False

    def test_cert_required_with_hostname_check(self):
        ctx = _ssl_dict_to_context(
            {"verify_mode": "CERT_REQUIRED", "check_hostname": True}
        )
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is True

    def test_cert_required_without_hostname_check(self):
        ctx = _ssl_dict_to_context(
            {"verify_mode": "CERT_REQUIRED", "check_hostname": False}
        )
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is False

    def test_cert_none_with_hostname_check_true_raises(self):
        # CPython forbids CERT_NONE + check_hostname=True. Surface the
        # contradiction loudly rather than silently flipping one of them.
        with pytest.raises(ValueError, match="cannot set verify_mode=CERT_NONE"):
            _ssl_dict_to_context(
                {"verify_mode": "CERT_NONE", "check_hostname": True}
            )

    def test_unknown_verify_mode_rejected(self):
        with pytest.raises(ValueError, match="not recognized"):
            _ssl_dict_to_context({"verify_mode": "CERT_TYPO"})

    def test_check_hostname_must_be_bool(self):
        with pytest.raises(TypeError, match="check_hostname must be a boolean"):
            _ssl_dict_to_context({"check_hostname": "yes"})


class TestMaterializeSSLArg:
    def test_passthrough_bools(self):
        assert _materialize_ssl_arg(True) is True
        assert _materialize_ssl_arg(False) is False

    def test_passthrough_strings(self):
        # asyncpg accepts native libpq names like "prefer" / "require".
        assert _materialize_ssl_arg("prefer") == "prefer"

    def test_passthrough_ssl_context(self):
        ctx = ssl.create_default_context()
        assert _materialize_ssl_arg(ctx) is ctx

    def test_dict_translated_to_ssl_context(self):
        out = _materialize_ssl_arg(
            {"verify_mode": "CERT_NONE", "check_hostname": False}
        )
        assert isinstance(out, ssl.SSLContext)
        assert out.verify_mode == ssl.CERT_NONE

    def test_none_passes_through(self):
        assert _materialize_ssl_arg(None) is None

    def test_unsupported_type_rejected(self):
        with pytest.raises(TypeError, match="Unsupported ssl arg type"):
            _materialize_ssl_arg(42)


# ---------------------------------------------------------------------------
# _materialize_derived (fixpoint resolution + cycle detection)
# ---------------------------------------------------------------------------


class TestMaterializeDerived:
    def test_simple_chain_resolves(self):
        connector = {
            "slug": "demo",
            "derived": {
                "creds": {
                    "function": "basic_auth",
                    "input": {
                        "username": {"ref": "connection.parameters.user"},
                        "password": {"ref": "secrets.password"},
                    },
                },
            },
        }
        ctx = ResolutionContext(
            connector=connector,
            connection={"parameters": {"user": "u"}},
            secrets={"password": "p"},
        )
        out = _materialize_derived(connector, ctx)
        assert "creds" in out

    def test_chain_of_three_resolves_in_multiple_passes(self):
        # b depends on a, c depends on b. The fixpoint loop must keep
        # iterating until everything resolves, not stop at two passes.
        connector = {
            "slug": "demo",
            "derived": {
                "a": {"literal": "first"},
                "b": {"template": "${derived.a}-second"},
                "c": {"template": "${derived.b}-third"},
            },
        }
        ctx = ResolutionContext(connector=connector)
        out = _materialize_derived(connector, ctx)
        assert out == {"a": "first", "b": "first-second", "c": "first-second-third"}

    def test_unresolvable_reference_raises_with_name(self):
        connector = {
            "slug": "demo",
            "derived": {"a": {"ref": "connection.parameters.missing"}},
        }
        ctx = ResolutionContext(connector=connector, connection={"parameters": {}})
        with pytest.raises(KeyError, match="cannot be resolved"):
            _materialize_derived(connector, ctx)

    def test_circular_dependency_raises(self):
        connector = {
            "slug": "demo",
            "derived": {
                "a": {"ref": "derived.b"},
                "b": {"ref": "derived.a"},
            },
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(KeyError, match="cannot be resolved"):
            _materialize_derived(connector, ctx)

    def test_empty_derived_block_returns_empty_dict(self):
        ctx = ResolutionContext()
        assert _materialize_derived({}, ctx) == {}
        assert _materialize_derived({"derived": {}}, ctx) == {}

    def test_derived_must_be_object(self):
        ctx = ResolutionContext()
        with pytest.raises(TypeError, match="`derived` must be an object"):
            _materialize_derived({"slug": "x", "derived": ["not", "an", "object"]}, ctx)


# ---------------------------------------------------------------------------
# resolve_transport_spec
# ---------------------------------------------------------------------------


class TestResolveTransportSpec:
    def test_default_transport_used_when_ref_not_given(self):
        connector = {
            "slug": "demo",
            "default_transport": "api",
            "transports": {
                "api": {"kind": "http", "base_url": "https://api.example.com"}
            },
        }
        ctx = ResolutionContext(connector=connector)
        spec = resolve_transport_spec(connector, context=ctx)
        assert spec == {"kind": "http", "base_url": "https://api.example.com"}

    def test_transport_defaults_merged_into_named_transport(self):
        connector = {
            "slug": "demo",
            "default_transport": "api",
            "transport_defaults": {
                "kind": "http",
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
        assert spec["kind"] == "http"
        assert spec["headers"] == {
            "Accept": "application/json",
            "Authorization": "Bearer x",
        }

    def test_transport_specific_value_overrides_default(self):
        connector = {
            "slug": "demo",
            "default_transport": "api",
            "transport_defaults": {
                "headers": {"Authorization": "Bearer default"},
            },
            "transports": {
                "api": {
                    "kind": "http",
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
            "slug": "demo",
            "default_transport": "api",
            "transports": {"api": {"kind": "http", "base_url": "https://x"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(KeyError, match="not in declared transports"):
            resolve_transport_spec(connector, transport_ref="other", context=ctx)

    def test_no_transports_block_rejected(self):
        ctx = ResolutionContext()
        with pytest.raises(ValueError, match="has no `transports` block"):
            resolve_transport_spec({"slug": "demo"}, context=ctx)

    def test_no_default_transport_rejected(self):
        connector = {
            "slug": "demo",
            "transports": {"api": {"kind": "http", "base_url": "https://x"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(ValueError, match="`default_transport` not declared"):
            resolve_transport_spec(connector, context=ctx)

    def test_derived_values_available_to_template(self):
        connector = {
            "slug": "demo",
            "default_transport": "api",
            "derived": {
                "url_user": {
                    "function": "url_encode",
                    "input": {"ref": "connection.parameters.user"},
                }
            },
            "transports": {
                "api": {
                    "kind": "sqlalchemy",
                    "driver": "postgresql+asyncpg",
                    "dsn": {
                        "template": (
                            "postgresql+asyncpg://${derived.url_user}@h:5432/d"
                        )
                    },
                }
            },
        }
        ctx = ResolutionContext(
            connector=connector,
            connection={"parameters": {"user": "a@b"}},
        )
        spec = resolve_transport_spec(connector, context=ctx)
        # @ encoded as %40
        assert spec["dsn"] == "postgresql+asyncpg://a%40b@h:5432/d"


# ---------------------------------------------------------------------------
# build_sqlalchemy_transport (sync validation paths only)
# ---------------------------------------------------------------------------


class TestBuildSqlAlchemyTransport:
    @pytest.mark.asyncio
    async def test_missing_driver_rejected(self):
        with pytest.raises(ValueError, match="requires `driver`"):
            await build_sqlalchemy_transport(
                {"kind": "sqlalchemy", "dsn": "postgresql+asyncpg://u:p@h/d"}
            )

    @pytest.mark.asyncio
    async def test_missing_dsn_rejected(self):
        with pytest.raises(ValueError, match="`dsn` must resolve"):
            await build_sqlalchemy_transport(
                {"kind": "sqlalchemy", "driver": "postgresql+asyncpg"}
            )

    @pytest.mark.asyncio
    async def test_connect_args_must_be_object(self):
        with pytest.raises(TypeError, match="`connect_args` must be an object"):
            await build_sqlalchemy_transport(
                {
                    "kind": "sqlalchemy",
                    "driver": "postgresql+asyncpg",
                    "dsn": "postgresql+asyncpg://u:p@h/d",
                    "connect_args": "nope",
                }
            )

    @pytest.mark.asyncio
    async def test_options_must_be_object(self):
        with pytest.raises(TypeError, match="`options` must be an object"):
            await build_sqlalchemy_transport(
                {
                    "kind": "sqlalchemy",
                    "driver": "postgresql+asyncpg",
                    "dsn": "postgresql+asyncpg://u:p@h/d",
                    "options": "nope",
                }
            )

    @pytest.mark.asyncio
    async def test_returns_sqlalchemy_transport_with_base_dialect(self):
        # Mock the engine creation + probe so this is a unit test, not
        # an integration test.
        fake_engine = MagicMock()

        @ensure_async
        async def _fake_begin():
            yield AsyncMock()

        # The probe path: engine.connect() context manager returning
        # something with execute() coroutine.
        connect_cm = MagicMock()
        connect_cm.__aenter__ = AsyncMock(
            return_value=MagicMock(execute=AsyncMock())
        )
        connect_cm.__aexit__ = AsyncMock(return_value=False)
        fake_engine.connect = MagicMock(return_value=connect_cm)
        fake_engine.dispose = AsyncMock()

        with patch(
            "src.shared.transport_factory.create_async_engine",
            return_value=fake_engine,
        ):
            transport = await build_sqlalchemy_transport(
                {
                    "kind": "sqlalchemy",
                    "driver": "postgresql+asyncpg",
                    "dsn": "postgresql+asyncpg://u:p@h:5432/d",
                }
            )
        assert isinstance(transport, SqlAlchemyTransport)
        assert transport.dialect == "postgresql"
        assert transport.driver == "postgresql+asyncpg"


# ---------------------------------------------------------------------------
# build_http_transport
# ---------------------------------------------------------------------------


def ensure_async(fn):
    """No-op decorator placeholder used for clarity in test mocks."""
    return fn


class TestBuildHttpTransport:
    @pytest.mark.asyncio
    async def test_missing_base_url_rejected(self):
        with pytest.raises(ValueError, match="`base_url` must resolve"):
            await build_http_transport({"kind": "http"})

    @pytest.mark.asyncio
    async def test_headers_must_be_object(self):
        with pytest.raises(TypeError, match="`headers` must be an object"):
            await build_http_transport(
                {"kind": "http", "base_url": "https://x", "headers": "nope"}
            )

    @pytest.mark.asyncio
    async def test_rate_limit_must_be_object(self):
        with pytest.raises(TypeError, match="`rate_limit` must be an object"):
            await build_http_transport(
                {"kind": "http", "base_url": "https://x", "rate_limit": "nope"}
            )

    @pytest.mark.asyncio
    async def test_rate_limit_requires_both_keys_or_neither(self):
        # The reviewer flagged the dual-key alias (`time_window` vs
        # `time_window_seconds`); we also reject half-specified configs.
        with pytest.raises(ValueError, match="requires both"):
            await build_http_transport(
                {
                    "kind": "http",
                    "base_url": "https://x",
                    "rate_limit": {"max_requests": 10},
                }
            )
        with pytest.raises(ValueError, match="requires both"):
            await build_http_transport(
                {
                    "kind": "http",
                    "base_url": "https://x",
                    "rate_limit": {"time_window_seconds": 60},
                }
            )

    @pytest.mark.asyncio
    async def test_rate_limit_legacy_alias_rejected(self):
        # `time_window` (without `_seconds`) is no longer accepted; the
        # spec is the single source of truth and the alias enabled drift.
        with pytest.raises(ValueError, match="requires both"):
            await build_http_transport(
                {
                    "kind": "http",
                    "base_url": "https://x",
                    "rate_limit": {
                        "max_requests": 10,
                        "time_window": 60,
                    },
                }
            )

    @pytest.mark.asyncio
    async def test_headers_resolved_and_session_built(self):
        transport = await build_http_transport(
            {
                "kind": "http",
                "base_url": "https://api.example.com/",
                "headers": {
                    "Authorization": "Bearer abc",
                    "Accept": "application/json",
                },
                "timeout_seconds": 5,
                "rate_limit": {"max_requests": 10, "time_window_seconds": 60},
            }
        )
        try:
            assert isinstance(transport, HttpTransport)
            # base_url is right-stripped of trailing slash
            assert transport.base_url == "https://api.example.com"
            assert transport.headers["Authorization"] == "Bearer abc"
            assert transport.rate_limiter is not None
        finally:
            await transport.session.close()

    @pytest.mark.asyncio
    async def test_none_header_values_dropped(self):
        transport = await build_http_transport(
            {
                "kind": "http",
                "base_url": "https://x",
                "headers": {"X-Optional": None, "X-Always": "yes"},
            }
        )
        try:
            assert "X-Optional" not in transport.headers
            assert transport.headers["X-Always"] == "yes"
        finally:
            await transport.session.close()


# ---------------------------------------------------------------------------
# build_transport (dispatch)
# ---------------------------------------------------------------------------


class TestBuildTransportDispatch:
    @pytest.mark.asyncio
    async def test_http_kind_dispatches_to_http_builder(self):
        connector = {
            "slug": "demo",
            "default_transport": "api",
            "transports": {
                "api": {"kind": "http", "base_url": "https://api.example.com"}
            },
        }
        ctx = ResolutionContext(connector=connector)
        transport = await build_transport(connector, context=ctx)
        try:
            assert isinstance(transport, HttpTransport)
        finally:
            await transport.session.close()

    @pytest.mark.asyncio
    async def test_unknown_kind_rejected(self):
        connector = {
            "slug": "demo",
            "default_transport": "api",
            "transports": {"api": {"kind": "kafka", "base_url": "x"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(NotImplementedError, match="Unsupported transport kind"):
            await build_transport(connector, context=ctx)

    @pytest.mark.asyncio
    async def test_missing_kind_rejected(self):
        connector = {
            "slug": "demo",
            "default_transport": "api",
            "transports": {"api": {"base_url": "x"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(ValueError, match="missing `kind`"):
            await build_transport(connector, context=ctx)
