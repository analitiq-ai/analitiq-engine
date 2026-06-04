"""TLS handling across the resolve/build split.

The trusted side resolves ``tls.mode`` / ``tls.ca_certificate`` to plain
strings (``_resolve_tls_mode``) — JSON-safe, bootstrap-ready. The build
side turns them into the driver's connect argument through the connector
dialect's ``build_tls_connect_arg`` hook; the per-driver SSL vocabularies
live in the connector packages and are tested there. Here we cover the
CDK machinery: resolution, hook wiring, the no-dialect failure, and the
shared ``ca_ssl_context`` helper.
"""

from __future__ import annotations

import ssl as _ssl
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cdk.sql.dialects import SqlDialect
from cdk.sql.exceptions import UnsupportedDialectOperationError
from cdk.transport_factory import (
    _resolve_tls_mode,
    build_sqlalchemy_from_spec,
    ca_ssl_context,
)


def _resolver(mapping: dict | None = None) -> MagicMock:
    """Resolver that returns each ref verbatim, or via ``mapping``."""
    resolver = MagicMock()
    if mapping is None:
        resolver.resolve = MagicMock(side_effect=lambda v: v)
    else:
        resolver.resolve = MagicMock(side_effect=lambda v: mapping.get(v, v))
    return resolver


class TestResolveTlsMode:
    def test_no_spec_resolves_to_none(self):
        assert _resolve_tls_mode(None, _resolver()) == (None, None)

    def test_spec_without_mode_resolves_to_none(self):
        assert _resolve_tls_mode({"ca_certificate": "x"}, _resolver()) == (
            None,
            None,
        )

    def test_mode_and_ca_resolve_to_plain_strings(self):
        resolver = _resolver({"MODE-REF": "verify-ca", "PEM-REF": "PEM-BUNDLE"})
        mode, ca = _resolve_tls_mode(
            {"mode": "MODE-REF", "ca_certificate": "PEM-REF"}, resolver
        )
        assert (mode, ca) == ("verify-ca", "PEM-BUNDLE")

    def test_missing_ca_ref_resolves_to_none(self):
        def raise_keyerror(v):
            if v == "PEM-REF":
                raise KeyError(v)
            return v

        resolver = MagicMock()
        resolver.resolve = MagicMock(side_effect=raise_keyerror)
        mode, ca = _resolve_tls_mode(
            {"mode": "require", "ca_certificate": "PEM-REF"}, resolver
        )
        assert (mode, ca) == ("require", None)

    def test_non_mapping_spec_rejected(self):
        with pytest.raises(TypeError, match="tls"):
            _resolve_tls_mode("require", _resolver())


class _FixtureDialect(SqlDialect):
    """Dialect with a TLS vocabulary, standing in for a connector package."""

    name = "fixture"

    def build_tls_connect_arg(self, mode, ca_pem):
        if mode == "off":
            return None
        return f"ssl<{mode}:{ca_pem}>"


class TestBuildWiresTlsThroughDialect:
    @pytest.mark.asyncio
    async def test_tls_value_reaches_connect_args(self):
        captured = {}

        def fake_create(dsn, connect_args=None, **kw):
            captured["dsn"] = dsn
            captured["connect_args"] = connect_args
            engine = MagicMock()
            engine.connect = MagicMock(
                side_effect=RuntimeError("stop before probe")
            )
            engine.dispose = AsyncMock()
            return engine

        with patch(
            "cdk.transport_factory.create_async_engine", side_effect=fake_create
        ):
            with pytest.raises(RuntimeError, match="stop before probe"):
                await build_sqlalchemy_from_spec(
                    {
                        "transport_type": "sqlalchemy",
                        "driver": "postgresql+asyncpg",
                        "dsn": "postgresql+asyncpg://u:p@h:5432/db",
                        "tls": {"mode": "require", "ca_pem": None},
                        "engine_kwargs": {},
                    },
                    sql_dialect=_FixtureDialect(),
                )
        assert captured["connect_args"] == {"ssl": "ssl<require:None>"}

    @pytest.mark.asyncio
    async def test_hook_returning_none_omits_ssl_arg(self):
        captured = {}

        def fake_create(dsn, connect_args=None, **kw):
            captured["connect_args"] = connect_args
            engine = MagicMock()
            engine.connect = MagicMock(side_effect=RuntimeError("stop"))
            engine.dispose = AsyncMock()
            return engine

        with patch(
            "cdk.transport_factory.create_async_engine", side_effect=fake_create
        ):
            with pytest.raises(RuntimeError, match="stop"):
                await build_sqlalchemy_from_spec(
                    {
                        "transport_type": "sqlalchemy",
                        "driver": "postgresql+asyncpg",
                        "dsn": "postgresql+asyncpg://u:p@h:5432/db",
                        "tls": {"mode": "off", "ca_pem": None},
                        "engine_kwargs": {},
                    },
                    sql_dialect=_FixtureDialect(),
                )
        assert captured["connect_args"] == {}

    @pytest.mark.asyncio
    async def test_tls_without_dialect_fails_loudly(self):
        with pytest.raises(ValueError, match="no\\s+connector dialect"):
            await build_sqlalchemy_from_spec(
                {
                    "transport_type": "sqlalchemy",
                    "driver": "postgresql+asyncpg",
                    "dsn": "postgresql+asyncpg://u:p@h:5432/db",
                    "tls": {"mode": "require", "ca_pem": None},
                    "engine_kwargs": {},
                }
            )

    def test_base_dialect_hook_is_unsupported(self):
        with pytest.raises(UnsupportedDialectOperationError, match="build_tls_connect_arg"):
            SqlDialect().build_tls_connect_arg("require", None)


# A throwaway self-signed-style PEM is overkill here: ca_ssl_context only
# needs to be exercised for flag wiring with a real CA bundle. Use the
# certifi bundle shipped with the venv if importable; otherwise skip.
class TestCaSslContext:
    def test_flags(self):
        certifi = pytest.importorskip("certifi")
        pem = Path(certifi.where()).read_text()
        ctx = ca_ssl_context(pem, check_hostname=False)
        assert isinstance(ctx, _ssl.SSLContext)
        assert ctx.check_hostname is False
        assert ctx.verify_mode == _ssl.CERT_REQUIRED
        ctx2 = ca_ssl_context(pem, check_hostname=True)
        assert ctx2.check_hostname is True
