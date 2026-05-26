"""End-to-end TLS-mode threading tests.

The TLS-mode posture travels: transport_factory._materialize_tls_for_driver
-> SqlAlchemyTransport (tls_mode / tls_ca_bundle_present) -> ConnectionRuntime
(tls_mode / tls_ca_bundle_present properties) -> build_adbc_uri.

Each link has a direct test here so a regression at any layer fails
loudly. Existing test_adbc_registry.py covers the URI layer; this
file covers the transport / runtime layers and the close-path reset.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.shared.connection_runtime import ConnectionRuntime
from src.shared.transport_factory import (
    HttpTransport,
    SqlAlchemyTransport,
    _materialize_tls_for_driver,
)


class TestMaterializeTlsForDriverReturnsTuple:
    """``_materialize_tls_for_driver`` returns ``(value, mode, has_ca)``.
    The tuple's order and contents are load-bearing -- a future shuffle
    would propagate silently to SqlAlchemyTransport and then to the
    ADBC URI builder, demoting (or worse, downgrading) TLS without a
    test fail.
    """

    def test_no_tls_returns_all_nones(self):
        resolver = MagicMock()
        value, mode, has_ca = _materialize_tls_for_driver(
            "postgresql+asyncpg", None, resolver
        )
        assert value is None
        assert mode is None
        assert has_ca is False

    def test_pg_prefer_returns_mode_without_ca(self):
        resolver = MagicMock()
        resolver.resolve = MagicMock(side_effect=lambda v: v)
        tls_spec = {"mode": "prefer"}
        value, mode, has_ca = _materialize_tls_for_driver(
            "postgresql+asyncpg", tls_spec, resolver
        )
        # asyncpg accepts the libpq mode string for non-verify modes.
        assert value == "prefer"
        assert mode == "prefer"
        assert has_ca is False

    def test_pg_verify_ca_with_pem_returns_sslcontext_and_flag(self):
        resolver = MagicMock()
        resolver.resolve = MagicMock(
            side_effect=lambda v: "verify-ca" if v == "verify-ca" else "PEM-BUNDLE"
        )
        tls_spec = {"mode": "verify-ca", "ca_certificate": "PEM-BUNDLE-REF"}
        # Patch ssl.create_default_context so we don't try to parse the
        # stub PEM as a real cert.
        with patch("src.shared.transport_factory._ca_ssl_context", return_value="<ctx>"):
            value, mode, has_ca = _materialize_tls_for_driver(
                "postgresql+asyncpg", tls_spec, resolver
            )
        assert value == "<ctx>"
        assert mode == "verify-ca"
        assert has_ca is True

    def test_mysql_preferred_returns_mode(self):
        resolver = MagicMock()
        resolver.resolve = MagicMock(side_effect=lambda v: v)
        tls_spec = {"mode": "PREFERRED"}
        value, mode, has_ca = _materialize_tls_for_driver(
            "mysql+aiomysql", tls_spec, resolver
        )
        # MySQL materializer always returns False/SSLContext (not a
        # mode string), but the raw mode survives in the tuple.
        assert mode == "PREFERRED"
        assert has_ca is False


class TestSqlAlchemyTransportTlsFields:
    """``SqlAlchemyTransport`` carries ``tls_mode`` and
    ``tls_ca_bundle_present`` so ``ConnectionRuntime`` can expose them
    to the ADBC URI builder. Defaults preserve the pre-fix behavior."""

    def test_defaults_are_none_and_false(self):
        t = SqlAlchemyTransport(
            engine=MagicMock(),
            driver="postgresql+asyncpg",
            dialect="postgresql",
        )
        assert t.tls_mode is None
        assert t.tls_ca_bundle_present is False

    def test_explicit_values_survive(self):
        t = SqlAlchemyTransport(
            engine=MagicMock(),
            driver="postgresql+asyncpg",
            dialect="postgresql",
            tls_mode="require",
            tls_ca_bundle_present=True,
        )
        assert t.tls_mode == "require"
        assert t.tls_ca_bundle_present is True


class TestConnectionRuntimeTlsPropagation:
    """The runtime mirrors the transport's TLS posture so callers
    that already hold a runtime reference (source connector,
    destination handler) can read it without re-deriving from spec."""

    def _runtime(self):
        resolver = MagicMock()
        resolver.resolve = AsyncMock(return_value={})
        return ConnectionRuntime(
            raw_config={},
            connection_id="test-conn",
            connector_type="database",
            resolver=resolver,
            connector_definition={"transports": {"db": {}}},
        )

    @pytest.mark.asyncio
    async def test_materialize_copies_tls_fields_from_transport(self):
        runtime = self._runtime()
        fake_engine = MagicMock()
        fake_transport = SqlAlchemyTransport(
            engine=fake_engine,
            driver="postgresql+asyncpg",
            dialect="postgresql",
            tls_mode="require",
            tls_ca_bundle_present=True,
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=fake_transport),
        ):
            await runtime.materialize()
        assert runtime.tls_mode == "require"
        assert runtime.tls_ca_bundle_present is True

    @pytest.mark.asyncio
    async def test_close_resets_tls_fields(self):
        runtime = self._runtime()
        fake_engine = MagicMock()
        fake_engine.dispose = AsyncMock()
        fake_transport = SqlAlchemyTransport(
            engine=fake_engine,
            driver="postgresql+asyncpg",
            dialect="postgresql",
            tls_mode="verify-full",
            tls_ca_bundle_present=True,
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=fake_transport),
        ):
            await runtime.materialize()
        runtime.acquire()  # ref_count=1 so close actually disposes
        await runtime.close()
        assert runtime.tls_mode is None
        assert runtime.tls_ca_bundle_present is False

    @pytest.mark.asyncio
    async def test_http_transport_leaves_tls_fields_at_default(self):
        """An HTTP connector materializes a ``HttpTransport`` -- the
        runtime's TLS fields must stay at their defaults (the
        ``SqlAlchemyTransport`` branch is the only one that sets them).
        """
        resolver = MagicMock()
        resolver.resolve = AsyncMock(return_value={})
        runtime = ConnectionRuntime(
            raw_config={},
            connection_id="api-conn",
            connector_type="api",
            resolver=resolver,
            connector_definition={"transports": {"http": {}}},
        )
        fake_session = MagicMock()
        http_transport = HttpTransport(
            session=fake_session,
            base_url="https://example.com",
            headers={},
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=http_transport),
        ):
            await runtime.materialize()
        assert runtime.tls_mode is None
        assert runtime.tls_ca_bundle_present is False


class TestDestinationBuildAdbcUriReadsRuntime:
    """The destination handler's ``_build_adbc_uri`` must read TLS
    posture from ``self._runtime`` so its ADBC connection matches the
    SA-side TLS. Source-side coverage exists in
    ``test_source_database_adbc.py``; this asserts the destination
    side stays symmetric."""

    def _handler(self, tls_mode=None, tls_ca_bundle_present=False):
        from src.destination.connectors.database import DatabaseDestinationHandler

        h = DatabaseDestinationHandler()
        h._driver = "postgresql"
        url = SimpleNamespace(
            host="db.example.com",
            port=5432,
            username="u",
            password="pw",
            database="warehouse",
            query={},
            get_backend_name=lambda: "postgresql",
        )
        engine = MagicMock()
        engine.url = url
        h._engine = engine
        runtime = MagicMock()
        runtime.tls_mode = tls_mode
        runtime.tls_ca_bundle_present = tls_ca_bundle_present
        h._runtime = runtime
        return h

    def test_require_mode_embeds_sslmode_in_uri(self):
        h = self._handler(tls_mode="require")
        uri = h._build_adbc_uri()
        assert uri is not None
        assert "sslmode=require" in uri

    def test_verify_full_demotes_to_none(self):
        h = self._handler(tls_mode="verify-full", tls_ca_bundle_present=True)
        assert h._build_adbc_uri() is None

    def test_no_tls_emits_no_sslmode(self):
        h = self._handler(tls_mode=None)
        uri = h._build_adbc_uri()
        assert uri is not None
        assert "sslmode" not in uri

    def test_engine_none_returns_none(self):
        h = self._handler(tls_mode="require")
        h._engine = None
        assert h._build_adbc_uri() is None
