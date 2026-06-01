"""Tests for SQLAlchemy TLS materialization.

``_materialize_tls_for_driver`` resolves a connector's ``tls`` spec into
the value placed in the engine's ``connect_args["ssl"]``. Each driver
speaks its own native SSL vocabulary, so the dispatch is per-driver.
"""

from __future__ import annotations

import ssl as _ssl
from unittest.mock import MagicMock, patch

import pytest

from cdk.transport_factory import _materialize_tls_for_driver


def _resolver(mapping: dict | None = None) -> MagicMock:
    """Resolver that returns each ref verbatim, or via ``mapping``."""
    resolver = MagicMock()
    if mapping is None:
        resolver.resolve = MagicMock(side_effect=lambda v: v)
    else:
        resolver.resolve = MagicMock(side_effect=lambda v: mapping.get(v, v))
    return resolver


class TestMaterializeTlsForDriver:
    def test_no_tls_spec_returns_none(self):
        assert _materialize_tls_for_driver(
            "postgresql+asyncpg", None, _resolver()
        ) is None

    def test_spec_without_mode_returns_none(self):
        assert _materialize_tls_for_driver(
            "postgresql+asyncpg", {"ca_certificate": "x"}, _resolver()
        ) is None

    @pytest.mark.parametrize("mode", ["disable", "allow", "prefer", "require"])
    def test_postgres_non_verify_modes_pass_through_as_string(self, mode):
        # asyncpg accepts the libpq mode string directly for non-verify
        # modes.
        value = _materialize_tls_for_driver(
            "postgresql+asyncpg", {"mode": mode}, _resolver()
        )
        assert value == mode

    def test_postgres_verify_ca_builds_sslcontext(self):
        resolver = _resolver({"verify-ca": "verify-ca", "PEM-REF": "PEM-BUNDLE"})
        with patch(
            "cdk.transport_factory._ca_ssl_context", return_value="<ctx>"
        ) as ctx:
            value = _materialize_tls_for_driver(
                "postgresql+asyncpg",
                {"mode": "verify-ca", "ca_certificate": "PEM-REF"},
                resolver,
            )
        assert value == "<ctx>"
        ctx.assert_called_once_with("PEM-BUNDLE", check_hostname=False)

    def test_postgres_verify_full_requires_ca(self):
        with pytest.raises(ValueError):
            _materialize_tls_for_driver(
                "postgresql+asyncpg", {"mode": "verify-full"}, _resolver()
            )

    def test_mysql_disabled_returns_false(self):
        value = _materialize_tls_for_driver(
            "mysql+aiomysql", {"mode": "DISABLED"}, _resolver()
        )
        assert value is False

    def test_mysql_preferred_returns_sslcontext(self):
        # aiomysql does not accept native string modes; PREFERRED maps to
        # a non-verifying SSLContext.
        value = _materialize_tls_for_driver(
            "mysql+aiomysql", {"mode": "PREFERRED"}, _resolver()
        )
        assert isinstance(value, _ssl.SSLContext)
        assert value.verify_mode == _ssl.CERT_NONE

    def test_unknown_driver_with_ca_builds_sslcontext(self):
        resolver = _resolver({"some-mode": "some-mode", "PEM-REF": "PEM-BUNDLE"})
        with patch(
            "cdk.transport_factory._ca_ssl_context", return_value="<ctx>"
        ) as ctx:
            value = _materialize_tls_for_driver(
                "snowflake",
                {"mode": "some-mode", "ca_certificate": "PEM-REF"},
                resolver,
            )
        assert value == "<ctx>"
        ctx.assert_called_once_with("PEM-BUNDLE", check_hostname=False)

    def test_unknown_driver_without_ca_passes_mode_through(self):
        value = _materialize_tls_for_driver(
            "snowflake", {"mode": "require"}, _resolver()
        )
        assert value == "require"
