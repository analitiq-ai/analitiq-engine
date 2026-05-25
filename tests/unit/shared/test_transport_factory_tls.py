"""TLS materialiser tests for the MySQL libpq alias map and PG modes.

Targets the in-scope changes from this PR: the libpq->MySQL ssl_mode
alias map. The wider ``test_transport_factory.py`` file currently
fails to import (references removed symbols from an earlier
refactor); these tests live in a separate file so they collect
independently of that breakage.
"""

from __future__ import annotations

import pytest

from src.shared.transport_factory import (
    _LIBPQ_TO_MYSQL_TLS_MODE,
    _materialize_tls_mysql,
)


class TestLibpqToMysqlAliases:
    @pytest.mark.parametrize(
        "input_mode,expected_native",
        [
            ("prefer", "PREFERRED"),
            ("Prefer", "PREFERRED"),
            ("PREFER", "PREFERRED"),
            ("require", "REQUIRED"),
            ("disable", "DISABLED"),
            ("verify-ca", "VERIFY_CA"),
            ("verify-full", "VERIFY_IDENTITY"),
        ],
    )
    def test_libpq_aliases_resolve(self, input_mode, expected_native):
        # The map normalises to upper-case keys.
        canonical = _LIBPQ_TO_MYSQL_TLS_MODE.get(input_mode.upper())
        assert canonical == expected_native

    def test_disabled_returns_false(self):
        # MySQL "off" semantics — aiomysql wants ``False`` for no TLS.
        assert _materialize_tls_mysql("disable", None) is False
        assert _materialize_tls_mysql("DISABLED", None) is False

    def test_prefer_alias_negotiates_tls_without_ca(self):
        ctx = _materialize_tls_mysql("prefer", None)
        # Returns an SSLContext (not False, not None).
        assert ctx is not False
        assert ctx is not None
        # No CA bundle → no hostname check, no cert verification.
        assert ctx.check_hostname is False

    def test_verify_full_requires_ca(self):
        with pytest.raises(ValueError, match="ca_certificate"):
            _materialize_tls_mysql("verify-full", None)

    def test_allow_is_not_aliased(self):
        """``allow`` is the libpq mode without a MySQL equivalent --
        mapping it would silently flip the user's intent (libpq
        ``allow`` prefers plaintext, MySQL ``PREFERRED`` prefers TLS).
        Should reject loudly rather than guess."""
        with pytest.raises(ValueError, match="not recognized"):
            _materialize_tls_mysql("allow", None)

    def test_unknown_mode_raises_with_full_vocabulary(self):
        with pytest.raises(ValueError) as ei:
            _materialize_tls_mysql("definitely_not_a_mode", None)
        msg = str(ei.value)
        assert "DISABLED" in msg and "PREFERRED" in msg and "REQUIRED" in msg
