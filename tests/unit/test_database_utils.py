"""Unit tests for database utilities."""

import ssl
import pytest
from datetime import datetime, timezone

from src.shared.database_utils import (
    is_ssl_handshake_error,
    convert_db_to_python,
    convert_record_from_db,
)


class TestConvertDbToPython:
    """Test convert_db_to_python utility."""

    def test_none_value(self):
        assert convert_db_to_python(None) is None

    def test_datetime_to_iso_string(self):
        dt = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        result = convert_db_to_python(dt)
        assert isinstance(result, str)
        assert result.startswith("2025-01-15T10:00:00")

    def test_regular_values_passthrough(self):
        assert convert_db_to_python(42) == 42
        assert convert_db_to_python("hello") == "hello"
        assert convert_db_to_python(True) is True
        assert convert_db_to_python(3.14) == 3.14
        assert convert_db_to_python("") == ""


class TestConvertRecordFromDb:
    """Test convert_record_from_db utility."""

    def test_converts_datetime_fields(self):
        dt = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        record = {
            "id": 1,
            "name": "Test User",
            "created_at": dt,
            "is_active": True
        }
        result = convert_record_from_db(record)
        assert result["id"] == 1
        assert result["name"] == "Test User"
        assert isinstance(result["created_at"], str)
        assert result["is_active"] is True

    def test_empty_record(self):
        assert convert_record_from_db({}) == {}


class TestIsSSLHandshakeError:
    """Tests for is_ssl_handshake_error() helper."""

    def test_ssl_error_returns_true(self):
        exc = ssl.SSLError("SSL handshake failed")
        assert is_ssl_handshake_error(exc) is True

    def test_ssl_cert_verification_error_returns_false(self):
        exc = ssl.SSLCertVerificationError("certificate verify failed")
        assert is_ssl_handshake_error(exc) is False

    def test_connection_reset_error_returns_true(self):
        assert is_ssl_handshake_error(ConnectionResetError()) is True

    def test_connection_refused_error_returns_true(self):
        assert is_ssl_handshake_error(ConnectionRefusedError()) is True

    def test_bare_connection_error_returns_true(self):
        """asyncpg raises bare ConnectionError on SSL rejection."""
        assert is_ssl_handshake_error(ConnectionError("rejected SSL upgrade")) is True

    def test_generic_os_error_returns_false(self):
        assert is_ssl_handshake_error(OSError("generic")) is False

    def test_timeout_error_returns_false(self):
        assert is_ssl_handshake_error(TimeoutError("timed out")) is False

    def test_sqlalchemy_wrapping_ssl_error_via_cause(self):
        ssl_exc = ssl.SSLError("handshake failed")
        wrapper = Exception("connection failed")
        wrapper.__cause__ = ssl_exc
        assert is_ssl_handshake_error(wrapper) is True

    def test_sqlalchemy_wrapping_ssl_error_via_context(self):
        ssl_exc = ssl.SSLError("handshake failed")
        wrapper = Exception("connection failed")
        wrapper.__context__ = ssl_exc
        assert is_ssl_handshake_error(wrapper) is True

    def test_sqlalchemy_operational_error_with_orig(self):
        ssl_exc = ssl.SSLError("handshake failed")
        wrapper = Exception("operational error")
        wrapper.orig = ssl_exc
        assert is_ssl_handshake_error(wrapper) is True

    def test_cert_error_in_chain_returns_false(self):
        """Cert verification error anywhere in chain should return False."""
        cert_exc = ssl.SSLCertVerificationError("cert verify failed")
        wrapper = Exception("connection failed")
        wrapper.__cause__ = cert_exc
        assert is_ssl_handshake_error(wrapper) is False

    def test_cert_error_after_connection_reset_returns_false(self):
        """Cert verification error later in the chain takes precedence."""
        reset_exc = ConnectionResetError("connection reset")
        cert_exc = ssl.SSLCertVerificationError("cert verify failed")
        wrapper = Exception("operational error")
        wrapper.orig = reset_exc
        wrapper.__cause__ = cert_exc
        assert is_ssl_handshake_error(wrapper) is False

    def test_cycle_in_exception_chain_no_infinite_loop(self):
        exc_a = Exception("a")
        exc_b = Exception("b")
        exc_a.__cause__ = exc_b
        exc_b.__cause__ = exc_a
        # Should terminate without hanging
        assert is_ssl_handshake_error(exc_a) is False
