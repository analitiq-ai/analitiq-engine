"""Unit tests for database utilities."""

import ssl
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from src.shared.database_utils import is_ssl_handshake_error

from src.source.drivers.utils import (
    convert_python_to_db,
    convert_db_to_python,
    convert_record_for_db,
    convert_record_from_db,
    extract_values_for_columns,
    validate_datetime_conversion
)


class TestDatabaseTypeConversion:
    """Test database type conversion utilities."""

    def test_convert_datetime_strings_to_db(self):
        """Test converting datetime strings to database-compatible datetime objects."""
        # ISO format with Z timezone
        iso_z = "2025-01-15T10:00:00Z"
        result = convert_python_to_db(iso_z, "TIMESTAMPTZ")
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10

        # ISO format with timezone offset
        iso_offset = "2025-01-15T10:00:00+01:00"
        result = convert_python_to_db(iso_offset, "TIMESTAMPTZ")
        assert isinstance(result, datetime)

        # ISO format without timezone
        iso_simple = "2025-01-15T10:00:00"
        result = convert_python_to_db(iso_simple, "TIMESTAMP")
        assert isinstance(result, datetime)

    def test_convert_datetime_objects_from_db(self):
        """Test converting datetime objects to ISO strings."""
        dt = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        result = convert_db_to_python(dt)
        assert isinstance(result, str)
        assert result.startswith("2025-01-15T10:00:00")

    def test_convert_json_data_to_db(self):
        """Test converting dict/list to JSON strings."""
        # Dictionary
        data_dict = {"key": "value", "number": 42}
        result = convert_python_to_db(data_dict, "JSONB")
        assert isinstance(result, str)
        assert '"key":"value"' in result.replace(" ", "")

        # List
        data_list = ["item1", "item2", 123]
        result = convert_python_to_db(data_list, "JSONB")
        assert isinstance(result, str)
        assert "item1" in result

    def test_convert_none_values(self):
        """Test handling None values."""
        assert convert_python_to_db(None, "TEXT") is None
        assert convert_db_to_python(None) is None

    def test_convert_regular_values(self):
        """Test that regular values pass through unchanged."""
        # Strings that are not datetime-like
        regular_string = "just a string"
        assert convert_python_to_db(regular_string, "TEXT") == regular_string

        # Numbers
        assert convert_python_to_db(42, "INTEGER") == 42
        assert convert_python_to_db(3.14, "DECIMAL") == 3.14

        # Booleans
        assert convert_python_to_db(True, "BOOLEAN") is True
        assert convert_python_to_db(False, "BOOLEAN") is False

    def test_convert_record_for_db(self):
        """Test converting entire records for database writing."""
        record = {
            "id": 1,
            "name": "Test User",
            "created_at": "2025-01-15T10:00:00Z",
            "metadata": {"key": "value"},
            "is_active": True
        }

        column_types = {
            "id": "INTEGER",
            "name": "TEXT",
            "created_at": "TIMESTAMPTZ",
            "metadata": "JSONB",
            "is_active": "BOOLEAN"
        }

        result = convert_record_for_db(record, column_types)

        assert result["id"] == 1
        assert result["name"] == "Test User"
        assert isinstance(result["created_at"], datetime)
        assert isinstance(result["metadata"], str)  # JSON string
        assert result["is_active"] is True

    def test_convert_record_from_db(self):
        """Test converting records from database reading."""
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
        assert isinstance(result["created_at"], str)  # ISO string
        assert result["is_active"] is True

    def test_extract_values_for_columns(self):
        """Test extracting values in column order with conversion."""
        record = {
            "name": "Test User",
            "id": 1,
            "created_at": "2025-01-15T10:00:00Z",
            "metadata": {"key": "value"}
        }
        columns = ["id", "name", "created_at", "metadata"]
        column_types = {
            "id": "INTEGER",
            "name": "TEXT",
            "created_at": "TIMESTAMPTZ",
            "metadata": "JSONB"
        }

        values = extract_values_for_columns(record, columns, column_types)

        assert len(values) == 4
        assert values[0] == 1  # id
        assert values[1] == "Test User"  # name
        assert isinstance(values[2], datetime)  # created_at converted
        assert isinstance(values[3], str)  # metadata as JSON string

    def test_validate_datetime_conversion(self):
        """Test datetime string validation."""
        # Valid datetime strings
        assert validate_datetime_conversion("2025-01-15T10:00:00Z") is True
        assert validate_datetime_conversion("2025-01-15T10:00:00+01:00") is True
        assert validate_datetime_conversion("2025-01-15 10:00:00") is True

        # Invalid datetime strings
        assert validate_datetime_conversion("just text") is False
        assert validate_datetime_conversion("2025") is False
        assert validate_datetime_conversion("") is False
        assert validate_datetime_conversion(None) is False
        assert validate_datetime_conversion(123) is False

    def test_edge_cases(self):
        """Test edge cases and error handling."""
        # Invalid datetime string should return as-is for non-timestamp columns
        invalid_dt = "not-a-datetime-2025-01-15"
        result = convert_python_to_db(invalid_dt, "TEXT")
        assert result == invalid_dt

        # Short strings should not be processed as datetime
        short_string = "2025"
        result = convert_python_to_db(short_string, "TEXT")
        assert result == short_string

        # Empty values
        assert convert_python_to_db("", "TEXT") == ""
        assert convert_db_to_python("") == ""

    def test_preserve_existing_datetime_objects(self):
        """Test that existing datetime objects are preserved."""
        dt = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        result = convert_python_to_db(dt, "TIMESTAMPTZ")
        assert result is dt  # Should be the same object


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