"""Unit tests for read-side value conversions in ``src.shared.database_utils``.

The legacy ``is_ssl_handshake_error`` helper this file used to cover was
intentionally replaced with the narrower private ``_has_ssl_error_in_chain``
(see the comment in the module — drivers' network-failure subclasses are
excluded on purpose now). Its behaviour is exercised end-to-end in
``tests/unit/shared/test_create_database_engine.py`` via the plaintext-retry
path, so we only keep the value-conversion coverage here.
"""

from datetime import datetime, timezone

from src.shared.database_utils import (
    convert_db_to_python,
    convert_record_from_db,
)


class TestConvertDbToPython:
    """``convert_db_to_python`` normalises driver-native values for JSON."""

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


class TestConvertRecordFromDb:
    """``convert_record_from_db`` applies the per-value conversion to a dict."""

    def test_converts_datetime_fields(self):
        record = {
            "id": 1,
            "name": "Test User",
            "created_at": datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            "is_active": True,
        }
        result = convert_record_from_db(record)
        assert result["id"] == 1
        assert result["name"] == "Test User"
        assert isinstance(result["created_at"], str)
        assert result["is_active"] is True

    def test_empty_record(self):
        assert convert_record_from_db({}) == {}
