"""Unit tests for the shared ADBC error type."""

from __future__ import annotations

from cdk.adbc_registry import AdbcConfigurationError


class TestErrorClass:
    def test_is_runtime_error_subclass(self):
        # Callers ``except AdbcConfigurationError`` to classify fatal.
        assert issubclass(AdbcConfigurationError, RuntimeError)
