"""Unit tests for the native-Arrow client registry."""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from src.shared import native_arrow_clients
from src.shared.native_arrow_clients import (
    _NATIVE_ARROW_IMPORT_FAILED,
    _NATIVE_ARROW_MODULES,
    load_native_arrow_module,
    native_arrow_flag_enabled,
    native_arrow_supported,
)


@pytest.fixture(autouse=True)
def _disable_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ADBC_FAST_PATH", raising=False)


class TestRegistration:
    def test_clickhouse_is_registered(self):
        assert "clickhouse" in _NATIVE_ARROW_MODULES
        assert _NATIVE_ARROW_MODULES["clickhouse"] == "clickhouse_connect"

    def test_native_arrow_supported_is_case_insensitive(self):
        assert native_arrow_supported("clickhouse") is True
        assert native_arrow_supported("ClickHouse") is True
        assert native_arrow_supported("CLICKHOUSE") is True

    def test_native_arrow_supported_rejects_unknown(self):
        assert native_arrow_supported("postgresql") is False
        assert native_arrow_supported("") is False


class TestFlag:
    def test_shares_adbc_flag(self, monkeypatch):
        # Same env var as ADBC — flipping it on enables both arms so
        # the user has one knob, not two.
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        assert native_arrow_flag_enabled() is True

    def test_off_by_default(self):
        assert native_arrow_flag_enabled() is False


class TestLoadModule:
    def test_empty_dialect_returns_sentinel(self):
        assert load_native_arrow_module("") is _NATIVE_ARROW_IMPORT_FAILED

    def test_unknown_dialect_returns_sentinel(self):
        assert load_native_arrow_module("postgresql") is _NATIVE_ARROW_IMPORT_FAILED

    def test_known_dialect_imports_module(self):
        with patch.object(native_arrow_clients.importlib, "import_module") as imp:
            imp.return_value = "stub"
            assert load_native_arrow_module("clickhouse") == "stub"
        imp.assert_called_once_with("clickhouse_connect")

    def test_import_error_returns_sentinel(self):
        with patch.object(
            native_arrow_clients.importlib,
            "import_module",
            side_effect=ImportError("missing"),
        ):
            assert load_native_arrow_module("clickhouse") is _NATIVE_ARROW_IMPORT_FAILED

    def test_warning_logged_when_flag_on_and_missing(self, monkeypatch, caplog):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            native_arrow_clients.importlib,
            "import_module",
            side_effect=ImportError("missing"),
        ):
            with caplog.at_level(
                logging.WARNING, logger=native_arrow_clients.logger.name
            ):
                load_native_arrow_module("clickhouse")
        warnings = [
            r for r in caplog.records
            if "not importable" in r.message and r.levelno == logging.WARNING
        ]
        assert warnings
