"""Unit tests for the shared ADBC registry.

Exercises the dispatch table, env-flag check, lazy module loader, and
per-dialect URI builders. ADBC drivers are mocked — no real driver or
database is required.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.shared import adbc_registry
from src.shared.adbc_registry import (
    _ADBC_IMPORT_FAILED,
    _ADBC_MODULES,
    AdbcConfigurationError,
    adbc_flag_enabled,
    adbc_uri_supported,
    build_adbc_uri,
    load_adbc_module,
)


def _url(
    backend: str = "postgresql",
    host: str = "db.example.com",
    port: int = 5432,
    username: str = "u",
    password: str = "p@ss/word",
    database: str = "warehouse",
    query: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        query=query or {},
        get_backend_name=lambda: backend,
    )


def _engine(url) -> MagicMock:
    engine = MagicMock()
    engine.url = url
    return engine


@pytest.fixture(autouse=True)
def _disable_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ADBC_FAST_PATH", raising=False)


class TestFlag:
    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
    def test_truthy_values_enable(self, monkeypatch, value):
        monkeypatch.setenv("ADBC_FAST_PATH", value)
        assert adbc_flag_enabled() is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "off", "", "maybe"])
    def test_other_values_disable(self, monkeypatch, value):
        monkeypatch.setenv("ADBC_FAST_PATH", value)
        assert adbc_flag_enabled() is False

    def test_unset_disables(self):
        assert adbc_flag_enabled() is False


class TestLoadAdbcModule:
    def test_empty_dialect_returns_sentinel(self):
        assert load_adbc_module("") is _ADBC_IMPORT_FAILED

    def test_unknown_dialect_returns_sentinel(self):
        assert load_adbc_module("foobardb") is _ADBC_IMPORT_FAILED

    def test_known_dialect_imports_module(self):
        with patch.object(adbc_registry.importlib, "import_module") as imp:
            imp.return_value = "stub"
            result = load_adbc_module("postgresql")
        assert result == "stub"
        imp.assert_called_once_with("adbc_driver_postgresql.dbapi")

    def test_dialect_is_case_insensitive(self):
        with patch.object(adbc_registry.importlib, "import_module") as imp:
            imp.return_value = "stub"
            assert load_adbc_module("Postgresql") == "stub"
            assert load_adbc_module("POSTGRES") == "stub"

    def test_import_error_returns_sentinel(self):
        with patch.object(
            adbc_registry.importlib,
            "import_module",
            side_effect=ImportError("boom"),
        ):
            assert load_adbc_module("postgresql") is _ADBC_IMPORT_FAILED

    def test_import_error_logs_warning_when_flag_on(
        self, monkeypatch, caplog
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            adbc_registry.importlib,
            "import_module",
            side_effect=ImportError("missing pkg"),
        ):
            with caplog.at_level(logging.WARNING, logger=adbc_registry.logger.name):
                load_adbc_module("snowflake")
        warnings = [
            r for r in caplog.records
            if "not importable" in r.message and r.levelno == logging.WARNING
        ]
        assert warnings, "expected a WARNING when the opted-in driver is missing"

    def test_import_error_logs_debug_when_flag_off(self, caplog):
        with patch.object(
            adbc_registry.importlib,
            "import_module",
            side_effect=ImportError("missing pkg"),
        ):
            with caplog.at_level(logging.DEBUG, logger=adbc_registry.logger.name):
                load_adbc_module("snowflake")
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert not warnings


class TestBuildAdbcUri:
    def test_postgres_renders_libpq_uri_with_quoted_password(self):
        uri = build_adbc_uri("postgresql", _engine(_url(password="p@ss/word")))
        assert uri == "postgresql://u:p%40ss%2Fword@db.example.com:5432/warehouse"

    def test_postgres_alias_works(self):
        uri = build_adbc_uri("postgres", _engine(_url()))
        assert uri is not None
        assert uri.startswith("postgresql://")

    def test_redshift_uses_pg_wire_uri(self):
        url = _url(backend="redshift", host="rs.example.com", port=5439)
        uri = build_adbc_uri("redshift", _engine(url))
        assert uri is not None
        assert "rs.example.com:5439" in uri

    def test_returns_none_for_unsupported_dialect(self):
        assert build_adbc_uri("foobardb", _engine(_url())) is None

    def test_returns_none_for_dialect_without_uri_builder(self):
        # Snowflake / BigQuery are in _ADBC_MODULES but have no URI
        # builder yet — they must demote to None.
        assert build_adbc_uri("snowflake", _engine(_url(backend="snowflake"))) is None
        assert build_adbc_uri("bigquery", _engine(_url(backend="bigquery"))) is None

    def test_returns_none_when_host_missing(self):
        assert build_adbc_uri("postgresql", _engine(_url(host=None))) is None

    def test_returns_none_when_database_missing(self):
        assert build_adbc_uri("postgresql", _engine(_url(database=None))) is None

    def test_returns_none_when_backend_mismatched(self):
        # Driver claims postgresql but engine URL says sqlite — refuse
        # to build a libpq URI from a sqlite engine.
        assert build_adbc_uri("postgresql", _engine(_url(backend="sqlite"))) is None

    def test_forwards_query_params(self):
        url = _url(query={"sslmode": "require", "application_name": "engine"})
        uri = build_adbc_uri("postgresql", _engine(url))
        assert uri is not None
        assert "sslmode=require" in uri
        assert "application_name=engine" in uri

    def test_forwards_tuple_valued_query_params(self):
        url = _url(query={"option": ("a", "b")})
        uri = build_adbc_uri("postgresql", _engine(url))
        assert uri is not None
        assert "option=a" in uri
        assert "option=b" in uri

    def test_sqlite_returns_database_path(self):
        url = _url(backend="sqlite", host=None, database="/tmp/test.db")
        assert build_adbc_uri("sqlite", _engine(url)) == "/tmp/test.db"

    def test_sqlite_empty_db_means_memory(self):
        url = _url(backend="sqlite", host=None, database=None)
        assert build_adbc_uri("sqlite", _engine(url)) == ":memory:"

    def test_duckdb_returns_database_path(self):
        url = _url(backend="duckdb", host=None, database="warehouse.duckdb")
        assert build_adbc_uri("duckdb", _engine(url)) == "warehouse.duckdb"

    def test_file_builder_rejects_mismatched_backend(self):
        # Driver says sqlite but engine URL claims postgres — refuse.
        url = _url(backend="postgresql", host=None, database="/tmp/test.db")
        assert build_adbc_uri("sqlite", _engine(url)) is None

    def test_none_engine_returns_none(self):
        assert build_adbc_uri("postgresql", None) is None

    def test_empty_dialect_returns_none(self):
        assert build_adbc_uri("", _engine(_url())) is None


class TestBuildAdbcUriWithTls:
    """The ADBC URI must carry the SA-side TLS mode so the ADBC
    connection matches the engine's posture instead of silently
    going plaintext."""

    @pytest.mark.parametrize("mode", ["disable", "allow", "prefer", "require"])
    def test_safe_modes_become_sslmode_query_param(self, mode):
        uri = build_adbc_uri("postgresql", _engine(_url()), tls_mode=mode)
        assert uri is not None
        assert f"sslmode={mode}" in uri

    def test_explicit_query_sslmode_is_not_overridden(self):
        url = _url(query={"sslmode": "require"})
        uri = build_adbc_uri("postgresql", _engine(url), tls_mode="prefer")
        # Existing query wins -- we don't silently downgrade what the
        # connector author put in the DSN template.
        assert "sslmode=require" in uri
        assert "sslmode=prefer" not in uri

    def test_mode_case_normalised_to_lowercase(self):
        uri = build_adbc_uri("postgresql", _engine(_url()), tls_mode="REQUIRE")
        assert "sslmode=require" in uri

    @pytest.mark.parametrize("ca_present", [True, False])
    @pytest.mark.parametrize("mode", ["verify-ca", "verify-full", "VERIFY-CA"])
    def test_verify_modes_always_demote(self, mode, ca_present):
        """Verify-* modes need a CA file path that the URI cannot
        inline. Demotion fires regardless of ``tls_ca_bundle_present``
        so a future caller that forgets the flag can't silently
        downgrade to plaintext."""
        uri = build_adbc_uri(
            "postgresql",
            _engine(_url()),
            tls_mode=mode,
            tls_ca_bundle_present=ca_present,
        )
        assert uri is None

    def test_require_with_ca_bundle_does_not_demote(self):
        # CA-bundle-present + non-verify mode is fine -- the CA is
        # used by SA for SSLContext, but the URI just needs sslmode=
        # to match the negotiation posture.
        uri = build_adbc_uri(
            "postgresql",
            _engine(_url()),
            tls_mode="require",
            tls_ca_bundle_present=True,
        )
        assert uri is not None
        assert "sslmode=require" in uri

    def test_no_tls_mode_means_no_sslmode_param(self):
        uri = build_adbc_uri("postgresql", _engine(_url()))
        assert "sslmode" not in uri


class TestUriSupported:
    @pytest.mark.parametrize(
        "dialect",
        ["postgresql", "postgres", "redshift", "sqlite", "duckdb"],
    )
    def test_dialects_with_builders(self, dialect):
        assert adbc_uri_supported(dialect) is True

    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery", "foo", ""])
    def test_dialects_without_builders(self, dialect):
        assert adbc_uri_supported(dialect) is False


class TestRegistry:
    def test_modules_registered_for_expected_dialects(self):
        # Lightweight regression guard so an accidental removal from
        # _ADBC_MODULES gets flagged here rather than at runtime.
        for dialect in (
            "postgresql", "postgres", "redshift",
            "sqlite", "duckdb", "snowflake", "bigquery",
        ):
            assert dialect in _ADBC_MODULES, f"{dialect} should map to a driver"


class TestErrorClass:
    def test_is_runtime_error_subclass(self):
        # Callers ``except AdbcConfigurationError`` to classify fatal.
        assert issubclass(AdbcConfigurationError, RuntimeError)
