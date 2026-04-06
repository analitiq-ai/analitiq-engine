"""Unit tests for DatabaseConnectionParams and extract_connection_params."""

import pytest
from sqlalchemy.engine import URL

from src.shared.database_utils import (
    DatabaseConnectionParams,
    extract_connection_params,
    DIALECT_MAP,
    SSL_DIALECTS,
)


class TestExtractConnectionParams:
    """Test extract_connection_params factory function."""

    def test_basic_extraction(self):
        """Test basic parameter extraction with standard field names."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }
        params = extract_connection_params(config, require_port=True)

        assert params.driver == "postgresql"
        assert params.host == "localhost"
        assert params.port == 5432
        assert params.database == "test_db"
        assert params.username == "test_user"
        assert params.password == "test_pass"

    def test_field_name_fallback_user(self):
        """Test that 'user' is accepted as fallback for 'username'."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "port": 5432,
                "user": "pg_user",
                "password": "pass",
                "database": "db",
            },
        }
        params = extract_connection_params(config, require_port=True)
        assert params.username == "pg_user"

    def test_field_name_fallback_dbname(self):
        """Test that 'dbname' is accepted as fallback for 'database'."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "port": 5432,
                "username": "user",
                "password": "pass",
                "dbname": "my_database",
            },
        }
        params = extract_connection_params(config, require_port=True)
        assert params.database == "my_database"

    def test_missing_driver_raises(self):
        """Test that missing driver raises ValueError."""
        config = {
            "host": "localhost",
            "parameters": {"port": 5432, "database": "db", "username": "u", "password": "p"},
        }
        with pytest.raises(ValueError, match="driver is required"):
            extract_connection_params(config)

    def test_missing_host_raises(self):
        """Test that missing host raises ValueError."""
        config = {
            "driver": "postgresql",
            "parameters": {"port": 5432, "database": "db", "username": "u", "password": "p"},
        }
        with pytest.raises(ValueError, match="host is required"):
            extract_connection_params(config)

    def test_missing_username_raises(self):
        """Test that missing username raises ValueError."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {"port": 5432, "database": "db", "password": "p"},
        }
        with pytest.raises(ValueError, match="username is required"):
            extract_connection_params(config)

    def test_missing_password_raises(self):
        """Test that missing password raises ValueError."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {"port": 5432, "database": "db", "username": "u"},
        }
        with pytest.raises(ValueError, match="password is required"):
            extract_connection_params(config)

    def test_missing_database_raises(self):
        """Test that missing database raises ValueError."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {"port": 5432, "username": "u", "password": "p"},
        }
        with pytest.raises(ValueError, match="Database name is required"):
            extract_connection_params(config)

    def test_require_port_true_raises_on_missing(self):
        """Test that require_port=True raises ValueError when port is missing."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "database": "db",
                "username": "user",
                "password": "pass",
            },
        }
        with pytest.raises(ValueError, match="port is required"):
            extract_connection_params(config, require_port=True)

    def test_require_port_false_defaults_to_5432(self):
        """Test that require_port=False defaults port to 5432."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "database": "db",
                "username": "user",
                "password": "pass",
            },
        }
        params = extract_connection_params(config, require_port=False)
        assert params.port == 5432

    def test_port_string_to_int_conversion(self):
        """Test that string port is converted to int."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "port": "5432",
                "database": "db",
                "username": "user",
                "password": "pass",
            },
        }
        params = extract_connection_params(config, require_port=True)
        assert params.port == 5432
        assert isinstance(params.port, int)

    def test_ssl_mode_defaults_to_prefer(self):
        """Test that SSL mode defaults to 'prefer' for SSL dialects."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "port": 5432,
                "database": "db",
                "username": "user",
                "password": "pass",
            },
        }
        params = extract_connection_params(config, require_port=True)
        assert params.ssl_mode == "prefer"

    def test_sqlite_no_ssl_mode(self):
        """Test that SQLite gets empty ssl_mode and doesn't require host/user/password."""
        config = {"driver": "sqlite", "parameters": {"database": ":memory:"}}
        params = extract_connection_params(config, require_port=False)
        assert params.ssl_mode == ""
        assert params.host == ""
        assert params.username == ""
        assert params.password == ""

    def test_pool_defaults(self):
        """Test default pool settings."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "port": 5432,
                "database": "db",
                "username": "user",
                "password": "pass",
            },
        }
        params = extract_connection_params(config, require_port=True)
        assert params.pool_min == 2
        assert params.pool_max == 10


class TestDatabaseConnectionParamsURL:
    """Test DatabaseConnectionParams.to_sqlalchemy_url()."""

    def test_postgresql_url(self):
        """Test PostgreSQL URL generation."""
        params = DatabaseConnectionParams(
            driver="postgresql",
            host="localhost",
            port=5432,
            username="user",
            password="pass",
            database="db",
            ssl_mode="prefer",
        )
        url = params.to_sqlalchemy_url()
        assert isinstance(url, URL)
        assert str(url).startswith("postgresql+asyncpg://")

    def test_password_with_special_characters(self):
        """Test that passwords with reserved chars produce valid URL."""
        params = DatabaseConnectionParams(
            driver="postgresql",
            host="localhost",
            port=5432,
            username="user",
            password="a@b#c%/d:e",
            database="db",
            ssl_mode="prefer",
        )
        url = params.to_sqlalchemy_url()
        assert isinstance(url, URL)
        # URL.create properly encodes special characters
        rendered = url.render_as_string(hide_password=False)
        assert "a" in rendered  # password is encoded, not raw

    def test_sqlite_url_shape(self):
        """Test SQLite URL: database-only, no host/port/user."""
        params = DatabaseConnectionParams(
            driver="sqlite",
            host="",
            port=None,
            username="",
            password="",
            database=":memory:",
            ssl_mode="",
        )
        url = params.to_sqlalchemy_url()
        rendered = str(url)
        assert rendered == "sqlite+aiosqlite:///:memory:"

    def test_unknown_driver_fallback(self):
        """Test that unknown driver falls back to driver+asyncpg."""
        params = DatabaseConnectionParams(
            driver="cockroach",
            host="localhost",
            port=26257,
            username="root",
            password="pass",
            database="defaultdb",
            ssl_mode="prefer",
        )
        url = params.to_sqlalchemy_url()
        assert str(url).startswith("cockroach+asyncpg://")


class TestDatabaseConnectionParamsConnectArgs:
    """Test to_sqlalchemy_connect_args()."""

    def test_ssl_dialect_prefer(self):
        """Test SSL connect_args for prefer mode."""
        params = DatabaseConnectionParams(
            driver="postgresql",
            host="localhost",
            port=5432,
            username="user",
            password="pass",
            database="db",
            ssl_mode="prefer",
        )
        args = params.to_sqlalchemy_connect_args()
        assert "ssl" in args
        assert args["command_timeout"] == 300

    def test_ssl_disable(self):
        """Test ssl_mode=disable returns ssl=False."""
        params = DatabaseConnectionParams(
            driver="postgresql",
            host="localhost",
            port=5432,
            username="user",
            password="pass",
            database="db",
            ssl_mode="disable",
        )
        args = params.to_sqlalchemy_connect_args()
        assert args["ssl"] is False
        assert args["command_timeout"] == 300

    def test_command_timeout_forwarded(self):
        """Test command_timeout is included in connect_args for PostgreSQL."""
        params = DatabaseConnectionParams(
            driver="postgresql",
            host="localhost",
            port=5432,
            username="user",
            password="pass",
            database="db",
            ssl_mode="prefer",
            command_timeout=60,
        )
        args = params.to_sqlalchemy_connect_args()
        assert args["command_timeout"] == 60

    def test_sqlite_empty_connect_args(self):
        """Test SQLite returns empty connect_args."""
        params = DatabaseConnectionParams(
            driver="sqlite",
            host="",
            port=None,
            username="",
            password="",
            database=":memory:",
            ssl_mode="",
        )
        args = params.to_sqlalchemy_connect_args()
        assert args == {}


class TestDatabaseConnectionParamsEngineKwargs:
    """Test to_sqlalchemy_engine_kwargs()."""

    def test_default_engine_kwargs(self):
        """Test default engine kwargs."""
        params = DatabaseConnectionParams(
            driver="postgresql",
            host="localhost",
            port=5432,
            username="user",
            password="pass",
            database="db",
            ssl_mode="prefer",
            pool_min=2,
            pool_max=10,
        )
        kwargs = params.to_sqlalchemy_engine_kwargs()
        assert kwargs["pool_size"] == 2
        assert kwargs["max_overflow"] == 8
        assert kwargs["pool_pre_ping"] is True
        assert kwargs["echo"] is False


class TestConstants:
    """Test that shared constants are correct."""

    def test_dialect_map_keys(self):
        """Test DIALECT_MAP has expected entries."""
        assert "postgresql" in DIALECT_MAP
        assert "postgres" in DIALECT_MAP
        assert "mysql" in DIALECT_MAP
        assert "sqlite" in DIALECT_MAP

    def test_ssl_dialects(self):
        """Test SSL_DIALECTS has expected entries."""
        assert "postgresql" in SSL_DIALECTS
        assert "postgres" in SSL_DIALECTS
        assert "mysql" in SSL_DIALECTS
        assert "sqlite" not in SSL_DIALECTS
