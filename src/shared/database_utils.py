"""Shared database utilities for source and destination components.

Provides a single engine factory, SSL-prefer fallback, connection acquisition,
and read-side type conversion used by both source connectors and destination handlers.
"""

import logging
import ssl
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Optional, Union

from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

logger = logging.getLogger(__name__)


# SQLAlchemy dialect mapping
DIALECT_MAP = {
    "postgresql": "postgresql+asyncpg",
    "postgres": "postgresql+asyncpg",
    "mysql": "mysql+aiomysql",
    "mariadb": "mysql+aiomysql",
    "sqlite": "sqlite+aiosqlite",
}

# Dialects that support SSL connections
SSL_DIALECTS = {"postgresql", "postgres", "mysql", "mariadb"}


@dataclass(frozen=True)
class DatabaseConnectionParams:
    """Immutable container for parsed database connection parameters."""

    driver: str
    host: str
    port: Optional[int]
    username: str
    password: str
    database: str
    ssl_mode: str
    pool_min: int = 2
    pool_max: int = 10
    pool_pre_ping: bool = True
    echo: bool = False
    command_timeout: int = 300

    def to_sqlalchemy_url(self) -> URL:
        """Build a SQLAlchemy URL using URL.create() (handles special characters)."""
        dialect = DIALECT_MAP.get(self.driver.lower(), f"{self.driver}+asyncpg")

        # SQLite: database-only, no host/port/user
        if self.driver.lower() == "sqlite":
            return URL.create(
                drivername="sqlite+aiosqlite",
                database=self.database,
            )

        return URL.create(
            drivername=dialect,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    def to_sqlalchemy_connect_args(self) -> Dict[str, Any]:
        """Build connect_args dict (SSL, timeout settings for supported dialects)."""
        args: Dict[str, Any] = {}

        # asyncpg command_timeout (seconds before a query is cancelled)
        if self.driver.lower() in ("postgresql", "postgres"):
            args["command_timeout"] = self.command_timeout

        if self.driver.lower() not in SSL_DIALECTS:
            return args

        if self.ssl_mode == "disable":
            args["ssl"] = False
        else:
            args["ssl"] = convert_ssl_mode(self.ssl_mode)

        return args

    def to_sqlalchemy_engine_kwargs(self) -> Dict[str, Any]:
        """Build engine keyword arguments (pool, echo, etc.)."""
        return {
            "pool_size": self.pool_min,
            "max_overflow": max(self.pool_max - self.pool_min, 0),
            "pool_pre_ping": self.pool_pre_ping,
            "echo": self.echo,
        }


def extract_connection_params(
    config: Dict[str, Any],
    *,
    require_port: bool = True,
) -> DatabaseConnectionParams:
    """Extract and validate connection parameters from a config dict.

    Handles field name variations (username/user, database/dbname) and
    builds a DatabaseConnectionParams with sensible defaults.

    Args:
        config: Raw connection config dictionary.
        require_port: If True, raise ValueError when port is missing.
                      If False, default to 5432.

    Returns:
        DatabaseConnectionParams frozen dataclass.
    """
    driver = config.get("driver", "postgresql")

    host = config.get("host", "localhost")
    username = config.get("username") or config.get("user", "postgres")
    password = config.get("password", "")
    database = config.get("database") or config.get("dbname", "postgres")

    # Port handling
    port_value = config.get("port")
    if port_value is None:
        if require_port:
            raise ValueError("Database port is required")
        port = 5432
    else:
        port = int(port_value)

    # SSL mode: default to "prefer" for SSL dialects, None for others
    if driver.lower() in SSL_DIALECTS:
        ssl_mode = config.get("ssl_mode", "prefer")
    else:
        ssl_mode = config.get("ssl_mode", "")

    # Pool configuration
    pool_config = config.get("connection_pool", {})
    pool_min = pool_config.get("min_connections", 2)
    pool_max = pool_config.get("max_connections", 10)

    return DatabaseConnectionParams(
        driver=driver,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        ssl_mode=ssl_mode,
        pool_min=pool_min,
        pool_max=pool_max,
        pool_pre_ping=True,
        echo=config.get("echo_sql", False),
        command_timeout=config.get("command_timeout", 300),
    )


def is_ssl_handshake_error(exc: BaseException) -> bool:
    """Check if exception indicates an SSL handshake/protocol failure.

    Used exclusively in ssl_mode='prefer' connection paths to decide
    whether to retry without SSL. ConnectionError and its subclasses
    (ConnectionResetError, ConnectionRefusedError) are treated as
    handshake failures here because they occur when a non-SSL server
    rejects the SSL negotiation attempt (e.g. asyncpg raises a bare
    ConnectionError on SSL rejection). Do NOT use this function
    outside of SSL-prefer fallback logic.
    """
    seen: set[int] = set()
    to_check: list[BaseException] = []
    stack: list[BaseException] = [exc]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        to_check.append(current)
        if current.__cause__ is not None:
            stack.append(current.__cause__)
        if current.__context__ is not None:
            stack.append(current.__context__)
        if hasattr(current, "orig") and current.orig is not None:
            stack.append(current.orig)

    has_handshake_error = False
    for e in to_check:
        if isinstance(e, ssl.SSLCertVerificationError):
            return False
        if isinstance(e, (ssl.SSLError, ConnectionError)):
            has_handshake_error = True
    return has_handshake_error


def convert_ssl_mode(ssl_mode: str) -> Union[bool, ssl.SSLContext]:
    """Convert PostgreSQL ssl_mode string to asyncpg ssl parameter.

    Both asyncpg and aiomysql accept True/False or ssl.SSLContext.

    ssl_mode semantics:
    - disable: No SSL
    - prefer/require: Encrypt connection, but don't verify server certificate
    - verify-ca/verify-full: Encrypt and verify server certificate

    When ssl=True is passed, Python creates an SSLContext with
    certificate verification enabled by default, which fails for RDS and
    other services using non-public CA chains. For 'prefer' and 'require',
    we create an SSLContext that skips certificate verification, matching
    standard PostgreSQL/libpq behavior.

    Args:
        ssl_mode: PostgreSQL SSL mode string

    Returns:
        SSL parameter suitable for asyncpg/aiomysql connection
    """
    if ssl_mode == "disable":
        return False

    if ssl_mode in ("prefer", "require"):
        # Create SSL context without certificate verification
        # This matches PostgreSQL libpq behavior for these modes
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # For 'verify-ca' and 'verify-full', use default verification
    # Note: For proper verification, sslrootcert should be configured
    # with the appropriate CA bundle (e.g., AWS RDS CA bundle)
    return True


def validate_sql_identifier(identifier: str) -> bool:
    """Validate SQL identifier (table name, column name, schema name, etc.).

    Args:
        identifier: The SQL identifier to validate

    Returns:
        True if the identifier is valid, False otherwise
    """
    if not identifier:
        return False
    if not identifier.replace('_', '').replace('-', '').isalnum():
        return False
    if identifier[0].isdigit():
        return False
    return True


def get_full_table_name(schema_name: str, table_name: str) -> str:
    """Get fully qualified table name.

    Args:
        schema_name: Database schema name (can be empty/None)
        table_name: Table name

    Returns:
        Fully qualified table name (schema.table or just table)
    """
    return f"{schema_name}.{table_name}" if schema_name else table_name


async def create_database_engine(
    config: Dict[str, Any],
    *,
    require_port: bool = True,
) -> tuple[AsyncEngine, str]:
    """Create and probe a SQLAlchemy async engine from a connection config.

    Extracts connection parameters, builds a URL, creates the engine, and
    probes with ``SELECT 1``.  When *ssl_mode* is ``prefer`` and the probe
    fails with an SSL handshake error, the engine is disposed and a second
    attempt is made with ``ssl=False``.

    Args:
        config: Raw connection config dictionary.
        require_port: If True, raise ValueError when port is missing.

    Returns:
        Tuple of (AsyncEngine, driver_string).

    Raises:
        ValueError: If required fields (e.g. port) are missing.
        Exception: Any connection error after disposal of failed engines.
    """
    conn_params = extract_connection_params(config, require_port=require_port)

    url = conn_params.to_sqlalchemy_url()
    connect_args = conn_params.to_sqlalchemy_connect_args()
    engine_kwargs = conn_params.to_sqlalchemy_engine_kwargs()

    engine = create_async_engine(url, connect_args=connect_args, **engine_kwargs)

    ssl_mode = conn_params.ssl_mode
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as e:
        if ssl_mode == "prefer" and is_ssl_handshake_error(e):
            logger.warning(
                "SSL failed with ssl_mode='prefer', retrying without SSL: %s", e
            )
            await engine.dispose()
            connect_args["ssl"] = False
            engine = create_async_engine(
                url, connect_args=connect_args, **engine_kwargs
            )
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
            except Exception:
                await engine.dispose()
                raise
        else:
            await engine.dispose()
            raise

    return engine, conn_params.driver.lower()


@asynccontextmanager
async def acquire_connection(engine: AsyncEngine) -> AsyncIterator:
    """Acquire a connection from the engine as an async context manager.

    Raises:
        RuntimeError: If *engine* is None.
    """
    if engine is None:
        raise RuntimeError("Engine not initialized")
    async with engine.connect() as conn:
        yield conn


# ---------------------------------------------------------------------------
# Read-side type conversions
# ---------------------------------------------------------------------------

def convert_db_to_python(value: Any) -> Any:
    """Convert database values to Python-friendly types.

    Used when reading data from database.

    Args:
        value: Database value to convert.

    Returns:
        Python-compatible value.
    """
    if value is None:
        return None

    # Convert datetime objects to ISO strings for JSON serialization
    if isinstance(value, datetime):
        return value.isoformat()

    return value


def convert_record_from_db(record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert all values in a record from database reading.

    Args:
        record: Dictionary with database values.

    Returns:
        Dictionary with Python-compatible values.
    """
    return {key: convert_db_to_python(value) for key, value in record.items()}


def get_default_clause(field_def: Dict[str, Any]) -> str:
    """Get DEFAULT clause for column definition.

    Args:
        field_def: JSON Schema field definition

    Returns:
        SQL DEFAULT clause string (including leading space) or empty string
    """
    default_value = field_def.get("default")
    if default_value is None:
        return ""

    if isinstance(default_value, str):
        if default_value.upper() in ("NOW()", "CURRENT_TIMESTAMP"):
            return f" DEFAULT {default_value}"
        return f" DEFAULT '{default_value}'"
    elif isinstance(default_value, bool):
        return f" DEFAULT {str(default_value).lower()}"
    elif isinstance(default_value, (int, float)):
        return f" DEFAULT {default_value}"

    return ""
