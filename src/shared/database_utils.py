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

        args["ssl"] = canonical_ssl_to_connect_arg(self.ssl_mode)

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
    driver = config.get("driver")
    if not driver:
        raise ValueError("Database driver is required (e.g. 'postgresql', 'mysql')")

    host = config.get("host")
    params = config.get("parameters", {})

    database = params.get("database") or params.get("dbname")
    if not database:
        raise ValueError("Database name is required")

    # SQLite only needs driver + database
    if driver.lower() == "sqlite":
        host = host or ""
        username = params.get("username") or params.get("user", "")
        password = params.get("password", "")
    else:
        if not host:
            raise ValueError("Database host is required")

        username = params.get("username") or params.get("user")
        if not username:
            raise ValueError("Database username is required")

        password = params.get("password")
        if password is None:
            raise ValueError("Database password is required")

    # Port handling
    port_value = params.get("port")
    if port_value is None:
        if require_port:
            raise ValueError("Database port is required")
        port = 5432
    else:
        port = int(port_value)

    # SSL mode: expects canonical values (none/encrypt/verify) from connector.
    # Default to "encrypt" for SSL dialects (encrypted, no cert verification).
    if driver.lower() in SSL_DIALECTS:
        ssl_mode = params.get("ssl_mode", "encrypt")
    else:
        ssl_mode = params.get("ssl_mode", "none")

    # Pool configuration
    pool_config = params.get("connection_pool", {})
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
        echo=params.get("echo_sql", False),
        command_timeout=params.get("command_timeout", 300),
    )



def _has_ssl_error_in_chain(exc: BaseException) -> bool:
    """Return True if the exception chain indicates an SSL negotiation failure.

    Drivers wrap the underlying cause inside their own exception types (asyncpg,
    aiomysql, SQLAlchemy), so we walk __cause__ and __context__ to find it.

    Two SSL failure modes are recognised:
    - ``ssl.SSLError`` (and subclasses): TLS handshake failed at the Python
      ssl layer (bad cert, protocol mismatch, etc.).
    - Plain ``ConnectionError`` with "SSL" in the message: asyncpg raises this
      when the PostgreSQL server rejects the SSL upgrade request (responds 'N'
      to SSLRequest). Subclasses like ConnectionResetError/RefusedError/
      AbortedError are intentionally excluded — those are network failures,
      not SSL negotiation failures.
    """
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ssl.SSLError):
            return True
        if type(current) is ConnectionError and "SSL" in str(current):
            return True
        current = current.__cause__ or current.__context__
    return False


def canonical_ssl_to_connect_arg(ssl_mode: str) -> Union[bool, ssl.SSLContext]:
    """Convert a canonical ssl_mode to the Python ssl argument for asyncpg/aiomysql.

    Canonical values (stored in connection config by the connection-creator agent):
    - "none":    no SSL
    - "encrypt": SSL without certificate verification
    - "prefer":  same as encrypt (fallback to none is handled by create_database_engine)
    - "verify":  SSL with certificate verification

    Args:
        ssl_mode: Canonical SSL mode string

    Returns:
        SSL parameter suitable for asyncpg/aiomysql connection
    """
    if ssl_mode == "none":
        return False

    if ssl_mode in ("encrypt", "prefer"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # "verify" or any unrecognised value — full cert verification
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
    fails with an SSL negotiation error (see ``_has_ssl_error_in_chain``),
    the engine is disposed and a second attempt is made with ``ssl=False``.
    Non-SSL failures (auth, missing database, unreachable host) propagate
    immediately without plaintext retry.

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
        if ssl_mode == "prefer" and _has_ssl_error_in_chain(e):
            logger.warning(
                "SSL negotiation failed with ssl_mode='prefer', retrying without SSL: %s",
                e,
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
