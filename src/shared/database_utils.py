"""Shared database utilities for source and destination components.

These utilities are extracted from duplicated code across:
- src/connectors/database/postgresql_driver.py
- src/destination/handlers/postgresql.py
- src/destination/handlers/database.py
"""

import ssl
from typing import Any, Dict, Union


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
