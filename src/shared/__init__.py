"""Shared utilities used by both source and destination components.

This module contains utilities that are used across the codebase to avoid
code duplication between source connectors and destination handlers.
"""

from .database_utils import (
    canonical_ssl_to_connect_arg,
    validate_sql_identifier,
    get_full_table_name,
    get_default_clause,
    DatabaseConnectionParams,
    extract_connection_params,
    create_database_engine,
    acquire_connection,
    convert_db_to_python,
    convert_record_from_db,
    DIALECT_MAP,
    SSL_DIALECTS,
)
from .rate_limiter import RateLimiter
from .connection_runtime import ConnectionRuntime
from .connector_utils import (
    find_connector,
    get_connector_type_from_list,
)
from .run_id import (
    get_run_id,
    get_or_generate_run_id,
    initialize_run_id,
)

__all__ = [
    "canonical_ssl_to_connect_arg",
    "validate_sql_identifier",
    "get_full_table_name",
    "get_default_clause",
    "DatabaseConnectionParams",
    "extract_connection_params",
    "create_database_engine",
    "acquire_connection",
    "convert_db_to_python",
    "convert_record_from_db",
    "DIALECT_MAP",
    "SSL_DIALECTS",
    "RateLimiter",
    "ConnectionRuntime",
    "find_connector",
    "get_connector_type_from_list",
    "get_run_id",
    "get_or_generate_run_id",
    "initialize_run_id",
]
