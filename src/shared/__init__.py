"""Shared utilities used by both source and destination components.

This module contains utilities that are used across the codebase to avoid
code duplication between source connectors and destination handlers.
"""

from .database_utils import (
    convert_ssl_mode,
    validate_sql_identifier,
    get_full_table_name,
    get_default_clause,
)
from .rate_limiter import RateLimiter

__all__ = [
    "convert_ssl_mode",
    "validate_sql_identifier",
    "get_full_table_name",
    "get_default_clause",
    "RateLimiter",
]
