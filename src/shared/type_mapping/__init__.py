"""Unified type mapping system with factory pattern.

This module provides a unified approach to type mapping for database destinations,
combining type declaration (JSON Schema -> SQL type) and type coercion
(Python value -> DB-ready value) into a single interface.

Usage:
    from src.shared.type_mapping import get_type_mapper

    mapper = get_type_mapper("postgresql")
    sql_type = mapper.json_schema_to_native({"type": "string", "format": "date-time"})
    coerced_value = mapper.coerce_value("2024-01-01T00:00:00Z", "TIMESTAMPTZ")
"""

import logging
from typing import Dict, Optional, Type

from .base import BaseTypeMapper

logger = logging.getLogger(__name__)

# Registry mapping dialect -> type mapper class
_MAPPER_REGISTRY: Dict[str, Type[BaseTypeMapper]] = {}

# Cache for instantiated mappers (singleton per dialect)
_MAPPER_CACHE: Dict[str, BaseTypeMapper] = {}


def register_type_mapper(dialect: str, mapper_class: Type[BaseTypeMapper]) -> None:
    """Register a type mapper for a database dialect.

    Args:
        dialect: Database dialect name (e.g., 'postgresql', 'mysql')
        mapper_class: Type mapper class to register
    """
    _MAPPER_REGISTRY[dialect.lower()] = mapper_class
    # Clear cache for this dialect if it exists
    if dialect.lower() in _MAPPER_CACHE:
        del _MAPPER_CACHE[dialect.lower()]


def get_type_mapper(dialect: str) -> BaseTypeMapper:
    """Factory function to get type mapper for dialect.

    Returns a cached instance of the appropriate type mapper for the given dialect.
    Falls back to GenericSQLTypeMapper for unknown dialects.

    Args:
        dialect: Database dialect name (e.g., 'postgresql', 'mysql')

    Returns:
        BaseTypeMapper instance for the dialect
    """
    dialect_lower = dialect.lower() if dialect else "generic"

    # Check cache first
    if dialect_lower in _MAPPER_CACHE:
        return _MAPPER_CACHE[dialect_lower]

    # Get mapper class from registry
    mapper_class = _MAPPER_REGISTRY.get(dialect_lower)

    if not mapper_class:
        # Try fallback mappings
        fallback_map = {
            "postgres": "postgresql",
            "mariadb": "mysql",
        }
        fallback_dialect = fallback_map.get(dialect_lower)
        if fallback_dialect:
            mapper_class = _MAPPER_REGISTRY.get(fallback_dialect)

    if not mapper_class:
        # Fall back to generic mapper
        from .generic import GenericSQLTypeMapper
        logger.debug(f"No specific mapper for {dialect}, using GenericSQLTypeMapper")
        mapper = GenericSQLTypeMapper()
    else:
        mapper = mapper_class()

    # Cache and return
    _MAPPER_CACHE[dialect_lower] = mapper
    logger.debug(f"Loaded type mapper for {dialect}")
    return mapper


def clear_cache() -> None:
    """Clear the type mapper cache. Useful for testing."""
    _MAPPER_CACHE.clear()


# Auto-register mappers on import
def _auto_register() -> None:
    """Auto-register all type mappers."""
    from .postgresql import PostgreSQLTypeMapper
    from .mysql import MySQLTypeMapper
    from .snowflake import SnowflakeTypeMapper
    from .generic import GenericSQLTypeMapper

    register_type_mapper("postgresql", PostgreSQLTypeMapper)
    register_type_mapper("postgres", PostgreSQLTypeMapper)
    register_type_mapper("mysql", MySQLTypeMapper)
    register_type_mapper("mariadb", MySQLTypeMapper)
    register_type_mapper("snowflake", SnowflakeTypeMapper)
    register_type_mapper("generic", GenericSQLTypeMapper)
    register_type_mapper("sqlite", GenericSQLTypeMapper)


_auto_register()


__all__ = [
    "BaseTypeMapper",
    "get_type_mapper",
    "register_type_mapper",
    "clear_cache",
]
