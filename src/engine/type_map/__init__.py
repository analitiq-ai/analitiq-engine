"""Connector-owned type-map runtime.

Filesystem I/O lives in :mod:`loader`; everything else is pure logic.
"""

from .arrow import parse_arrow_type, resolve_arrow_type
from .exceptions import (
    InvalidTypeMapError,
    TypeMapError,
    UnmappedTypeError,
)
from .loader import (
    TYPE_MAP_FILENAME,
    WRITE_TYPE_MAP_FILENAME,
    load_connection_type_map,
    load_type_map,
)
from .mapper import TypeMapper
from .rules import (
    TypeMapRule,
    WriteTypeMapRule,
    normalize_canonical_type,
    normalize_native_type,
    parse_rules,
    parse_write_rules,
)

__all__ = [
    "InvalidTypeMapError",
    "TYPE_MAP_FILENAME",
    "WRITE_TYPE_MAP_FILENAME",
    "TypeMapError",
    "TypeMapRule",
    "TypeMapper",
    "UnmappedTypeError",
    "WriteTypeMapRule",
    "parse_arrow_type",
    "resolve_arrow_type",
    "load_connection_type_map",
    "load_type_map",
    "normalize_canonical_type",
    "normalize_native_type",
    "parse_rules",
    "parse_write_rules",
]