"""Connector-owned type-map and ssl-mode-map runtime.

Filesystem I/O lives in :mod:`loader`; everything else is pure logic.
"""

from .arrow import parse_arrow_type, resolve_arrow_type
from .exceptions import (
    InvalidSSLModeMapError,
    InvalidTypeMapError,
    TypeMapError,
    UnmappedSSLModeError,
    UnmappedTypeError,
)
from .loader import (
    SSL_MODE_MAP_FILENAME,
    TYPE_MAP_FILENAME,
    load_connection_type_map,
    load_ssl_mode_map,
    load_type_map,
)
from .mapper import CANONICAL_SSL_MODES, SSLModeMapper, TypeMapper
from .rules import TypeMapRule, normalize_native_type, parse_rules

__all__ = [
    "CANONICAL_SSL_MODES",
    "InvalidSSLModeMapError",
    "InvalidTypeMapError",
    "SSLModeMapper",
    "SSL_MODE_MAP_FILENAME",
    "TYPE_MAP_FILENAME",
    "TypeMapError",
    "TypeMapRule",
    "TypeMapper",
    "UnmappedSSLModeError",
    "UnmappedTypeError",
    "parse_arrow_type",
    "resolve_arrow_type",
    "load_connection_type_map",
    "load_ssl_mode_map",
    "load_type_map",
    "normalize_native_type",
    "parse_rules",
]