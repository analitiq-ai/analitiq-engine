"""Connector-owned type-map and ssl-mode-map runtime.

Exposes the deterministic matcher used for native-to-canonical (and canonical-
to-native) type translation, plus the SSL mode lookup used on the connection
path. Everything in this package is pure logic; filesystem I/O lives in
:mod:`loader`.
"""

from .arrow import canonical_to_arrow
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
    "canonical_to_arrow",
    "load_connection_type_map",
    "load_ssl_mode_map",
    "load_type_map",
    "normalize_native_type",
    "parse_rules",
]