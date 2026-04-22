"""Connector-owned type-map and ssl-mode-map runtime.

Exposes the deterministic native → canonical matcher, an Arrow-type parser
for the canonical vocabulary, and the SSL mode lookup used on the connection
path. The matcher is single-direction by design; a destination connector
will ship its own file authored from the canonical end (same format, same
matcher, different rules) when that ticket lands.

Everything in this package is pure logic; filesystem I/O lives in
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