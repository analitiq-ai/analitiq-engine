"""Connector-owned type-map runtime.

Filesystem I/O lives in :mod:`loader`; everything else is pure logic.

``parse_arrow_type`` / ``resolve_arrow_type`` build ``pyarrow`` datatypes and
so resolve through a lazy (PEP 562) accessor: importing ``cdk.type_map`` for
its string-only surface (``TypeMapper``, the rule parsers, the exceptions)
does not pull ``pyarrow``. The Arrow helpers load on first attribute access
and require the ``analitiq-cdk[arrow]`` extra.
"""

from typing import TYPE_CHECKING, Any

from .._extras import reraise_for_missing_extra
from .conversions import (
    Conversion,
    build_conversion_matrix,
    classify_conversion,
    load_published_matrix,
    render_conversion_matrix,
)
from .exceptions import (
    InvalidTypeMapError,
    TypeMapError,
    TypeMapNotFoundError,
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

# Arrow datatype builders: kept off the eager import graph (they pull pyarrow).
_LAZY_ARROW = frozenset(
    {
        "parse_arrow_type",
        "resolve_arrow_type",
        "arrow_family",
        "classify_arrow_conversion",
    }
)

if TYPE_CHECKING:
    from .arrow import (  # noqa: F401
        arrow_family,
        classify_arrow_conversion,
        parse_arrow_type,
        resolve_arrow_type,
    )


def __getattr__(name: str) -> Any:
    if name in _LAZY_ARROW:
        try:
            from . import arrow
        except ImportError as exc:
            reraise_for_missing_extra(
                exc,
                feature=f"cdk.type_map.{name}",
                extra="arrow",
                modules=("pyarrow",),
            )
        return getattr(arrow, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Conversion",
    "InvalidTypeMapError",
    "TYPE_MAP_FILENAME",
    "WRITE_TYPE_MAP_FILENAME",
    "TypeMapError",
    "TypeMapNotFoundError",
    "TypeMapRule",
    "TypeMapper",
    "UnmappedTypeError",
    "WriteTypeMapRule",
    "arrow_family",
    "build_conversion_matrix",
    "classify_arrow_conversion",
    "classify_conversion",
    "load_published_matrix",
    "render_conversion_matrix",
    "parse_arrow_type",
    "resolve_arrow_type",
    "load_connection_type_map",
    "load_type_map",
    "normalize_canonical_type",
    "normalize_native_type",
    "parse_rules",
    "parse_write_rules",
]
