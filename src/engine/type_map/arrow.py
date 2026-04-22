"""Parse canonical Arrow type strings into PyArrow ``DataType`` objects.

Canonical type strings are the output of :class:`TypeMapper`. They follow the
Apache Arrow naming convention defined in the plugin repo's
``canonical-types.json`` (``$id``: ``https://analitiq.dev/schemas/canonical-types.json``).

Only the families the engine materializes today are supported here — adding
further families is a matter of adding one branch to
:func:`canonical_to_arrow`.
"""

from __future__ import annotations

import re
from typing import Final, Pattern

import pyarrow as pa

from .exceptions import InvalidTypeMapError


_PARAM_SPLIT: Final[Pattern[str]] = re.compile(r"\s*,\s*")


def _parse_head(canonical: str) -> tuple[str, tuple[str, ...]]:
    """Split ``Name(arg1, arg2)`` into (``Name``, (``arg1``, ``arg2``))."""
    trimmed = canonical.strip()
    if "(" not in trimmed:
        return trimmed, ()
    if not trimmed.endswith(")"):
        raise InvalidTypeMapError(
            f"canonical type {canonical!r} has unbalanced parentheses"
        )
    head, _, rest = trimmed.partition("(")
    body = rest[:-1]
    if not body.strip():
        return head.strip(), ()
    args = tuple(part.strip() for part in _PARAM_SPLIT.split(body))
    return head.strip(), args


def canonical_to_arrow(canonical: str) -> pa.DataType:
    """Parse a canonical Arrow type string into a PyArrow ``DataType``.

    Raises :class:`InvalidTypeMapError` for malformed input or unsupported
    families. The matcher is deliberately strict — an unknown family
    indicates an author-time mistake that should surface loudly.
    """
    head, args = _parse_head(canonical)
    match head:
        case "Null":
            return pa.null()
        case "Boolean":
            return pa.bool_()
        case "Int8":
            return pa.int8()
        case "Int16":
            return pa.int16()
        case "Int32":
            return pa.int32()
        case "Int64":
            return pa.int64()
        case "UInt8":
            return pa.uint8()
        case "UInt16":
            return pa.uint16()
        case "UInt32":
            return pa.uint32()
        case "UInt64":
            return pa.uint64()
        case "Float16":
            return pa.float16()
        case "Float32":
            return pa.float32()
        case "Float64":
            return pa.float64()
        case "Utf8":
            return pa.string()
        case "LargeUtf8":
            return pa.large_string()
        case "Binary":
            return pa.binary()
        case "LargeBinary":
            return pa.large_binary()
        case "Date32":
            return pa.date32()
        case "Date64":
            return pa.date64()
        case "Time32":
            return pa.time32(_require_unit(args, head, ("s", "ms")))
        case "Time64":
            return pa.time64(_require_unit(args, head, ("us", "ns")))
        case "Timestamp":
            return _parse_timestamp(args)
        case "Decimal128":
            return _parse_decimal(args, pa.decimal128, head)
        case "Decimal256":
            return _parse_decimal(args, pa.decimal256, head)
        case "FixedSizeBinary":
            return _parse_fixed_binary(args, head)
    raise InvalidTypeMapError(
        f"canonical type family {head!r} (from {canonical!r}) is not supported"
    )


def _require_unit(
    args: tuple[str, ...], head: str, allowed: tuple[str, ...]
) -> str:
    if len(args) != 1 or args[0] not in allowed:
        raise InvalidTypeMapError(
            f"{head}{args} requires exactly one unit from {allowed}"
        )
    return args[0]


def _parse_timestamp(args: tuple[str, ...]) -> pa.DataType:
    if not args:
        raise InvalidTypeMapError(
            "Timestamp requires at least a unit (e.g. Timestamp(us))"
        )
    unit = args[0]
    if unit not in ("s", "ms", "us", "ns"):
        raise InvalidTypeMapError(
            f"Timestamp unit must be one of s/ms/us/ns, got {unit!r}"
        )
    tz = args[1] if len(args) > 1 and args[1] else None
    return pa.timestamp(unit, tz=tz)


def _parse_decimal(
    args: tuple[str, ...], factory, head: str
) -> pa.DataType:
    if len(args) != 2:
        raise InvalidTypeMapError(
            f"{head} requires (precision, scale); got {args}"
        )
    try:
        precision = int(args[0])
        scale = int(args[1])
    except ValueError as err:
        raise InvalidTypeMapError(
            f"{head}({', '.join(args)}) has non-integer parameters"
        ) from err
    return factory(precision, scale)


def _parse_fixed_binary(args: tuple[str, ...], head: str) -> pa.DataType:
    if len(args) != 1:
        raise InvalidTypeMapError(f"{head} requires (byte_width); got {args}")
    try:
        width = int(args[0])
    except ValueError as err:
        raise InvalidTypeMapError(
            f"{head}({args[0]}) byte_width is not an integer"
        ) from err
    return pa.binary(width)