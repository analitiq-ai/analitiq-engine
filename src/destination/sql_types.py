"""Canonical Arrow → SQLAlchemy type translation for destination DDL.

The destination handler receives native SQL type strings (e.g. ``BIGINT``,
``VARCHAR(255)``) from the endpoint schema, normalises them through the
connector's ``type-map.json`` to a canonical Arrow type, and then picks a
SQLAlchemy type from the canonical. This module owns the second leg —
translating ``pa.DataType`` into a SQLAlchemy column type for ``CREATE TABLE``.

The mapping is intentionally small and explicit: the canonical vocabulary
already abstracts over dialect quirks, so we only need one SQLAlchemy pick
per Arrow family.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    Numeric,
    SmallInteger,
    Text,
    Time,
)

from src.engine.type_map import TypeMapper, canonical_to_arrow


def arrow_to_sqlalchemy(dtype: pa.DataType) -> Any:
    """Return a SQLAlchemy column type for the given Arrow ``DataType``.

    Raises ``ValueError`` for Arrow families the engine cannot currently
    materialize — callers should either extend this mapping or reject the
    schema at load time.
    """
    if pa.types.is_boolean(dtype):
        return Boolean()
    if pa.types.is_int8(dtype) or pa.types.is_int16(dtype):
        return SmallInteger()
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return Integer()
    if pa.types.is_int64(dtype) or pa.types.is_uint32(dtype) or pa.types.is_uint64(dtype):
        return BigInteger()
    if pa.types.is_uint8(dtype):
        return SmallInteger()
    if pa.types.is_floating(dtype):
        return Float()
    if pa.types.is_decimal(dtype):
        return Numeric(precision=dtype.precision, scale=dtype.scale)
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return Text()
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_fixed_size_binary(dtype):
        return LargeBinary()
    if pa.types.is_date(dtype):
        return Date()
    if pa.types.is_time(dtype):
        return Time()
    if pa.types.is_timestamp(dtype):
        return DateTime(timezone=dtype.tz is not None)
    raise ValueError(
        f"Arrow type {dtype!s} has no SQLAlchemy mapping in destination DDL"
    )


def native_to_sqlalchemy(native_type: str, type_mapper: TypeMapper) -> Any:
    """Convenience wrapper: native SQL type → canonical → SQLAlchemy type."""
    canonical = type_mapper.to_canonical(native_type)
    return arrow_to_sqlalchemy(canonical_to_arrow(canonical))
