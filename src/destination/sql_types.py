"""Arrow → SQLAlchemy type translation for destination DDL."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from sqlalchemy import (
    JSON,
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
from sqlalchemy.dialects.postgresql import JSONB

from src.engine.type_map import TypeMapper, parse_arrow_type


def arrow_to_sqlalchemy(dtype: pa.DataType) -> Any:
    """Return a SQLAlchemy column type for the given Arrow ``DataType``.

    Nested types (``pa.struct``, ``pa.list_``) map to a SQLAlchemy ``JSON``
    column with a PostgreSQL ``JSONB`` variant so SA serializes Python
    dicts/lists into the column without per-record encoding.
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
    if (
        pa.types.is_struct(dtype)
        or pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
    ):
        return JSON().with_variant(JSONB(), "postgresql")
    raise ValueError(
        f"Arrow type {dtype!s} has no SQLAlchemy mapping in destination DDL"
    )


def native_to_sqlalchemy(native_type: str, type_mapper: TypeMapper) -> Any:
    """Convenience wrapper: native SQL type → Arrow type → SQLAlchemy type.

    Native columns whose type-map points to the opaque ``"Json"`` marker
    short-circuit to a SQLAlchemy ``JSON`` column (``JSONB`` on Postgres)
    — ``parse_arrow_type("Json")`` returns ``pa.large_string()`` (the
    wire shape), which alone would land in a ``TEXT`` column and lose
    the per-dialect JSON semantics.
    """
    arrow_type = type_mapper.to_arrow_type(native_type)
    if arrow_type == "Json":
        return JSON().with_variant(JSONB(), "postgresql")
    return arrow_to_sqlalchemy(parse_arrow_type(arrow_type))
