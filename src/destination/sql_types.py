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


def arrow_to_snowflake_native(dtype: pa.DataType) -> str:
    """Return a Snowflake DDL type string for the given Arrow ``DataType``.

    Used by the ADBC-only destination path: Snowflake has no async
    SQLAlchemy driver, so the engine cannot compile DDL via the
    snowflake-sqlalchemy dialect. Nested types map to ``VARIANT`` —
    Snowflake's semi-structured column type that accepts arbitrary
    JSON without per-element typing.
    """
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if (
        pa.types.is_int8(dtype)
        or pa.types.is_int16(dtype)
        or pa.types.is_int32(dtype)
        or pa.types.is_int64(dtype)
        or pa.types.is_uint8(dtype)
        or pa.types.is_uint16(dtype)
        or pa.types.is_uint32(dtype)
        or pa.types.is_uint64(dtype)
    ):
        # Snowflake collapses INTEGER/BIGINT into NUMBER(38, 0) under
        # the hood; the keyword chosen here is purely cosmetic.
        return "INTEGER"
    if pa.types.is_floating(dtype):
        return "FLOAT"
    if pa.types.is_decimal(dtype):
        return f"NUMBER({dtype.precision}, {dtype.scale})"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "VARCHAR"
    if (
        pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_fixed_size_binary(dtype)
    ):
        return "BINARY"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_time(dtype):
        return "TIME"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP_TZ" if dtype.tz is not None else "TIMESTAMP_NTZ"
    if (
        pa.types.is_struct(dtype)
        or pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
    ):
        return "VARIANT"
    raise ValueError(
        f"Arrow type {dtype!s} has no Snowflake DDL mapping"
    )


def native_to_snowflake(native_type: str, type_mapper: TypeMapper) -> str:
    """Convenience wrapper: native SQL type → Arrow type → Snowflake DDL string."""
    arrow_type = type_mapper.to_arrow_type(native_type)
    if arrow_type == "Json":
        return "VARIANT"
    return arrow_to_snowflake_native(parse_arrow_type(arrow_type))


def arrow_to_bigquery_native(dtype: pa.DataType) -> str:
    """Return a BigQuery Standard SQL DDL type string for an Arrow ``DataType``.

    Used by the ADBC-only destination path: BigQuery has no async
    SQLAlchemy driver. BigQuery's NUMERIC tops out at 38 digit
    precision; anything wider falls through to BIGNUMERIC (76 digits).
    Nested types map to ``JSON`` rather than STRUCT/ARRAY — STRUCT/ARRAY
    would require materialising the Arrow nested schema as BigQuery
    field declarations, which the contract does not yet carry.
    """
    if pa.types.is_boolean(dtype):
        return "BOOL"
    if (
        pa.types.is_int8(dtype)
        or pa.types.is_int16(dtype)
        or pa.types.is_int32(dtype)
        or pa.types.is_int64(dtype)
        or pa.types.is_uint8(dtype)
        or pa.types.is_uint16(dtype)
        or pa.types.is_uint32(dtype)
    ):
        return "INT64"
    if pa.types.is_uint64(dtype):
        # uint64 max exceeds INT64; demote to BIGNUMERIC to preserve range.
        return "BIGNUMERIC"
    if pa.types.is_floating(dtype):
        return "FLOAT64"
    if pa.types.is_decimal(dtype):
        # BigQuery NUMERIC: max precision 38, max scale 9. Anything
        # outside that range goes to BIGNUMERIC (max precision 76,
        # max scale 38). The contract's Decimal128(38, 9) is the
        # widest type guaranteed to fit NUMERIC.
        precision = dtype.precision
        scale = dtype.scale
        if precision <= 38 and scale <= 9:
            return f"NUMERIC({precision}, {scale})"
        if precision <= 76 and scale <= 38:
            return f"BIGNUMERIC({precision}, {scale})"
        raise ValueError(
            f"Arrow Decimal({precision}, {scale}) exceeds BigQuery's "
            "BIGNUMERIC limits (precision <= 76, scale <= 38)"
        )
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "STRING"
    if (
        pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_fixed_size_binary(dtype)
    ):
        return "BYTES"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_time(dtype):
        return "TIME"
    if pa.types.is_timestamp(dtype):
        # BigQuery TIMESTAMP is always UTC; DATETIME has no zone. The
        # contract's tz flag distinguishes them at the column level.
        return "TIMESTAMP" if dtype.tz is not None else "DATETIME"
    if (
        pa.types.is_struct(dtype)
        or pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
    ):
        return "JSON"
    raise ValueError(
        f"Arrow type {dtype!s} has no BigQuery DDL mapping"
    )


def native_to_bigquery(native_type: str, type_mapper: TypeMapper) -> str:
    """Convenience wrapper: native SQL type → Arrow type → BigQuery DDL string."""
    arrow_type = type_mapper.to_arrow_type(native_type)
    if arrow_type == "Json":
        return "JSON"
    return arrow_to_bigquery_native(parse_arrow_type(arrow_type))


def arrow_to_postgres_native(dtype: pa.DataType) -> str:
    """Return a PostgreSQL DDL type string for an Arrow ``DataType``.

    Used by the ADBC-only destination path when a connector declares
    ``transport_type: "adbc", driver: "postgresql"`` (typical for
    Redshift via the libpq-compatible ADBC driver). When the connector
    can use ``transport_type: "sqlalchemy"`` instead, the SA path
    handles DDL generically via the dialect compiler — this renderer
    only fires for the ADBC-only path.
    """
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if pa.types.is_int8(dtype) or pa.types.is_int16(dtype):
        return "SMALLINT"
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return "INTEGER"
    if (
        pa.types.is_int64(dtype)
        or pa.types.is_uint32(dtype)
        or pa.types.is_uint64(dtype)
    ):
        return "BIGINT"
    if pa.types.is_uint8(dtype):
        return "SMALLINT"
    if pa.types.is_floating(dtype):
        return "DOUBLE PRECISION"
    if pa.types.is_decimal(dtype):
        return f"NUMERIC({dtype.precision}, {dtype.scale})"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "TEXT"
    if (
        pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_fixed_size_binary(dtype)
    ):
        return "BYTEA"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_time(dtype):
        return "TIME"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP WITH TIME ZONE" if dtype.tz is not None else "TIMESTAMP"
    if (
        pa.types.is_struct(dtype)
        or pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
    ):
        return "JSONB"
    raise ValueError(
        f"Arrow type {dtype!s} has no PostgreSQL DDL mapping"
    )


def native_to_postgres(native_type: str, type_mapper: TypeMapper) -> str:
    """Convenience wrapper: native SQL type → Arrow type → PostgreSQL DDL string."""
    arrow_type = type_mapper.to_arrow_type(native_type)
    if arrow_type == "Json":
        return "JSONB"
    return arrow_to_postgres_native(parse_arrow_type(arrow_type))
