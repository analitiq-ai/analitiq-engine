"""Tests for canonical Arrow → SQLAlchemy translation.

Covers every Arrow family :func:`arrow_to_sqlalchemy` can reach, the
end-to-end ``native → canonical → Arrow → SQLAlchemy`` chain via
:func:`native_to_sqlalchemy`, and the rejection path for unsupported
Arrow types.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
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

from src.destination.sql_types import arrow_to_sqlalchemy, native_to_sqlalchemy
from src.engine.type_map import TypeMapper
from src.engine.type_map.rules import parse_rules


class TestArrowToSqlAlchemyPrimitives:
    @pytest.mark.parametrize(
        "arrow_type, sa_cls",
        [
            (pa.bool_(), Boolean),
            (pa.int8(), SmallInteger),
            (pa.int16(), SmallInteger),
            (pa.int32(), Integer),
            (pa.int64(), BigInteger),
            (pa.uint8(), SmallInteger),
            (pa.uint16(), Integer),
            (pa.uint32(), BigInteger),
            (pa.uint64(), BigInteger),
            (pa.float16(), Float),
            (pa.float32(), Float),
            (pa.float64(), Float),
            (pa.string(), Text),
            (pa.large_string(), Text),
            (pa.binary(), LargeBinary),
            (pa.large_binary(), LargeBinary),
            (pa.binary(16), LargeBinary),
            (pa.date32(), Date),
            (pa.date64(), Date),
            (pa.time32("s"), Time),
            (pa.time64("us"), Time),
        ],
    )
    def test_primitive_mapping(self, arrow_type, sa_cls):
        assert isinstance(arrow_to_sqlalchemy(arrow_type), sa_cls)


class TestArrowToSqlAlchemyParameterized:
    def test_decimal_preserves_precision_and_scale(self):
        sa_type = arrow_to_sqlalchemy(pa.decimal128(18, 2))
        assert isinstance(sa_type, Numeric)
        assert sa_type.precision == 18
        assert sa_type.scale == 2

    def test_decimal256_preserves_precision_and_scale(self):
        sa_type = arrow_to_sqlalchemy(pa.decimal256(38, 10))
        assert isinstance(sa_type, Numeric)
        assert sa_type.precision == 38
        assert sa_type.scale == 10

    def test_timestamp_without_tz_is_naive_datetime(self):
        sa_type = arrow_to_sqlalchemy(pa.timestamp("us"))
        assert isinstance(sa_type, DateTime)
        assert sa_type.timezone is False

    def test_timestamp_with_tz_is_tz_aware(self):
        sa_type = arrow_to_sqlalchemy(pa.timestamp("us", tz="UTC"))
        assert isinstance(sa_type, DateTime)
        assert sa_type.timezone is True


class TestArrowToSqlAlchemyRejection:
    def test_list_type_rejected(self):
        with pytest.raises(ValueError, match="has no SQLAlchemy mapping"):
            arrow_to_sqlalchemy(pa.list_(pa.int32()))

    def test_struct_type_rejected(self):
        with pytest.raises(ValueError, match="has no SQLAlchemy mapping"):
            arrow_to_sqlalchemy(pa.struct([pa.field("x", pa.int32())]))


class TestNativeToSqlAlchemyChain:
    """End-to-end: native SQL string → canonical → Arrow → SQLAlchemy."""

    @pytest.fixture
    def mapper(self) -> TypeMapper:
        return TypeMapper(
            "test",
            parse_rules(
                [
                    {"match": "exact", "native": "BIGINT", "canonical": "Int64"},
                    {"match": "exact", "native": "BOOLEAN", "canonical": "Boolean"},
                    {
                        "match": "regex",
                        "native": r"^VARCHAR\(\s*\d+\s*\)$",
                        "canonical": "Utf8",
                    },
                    {
                        "match": "regex",
                        "native": r"^NUMERIC\(\s*(?<p>\d+)\s*,\s*(?<s>\d+)\s*\)$",
                        "canonical": "Decimal128(${p}, ${s})",
                    },
                ],
                source="<test>",
            ),
        )

    def test_exact_chain(self, mapper):
        assert isinstance(native_to_sqlalchemy("BIGINT", mapper), BigInteger)
        assert isinstance(native_to_sqlalchemy("boolean", mapper), Boolean)

    def test_regex_chain_carries_parameters(self, mapper):
        sa_type = native_to_sqlalchemy("NUMERIC(18, 2)", mapper)
        assert isinstance(sa_type, Numeric)
        assert sa_type.precision == 18
        assert sa_type.scale == 2

    def test_regex_without_tokens(self, mapper):
        assert isinstance(native_to_sqlalchemy("VARCHAR(255)", mapper), Text)

    def test_unmapped_native_bubbles_up(self, mapper):
        from src.engine.type_map import UnmappedTypeError

        with pytest.raises(UnmappedTypeError):
            native_to_sqlalchemy("MONEY", mapper)
