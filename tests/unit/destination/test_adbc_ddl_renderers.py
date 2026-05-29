"""DDL renderers for ADBC-only destination dialects.

Each renderer maps ``pa.DataType`` to a native warehouse DDL fragment.
They're tiny pure functions; the tests pin the Arrow vocabulary the
schema contract emits to each warehouse's column type so a renderer
change doesn't silently regress what gets created.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from src.destination.sql_types import (
    arrow_to_bigquery_native,
    arrow_to_postgres_native,
    arrow_to_snowflake_native,
)


class TestArrowToSnowflakeNative:
    def test_scalars(self):
        assert arrow_to_snowflake_native(pa.bool_()) == "BOOLEAN"
        assert arrow_to_snowflake_native(pa.int8()) == "INTEGER"
        assert arrow_to_snowflake_native(pa.int64()) == "INTEGER"
        assert arrow_to_snowflake_native(pa.uint32()) == "INTEGER"
        assert arrow_to_snowflake_native(pa.float64()) == "FLOAT"
        assert arrow_to_snowflake_native(pa.string()) == "VARCHAR"
        assert arrow_to_snowflake_native(pa.large_string()) == "VARCHAR"
        assert arrow_to_snowflake_native(pa.binary()) == "BINARY"
        assert arrow_to_snowflake_native(pa.date32()) == "DATE"

    def test_decimal(self):
        assert (
            arrow_to_snowflake_native(pa.decimal128(38, 9))
            == "NUMBER(38, 9)"
        )

    def test_timestamp_tz_split(self):
        assert (
            arrow_to_snowflake_native(pa.timestamp("us", tz="UTC"))
            == "TIMESTAMP_TZ"
        )
        assert arrow_to_snowflake_native(pa.timestamp("us")) == "TIMESTAMP_NTZ"

    def test_nested_to_variant(self):
        assert (
            arrow_to_snowflake_native(pa.struct([("a", pa.int64())]))
            == "VARIANT"
        )
        assert arrow_to_snowflake_native(pa.list_(pa.int64())) == "VARIANT"

    def test_unmappable_raises(self):
        with pytest.raises(ValueError, match="no Snowflake DDL mapping"):
            arrow_to_snowflake_native(pa.dictionary(pa.int32(), pa.string()))


class TestArrowToBigqueryNative:
    def test_scalars(self):
        assert arrow_to_bigquery_native(pa.bool_()) == "BOOL"
        assert arrow_to_bigquery_native(pa.int64()) == "INT64"
        assert arrow_to_bigquery_native(pa.uint64()) == "BIGNUMERIC"
        assert arrow_to_bigquery_native(pa.float64()) == "FLOAT64"
        assert arrow_to_bigquery_native(pa.string()) == "STRING"
        assert arrow_to_bigquery_native(pa.binary()) == "BYTES"
        assert arrow_to_bigquery_native(pa.date32()) == "DATE"

    def test_decimal_within_numeric(self):
        assert (
            arrow_to_bigquery_native(pa.decimal128(18, 2))
            == "NUMERIC(18, 2)"
        )

    def test_decimal_overflows_to_bignumeric(self):
        # precision 38, scale 10 — scale exceeds NUMERIC's max of 9
        assert (
            arrow_to_bigquery_native(pa.decimal128(38, 10))
            == "BIGNUMERIC(38, 10)"
        )

    # The BIGNUMERIC limits branch (precision > 76 OR scale > 38) is
    # defensive: pyarrow already rejects ``decimal256(precision > 76)``
    # at construction, and Arrow requires ``scale <= precision`` so
    # scale > 38 only fires for precision > 38 which already lands in
    # BIGNUMERIC. The branch lives in the renderer to make the failure
    # mode obvious if a future Arrow ever widens the type.

    def test_timestamp_tz_split(self):
        assert (
            arrow_to_bigquery_native(pa.timestamp("us", tz="UTC"))
            == "TIMESTAMP"
        )
        assert arrow_to_bigquery_native(pa.timestamp("us")) == "DATETIME"

    def test_nested_to_json(self):
        assert (
            arrow_to_bigquery_native(pa.struct([("a", pa.int64())]))
            == "JSON"
        )
        assert arrow_to_bigquery_native(pa.list_(pa.int64())) == "JSON"


class TestArrowToPostgresNative:
    def test_scalars(self):
        assert arrow_to_postgres_native(pa.bool_()) == "BOOLEAN"
        assert arrow_to_postgres_native(pa.int8()) == "SMALLINT"
        assert arrow_to_postgres_native(pa.int32()) == "INTEGER"
        assert arrow_to_postgres_native(pa.int64()) == "BIGINT"
        assert arrow_to_postgres_native(pa.float64()) == "DOUBLE PRECISION"
        assert arrow_to_postgres_native(pa.string()) == "TEXT"
        assert arrow_to_postgres_native(pa.binary()) == "BYTEA"

    def test_decimal(self):
        assert (
            arrow_to_postgres_native(pa.decimal128(18, 2))
            == "NUMERIC(18, 2)"
        )

    def test_timestamp_tz_split(self):
        assert (
            arrow_to_postgres_native(pa.timestamp("us", tz="UTC"))
            == "TIMESTAMP WITH TIME ZONE"
        )
        assert arrow_to_postgres_native(pa.timestamp("us")) == "TIMESTAMP"

    def test_nested_to_jsonb(self):
        assert (
            arrow_to_postgres_native(pa.struct([("a", pa.int64())]))
            == "JSONB"
        )
        assert arrow_to_postgres_native(pa.list_(pa.int64())) == "JSONB"
