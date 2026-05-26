"""Tests for the Snowflake-specific DDL helpers in the destination handler.

The handler under test is constructed without a runtime — these tests
poke the pure helpers (identifier quoting, CREATE TABLE rendering, the
``_batch_commits`` DDL) which is enough to catch regressions in the
ADBC-only schema path without spinning up Snowflake itself.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from src.destination.connectors.database import DatabaseDestinationHandler
from src.destination.sql_types import (
    arrow_to_snowflake_native,
    native_to_snowflake,
)
from src.engine.type_map.mapper import TypeMapper
from src.engine.type_map.rules import TypeMapRule


def _type_mapper() -> TypeMapper:
    """Minimal type-map that mirrors the Snowflake connector contract."""
    return TypeMapper(
        "snowflake",
        [
            TypeMapRule(match="exact", native="INTEGER", canonical="Int64"),
            TypeMapRule(match="exact", native="VARCHAR", canonical="Utf8"),
            TypeMapRule(
                match="exact", native="TIMESTAMP_TZ", canonical="Timestamp(us, UTC)"
            ),
            TypeMapRule(match="exact", native="VARIANT", canonical="Json"),
        ],
    )


class TestArrowToSnowflakeNative:
    def test_int_collapses_to_integer(self):
        assert arrow_to_snowflake_native(pa.int32()) == "INTEGER"
        assert arrow_to_snowflake_native(pa.int64()) == "INTEGER"

    def test_decimal_carries_precision_and_scale(self):
        assert arrow_to_snowflake_native(pa.decimal128(12, 4)) == "NUMBER(12, 4)"

    def test_string_becomes_varchar(self):
        assert arrow_to_snowflake_native(pa.string()) == "VARCHAR"
        assert arrow_to_snowflake_native(pa.large_string()) == "VARCHAR"

    def test_timestamp_with_tz_uses_tz_variant(self):
        assert arrow_to_snowflake_native(pa.timestamp("us", tz="UTC")) == "TIMESTAMP_TZ"
        assert arrow_to_snowflake_native(pa.timestamp("us")) == "TIMESTAMP_NTZ"

    def test_struct_and_list_map_to_variant(self):
        struct_type = pa.struct([("a", pa.int64())])
        assert arrow_to_snowflake_native(struct_type) == "VARIANT"
        assert arrow_to_snowflake_native(pa.list_(pa.int64())) == "VARIANT"

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="no Snowflake DDL mapping"):
            arrow_to_snowflake_native(pa.duration("us"))


class TestNativeToSnowflake:
    def test_json_marker_short_circuits_to_variant(self):
        assert native_to_snowflake("VARIANT", _type_mapper()) == "VARIANT"

    def test_round_trips_through_arrow(self):
        assert native_to_snowflake("INTEGER", _type_mapper()) == "INTEGER"
        assert native_to_snowflake("VARCHAR", _type_mapper()) == "VARCHAR"


class TestSnowflakeDdlBuilders:
    @pytest.fixture
    def handler(self) -> DatabaseDestinationHandler:
        h = DatabaseDestinationHandler()
        h._driver = "snowflake"
        return h

    def test_quote_ident_escapes_internal_quotes(self, handler):
        assert handler._adbc_quote_ident('weird"name') == '"weird""name"'

    def test_quote_qualified_handles_empty_schema(self, handler):
        assert handler._adbc_quote_qualified("", "t") == '"t"'
        assert handler._adbc_quote_qualified("s", "t") == '"s"."t"'

    def test_create_table_includes_primary_key_and_synced_at(self, handler):
        endpoint = {
            "columns": [
                {"name": "id", "native_type": "INTEGER", "nullable": False},
                {"name": "name", "native_type": "VARCHAR"},
            ],
        }
        ddl = handler._build_snowflake_create_table_ddl(
            "orders", "public", endpoint, ["id"], _type_mapper(),
        )
        assert 'CREATE TABLE IF NOT EXISTS "public"."orders"' in ddl
        assert '"id" INTEGER NOT NULL' in ddl
        assert '"name" VARCHAR' in ddl
        # _synced_at is auto-appended; it must be timezone-aware to match
        # the SA path's DateTime(timezone=True).
        assert '"_synced_at" TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP' in ddl
        assert 'PRIMARY KEY ("id")' in ddl

    def test_create_table_does_not_double_add_synced_at(self, handler):
        endpoint = {
            "columns": [
                {"name": "_synced_at", "native_type": "TIMESTAMP_TZ"},
            ],
        }
        ddl = handler._build_snowflake_create_table_ddl(
            "t", "s", endpoint, [], _type_mapper(),
        )
        assert ddl.count('"_synced_at"') == 1

    def test_batch_commits_ddl_uses_binary_for_cursor(self, handler):
        ddl = handler._build_snowflake_batch_commits_ddl("ops")
        assert 'CREATE TABLE IF NOT EXISTS "ops"."_batch_commits"' in ddl
        assert '"committed_cursor" BINARY' in ddl
        assert 'PRIMARY KEY ("run_id", "stream_id", "batch_seq")' in ddl

    def test_missing_native_type_is_rejected(self, handler):
        endpoint = {"columns": [{"name": "id"}]}
        with pytest.raises(ValueError, match="no 'native_type'"):
            handler._build_snowflake_create_table_ddl(
                "t", "s", endpoint, [], _type_mapper(),
            )
