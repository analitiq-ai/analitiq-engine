"""Tests for the Arrow-based ``SchemaContract``.

The contract is a single class used at both source and destination
boundaries. ``columns`` payloads require a ``TypeMapper`` — the contract
delegates native → canonical translation to the connector's type-map and
no longer ships a hardcoded dispatch.
"""

import pyarrow as pa
import pytest

from src.destination.schema_contract import SchemaContract
from src.engine.type_map import TypeMapper, UnmappedTypeError
from src.engine.type_map.rules import parse_rules


TEST_TYPE_MAP_RULES = [
    {"match": "exact", "native": "BOOLEAN", "canonical": "Boolean"},
    {"match": "exact", "native": "SMALLINT", "canonical": "Int16"},
    {"match": "exact", "native": "INTEGER", "canonical": "Int32"},
    {"match": "exact", "native": "BIGINT", "canonical": "Int64"},
    {"match": "exact", "native": "FLOAT", "canonical": "Float32"},
    {"match": "exact", "native": "TEXT", "canonical": "Utf8"},
    {"match": "exact", "native": "DATETIME", "canonical": "Timestamp(us)"},
    {"match": "exact", "native": "TIMESTAMP", "canonical": "Timestamp(us)"},
    {"match": "exact", "native": "TIMESTAMPTZ", "canonical": "Timestamp(us, UTC)"},
    {"match": "regex", "native": r"^VARCHAR\(\s*\d+\s*\)$", "canonical": "Utf8"},
    {
        "match": "regex",
        "native": r"^DECIMAL\(\s*(?<p>\d+)\s*,\s*(?<s>\d+)\s*\)$",
        "canonical": "Decimal128(${p}, ${s})",
    },
]


@pytest.fixture
def type_mapper() -> TypeMapper:
    return TypeMapper("test", parse_rules(TEST_TYPE_MAP_RULES, source="<test>"))


class TestSchemaContractColumnsFormat:
    """Schema construction for database ``columns`` array payloads."""

    def test_basic_columns_schema(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "native_type": "BIGINT", "nullable": False},
                {"name": "name", "native_type": "VARCHAR(100)", "nullable": True},
                {"name": "created", "native_type": "TIMESTAMP", "nullable": True},
            ]
        }

        contract = SchemaContract(schema, type_mapper=type_mapper)

        assert len(contract.arrow_schema) == 3
        assert set(contract.column_types) == {"id", "name", "created"}


class TestSchemaContractCastArrowBatch:
    """Casting an incoming ``pa.RecordBatch`` to the destination schema."""

    def test_cast_arrow_batch_basic(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "native_type": "BIGINT", "nullable": False},
                {"name": "name", "native_type": "VARCHAR(100)", "nullable": True},
            ]
        }
        contract = SchemaContract(schema, type_mapper=type_mapper)

        source = pa.RecordBatch.from_pylist(
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        )
        cast = contract.cast_arrow_batch(source)
        result = contract.to_dicts(cast)

        assert cast.schema == contract.arrow_schema
        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "Alice"}

    def test_cast_arrow_batch_with_type_coercion(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "native_type": "BIGINT", "nullable": False},
                {"name": "value", "native_type": "FLOAT", "nullable": True},
            ]
        }
        contract = SchemaContract(schema, type_mapper=type_mapper)

        # Source happens to ship the columns as strings (e.g. an API
        # source where everything came through JSON as text).
        source = pa.RecordBatch.from_pylist(
            [{"id": "1", "value": "3.14"}, {"id": "2", "value": "2.71"}]
        )
        cast = contract.cast_arrow_batch(source)
        result = contract.to_dicts(cast)

        assert result[0]["id"] == 1
        assert isinstance(result[0]["value"], float)

    def test_cast_arrow_batch_empty(self, type_mapper):
        schema = {"columns": [{"name": "id", "native_type": "BIGINT", "nullable": False}]}
        contract = SchemaContract(schema, type_mapper=type_mapper)

        empty = pa.RecordBatch.from_pylist([], schema=contract.arrow_schema)
        cast = contract.cast_arrow_batch(empty)

        assert cast.num_rows == 0
        assert contract.to_dicts(cast) == []

    def test_cast_arrow_batch_missing_column_fills_nulls(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "native_type": "BIGINT", "nullable": False},
                {"name": "optional_field", "native_type": "VARCHAR(50)", "nullable": True},
            ]
        }
        contract = SchemaContract(schema, type_mapper=type_mapper)

        source = pa.RecordBatch.from_pylist([{"id": 1}])
        cast = contract.cast_arrow_batch(source)
        result = contract.to_dicts(cast)

        assert result == [{"id": 1, "optional_field": None}]

    def test_cast_arrow_batch_drops_extra_columns(self, type_mapper):
        schema = {
            "columns": [{"name": "id", "native_type": "BIGINT", "nullable": False}]
        }
        contract = SchemaContract(schema, type_mapper=type_mapper)

        source = pa.RecordBatch.from_pylist([{"id": 1, "extra": "drop me"}])
        cast = contract.cast_arrow_batch(source)

        assert cast.schema.names == ["id"]
        assert contract.to_dicts(cast) == [{"id": 1}]


class TestSchemaContractFromPylist:
    """Source-side: building an Arrow batch from dicts."""

    def test_from_pylist_with_schema(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "native_type": "BIGINT", "nullable": False},
                {"name": "name", "native_type": "VARCHAR(100)", "nullable": True},
            ]
        }
        contract = SchemaContract(schema, type_mapper=type_mapper)

        batch = contract.from_pylist([{"id": 1, "name": "Alice"}])

        assert batch.schema == contract.arrow_schema
        assert batch.to_pylist() == [{"id": 1, "name": "Alice"}]


class TestSchemaContractJsonSchema:
    """JSON Schema payloads do not use the type-map — they are self-describing."""

    def test_json_schema_format(self):
        schema = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "active": {"type": "boolean"},
            },
            "required": ["id"],
        }
        contract = SchemaContract(schema)

        assert len(contract.arrow_schema) == 3
        assert set(contract.column_types) == {"id", "name", "active"}

    def test_json_schema_datetime_format(self):
        schema = {"properties": {"created_at": {"type": "string", "format": "date-time"}}}
        contract = SchemaContract(schema)
        assert "timestamp" in contract.column_types["created_at"].lower()

    def test_json_schema_date_format_is_date32(self):
        """``format: "date"`` must map to date32 on the Arrow path so it
        agrees with the SQLAlchemy DDL path (which uses ``Date``). If
        these drift, the column is cast to one shape at write time and
        DDL'd as another — failures surface far from the root cause."""
        schema = {"properties": {"dob": {"type": "string", "format": "date"}}}
        contract = SchemaContract(schema)
        assert "date32" in contract.column_types["dob"]

    def test_json_schema_unknown_type_raises(self):
        schema = {"properties": {"weird": {"type": "polygon"}}}
        with pytest.raises(ValueError, match="unsupported type/format"):
            SchemaContract(schema)


class TestSchemaContractTypeMapping:
    """Native SQL type → Arrow type mapping, driven by the connector's type-map."""

    def test_integer_types(self, type_mapper):
        schema = {
            "columns": [
                {"name": "big", "native_type": "BIGINT"},
                {"name": "normal", "native_type": "INTEGER"},
                {"name": "small", "native_type": "SMALLINT"},
            ]
        }
        contract = SchemaContract(schema, type_mapper=type_mapper)

        assert "int64" in contract.column_types["big"]
        assert "int32" in contract.column_types["normal"]
        assert "int16" in contract.column_types["small"]

    def test_string_types(self, type_mapper):
        schema = {
            "columns": [
                {"name": "var", "native_type": "VARCHAR(100)"},
                {"name": "text_col", "native_type": "TEXT"},
            ]
        }
        contract = SchemaContract(schema, type_mapper=type_mapper)

        assert "string" in contract.column_types["var"]
        assert "string" in contract.column_types["text_col"]

    def test_timestamp_types(self, type_mapper):
        schema = {
            "columns": [
                {"name": "ts", "native_type": "TIMESTAMP"},
                {"name": "tstz", "native_type": "TIMESTAMPTZ"},
                {"name": "dt", "native_type": "DATETIME"},
            ]
        }
        contract = SchemaContract(schema, type_mapper=type_mapper)

        for col in ["ts", "tstz", "dt"]:
            assert "timestamp" in contract.column_types[col]

    def test_boolean_type(self, type_mapper):
        schema = {"columns": [{"name": "flag", "native_type": "BOOLEAN"}]}
        contract = SchemaContract(schema, type_mapper=type_mapper)
        assert "bool" in contract.column_types["flag"]

    def test_decimal_type(self, type_mapper):
        schema = {"columns": [{"name": "price", "native_type": "DECIMAL(10,2)"}]}
        contract = SchemaContract(schema, type_mapper=type_mapper)
        assert "decimal128" in contract.column_types["price"]


class TestSchemaContractEdgeCases:
    def test_empty_schema(self):
        contract = SchemaContract({})
        assert len(contract.arrow_schema) == 0

    def test_columns_payload_requires_type_mapper(self):
        schema = {"columns": [{"name": "id", "native_type": "BIGINT"}]}
        with pytest.raises(ValueError, match="type_mapper is required"):
            SchemaContract(schema)

    def test_unmapped_native_type_raises(self, type_mapper):
        schema = {"columns": [{"name": "custom", "native_type": "CUSTOM_UNKNOWN_TYPE"}]}
        with pytest.raises(UnmappedTypeError):
            SchemaContract(schema, type_mapper=type_mapper)

    def test_column_without_name_raises(self, type_mapper):
        """Unnamed columns are a malformed-payload signal, not something
        to silently skip — silently dropping them hides author errors."""
        schema = {
            "columns": [
                {"native_type": "BIGINT"},  # No name
                {"name": "valid", "native_type": "BIGINT"},
            ]
        }
        with pytest.raises(ValueError, match="has no 'name' field"):
            SchemaContract(schema, type_mapper=type_mapper)
