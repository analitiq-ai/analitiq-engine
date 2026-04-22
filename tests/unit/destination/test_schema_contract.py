"""Tests for Arrow-based DestinationSchemaContract.

All ``columns`` payloads require a ``TypeMapper`` — the contract delegates
native → canonical translation to the destination connector's type-map and
no longer ships a hardcoded dispatch. These tests build a minimal mapper
that covers the fixtures instead of reaching for the real connector files.
"""

import pytest

from src.destination.schema_contract import DestinationSchemaContract
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


class TestDestinationSchemaContractColumnsFormat:
    """Test schema contract with database columns array format."""

    def test_basic_columns_schema(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
                {"name": "created", "type": "TIMESTAMP", "nullable": True},
            ]
        }

        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)

        assert len(contract.arrow_schema) == 3
        assert set(contract.column_types) == {"id", "name", "created"}

    def test_cast_batch_basic(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
            ]
        }

        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)

        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        result = contract.prepare_records(records)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Alice"

    def test_cast_batch_with_type_coercion(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "value", "type": "FLOAT", "nullable": True},
            ]
        }

        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)

        # String numbers should be cast to proper types.
        records = [
            {"id": "1", "value": "3.14"},
            {"id": "2", "value": "2.71"},
        ]
        result = contract.prepare_records(records)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert isinstance(result[0]["value"], float)

    def test_cast_batch_empty(self, type_mapper):
        schema = {"columns": [{"name": "id", "type": "BIGINT", "nullable": False}]}
        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)
        assert contract.prepare_records([]) == []

    def test_missing_column_creates_nulls(self, type_mapper):
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "optional_field", "type": "VARCHAR(50)", "nullable": True},
            ]
        }
        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)

        result = contract.prepare_records([{"id": 1}])

        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["optional_field"] is None


class TestDestinationSchemaContractJsonSchema:
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
        contract = DestinationSchemaContract(schema)

        assert len(contract.arrow_schema) == 3
        assert set(contract.column_types) == {"id", "name", "active"}

    def test_json_schema_datetime_format(self):
        schema = {"properties": {"created_at": {"type": "string", "format": "date-time"}}}
        contract = DestinationSchemaContract(schema)
        assert "timestamp" in contract.column_types["created_at"].lower()


class TestDestinationSchemaContractTypeMapping:
    """Native SQL type → Arrow type mapping, driven by the connector's type-map."""

    def test_integer_types(self, type_mapper):
        schema = {
            "columns": [
                {"name": "big", "type": "BIGINT"},
                {"name": "normal", "type": "INTEGER"},
                {"name": "small", "type": "SMALLINT"},
            ]
        }
        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)

        assert "int64" in contract.column_types["big"]
        assert "int32" in contract.column_types["normal"]
        assert "int16" in contract.column_types["small"]

    def test_string_types(self, type_mapper):
        schema = {
            "columns": [
                {"name": "var", "type": "VARCHAR(100)"},
                {"name": "text_col", "type": "TEXT"},
            ]
        }
        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)

        assert "string" in contract.column_types["var"]
        assert "string" in contract.column_types["text_col"]

    def test_timestamp_types(self, type_mapper):
        schema = {
            "columns": [
                {"name": "ts", "type": "TIMESTAMP"},
                {"name": "tstz", "type": "TIMESTAMPTZ"},
                {"name": "dt", "type": "DATETIME"},
            ]
        }
        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)

        for col in ["ts", "tstz", "dt"]:
            assert "timestamp" in contract.column_types[col]

    def test_boolean_type(self, type_mapper):
        schema = {"columns": [{"name": "flag", "type": "BOOLEAN"}]}
        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)
        assert "bool" in contract.column_types["flag"]

    def test_decimal_type(self, type_mapper):
        schema = {"columns": [{"name": "price", "type": "DECIMAL(10,2)"}]}
        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)
        assert "decimal128" in contract.column_types["price"]


class TestDestinationSchemaContractEdgeCases:
    def test_empty_schema(self):
        contract = DestinationSchemaContract({})
        assert len(contract.arrow_schema) == 0
        assert contract.prepare_records([]) == []

    def test_columns_payload_requires_type_mapper(self):
        schema = {"columns": [{"name": "id", "type": "BIGINT"}]}
        with pytest.raises(ValueError, match="type_mapper is required"):
            DestinationSchemaContract(schema)

    def test_unmapped_native_type_raises(self, type_mapper):
        schema = {"columns": [{"name": "custom", "type": "CUSTOM_UNKNOWN_TYPE"}]}
        with pytest.raises(UnmappedTypeError):
            DestinationSchemaContract(schema, type_mapper=type_mapper)

    def test_column_without_name_skipped(self, type_mapper):
        schema = {
            "columns": [
                {"type": "BIGINT"},  # No name
                {"name": "valid", "type": "BIGINT"},
            ]
        }
        contract = DestinationSchemaContract(schema, type_mapper=type_mapper)

        assert len(contract.arrow_schema) == 1
        assert "valid" in contract.column_types
