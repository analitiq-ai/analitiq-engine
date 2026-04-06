"""Tests for Arrow-based DestinationSchemaContract."""

import pytest
from datetime import datetime

from src.destination.schema_contract import DestinationSchemaContract


class TestDestinationSchemaContractColumnsFormat:
    """Test schema contract with database columns array format."""

    def test_basic_columns_schema(self):
        """Test building schema from columns array."""
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
                {"name": "created", "type": "TIMESTAMP", "nullable": True},
            ]
        }

        contract = DestinationSchemaContract(schema)

        assert len(contract.arrow_schema) == 3
        assert "id" in contract.column_types
        assert "name" in contract.column_types
        assert "created" in contract.column_types

    def test_cast_batch_basic(self):
        """Test basic batch casting."""
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
            ]
        }

        contract = DestinationSchemaContract(schema)

        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        result = contract.prepare_records(records)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Alice"

    def test_cast_batch_with_type_coercion(self):
        """Test batch casting with type coercion."""
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "value", "type": "FLOAT", "nullable": True},
            ]
        }

        contract = DestinationSchemaContract(schema)

        # String numbers should be cast to proper types
        records = [
            {"id": "1", "value": "3.14"},
            {"id": "2", "value": "2.71"},
        ]

        result = contract.prepare_records(records)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert isinstance(result[0]["value"], float)

    def test_cast_batch_empty(self):
        """Test casting empty batch."""
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
            ]
        }

        contract = DestinationSchemaContract(schema)
        result = contract.prepare_records([])

        assert result == []

    def test_missing_column_creates_nulls(self):
        """Test that missing columns create null values."""
        schema = {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "optional_field", "type": "VARCHAR(50)", "nullable": True},
            ]
        }

        contract = DestinationSchemaContract(schema)

        # Record missing optional_field
        records = [{"id": 1}]

        result = contract.prepare_records(records)

        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["optional_field"] is None


class TestDestinationSchemaContractJsonSchema:
    """Test schema contract with JSON Schema format."""

    def test_json_schema_format(self):
        """Test building schema from JSON Schema."""
        schema = {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "active": {"type": "boolean"},
            },
            "required": ["id"]
        }

        contract = DestinationSchemaContract(schema)

        assert len(contract.arrow_schema) == 3
        assert "id" in contract.column_types
        assert "name" in contract.column_types
        assert "active" in contract.column_types

    def test_json_schema_datetime_format(self):
        """Test JSON Schema with date-time format."""
        schema = {
            "properties": {
                "created_at": {"type": "string", "format": "date-time"},
            }
        }

        contract = DestinationSchemaContract(schema)

        assert "timestamp" in contract.column_types["created_at"].lower()


class TestDestinationSchemaContractTypeMapping:
    """Test native SQL type to Arrow type mapping."""

    def test_integer_types(self):
        """Test integer type mapping."""
        schema = {
            "columns": [
                {"name": "big", "type": "BIGINT"},
                {"name": "normal", "type": "INTEGER"},
                {"name": "small", "type": "SMALLINT"},
            ]
        }

        contract = DestinationSchemaContract(schema)

        assert "int64" in contract.column_types["big"]
        assert "int32" in contract.column_types["normal"]
        assert "int16" in contract.column_types["small"]

    def test_string_types(self):
        """Test string type mapping."""
        schema = {
            "columns": [
                {"name": "var", "type": "VARCHAR(100)"},
                {"name": "text_col", "type": "TEXT"},
            ]
        }

        contract = DestinationSchemaContract(schema)

        assert "string" in contract.column_types["var"]
        assert "string" in contract.column_types["text_col"]

    def test_timestamp_types(self):
        """Test timestamp type mapping."""
        schema = {
            "columns": [
                {"name": "ts", "type": "TIMESTAMP"},
                {"name": "tstz", "type": "TIMESTAMPTZ"},
                {"name": "dt", "type": "DATETIME"},
            ]
        }

        contract = DestinationSchemaContract(schema)

        for col in ["ts", "tstz", "dt"]:
            assert "timestamp" in contract.column_types[col]

    def test_boolean_type(self):
        """Test boolean type mapping."""
        schema = {
            "columns": [
                {"name": "flag", "type": "BOOLEAN"},
            ]
        }

        contract = DestinationSchemaContract(schema)

        assert "bool" in contract.column_types["flag"]

    def test_decimal_type(self):
        """Test decimal type mapping."""
        schema = {
            "columns": [
                {"name": "price", "type": "DECIMAL(10,2)"},
            ]
        }

        contract = DestinationSchemaContract(schema)

        assert "decimal128" in contract.column_types["price"]


class TestDestinationSchemaContractEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_schema(self):
        """Test with empty schema."""
        contract = DestinationSchemaContract({})

        assert len(contract.arrow_schema) == 0
        assert contract.prepare_records([]) == []

    def test_unknown_type_defaults_to_string(self):
        """Test that unknown types default to string."""
        schema = {
            "columns": [
                {"name": "custom", "type": "CUSTOM_UNKNOWN_TYPE"},
            ]
        }

        contract = DestinationSchemaContract(schema)

        assert "string" in contract.column_types["custom"]

    def test_column_without_name_skipped(self):
        """Test that columns without name are skipped."""
        schema = {
            "columns": [
                {"type": "BIGINT"},  # No name
                {"name": "valid", "type": "BIGINT"},
            ]
        }

        contract = DestinationSchemaContract(schema)

        assert len(contract.arrow_schema) == 1
        assert "valid" in contract.column_types
