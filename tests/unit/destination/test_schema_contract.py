"""Tests for the Arrow-based SchemaContract.

The contract is the single seam where an endpoint document (either a
database ``columns`` payload or a JSON-Schema ``properties`` payload)
becomes a typed :class:`pa.Schema`. Every field declares its own
fully-qualified canonical ``arrow_type`` — no inference, no type-map
lookup on the destination side.
"""

import pyarrow as pa
import pytest

from src.destination.schema_contract import SchemaContract


class TestSchemaContractColumnsFormat:
    """Schema construction for database ``columns`` array payloads."""

    def test_basic_columns_schema(self):
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "name", "arrow_type": "Utf8", "nullable": True},
                {
                    "name": "created",
                    "arrow_type": "Timestamp(MICROSECOND, UTC)",
                    "nullable": True,
                },
            ]
        }
        contract = SchemaContract(schema)

        assert len(contract.arrow_schema) == 3
        assert set(contract.column_types) == {"id", "name", "created"}
        assert contract.column_types["id"].startswith("int64")
        assert contract.column_types["name"].startswith("string")
        assert "timestamp" in contract.column_types["created"]


class TestSchemaContractCastArrowBatch:
    """Casting an incoming pa.RecordBatch to the destination schema."""

    def test_cast_arrow_batch_basic(self):
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "name", "arrow_type": "Utf8", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        source = pa.RecordBatch.from_pylist(
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        )
        cast = contract.cast_arrow_batch(source)
        result = contract.to_dicts(cast)

        assert cast.schema == contract.arrow_schema
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_cast_arrow_batch_with_type_coercion(self):
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "value", "arrow_type": "Float32", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        # API source ships everything as JSON strings; the contract must
        # cast them to the declared Arrow types.
        source = pa.RecordBatch.from_pylist(
            [{"id": "1", "value": "3.14"}, {"id": "2", "value": "2.71"}]
        )
        cast = contract.cast_arrow_batch(source)
        result = contract.to_dicts(cast)

        assert result[0]["id"] == 1
        assert isinstance(result[0]["value"], float)

    def test_cast_arrow_batch_empty(self):
        schema = {"columns": [{"name": "id", "arrow_type": "Int64", "nullable": False}]}
        contract = SchemaContract(schema)

        empty = pa.RecordBatch.from_pylist([], schema=contract.arrow_schema)
        cast = contract.cast_arrow_batch(empty)

        assert cast.num_rows == 0
        assert contract.to_dicts(cast) == []

    def test_cast_arrow_batch_missing_column_fills_nulls(self):
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "optional_field", "arrow_type": "Utf8", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        source = pa.RecordBatch.from_pylist([{"id": 1}])
        cast = contract.cast_arrow_batch(source)

        assert contract.to_dicts(cast) == [{"id": 1, "optional_field": None}]

    def test_cast_arrow_batch_drops_extra_columns(self):
        schema = {
            "columns": [{"name": "id", "arrow_type": "Int64", "nullable": False}]
        }
        contract = SchemaContract(schema)

        source = pa.RecordBatch.from_pylist([{"id": 1, "extra": "drop me"}])
        cast = contract.cast_arrow_batch(source)

        assert cast.schema.names == ["id"]
        assert contract.to_dicts(cast) == [{"id": 1}]

    def test_cast_arrow_batch_missing_required_column_raises(self):
        # A non-nullable column absent from the incoming batch must fail
        # loudly — silently null-filling would corrupt destinations that
        # enforce NOT NULL at the database level.
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "amount", "arrow_type": "Int64", "nullable": False},
            ]
        }
        contract = SchemaContract(schema)

        source = pa.RecordBatch.from_pylist([{"id": 1}])
        with pytest.raises(ValueError, match="'amount'.*required"):
            contract.cast_arrow_batch(source)

    def test_cast_arrow_batch_unparseable_timestamp_raises(self):
        schema = {
            "columns": [
                {
                    "name": "ts",
                    "arrow_type": "Timestamp(MICROSECOND, UTC)",
                    "nullable": True,
                },
            ]
        }
        contract = SchemaContract(schema)

        source = pa.RecordBatch.from_pylist([{"ts": "not-a-timestamp"}])
        with pytest.raises(ValueError, match="cannot cast"):
            contract.cast_arrow_batch(source)

    def test_cast_arrow_batch_unparseable_date_raises(self):
        schema = {
            "columns": [{"name": "d", "arrow_type": "Date32", "nullable": True}]
        }
        contract = SchemaContract(schema)

        source = pa.RecordBatch.from_pylist([{"d": "13/30/2025"}])
        with pytest.raises(ValueError, match="cannot cast"):
            contract.cast_arrow_batch(source)


class TestSchemaContractFromPylist:
    """Source-side: building an Arrow batch from dicts using the contract."""

    def test_from_pylist_with_schema(self):
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "name", "arrow_type": "Utf8", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist([{"id": 1, "name": "Alice"}])

        assert batch.schema == contract.arrow_schema
        assert batch.to_pylist() == [{"id": 1, "name": "Alice"}]

    def test_from_pylist_decimal_from_float(self):
        # Wise API ships decimal fields as JSON floats; the contract
        # must route them through Decimal(str(v)) to preserve precision.
        schema = {
            "columns": [
                {"name": "amount", "arrow_type": "Decimal128(18, 2)", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist([{"amount": 3.14}, {"amount": None}])

        assert batch.num_rows == 2
        assert pa.types.is_decimal(batch.schema.field("amount").type)

    def test_from_pylist_strptime_via_source_format(self):
        schema = {
            "columns": [
                {
                    "name": "created",
                    "arrow_type": "Timestamp(MICROSECOND, UTC)",
                    "nullable": True,
                    "source_format": "%Y-%m-%d %H:%M:%S",
                },
            ]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist([{"created": "2026-05-12 12:30:45"}])

        assert batch.num_rows == 1
        assert pa.types.is_timestamp(batch.schema.field("created").type)


class TestSchemaContractJsonSchema:
    """JSON-Schema payloads use ``properties`` and still require arrow_type."""

    def test_json_schema_format(self):
        schema = {
            "properties": {
                "id": {"type": "integer", "arrow_type": "Int64"},
                "name": {"type": "string", "arrow_type": "Utf8"},
                "active": {"type": "boolean", "arrow_type": "Boolean"},
            },
            "required": ["id"],
        }
        contract = SchemaContract(schema)

        assert len(contract.arrow_schema) == 3
        assert set(contract.column_types) == {"id", "name", "active"}
        assert not contract.arrow_schema.field("id").nullable
        assert contract.arrow_schema.field("name").nullable

    def test_json_schema_timestamp(self):
        schema = {
            "properties": {
                "created_at": {
                    "type": "string",
                    "arrow_type": "Timestamp(MICROSECOND, UTC)",
                },
            },
        }
        contract = SchemaContract(schema)
        assert "timestamp" in contract.column_types["created_at"]

    def test_json_schema_date32(self):
        schema = {
            "properties": {"dob": {"type": "string", "arrow_type": "Date32"}},
        }
        contract = SchemaContract(schema)
        assert "date32" in contract.column_types["dob"]


class TestSchemaContractValidation:
    """Malformed-payload signals must surface as ValueError, not be silently
    dropped or inferred — author mistakes should fail loudly."""

    def test_missing_columns_and_properties_raises(self):
        with pytest.raises(ValueError, match="must declare either"):
            SchemaContract({})

    def test_empty_columns_raises(self):
        with pytest.raises(ValueError, match="'columns' is present but empty"):
            SchemaContract({"columns": []})

    def test_empty_properties_raises(self):
        with pytest.raises(ValueError, match="'properties' is present but empty"):
            SchemaContract({"properties": {}})

    def test_column_without_name_raises(self):
        schema = {
            "columns": [
                {"arrow_type": "Int64"},
                {"name": "valid", "arrow_type": "Int64"},
            ]
        }
        with pytest.raises(ValueError, match="has no 'name' field"):
            SchemaContract(schema)

    def test_column_without_arrow_type_raises(self):
        schema = {"columns": [{"name": "id"}]}
        with pytest.raises(ValueError, match="no 'arrow_type' declaration"):
            SchemaContract(schema)

    def test_property_without_arrow_type_raises(self):
        schema = {"properties": {"id": {"type": "integer"}}}
        with pytest.raises(ValueError, match="no 'arrow_type' declaration"):
            SchemaContract(schema)

    def test_malformed_arrow_type_raises(self):
        schema = {"columns": [{"name": "id", "arrow_type": "Decimal128"}]}
        with pytest.raises(ValueError, match="cannot parse arrow_type"):
            SchemaContract(schema)
