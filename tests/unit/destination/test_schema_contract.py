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


class TestSchemaContractNestedObject:
    """``arrow_type: 'Object'`` recurses into the property's sub-properties
    to build a ``pa.struct`` column; dicts round-trip end-to-end without
    string-encoding."""

    def test_object_property_builds_struct(self):
        schema = {
            "properties": {
                "id": {"type": "string", "arrow_type": "Utf8"},
                "checkAccount": {
                    "type": "object",
                    "arrow_type": "Object",
                    "required": ["id", "objectName"],
                    "properties": {
                        "id": {"type": "string", "arrow_type": "Utf8"},
                        "objectName": {"type": "string", "arrow_type": "Utf8"},
                    },
                },
            },
            "required": ["id", "checkAccount"],
        }
        contract = SchemaContract(schema)
        field = contract.arrow_schema.field("checkAccount")
        assert pa.types.is_struct(field.type)
        assert {f.name for f in field.type} == {"id", "objectName"}

    def test_object_round_trips_dict(self):
        schema = {
            "properties": {
                "checkAccount": {
                    "type": "object",
                    "arrow_type": "Object",
                    "properties": {
                        "id": {"arrow_type": "Utf8"},
                        "objectName": {"arrow_type": "Utf8"},
                    },
                },
            },
        }
        contract = SchemaContract(schema)
        batch = contract.from_pylist(
            [{"checkAccount": {"id": "42", "objectName": "CheckAccount"}}]
        )
        assert batch.to_pylist() == [
            {"checkAccount": {"id": "42", "objectName": "CheckAccount"}}
        ]

    def test_object_empty_properties_raises(self):
        schema = {
            "properties": {
                "thing": {"arrow_type": "Object", "properties": {}},
            }
        }
        with pytest.raises(ValueError, match="non-empty 'properties' map"):
            SchemaContract(schema)

    def test_object_missing_properties_raises(self):
        schema = {"properties": {"thing": {"arrow_type": "Object"}}}
        with pytest.raises(ValueError, match="non-empty 'properties' map"):
            SchemaContract(schema)

    def test_list_of_scalars(self):
        schema = {
            "properties": {
                "tags": {
                    "type": "array",
                    "arrow_type": "List",
                    "items": {"arrow_type": "Utf8"},
                }
            }
        }
        contract = SchemaContract(schema)
        field = contract.arrow_schema.field("tags")
        assert pa.types.is_list(field.type)
        assert pa.types.is_string(field.type.value_type)

        batch = contract.from_pylist([{"tags": ["a", "b"]}, {"tags": []}])
        assert batch.to_pylist() == [{"tags": ["a", "b"]}, {"tags": []}]

    def test_list_of_objects_round_trips(self):
        schema = {
            "properties": {
                "positions": {
                    "type": "array",
                    "arrow_type": "List",
                    "items": {
                        "arrow_type": "Object",
                        "properties": {
                            "sku": {"arrow_type": "Utf8"},
                            "qty": {"arrow_type": "Int32"},
                        },
                    },
                }
            }
        }
        contract = SchemaContract(schema)
        batch = contract.from_pylist(
            [{"positions": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 5}]}]
        )
        assert batch.to_pylist() == [
            {"positions": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 5}]}
        ]

    def test_list_missing_items_raises(self):
        schema = {"properties": {"tags": {"arrow_type": "List"}}}
        with pytest.raises(ValueError, match="'items' object"):
            SchemaContract(schema)

    def test_json_marker_round_trips_dict_as_string(self):
        schema = {
            "properties": {
                "metadata": {"type": "object", "arrow_type": "Json"},
            }
        }
        contract = SchemaContract(schema)
        f = contract.arrow_schema.field("metadata")
        assert pa.types.is_large_string(f.type)
        assert "metadata" in contract.json_columns

        batch = contract.from_pylist(
            [{"metadata": {"k": "v", "n": 1}}, {"metadata": None}]
        )
        # Wire value is a JSON-encoded string, not a struct
        first = batch.column("metadata")[0].as_py()
        assert isinstance(first, str)
        assert first == '{"k": "v", "n": 1}'

    def test_json_marker_accepts_list_value(self):
        schema = {
            "properties": {"tags": {"type": "array", "arrow_type": "Json"}}
        }
        contract = SchemaContract(schema)
        batch = contract.from_pylist([{"tags": ["a", "b", "c"]}])
        assert batch.column("tags")[0].as_py() == '["a", "b", "c"]'

    def test_json_marker_rejects_non_dict_non_list_value(self):
        # A Json column accepts only dict, list, or None — anything else
        # is an author mistake. The encoder must surface it with the
        # row index, not let a stray string round-trip and crash the
        # decoder downstream.
        schema = {
            "properties": {"metadata": {"type": "object", "arrow_type": "Json"}}
        }
        contract = SchemaContract(schema)
        with pytest.raises(ValueError, match="row 0 carries str"):
            contract.from_pylist([{"metadata": "not-a-dict"}])

    def test_json_marker_rejects_int_value(self):
        schema = {
            "properties": {"metadata": {"type": "object", "arrow_type": "Json"}}
        }
        contract = SchemaContract(schema)
        with pytest.raises(ValueError, match="row 1 carries int"):
            contract.from_pylist([
                {"metadata": {"ok": True}},
                {"metadata": 42},
            ])

    def test_decode_json_columns_inverts_serialization(self):
        schema = {
            "properties": {"metadata": {"type": "object", "arrow_type": "Json"}}
        }
        contract = SchemaContract(schema)
        batch = contract.from_pylist([{"metadata": {"k": "v"}}])
        records = batch.to_pylist()
        decoded = contract.decode_json_columns(records)
        assert decoded == [{"metadata": {"k": "v"}}]

    def test_decode_json_columns_surfaces_column_context_on_malformed(self):
        schema = {
            "properties": {"metadata": {"type": "object", "arrow_type": "Json"}}
        }
        contract = SchemaContract(schema)
        # The decoder must mention the column name and row index — a bare
        # JSONDecodeError from deep in the stack would force the caller
        # to grep through stack frames to find the offending field.
        with pytest.raises(
            ValueError, match=r"Json column 'metadata' at row 0"
        ):
            contract.decode_json_columns([{"metadata": "not-valid-json{"}])

    def test_decode_json_columns_is_idempotent_on_dicts(self):
        # If a caller decodes twice (or the value already came as a dict),
        # the second pass must not raise.
        schema = {
            "properties": {"metadata": {"type": "object", "arrow_type": "Json"}}
        }
        contract = SchemaContract(schema)
        decoded = contract.decode_json_columns([{"metadata": {"k": "v"}}])
        decoded = contract.decode_json_columns(decoded)
        assert decoded == [{"metadata": {"k": "v"}}]

    def test_columns_payload_supports_object_marker(self):
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Utf8", "nullable": False},
                {
                    "name": "payload",
                    "arrow_type": "Object",
                    "nullable": True,
                    "properties": {
                        "k": {"arrow_type": "Utf8"},
                        "v": {"arrow_type": "Int64"},
                    },
                },
            ]
        }
        contract = SchemaContract(schema)
        assert pa.types.is_struct(contract.arrow_schema.field("payload").type)
