"""Tests for the Arrow-based SchemaContract.

The contract is the single seam where an endpoint document (either a
database ``columns`` payload or a JSON-Schema ``properties`` payload)
becomes a typed :class:`pa.Schema`. Every field declares its own
fully-qualified canonical ``arrow_type`` — no inference, no type-map
lookup on the destination side.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from cdk.schema_contract import SchemaContract


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
        schema = {"columns": [{"name": "id", "arrow_type": "Int64", "nullable": False}]}
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

    def test_cast_arrow_batch_non_nullable_with_null_raises(self):
        # A None in a non-nullable column must fail loud on the Arrow-batch
        # path exactly as it does on the from_pylist path — same intent, same
        # behaviour across build paths. Before the shared guard this null was
        # silently admitted.
        schema = {"columns": [{"name": "id", "arrow_type": "Int64", "nullable": False}]}
        contract = SchemaContract(schema)

        source = pa.RecordBatch.from_pylist([{"id": 1}, {"id": None}])
        with pytest.raises(ValueError, match="'id' is non-nullable but rows"):
            contract.cast_arrow_batch(source)

    def test_cast_arrow_batch_overflow_raises(self):
        # safe=True: an int that overflows the narrower destination width must
        # fail loud, mirroring the per-row range check from_pylist enforces.
        # Before this, safe=False let the value saturate silently.
        schema = {"columns": [{"name": "n", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        source = pa.record_batch([pa.array([2**40], type=pa.int64())], names=["n"])
        with pytest.raises(ValueError, match="cannot cast"):
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
        schema = {"columns": [{"name": "d", "arrow_type": "Date32", "nullable": True}]}
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

    def test_from_pylist_integer_from_strings(self):
        # sevdesk (and many JSON APIs) return integer fields as strings;
        # the contract must coerce them rather than raising ArrowInvalid.
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int32", "nullable": True},
                {"name": "count", "arrow_type": "Int64", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist(
            [{"id": "0", "count": "14"}, {"id": "32", "count": None}]
        )

        assert batch.to_pylist() == [
            {"id": 0, "count": 14},
            {"id": 32, "count": None},
        ]
        assert pa.types.is_int32(batch.schema.field("id").type)
        assert pa.types.is_int64(batch.schema.field("count").type)

    def test_from_pylist_float_from_strings(self):
        schema = {
            "columns": [
                {"name": "rate", "arrow_type": "Float64", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist([{"rate": "3.14"}, {"rate": None}])

        result = batch.to_pylist()
        assert abs(result[0]["rate"] - 3.14) < 1e-9
        assert result[1]["rate"] is None

    def test_from_pylist_integer_from_mixed_types(self):
        # Some rows carry native ints; some carry strings. Both are accepted.
        schema = {"columns": [{"name": "n", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        batch = contract.from_pylist([{"n": 5}, {"n": "10"}, {"n": None}])
        assert batch.to_pylist() == [{"n": 5}, {"n": 10}, {"n": None}]

    def test_from_pylist_unparseable_integer_string_raises(self):
        schema = {"columns": [{"name": "val", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'val' at row 1"):
            contract.from_pylist([{"val": "42"}, {"val": "abc"}])

    def test_from_pylist_float_string_for_integer_raises(self):
        # "14.5" cannot be losslessly parsed as int — must fail, not silently truncate.
        schema = {"columns": [{"name": "val", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'val' at row 0"):
            contract.from_pylist([{"val": "14.5"}])

    def test_from_pylist_uint_from_strings(self):
        # UInt32 and UInt64 are integer types; the same coercion path must apply.
        schema = {
            "columns": [
                {"name": "flags", "arrow_type": "UInt32", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist(
            [{"flags": "0"}, {"flags": "4294967295"}, {"flags": None}]
        )
        assert batch.to_pylist() == [
            {"flags": 0},
            {"flags": 4294967295},
            {"flags": None},
        ]
        assert pa.types.is_unsigned_integer(batch.schema.field("flags").type)

    def test_from_pylist_bool_mixed_with_string_for_integer_raises(self):
        # bool is a Python int subclass; it must be rejected, not coerced to 0/1.
        schema = {"columns": [{"name": "val", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'val' at row 1.*bool"):
            contract.from_pylist([{"val": "42"}, {"val": True}])

    def test_from_pylist_bool_for_float_raises(self):
        # bool must be rejected for float columns too — silent coercion to
        # 0.0/1.0 is data corruption.
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float64", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'rate' at row 1.*bool"):
            contract.from_pylist([{"rate": "3.14"}, {"rate": False}])

    def test_from_pylist_nan_string_for_float_raises(self):
        # "nan" is valid Python float() input but must not silently produce an
        # IEEE 754 NaN in the Arrow column — it would pass nullability checks
        # and corrupt downstream queries.
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float64", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'rate' at row 0.*non-finite"):
            contract.from_pylist([{"rate": "nan"}])

    def test_from_pylist_inf_string_for_float_raises(self):
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float64", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'rate' at row 0.*non-finite"):
            contract.from_pylist([{"rate": "inf"}])

    def test_from_pylist_native_nonfinite_float_mixed_with_string_raises(self):
        # A native float('nan')/float('inf') next to a numeric string must be
        # rejected exactly like the strings "nan"/"inf" — same intent, same
        # semantics — not pass through into the Arrow column unchecked.
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float64", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'rate' at row 1.*non-finite"):
            contract.from_pylist([{"rate": "3.14"}, {"rate": float("nan")}])
        with pytest.raises(ValueError, match=r"column 'rate' at row 1.*non-finite"):
            contract.from_pylist([{"rate": "3.14"}, {"rate": float("inf")}])

    def test_from_pylist_float32_string_overflow_raises(self):
        # "1e40" parses to a finite float64 but overflows to inf when stored
        # in a Float32 array — must fail loudly, not corrupt the destination.
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float32", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(
            ValueError, match=r"column 'rate' at row 0.*overflows float"
        ):
            contract.from_pylist([{"rate": "1e40"}])

    def test_from_pylist_float32_native_overflow_mixed_with_string_raises(self):
        # The same overflow guard applies to native floats in a mixed column.
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float32", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(
            ValueError, match=r"column 'rate' at row 1.*overflows float"
        ):
            contract.from_pylist([{"rate": "3.14"}, {"rate": 1e40}])

    def test_from_pylist_float32_native_only_overflow_raises(self):
        # JSON 1e40 decodes to a native float; a batch with no strings at all
        # must hit the same overflow guard, not bypass validation entirely.
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float32", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(
            ValueError, match=r"column 'rate' at row 0.*overflows float"
        ):
            contract.from_pylist([{"rate": 1e40}])

    def test_from_pylist_native_only_nonfinite_float_raises(self):
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float64", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'rate' at row 0.*non-finite"):
            contract.from_pylist([{"rate": float("nan")}])

    def test_from_pylist_native_only_integer_out_of_range_raises(self):
        schema = {"columns": [{"name": "val", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'val' at row 1.*out of range"):
            contract.from_pylist([{"val": 1}, {"val": 2147483648}])

    def test_from_pylist_native_only_numerics_pass(self):
        # Pure-native numeric batches go through the same validated path.
        schema = {
            "columns": [
                {"name": "n", "arrow_type": "Int32", "nullable": True},
                {"name": "rate", "arrow_type": "Float32", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist(
            [{"n": 5, "rate": 1.5}, {"n": -7, "rate": None}, {"n": None, "rate": 0.25}]
        )
        assert batch.to_pylist() == [
            {"n": 5, "rate": 1.5},
            {"n": -7, "rate": None},
            {"n": None, "rate": 0.25},
        ]

    def test_from_pylist_float32_in_range_strings_pass(self):
        schema = {
            "columns": [{"name": "rate", "arrow_type": "Float32", "nullable": True}]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist([{"rate": "3.14"}, {"rate": -2.5}, {"rate": None}])
        result = batch.to_pylist()
        assert abs(result[0]["rate"] - 3.14) < 1e-6
        assert result[1]["rate"] == -2.5
        assert result[2]["rate"] is None

    def test_from_pylist_integer_string_out_of_range_raises_with_row(self):
        # "2147483648" parses cleanly with int() but exceeds Int32; the error
        # must name the offending row, not blame the first non-null value.
        schema = {"columns": [{"name": "val", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'val' at row 1.*out of range"):
            contract.from_pylist([{"val": "1"}, {"val": "2147483648"}])

    def test_from_pylist_native_integer_out_of_range_raises_with_row(self):
        # Native out-of-range ints in a mixed column previously surfaced as
        # ArrowInvalid blaming row 0; they get the same per-row range check.
        schema = {"columns": [{"name": "val", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'val' at row 1.*out of range"):
            contract.from_pylist([{"val": "1"}, {"val": 2147483648}])

    def test_from_pylist_uint_negative_string_raises(self):
        schema = {
            "columns": [{"name": "flags", "arrow_type": "UInt32", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'flags' at row 0.*out of range"):
            contract.from_pylist([{"flags": "-1"}])

    def test_from_pylist_native_float_for_integer_raises(self):
        # pa.array silently truncates 1.5 -> 1 for integer types; a native
        # float must be rejected like the string "1.5", not silently floored.
        schema = {"columns": [{"name": "val", "arrow_type": "Int32", "nullable": True}]}
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'val' at row 1.*got float"):
            contract.from_pylist([{"val": "1"}, {"val": 1.5}])

    def test_from_pylist_non_numeric_object_mixed_with_string_raises(self):
        # Anything outside int/float/numeric-string (here a dict) is rejected
        # with row context instead of falling through to an ArrowInvalid that
        # blames the first non-null row.
        schema = {
            "columns": [{"name": "val", "arrow_type": "Float64", "nullable": True}]
        }
        contract = SchemaContract(schema)

        with pytest.raises(ValueError, match=r"column 'val' at row 1.*got dict"):
            contract.from_pylist([{"val": "1.5"}, {"val": {"x": 1}}])

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

    def test_from_pylist_iso_z_suffix(self):
        # ISO-8601 'Z' suffix is parsed natively by fromisoformat on
        # Python 3.11+ and must land as a UTC-aware timestamp.
        schema = {
            "columns": [
                {
                    "name": "created",
                    "arrow_type": "Timestamp(MICROSECOND, UTC)",
                    "nullable": True,
                },
            ]
        }
        contract = SchemaContract(schema)

        batch = contract.from_pylist([{"created": "2026-05-12T12:30:45Z"}])

        assert batch.num_rows == 1
        value = batch.to_pylist()[0]["created"]
        assert value.utcoffset().total_seconds() == 0
        assert (value.year, value.hour, value.second) == (2026, 12, 45)


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
        schema = {"properties": {"tags": {"type": "array", "arrow_type": "Json"}}}
        contract = SchemaContract(schema)
        batch = contract.from_pylist([{"tags": ["a", "b", "c"]}])
        assert batch.column("tags")[0].as_py() == '["a", "b", "c"]'

    def test_json_marker_rejects_non_dict_non_list_value(self):
        # A Json column accepts only dict, list, or None — anything else
        # is an author mistake. The encoder must surface it with the
        # row index, not let a stray string round-trip and crash the
        # decoder downstream.
        schema = {"properties": {"metadata": {"type": "object", "arrow_type": "Json"}}}
        contract = SchemaContract(schema)
        with pytest.raises(ValueError, match="row 0 carries str"):
            contract.from_pylist([{"metadata": "not-a-dict"}])

    def test_json_marker_rejects_int_value(self):
        schema = {"properties": {"metadata": {"type": "object", "arrow_type": "Json"}}}
        contract = SchemaContract(schema)
        with pytest.raises(ValueError, match="row 1 carries int"):
            contract.from_pylist(
                [
                    {"metadata": {"ok": True}},
                    {"metadata": 42},
                ]
            )

    def test_from_pylist_preserves_inner_row_index(self):
        """The encoder names the exact offending row; the outer
        ``from_pylist`` wrapper must not overwrite it with the
        first-non-null heuristic intended for opaque PyArrow errors."""
        schema = {"properties": {"metadata": {"type": "object", "arrow_type": "Json"}}}
        contract = SchemaContract(schema)
        rows = [{"metadata": {"ok": 1}}] * 5 + [{"metadata": 99}]
        with pytest.raises(ValueError, match="row 5 carries int") as exc_info:
            contract.from_pylist(rows)
        # The misleading wrapper would have prefixed with "first non-null
        # at row 0"; assert that string is absent.
        assert "first non-null" not in str(exc_info.value)

    def test_decode_json_columns_inverts_serialization(self):
        schema = {"properties": {"metadata": {"type": "object", "arrow_type": "Json"}}}
        contract = SchemaContract(schema)
        batch = contract.from_pylist([{"metadata": {"k": "v"}}])
        records = batch.to_pylist()
        decoded = contract.decode_json_columns(records)
        assert decoded == [{"metadata": {"k": "v"}}]

    def test_decode_json_columns_surfaces_column_context_on_malformed(self):
        schema = {"properties": {"metadata": {"type": "object", "arrow_type": "Json"}}}
        contract = SchemaContract(schema)
        # The decoder must mention the column name and row index — a bare
        # JSONDecodeError from deep in the stack would force the caller
        # to grep through stack frames to find the offending field.
        with pytest.raises(ValueError, match=r"Json column 'metadata' at row 0"):
            contract.decode_json_columns([{"metadata": "not-valid-json{"}])

    def test_decode_json_columns_is_idempotent_on_dicts(self):
        # If a caller decodes twice (or the value already came as a dict),
        # the second pass must not raise.
        schema = {"properties": {"metadata": {"type": "object", "arrow_type": "Json"}}}
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


class TestFromPylistNullabilityEnforcement:
    """from_pylist must reject None values in non-nullable columns for every
    column family — numeric, string, temporal, decimal, JSON, and source_format.

    A single post-build check in from_pylist covers all mixed-None paths
    uniformly (some values present, some None). A separate earlier guard inside
    _build_column handles the all-None case. This class verifies both paths
    and that the coverage is truly global, not family-specific.
    """

    # --- helpers -------------------------------------------------------

    @staticmethod
    def _contract(arrow_type: str, nullable: bool) -> "SchemaContract":
        return SchemaContract(
            {"columns": [{"name": "v", "arrow_type": arrow_type, "nullable": nullable}]}
        )

    # --- mixed-None (some values present, some None) -------------------

    def test_string_non_nullable_mixed_raises(self):
        c = self._contract("Utf8", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[1\]"):
            c.from_pylist([{"v": "a"}, {"v": None}])

    def test_integer_non_nullable_mixed_raises(self):
        c = self._contract("Int64", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[0\]"):
            c.from_pylist([{"v": None}, {"v": 1}])

    def test_float_non_nullable_mixed_raises(self):
        c = self._contract("Float64", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[2\]"):
            c.from_pylist([{"v": 1.0}, {"v": 2.0}, {"v": None}])

    def test_decimal_non_nullable_mixed_raises(self):
        c = self._contract("Decimal128(18, 2)", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[1\]"):
            c.from_pylist([{"v": 1.5}, {"v": None}])

    def test_timestamp_non_nullable_mixed_raises(self):
        c = self._contract("Timestamp(MICROSECOND, UTC)", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[0\]"):
            c.from_pylist([{"v": None}, {"v": "2026-01-01T00:00:00Z"}])

    def test_date_non_nullable_mixed_raises(self):
        c = self._contract("Date32", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[1\]"):
            c.from_pylist([{"v": "2026-01-01"}, {"v": None}])

    def test_json_non_nullable_mixed_raises(self):
        c = self._contract("Json", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[1\]"):
            c.from_pylist([{"v": {"k": 1}}, {"v": None}])

    def test_time_non_nullable_mixed_raises(self):
        from datetime import time as _time

        c = self._contract("Time32(SECOND)", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[1\]"):
            c.from_pylist([{"v": _time(10, 0, 0)}, {"v": None}])

    def test_uint_non_nullable_mixed_raises(self):
        c = self._contract("UInt32", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[1\]"):
            c.from_pylist([{"v": 0}, {"v": None}])

    def test_source_format_non_nullable_mixed_raises(self):
        contract = SchemaContract(
            {
                "columns": [
                    {
                        "name": "ts",
                        "arrow_type": "Timestamp(MICROSECOND, UTC)",
                        "nullable": False,
                        "source_format": "%Y-%m-%d %H:%M:%S",
                    }
                ]
            }
        )
        with pytest.raises(ValueError, match=r"'ts' is non-nullable.*\[1\]"):
            contract.from_pylist([{"ts": "2026-01-01 00:00:00"}, {"ts": None}])

    def test_source_format_date_non_nullable_mixed_raises(self):
        contract = SchemaContract(
            {
                "columns": [
                    {
                        "name": "d",
                        "arrow_type": "Date32",
                        "nullable": False,
                        "source_format": "%Y/%m/%d",
                    }
                ]
            }
        )
        with pytest.raises(ValueError, match=r"'d' is non-nullable.*\[1\]"):
            contract.from_pylist([{"d": "2026/01/01"}, {"d": None}])

    def test_missing_key_treated_as_none_raises(self):
        # r.get(field.name) returns None for a missing key, so an absent
        # field is indistinguishable from an explicit None in a non-nullable column.
        c = self._contract("Int64", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[1\]"):
            c.from_pylist([{"v": 1}, {}])

    def test_error_lists_multiple_null_rows(self):
        c = self._contract("Int64", nullable=False)
        with pytest.raises(ValueError, match=r"'v' is non-nullable.*\[0, 2\]"):
            c.from_pylist([{"v": None}, {"v": 1}, {"v": None}])

    # --- all-None guard still fires (pre-existing behaviour) -----------

    def test_all_none_non_nullable_raises(self):
        c = self._contract("Int64", nullable=False)
        with pytest.raises(ValueError, match=r"'v'.*every source value is None"):
            c.from_pylist([{"v": None}, {"v": None}])

    # --- nullable columns with None pass through unaffected -----------

    def test_nullable_with_none_passes(self):
        c = self._contract("Int64", nullable=True)
        batch = c.from_pylist([{"v": 1}, {"v": None}])
        assert batch.to_pylist() == [{"v": 1}, {"v": None}]

    def test_non_nullable_fully_populated_passes(self):
        c = self._contract("Int64", nullable=False)
        batch = c.from_pylist([{"v": 1}, {"v": 2}])
        assert batch.to_pylist() == [{"v": 1}, {"v": 2}]


class TestToDbRecords:
    """One materialisation entry point for SA destinations: alignment +
    ``to_pylist`` + ``decode_json_columns``. The API destination uses the
    same building blocks (via ``record_batch.to_pylist`` + orjson at the
    handler) — no need for a parallel ``to_json_records`` method."""

    def test_to_db_records_keeps_native_python_types(self):
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {
                    "name": "created",
                    "arrow_type": "Timestamp(MICROSECOND, UTC)",
                    "nullable": True,
                },
                {"name": "amount", "arrow_type": "Decimal128(18, 4)", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "id": 1,
                    "created": datetime(2026, 3, 23, 10, 18, 24, tzinfo=timezone.utc),
                    "amount": Decimal("42.5000"),
                }
            ],
            schema=contract.arrow_schema,
        )
        records = contract.to_db_records(batch)
        assert isinstance(records[0]["created"], datetime)
        assert isinstance(records[0]["amount"], Decimal)

    def test_to_db_records_keeps_json_columns_as_strings(self):
        """JSON columns bind directly as their wire-format string.

        ``_build_column`` serialised the dict to a string when the batch
        was constructed; ``to_db_records`` does not parse it back, so
        SA receives the string and TEXT/JSONB columns accept it
        without any per-row coercion.
        """
        import json as _json

        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "metadata", "arrow_type": "Json", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)
        batch = contract.from_pylist([{"id": 1, "metadata": {"k": "v"}}])
        records = contract.to_db_records(batch)
        assert isinstance(records[0]["metadata"], str)
        assert _json.loads(records[0]["metadata"]) == {"k": "v"}

    def test_to_db_records_passes_through_null_json(self):
        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "metadata", "arrow_type": "Json", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)
        batch = contract.from_pylist([{"id": 1, "metadata": None}])
        records = contract.to_db_records(batch)
        assert records[0]["metadata"] is None

    def test_to_db_records_keeps_list_json_columns_as_strings(self):
        import json as _json

        schema = {
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "tags", "arrow_type": "Json", "nullable": True},
            ]
        }
        contract = SchemaContract(schema)
        batch = contract.from_pylist([{"id": 1, "tags": ["a", "b"]}])
        records = contract.to_db_records(batch)
        assert isinstance(records[0]["tags"], str)
        assert _json.loads(records[0]["tags"]) == ["a", "b"]
