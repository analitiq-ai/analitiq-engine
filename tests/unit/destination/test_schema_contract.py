"""Tests for the Arrow-based SchemaContract.

The contract is the single seam where an endpoint document (either a
database ``columns`` payload or a JSON-Schema ``properties`` payload)
becomes a typed :class:`pa.Schema`. Every field declares its own
fully-qualified canonical ``arrow_type`` — no inference, no type-map
lookup on the destination side.
"""

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from src.destination.schema_contract import SchemaContract, arrow_to_json_shape


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

    def test_from_pylist_preserves_inner_row_index(self):
        """The encoder names the exact offending row; the outer
        ``from_pylist`` wrapper must not overwrite it with the
        first-non-null heuristic intended for opaque PyArrow errors."""
        schema = {
            "properties": {"metadata": {"type": "object", "arrow_type": "Json"}}
        }
        contract = SchemaContract(schema)
        rows = [{"metadata": {"ok": 1}}] * 5 + [{"metadata": 99}]
        with pytest.raises(ValueError, match="row 5 carries int") as exc_info:
            contract.from_pylist(rows)
        # The misleading wrapper would have prefixed with "first non-null
        # at row 0"; assert that string is absent.
        assert "first non-null" not in str(exc_info.value)

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


class TestArrowToJsonShape:
    """``arrow_to_json_shape`` is the Arrow-space cast that turns non-
    JSON-native types into canonical strings before ``to_pylist``. It is
    the API-destination's equivalent of ``cast_arrow_batch`` — schema-
    driven, vectorised, type-preserving for already-JSON-native columns.
    """

    def test_timestamp_utc_becomes_iso_string(self):
        col = pa.array(
            [datetime(2026, 3, 23, 10, 18, 24, 123456, tzinfo=timezone.utc), None],
            type=pa.timestamp("us", tz="UTC"),
        )
        batch = pa.RecordBatch.from_arrays(
            [col], schema=pa.schema([pa.field("ts", col.type)])
        )
        out = arrow_to_json_shape(batch)
        assert pa.types.is_string(out.schema.field("ts").type)
        values = out.column("ts").to_pylist()
        # Must be T-separated (not space) and carry the offset.
        assert values[0] is not None
        assert "T" in values[0]
        assert values[0].endswith("+0000")
        assert values[1] is None

    def test_naive_timestamp_omits_offset(self):
        col = pa.array(
            [datetime(2026, 3, 23, 10, 18, 24)], type=pa.timestamp("us")
        )
        batch = pa.RecordBatch.from_arrays(
            [col], schema=pa.schema([pa.field("ts", col.type)])
        )
        out = arrow_to_json_shape(batch)
        v = out.column("ts").to_pylist()[0]
        assert "T" in v
        assert "+" not in v  # no tz suffix
        assert v.startswith("2026-03-23T10:18:24")

    def test_date_becomes_iso_date(self):
        col = pa.array([date(2026, 3, 23), None], type=pa.date32())
        batch = pa.RecordBatch.from_arrays(
            [col], schema=pa.schema([pa.field("d", col.type)])
        )
        out = arrow_to_json_shape(batch)
        assert out.column("d").to_pylist() == ["2026-03-23", None]

    def test_time_becomes_iso_time(self):
        col = pa.array([time(10, 18, 24), None], type=pa.time64("us"))
        batch = pa.RecordBatch.from_arrays(
            [col], schema=pa.schema([pa.field("t", col.type)])
        )
        out = arrow_to_json_shape(batch)
        values = out.column("t").to_pylist()
        assert values[0].startswith("10:18:24")
        assert values[1] is None

    def test_decimal_becomes_string(self):
        col = pa.array(
            [Decimal("42.5000"), Decimal("3.1400"), None],
            type=pa.decimal128(18, 4),
        )
        batch = pa.RecordBatch.from_arrays(
            [col], schema=pa.schema([pa.field("amt", col.type)])
        )
        out = arrow_to_json_shape(batch)
        assert out.column("amt").to_pylist() == ["42.5000", "3.1400", None]

    def test_binary_becomes_base64_string(self):
        col = pa.array([b"hello", b"\xff\xfe", None], type=pa.binary())
        batch = pa.RecordBatch.from_arrays(
            [col], schema=pa.schema([pa.field("blob", col.type)])
        )
        out = arrow_to_json_shape(batch)
        vals = out.column("blob").to_pylist()
        import base64
        assert vals[0] == base64.b64encode(b"hello").decode("ascii")
        assert vals[1] == base64.b64encode(b"\xff\xfe").decode("ascii")
        assert vals[2] is None

    def test_json_native_types_passthrough_unchanged(self):
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("score", pa.float64()),
        ])
        batch = pa.RecordBatch.from_pylist(
            [{"id": 1, "name": "Alice", "active": True, "score": 9.5}],
            schema=schema,
        )
        out = arrow_to_json_shape(batch)
        assert out.schema == schema
        assert out.to_pylist() == batch.to_pylist()

    def test_struct_with_nested_timestamp_recurses(self):
        struct_t = pa.struct([
            pa.field("id", pa.int64()),
            pa.field("created", pa.timestamp("us", tz="UTC")),
        ])
        struct_arr = pa.array(
            [
                {"id": 1, "created": datetime(2026, 3, 23, 10, 18, 24, tzinfo=timezone.utc)},
                None,
            ],
            type=struct_t,
        )
        batch = pa.RecordBatch.from_arrays(
            [struct_arr],
            schema=pa.schema([pa.field("payload", struct_t)]),
        )
        out = arrow_to_json_shape(batch)
        new_struct_type = out.schema.field("payload").type
        # The struct's `created` sub-field is now a string.
        assert pa.types.is_string(new_struct_type.field("created").type)
        assert pa.types.is_int64(new_struct_type.field("id").type)
        # And the parent null mask is preserved.
        records = out.to_pylist()
        assert records[0]["payload"]["id"] == 1
        assert "T" in records[0]["payload"]["created"]
        assert records[1]["payload"] is None

    def test_list_of_struct_with_decimal_recurses(self):
        struct_t = pa.struct([
            pa.field("sku", pa.string()),
            pa.field("price", pa.decimal128(18, 2)),
        ])
        list_t = pa.list_(struct_t)
        list_arr = pa.array(
            [[
                {"sku": "A", "price": Decimal("9.99")},
                {"sku": "B", "price": Decimal("12.50")},
            ]],
            type=list_t,
        )
        batch = pa.RecordBatch.from_arrays(
            [list_arr],
            schema=pa.schema([pa.field("items", list_t)]),
        )
        out = arrow_to_json_shape(batch)
        records = out.to_pylist()
        assert records[0]["items"][0]["price"] == "9.99"
        assert records[0]["items"][1]["price"] == "12.50"

    def test_output_dict_is_json_dumps_safe(self):
        """The full point of the cast: the produced dict must serialise
        with stdlib ``json.dumps`` and no ``default=`` handler."""
        struct_t = pa.struct([
            pa.field("id", pa.int64()),
            pa.field("created", pa.timestamp("us", tz="UTC")),
            pa.field("amount", pa.decimal128(18, 2)),
        ])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([
                {
                    "id": 1,
                    "created": datetime(2026, 3, 23, 10, 18, 24, tzinfo=timezone.utc),
                    "amount": Decimal("42.50"),
                },
            ], type=struct_t)],
            schema=pa.schema([pa.field("rec", struct_t)]),
        )
        records = arrow_to_json_shape(batch).to_pylist()
        # No TypeError — that's the contract.
        serialised = json.dumps(records)
        assert '"amount": "42.50"' in serialised
        assert "T10:18:24" in serialised


class TestToDbRecordsAndToJsonRecords:
    """Symmetric materialisation entry points on SchemaContract: one per
    destination kind, each one shortest-path to its receiver."""

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
            [{
                "id": 1,
                "created": datetime(2026, 3, 23, 10, 18, 24, tzinfo=timezone.utc),
                "amount": Decimal("42.5000"),
            }],
            schema=contract.arrow_schema,
        )
        records = contract.to_db_records(batch)
        # SA receives Python-native datetime/Decimal here.
        assert isinstance(records[0]["created"], datetime)
        assert isinstance(records[0]["amount"], Decimal)

    def test_to_json_records_stringifies_for_json_dumps(self):
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
            [{
                "id": 1,
                "created": datetime(2026, 3, 23, 10, 18, 24, tzinfo=timezone.utc),
                "amount": Decimal("42.5000"),
            }],
            schema=contract.arrow_schema,
        )
        records = contract.to_json_records(batch)
        # Boundary contract: stdlib json.dumps must work.
        json.dumps(records)
        assert isinstance(records[0]["created"], str)
        assert "T" in records[0]["created"]
        assert records[0]["amount"] == "42.5000"

    def test_to_json_records_decodes_json_columns_after_cast(self):
        schema = {
            "properties": {
                "metadata": {"type": "object", "arrow_type": "Json"},
                "ts": {"arrow_type": "Timestamp(MICROSECOND, UTC)"},
            }
        }
        contract = SchemaContract(schema)
        batch = contract.from_pylist(
            [{
                "metadata": {"k": "v"},
                "ts": datetime(2026, 3, 23, 10, 18, 24, tzinfo=timezone.utc),
            }]
        )
        records = contract.to_json_records(batch)
        # metadata back to a dict; ts to an ISO string. Both safe for
        # json.dumps.
        json.dumps(records)
        assert records[0]["metadata"] == {"k": "v"}
        assert "T" in records[0]["ts"]
