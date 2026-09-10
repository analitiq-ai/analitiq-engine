"""The per-record schema, and the Arrow type of every field in it."""

from __future__ import annotations

from typing import Any

import pytest
from analitiq.contracts.endpoints import ResponseExtraction
from analitiq.contracts.stream import validate_endpoint_ref
from pydantic import ValidationError

from cdk.api.response_schema import (
    FieldDeclaration,
    apply_read_type_map,
    record_field_declaration,
    records_items_schema,
    resolve_field_arrow_type,
)
from cdk.exceptions import ReadError
from cdk.type_map import UnmappedTypeError

pytestmark = pytest.mark.unit

#: The stream's binding, parsed the way the read path hands it over: a
#: scope-discriminated contract ref, never a dict.
_ENDPOINT_REF = validate_endpoint_ref(
    {"scope": "connector", "connection_id": "c", "endpoint_id": "items"}
)
_CONNECTION_REF = validate_endpoint_ref(
    {
        "scope": "connection",
        "connection_id": "c",
        "database_object": {"schema": "public", "name": "items"},
    }
)


def _response(
    schema: dict[str, Any], ref: str = "response.body.records"
) -> ResponseExtraction:
    """The read's ``response`` block, parsed the way the read path gets it.

    The block's own fields are contract fields and are read as attributes
    (``schema`` is spelled ``schema_`` on the model, ``in`` would be
    ``location``); the JSON Schema INSIDE it stays a free-form dict,
    because that is what the contract declares it to be and what the walk
    below descends through.
    """
    return ResponseExtraction.model_validate(
        {"schema": schema, "records": {"ref": ref}}
    )


class _Runtime:
    """A runtime that answers one mapper, or refuses."""

    def __init__(self, mapper: Any = None, error: Exception | None = None):
        self._mapper = mapper
        self._error = error
        self.asked: list[Any] = []

    def type_mapper_for(self, *, scope: Any) -> Any:
        self.asked.append(scope)
        if self._error is not None:
            raise self._error
        return self._mapper


class _Mapper:
    def __init__(self, rules: dict[str, str]):
        self._rules = rules

    def to_arrow_type(self, native: str) -> str:
        try:
            return self._rules[native]
        except KeyError as err:
            raise UnmappedTypeError("test-connector", "read", native) from err


class TestItemsSchema:
    def test_it_walks_the_declared_ref_to_the_item_properties(self) -> None:
        schema = {
            "type": "object",
            "properties": {
                "records": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"id": {}}},
                }
            },
        }
        assert records_items_schema("items", _response(schema)) == {
            "type": "object",
            "properties": {"id": {}},
        }

    def test_the_body_itself_can_be_the_records(self) -> None:
        schema = {
            "type": "array",
            "items": {"type": "object", "properties": {"id": {}}},
        }
        items = records_items_schema("items", _response(schema, "response.body"))
        assert items["properties"] == {"id": {}}

    def test_a_ref_in_a_real_scope_but_off_the_body_never_parses(self) -> None:
        # ``connector.foo`` satisfies the ref grammar's scope pattern, so
        # only the anchor rule refuses it -- and the contract is what
        # applies that rule, before ``records_items_schema`` sees a block at
        # all. The ``split_records_ref`` call inside the walk is the same
        # refusal one layer in, kept for the callers that receive a bare ref
        # string; this pins that no block reaching HERE can carry one.
        with pytest.raises(ValidationError):
            _response({"type": "object"}, "connector.foo")

    def test_a_field_the_schema_does_not_declare_names_what_is_available(self) -> None:
        schema = {"type": "object", "properties": {"data": {"type": "array"}}}
        with pytest.raises(ReadError, match=r"available: \['data'\]"):
            records_items_schema("items", _response(schema))

    def test_items_without_properties_cannot_be_a_record_schema(self) -> None:
        schema = {
            "type": "object",
            "properties": {"records": {"type": "array", "items": {"type": "string"}}},
        }
        with pytest.raises(ReadError, match="no 'properties'"):
            records_items_schema("items", _response(schema))


class TestReadTypeMap:
    def test_a_field_without_an_arrow_type_gets_one_from_the_map(self) -> None:
        items = {"properties": {"id": {"type": "integer"}}}
        apply_read_type_map(
            items, _ENDPOINT_REF, _Runtime(_Mapper({"integer": "Int64"}))
        )
        assert items["properties"]["id"]["arrow_type"] == "Int64"

    def test_a_format_narrows_the_native_type_looked_up(self) -> None:
        items = {"properties": {"at": {"type": "string", "format": "date-time"}}}
        apply_read_type_map(
            items,
            _ENDPOINT_REF,
            _Runtime(_Mapper({"string:date-time": "Timestamp(MICROSECOND, UTC)"})),
        )
        assert items["properties"]["at"]["arrow_type"] == "Timestamp(MICROSECOND, UTC)"

    def test_a_hand_annotated_field_keeps_its_type_and_needs_no_map(self) -> None:
        items = {"properties": {"id": {"type": "integer", "arrow_type": "Int32"}}}
        runtime = _Runtime(error=RuntimeError("no type-map here"))
        apply_read_type_map(items, _ENDPOINT_REF, runtime)
        assert items["properties"]["id"]["arrow_type"] == "Int32"
        assert runtime.asked == []

    def test_an_unmapped_type_fails_loud_naming_the_field(self) -> None:
        items = {"properties": {"weird": {"type": "geography"}}}
        with pytest.raises(ReadError, match="'weird'"):
            apply_read_type_map(items, _ENDPOINT_REF, _Runtime(_Mapper({})))

    def test_a_missing_type_map_is_a_config_defect_not_a_retryable_one(self) -> None:
        items = {"properties": {"id": {"type": "integer"}}}
        runtime = _Runtime(error=RuntimeError("mapper absent"))
        with pytest.raises(ReadError, match="no usable read type-map"):
            apply_read_type_map(items, _ENDPOINT_REF, runtime)


class TestNestedResolution:
    def test_it_descends_into_an_object_and_a_list(self) -> None:
        mapper = _Mapper({"integer": "Int64", "string": "Utf8"})
        field = {
            "arrow_type": "Object",
            "properties": {
                "inner": {"type": "integer"},
                "tags": {"arrow_type": "List", "items": {"type": "string"}},
            },
        }
        resolve_field_arrow_type(field, "outer", lambda: mapper)
        assert field["properties"]["inner"]["arrow_type"] == "Int64"
        assert field["properties"]["tags"]["items"]["arrow_type"] == "Utf8"

    def test_a_json_blobs_documentary_children_are_left_alone(self) -> None:
        # Descending would fail a read on a child type the schema build
        # never consults.
        field = {
            "arrow_type": "Json",
            "properties": {"anything": {"type": "geography"}},
        }
        resolve_field_arrow_type(field, "blob", lambda: _Mapper({}))
        assert "arrow_type" not in field["properties"]["anything"]

    def test_a_nullable_union_type_resolves_by_its_real_member(self) -> None:
        field = {"type": ["string", "null"]}
        resolve_field_arrow_type(field, "name", lambda: _Mapper({"string": "Utf8"}))
        assert field["arrow_type"] == "Utf8"


class TestCursorFieldType:
    def _schema(self, declared: Any) -> dict[str, Any]:
        return {"properties": {"updated_at": {"type": declared}}}

    def test_a_plain_type_is_read_as_declared(self) -> None:
        assert record_field_declaration(
            "items", self._schema("string"), "updated_at"
        ) == FieldDeclaration("string", None)

    @pytest.mark.parametrize(
        ("declared", "expected"), [("epoch_seconds", "epoch_seconds"), ("", None)]
    )
    def test_the_format_is_read_with_the_type(
        self, declared: str, expected: str | None
    ) -> None:
        schema = {"properties": {"ts": {"type": "integer", "format": declared}}}
        assert record_field_declaration("items", schema, "ts") == FieldDeclaration(
            "integer", expected
        )

    @pytest.mark.parametrize(
        ("declared", "expected"),
        [(["string", "null"], "string"), (["null", "integer"], "integer")],
    )
    def test_a_nullable_union_is_its_real_member(
        self, declared: list[str], expected: str
    ) -> None:
        # Null only says the field is nullable; the checkpoint never
        # stores None, so it says nothing about how a cursor reads back.
        assert record_field_declaration(
            "items", self._schema(declared), "updated_at"
        ) == FieldDeclaration(expected, None)

    @pytest.mark.parametrize("declared", [["string", "integer"], ["null"], None, 7])
    def test_anything_but_one_real_type_is_refused(self, declared: Any) -> None:
        with pytest.raises(ReadError, match="needs one plain JSON type"):
            record_field_declaration("items", self._schema(declared), "updated_at")

    def test_an_undeclared_cursor_field_is_refused(self) -> None:
        with pytest.raises(ReadError, match="'missing' is not declared"):
            record_field_declaration("items", self._schema("string"), "missing")

    def test_a_cursor_field_declaring_iso8601_is_accepted(self) -> None:
        # iso8601 doesn't change the raw wire value's shape from what was
        # always implicit before issue #503 -- an ISO string stays an ISO
        # string -- so the checkpoint mechanism's own ISO parse still reads
        # it back correctly.
        schema = {
            "properties": {
                "updated_at": {"type": "string", "encoding": {"name": "iso8601"}}
            }
        }
        assert record_field_declaration(
            "items", schema, "updated_at"
        ) == FieldDeclaration("string", None)

    @pytest.mark.parametrize("declared_encoding", [None, {"name": "iso8601"}])
    def test_a_time_of_day_cursor_field_is_refused(
        self, declared_encoding: dict[str, str] | None
    ) -> None:
        # _parse_cursor (cdk.api.replication) reads every string-typed
        # cursor field as an absolute ISO-8601 moment regardless of
        # whether 'iso8601' is declared explicitly or no encoding is
        # declared at all -- both reach the same branch. A Time32/Time64
        # field's wire value ("12:34:56") is a time of day, not a moment,
        # so it would checkpoint fine once and fail resuming on the next
        # run.
        field: dict[str, Any] = {"type": "string", "arrow_type": "Time64(MICROSECOND)"}
        if declared_encoding is not None:
            field["encoding"] = declared_encoding
        schema = {"properties": {"updated_at": field}}
        with pytest.raises(ReadError, match="time of day"):
            record_field_declaration("items", schema, "updated_at")

    def test_a_timestamp_cursor_field_is_unaffected_by_the_time_of_day_check(
        self,
    ) -> None:
        schema = {
            "properties": {
                "updated_at": {
                    "type": "string",
                    "arrow_type": "Timestamp(MICROSECOND, tz=UTC)",
                    "encoding": {"name": "iso8601"},
                }
            }
        }
        assert record_field_declaration(
            "items", schema, "updated_at"
        ) == FieldDeclaration("string", None)

    @pytest.mark.parametrize(
        ("unit", "fmt"),
        [("SECOND", "epoch_seconds"), ("MILLISECOND", "epoch_milliseconds")],
    )
    def test_a_cursor_field_declaring_epoch_with_a_matching_format_is_accepted(
        self, unit: str, fmt: str
    ) -> None:
        schema = {
            "properties": {
                "updated_at": {
                    "type": "integer",
                    "format": fmt,
                    "encoding": {"name": "epoch", "unit": unit},
                }
            }
        }
        assert record_field_declaration(
            "items", schema, "updated_at"
        ) == FieldDeclaration("integer", fmt)

    def test_a_cursor_field_declaring_epoch_on_a_string_type_is_refused(self) -> None:
        # cursor_bounds reads an epoch cursor back only from an
        # integer-typed field; a string-typed field checkpoints and reads
        # back through the ISO-8601 branch instead, which cannot parse a
        # bare epoch tick count.
        schema = {
            "properties": {
                "updated_at": {
                    "type": "string",
                    "format": "epoch_seconds",
                    "encoding": {"name": "epoch", "unit": "SECOND"},
                }
            }
        }
        with pytest.raises(ReadError, match="not 'integer'"):
            record_field_declaration("items", schema, "updated_at")

    def test_a_cursor_field_declaring_epoch_with_an_unsupported_unit_is_refused(
        self,
    ) -> None:
        # cursor_bounds only ever reads an epoch cursor back in seconds or
        # milliseconds; MICROSECOND/NANOSECOND/DAY have no matching format.
        schema = {
            "properties": {
                "updated_at": {
                    "type": "integer",
                    "format": "epoch_seconds",
                    "encoding": {"name": "epoch", "unit": "MICROSECOND"},
                }
            }
        }
        with pytest.raises(ReadError, match="only reads an epoch cursor back"):
            record_field_declaration("items", schema, "updated_at")

    def test_a_cursor_field_declaring_epoch_with_a_mismatched_format_is_refused(
        self,
    ) -> None:
        # The decoder would render milliseconds while cursor_bounds reads
        # the checkpoint back as seconds -- a resume from the wrong instant,
        # not a crash, so it must be refused rather than silently accepted.
        schema = {
            "properties": {
                "updated_at": {
                    "type": "integer",
                    "format": "epoch_seconds",
                    "encoding": {"name": "epoch", "unit": "MILLISECOND"},
                }
            }
        }
        with pytest.raises(ReadError, match="resumes from the wrong instant"):
            record_field_declaration("items", schema, "updated_at")

    def test_a_cursor_field_declaring_an_unsafe_encoding_is_refused(self) -> None:
        # The checkpoint stores this field's raw wire value and the next
        # run's cursor_bounds parses it back strictly as ISO-8601/epoch --
        # a regex_epoch wrapper string is neither, so it would checkpoint
        # fine once and then break the stream on its second run.
        schema = {
            "properties": {
                "updated_at": {
                    "type": "string",
                    "encoding": {
                        "name": "regex_epoch",
                        "pattern": r"/Date\((\d+)\)/",
                        "unit": "MILLISECOND",
                    },
                }
            }
        }
        with pytest.raises(ReadError, match="regex_epoch"):
            record_field_declaration("items", schema, "updated_at")


class TestMapperIsScoped:
    def test_the_endpoint_scope_chooses_the_mapper(self) -> None:
        from cdk.types import EndpointScope

        runtime = _Runtime(_Mapper({"integer": "Int64"}))
        apply_read_type_map(
            {"properties": {"id": {"type": "integer"}}},
            _CONNECTION_REF,
            runtime,
        )
        assert runtime.asked == [EndpointScope.CONNECTION]

    def test_the_mapper_is_resolved_once_for_the_whole_schema(self) -> None:
        runtime = _Runtime(_Mapper({"integer": "Int64", "string": "Utf8"}))
        apply_read_type_map(
            {"properties": {"id": {"type": "integer"}, "n": {"type": "string"}}},
            _ENDPOINT_REF,
            runtime,
        )
        assert len(runtime.asked) == 1
