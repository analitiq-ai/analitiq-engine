"""The per-record schema, and the Arrow type of every field in it."""

from __future__ import annotations

from typing import Any

import pytest
from analitiq.contracts.endpoints import ResponseExtraction

from cdk.api.response_schema import (
    apply_read_type_map,
    records_items_schema,
    resolve_field_arrow_type,
)
from cdk.exceptions import ReadError
from cdk.type_map import UnmappedTypeError

pytestmark = pytest.mark.unit

_ENDPOINT_REF = {"scope": "connector", "connection_id": "c", "endpoint_id": "items"}


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

    def test_an_endpoint_ref_without_a_scope_names_the_valid_scopes(self) -> None:
        with pytest.raises(ReadError, match="connector"):
            apply_read_type_map({"properties": {}}, {}, _Runtime())


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


class TestMapperIsScoped:
    def test_the_endpoint_scope_chooses_the_mapper(self) -> None:
        from cdk.types import EndpointScope

        runtime = _Runtime(_Mapper({"integer": "Int64"}))
        apply_read_type_map(
            {"properties": {"id": {"type": "integer"}}},
            {"scope": "connection"},
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
