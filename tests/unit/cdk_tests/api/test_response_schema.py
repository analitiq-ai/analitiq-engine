"""The per-record schema, and the Arrow type of every field in it."""

from __future__ import annotations

from typing import Any

import pytest
from analitiq.contracts.endpoints import ApiEndpointDoc, ResponseExtraction
from analitiq.contracts.stream import validate_endpoint_ref

from cdk.api.response_schema import (
    FieldDeclaration,
    apply_read_type_map,
    record_field_declaration,
    records_items_schema,
    resolve_field_arrow_type,
)
from cdk.exceptions import ReadError
from cdk.type_map import TypeMapper, UnmappedTypeError
from cdk.type_map.rules import parse_write_rules

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
    has_read_map = True

    def __init__(self, rules: dict[str, str]):
        self._rules = rules

    def to_arrow_type(self, native: str) -> str:
        try:
            return self._rules[native]
        except KeyError as err:
            raise UnmappedTypeError("test-connector", "read", native) from err


_RECORD = {"type": "object", "properties": {"id": {"type": "integer"}}}

#: The records paths the contract resolves through ``$ref``/``allOf``: the
#: response schema, and the ``records.ref`` that addresses its records.
_COMPOSED_RECORD_PATHS: list[tuple[str, dict[str, Any], str]] = [
    (
        "body_items_by_ref",
        {"type": "array", "items": {"$ref": "#/$defs/Rec"}, "$defs": {"Rec": _RECORD}},
        "response.body",
    ),
    (
        "nested_items_by_ref",
        {
            "type": "object",
            "properties": {"data": {"type": "array", "items": {"$ref": "#/$defs/Rec"}}},
            "$defs": {"Rec": _RECORD},
        },
        "response.body.data",
    ),
    (
        "body_by_ref",
        {
            "$ref": "#/$defs/Page",
            "$defs": {
                "Page": {
                    "type": "object",
                    "properties": {"data": {"type": "array", "items": _RECORD}},
                }
            },
        },
        "response.body.data",
    ),
    (
        "items_by_all_of",
        {"type": "array", "items": {"allOf": [_RECORD]}},
        "response.body",
    ),
]


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

    def test_the_walk_follows_a_ref_the_contract_accepts(self) -> None:
        # The contract resolves the records path through `$ref`/`$defs`, so a
        # document composed that way is valid and reaches the read. A second,
        # literal `properties` walk here refused it before the first request.
        schema = {
            "type": "object",
            "properties": {"page": {"$ref": "#/$defs/page"}},
            "$defs": {
                "page": {
                    "type": "object",
                    "properties": {
                        "rows": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {"id": {"type": "string"}},
                            },
                        }
                    },
                }
            },
        }
        items = records_items_schema(
            "items", _response(schema, "response.body.page.rows")
        )
        assert items["properties"] == {"id": {"type": "string"}}

    def test_a_ref_that_resolves_to_nothing_is_a_read_defect(self) -> None:
        # Unreachable through a contract-validated document -- the gate names
        # the segment first. Kept because the walk is also reached with blocks
        # a caller built itself, and answering the response ENVELOPE there
        # would enumerate its keys as the record's fields.
        schema = {"type": "object", "properties": {"data": {"type": "array"}}}
        with pytest.raises(ReadError, match="does not resolve to a record schema"):
            records_items_schema("items", _response(schema))

    @pytest.mark.parametrize(
        ("schema", "records_ref"),
        [(schema, ref) for _, schema, ref in _COMPOSED_RECORD_PATHS],
        ids=[name for name, _, _ in _COMPOSED_RECORD_PATHS],
    )
    def test_a_records_path_composed_through_ref_or_all_of_resolves(
        self, schema: dict[str, Any], records_ref: str
    ) -> None:
        response = {"schema": schema, "records": {"ref": records_ref}}
        document = ApiEndpointDoc.model_validate(
            {
                "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
                "endpoint_id": "items",
                "operations": {
                    "read": {
                        "request": {"method": "GET", "path": "/items"},
                        "response": response,
                    }
                },
            }
        )
        read = document.operations.read
        assert read is not None
        items = records_items_schema("items", read.response)
        assert items["properties"] == {"id": {"type": "integer"}}

    def test_items_without_properties_cannot_be_a_record_schema(self) -> None:
        schema = {
            "type": "object",
            "properties": {"records": {"type": "array", "items": {"type": "string"}}},
        }
        with pytest.raises(ReadError, match="does not resolve to a record schema"):
            records_items_schema("items", _response(schema))


class TestReadTypeMap:
    def test_a_field_without_an_arrow_type_gets_one_from_the_map(self) -> None:
        items = {"properties": {"id": {"type": "integer"}}}
        apply_read_type_map(
            items, {}, _ENDPOINT_REF, _Runtime(_Mapper({"integer": "Int64"}))
        )
        assert items["properties"]["id"]["arrow_type"] == "Int64"

    def test_a_format_narrows_the_native_type_looked_up(self) -> None:
        items = {"properties": {"at": {"type": "string", "format": "date-time"}}}
        apply_read_type_map(
            items,
            {},
            _ENDPOINT_REF,
            _Runtime(_Mapper({"string:date-time": "Timestamp(MICROSECOND, UTC)"})),
        )
        assert items["properties"]["at"]["arrow_type"] == "Timestamp(MICROSECOND, UTC)"

    def test_a_hand_annotated_field_keeps_its_type_and_needs_no_map(self) -> None:
        items = {"properties": {"id": {"type": "integer", "arrow_type": "Int32"}}}
        runtime = _Runtime(error=RuntimeError("no type-map here"))
        apply_read_type_map(items, {}, _ENDPOINT_REF, runtime)
        assert items["properties"]["id"]["arrow_type"] == "Int32"
        assert runtime.asked == []

    def test_an_unmapped_type_fails_loud_naming_the_field(self) -> None:
        items = {"properties": {"weird": {"type": "geography"}}}
        with pytest.raises(ReadError, match="'weird'"):
            apply_read_type_map(items, {}, _ENDPOINT_REF, _Runtime(_Mapper({})))

    def test_a_missing_type_map_is_a_config_defect_not_a_retryable_one(self) -> None:
        items = {"properties": {"id": {"type": "integer"}}}
        runtime = _Runtime(error=RuntimeError("mapper absent"))
        with pytest.raises(ReadError, match="no usable read type-map"):
            apply_read_type_map(items, {}, _ENDPOINT_REF, runtime)

    def test_a_write_only_type_map_is_no_read_type_map(self) -> None:
        items = {"properties": {"id": {"type": "integer"}}}
        write_only = TypeMapper(
            "test-connector",
            None,
            parse_write_rules(
                [{"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}],
                source="<w>",
            ),
        )
        with pytest.raises(ReadError, match="no usable read type-map"):
            apply_read_type_map(items, {}, _ENDPOINT_REF, _Runtime(write_only))


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
        resolve_field_arrow_type(field, "outer", lambda: mapper, {})
        assert field["properties"]["inner"]["arrow_type"] == "Int64"
        assert field["properties"]["tags"]["items"]["arrow_type"] == "Utf8"

    def test_a_json_blobs_documentary_children_are_left_alone(self) -> None:
        # Descending would fail a read on a child type the schema build
        # never consults.
        field = {
            "arrow_type": "Json",
            "properties": {"anything": {"type": "geography"}},
        }
        resolve_field_arrow_type(field, "blob", lambda: _Mapper({}), {})
        assert "arrow_type" not in field["properties"]["anything"]

    def test_a_nullable_union_type_resolves_by_its_real_member(self) -> None:
        field = {"type": ["string", "null"]}
        resolve_field_arrow_type(field, "name", lambda: _Mapper({"string": "Utf8"}), {})
        assert field["arrow_type"] == "Utf8"


#: One record field per way the contract lets a declaration be composed, each
#: paired with the Arrow type the read must build it as.
_COMPOSED_FIELDS: list[tuple[str, dict[str, Any], str]] = [
    ("by_ref", {"$ref": "#/$defs/Id"}, "int64"),
    ("by_all_of", {"allOf": [{"$ref": "#/$defs/Id"}, {"description": "n"}]}, "int64"),
    (
        "nested_by_ref",
        {"type": "object", "properties": {"zip": {"$ref": "#/$defs/Zip"}}},
        "struct<zip: string>",
    ),
    (
        "items_by_ref",
        {"type": "array", "items": {"$ref": "#/$defs/Zip"}},
        "list<item: string>",
    ),
]


class TestComposedFieldDeclarations:
    """A field reached through ``$ref``/``allOf`` reads as the inline one would.

    The contract accepts these documents, so the read must build the same
    Arrow schema from them; a raw ``{"$ref": ...}`` carries no type to map.
    """

    _MAPPER = _Mapper(
        {"integer": "Int64", "string": "Utf8", "object": "Object", "array": "List"}
    )

    def _arrow_type(
        self, name: str, declaration: dict[str, Any], mapper: _Mapper = _MAPPER
    ) -> str:
        from cdk.schema_contract import SchemaContract

        schema = {
            "type": "object",
            "properties": {
                "records": {
                    "type": "array",
                    "items": {"type": "object", "properties": {name: declaration}},
                }
            },
            "$defs": {
                "Id": {"type": "integer"},
                "Zip": {"type": "string"},
                # Recursive, which the contract permits.
                "Node": {
                    "type": "object",
                    "properties": {
                        "children": {"type": "array", "items": {"$ref": "#/$defs/Node"}}
                    },
                },
            },
        }
        items = records_items_schema("items", _response(schema))
        apply_read_type_map(items, schema, _ENDPOINT_REF, _Runtime(mapper))
        return str(SchemaContract(items).arrow_schema.field(name).type)

    @pytest.mark.parametrize(
        ("name", "declaration", "expected"),
        _COMPOSED_FIELDS,
        ids=[name for name, _, _ in _COMPOSED_FIELDS],
    )
    def test_the_field_reads_as_its_inline_equivalent(
        self, name: str, declaration: dict[str, Any], expected: str
    ) -> None:
        assert self._arrow_type(name, declaration) == expected

    def test_a_recursive_declaration_read_as_json_is_not_walked(self) -> None:
        json_objects = _Mapper({"object": "Json"})
        assert self._arrow_type("tree", {"$ref": "#/$defs/Node"}, json_objects) == (
            "large_string"
        )

    def test_a_recursive_declaration_read_as_an_object_is_refused(self) -> None:
        # An Arrow struct has no recursive form, so the walk would never end.
        with pytest.raises(
            ReadError, match="'tree.children\\[\\].children' contains itself"
        ):
            self._arrow_type("tree", {"$ref": "#/$defs/Node"})

    def test_the_document_is_not_annotated(self) -> None:
        # Materialized children are copies: a `$defs` entry two fields share
        # must not carry the first field's resolution into the document.
        schema = {
            "type": "object",
            "properties": {
                "records": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "addr": {
                                "type": "object",
                                "properties": {"zip": {"$ref": "#/$defs/Zip"}},
                            }
                        },
                    },
                }
            },
            "$defs": {"Zip": {"type": "string"}},
        }
        items = records_items_schema("items", _response(schema))
        apply_read_type_map(items, schema, _ENDPOINT_REF, _Runtime(self._MAPPER))
        assert schema["$defs"]["Zip"] == {"type": "string"}


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


class TestMapperIsScoped:
    def test_the_endpoint_scope_chooses_the_mapper(self) -> None:
        from cdk.types import EndpointScope

        runtime = _Runtime(_Mapper({"integer": "Int64"}))
        apply_read_type_map(
            {"properties": {"id": {"type": "integer"}}},
            {},
            _CONNECTION_REF,
            runtime,
        )
        assert runtime.asked == [EndpointScope.CONNECTION]

    def test_the_mapper_is_resolved_once_for_the_whole_schema(self) -> None:
        runtime = _Runtime(_Mapper({"integer": "Int64", "string": "Utf8"}))
        apply_read_type_map(
            {"properties": {"id": {"type": "integer"}, "n": {"type": "string"}}},
            {},
            _ENDPOINT_REF,
            runtime,
        )
        assert len(runtime.asked) == 1
