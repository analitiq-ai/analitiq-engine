"""Nested-object and JSON assignments through the mapping transform.

Covers:
- ``build_output_schema`` produces ``pa.struct``/``pa.list_`` fields from
  ``target.arrow_type: "Object" | "List"`` plus the recursive sub-schema.
- dict/list constants and nested ``get`` drilling flow end-to-end through
  ``compile_mapping(...).run(batch)``.
- ``Json`` targets encode a dict constant to a JSON string, pass an
  already-encoded string through, and reject a non-dict/list/str/None value
  loudly.

The documents here are written exactly as a stream carries them: one spelling,
no translation step between the contract and the transform.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from src.engine.batch_policy import ErrorStrategy
from src.engine.exceptions import TransformationError
from src.engine.mapping import MappingDocument, build_output_schema, compile_mapping

_CHECK_ACCOUNT_PROPERTIES = {
    "id": {"arrow_type": "Utf8"},
    "objectName": {"arrow_type": "Utf8"},
}


def _document(assignments):
    return MappingDocument.parse({"assignments": assignments})


def _run(records, assignments):
    """Compile *assignments* and run them.

    ``records`` may be a list of dicts (types inferred) or a built
    ``pa.RecordBatch`` when an exact source type matters.
    """
    batch = (
        records
        if isinstance(records, pa.RecordBatch)
        else pa.RecordBatch.from_pylist(records)
    )
    return _compile(assignments).run(batch).to_pylist()


def _compile(assignments):
    return compile_mapping(_document(assignments), default_strategy=ErrorStrategy.FAIL)


class TestBuildOutputSchemaNested:
    def test_object_target_yields_struct(self):
        assignments = [
            {
                "target": {
                    "path": "checkAccount",
                    "arrow_type": "Object",
                    "nullable": False,
                    "properties": _CHECK_ACCOUNT_PROPERTIES,
                },
                "value": {
                    "kind": "constant",
                    "constant": {
                        "arrow_type": "Object",
                        "value": {"id": "123456", "objectName": "CheckAccount"},
                        "properties": _CHECK_ACCOUNT_PROPERTIES,
                    },
                },
            }
        ]
        schema = build_output_schema(_document(assignments).assignments)
        f = schema.field("checkAccount")
        assert pa.types.is_struct(f.type)
        assert {sub.name for sub in f.type} == {"id", "objectName"}
        assert not f.nullable

    def test_list_target_yields_list_of_struct(self):
        assignments = [
            {
                "target": {
                    "path": "positions",
                    "arrow_type": "List",
                    "nullable": True,
                    "items": {
                        "arrow_type": "Object",
                        "properties": {
                            "sku": {"arrow_type": "Utf8"},
                            "qty": {"arrow_type": "Int32"},
                        },
                    },
                },
                "value": {
                    "kind": "constant",
                    "constant": {
                        "arrow_type": "List",
                        "value": [],
                        "items": {"arrow_type": "Utf8"},
                    },
                },
            }
        ]
        schema = build_output_schema(_document(assignments).assignments)
        f = schema.field("positions")
        assert pa.types.is_list(f.type)
        assert pa.types.is_struct(f.type.value_type)


class TestNestedConstantsEndToEnd:
    """Dict/list constants reach Arrow assembly with their nesting intact."""

    ASSIGNMENTS = [
        {
            "target": {"path": "id", "arrow_type": "Utf8", "nullable": False},
            "value": {
                "kind": "expression",
                "expression": {"op": "get", "path": ["id"]},
            },
        },
        {
            "target": {
                "path": "checkAccount",
                "arrow_type": "Object",
                "nullable": False,
                "properties": _CHECK_ACCOUNT_PROPERTIES,
            },
            "value": {
                "kind": "constant",
                "constant": {
                    "arrow_type": "Object",
                    "value": {"id": "42", "objectName": "CheckAccount"},
                    "properties": _CHECK_ACCOUNT_PROPERTIES,
                },
            },
        },
    ]

    def test_dict_constant_builds_struct_column(self):
        out = _compile(self.ASSIGNMENTS).run(
            pa.RecordBatch.from_pylist([{"id": "r1"}, {"id": "r2"}])
        )
        assert pa.types.is_struct(out.schema.field("checkAccount").type)
        assert out.to_pylist() == [
            {"id": "r1", "checkAccount": {"id": "42", "objectName": "CheckAccount"}},
            {"id": "r2", "checkAccount": {"id": "42", "objectName": "CheckAccount"}},
        ]

    def test_list_constant_builds_list_column(self):
        assignment = {
            "target": {
                "path": "tags",
                "arrow_type": "List",
                "items": {"arrow_type": "Utf8"},
            },
            "value": {
                "kind": "constant",
                "constant": {
                    "arrow_type": "List",
                    "value": ["a", "b"],
                    "items": {"arrow_type": "Utf8"},
                },
            },
        }
        out = _run([{"x": 1}], [assignment])
        assert out == [{"tags": ["a", "b"]}]

    def test_nested_get_drills_into_struct_source(self):
        batch = pa.record_batch(
            [pa.array([{"inner": "v1"}, {"inner": "v2"}])], names=["outer"]
        )
        assignment = {
            "target": {"path": "x", "arrow_type": "Utf8"},
            "value": {
                "kind": "expression",
                "expression": {"op": "get", "path": ["outer", "inner"]},
            },
        }
        assert _run(batch, [assignment]) == [{"x": "v1"}, {"x": "v2"}]

    def test_non_nullable_struct_target_with_missing_source_fails(self):
        """A non-nullable Object target whose source is absent fails the batch:
        the all-null column meets the nullability check loudly."""
        assignment = {
            "target": {
                "path": "checkAccount",
                "arrow_type": "Object",
                "nullable": False,
                "properties": {"id": {"arrow_type": "Utf8"}},
            },
            "value": {
                "kind": "expression",
                "expression": {"op": "get", "path": ["absent"]},
            },
        }
        with pytest.raises(TransformationError, match="not nullable"):
            _run([{"id": "r1"}], [assignment])


class TestJsonTarget:
    """A ``Json`` target carries an encoded string column. A dict constant is
    json.dumps-ed; an already-encoded string passes through; a non-encodable
    scalar (int) is an author mistake the transform rejects loudly."""

    def test_dict_constant_serialized_to_json_string(self):
        assignments = [
            {
                "target": {"path": "id", "arrow_type": "Utf8", "nullable": False},
                "value": {
                    "kind": "expression",
                    "expression": {"op": "get", "path": ["id"]},
                },
            },
            {
                "target": {"path": "metadata", "arrow_type": "Json", "nullable": True},
                "value": {
                    "kind": "constant",
                    "constant": {
                        "arrow_type": "Json",
                        "value": {"some_key": "some_value", "n": 42},
                    },
                },
            },
        ]
        out = _compile(assignments).run(pa.RecordBatch.from_pylist([{"id": "r1"}]))
        assert pa.types.is_large_string(out.schema.field("metadata").type)
        assert out.to_pylist() == [
            {"id": "r1", "metadata": '{"some_key": "some_value", "n": 42}'}
        ]

    def test_get_expression_string_passes_through(self):
        """A get pulling an already-encoded JSON string from a source column
        passes through so the destination's ``decode_json_columns`` reverses
        it."""
        assignment = {
            "target": {"path": "metadata", "arrow_type": "Json", "nullable": True},
            "value": {
                "kind": "expression",
                "expression": {"op": "get", "path": ["upstream_blob"]},
            },
        }
        out = _run([{"upstream_blob": '{"k": "v"}'}], [assignment])
        assert out == [{"metadata": '{"k": "v"}'}]

    def test_non_dict_non_str_non_none_value_is_rejected(self):
        """A Json target receiving an int is an author mistake: the transform
        rejects it loudly rather than passing it to a vague Arrow error far from
        the source."""
        assignment = {
            "target": {"path": "metadata", "arrow_type": "Json", "nullable": True},
            "value": {
                "kind": "expression",
                "expression": {"op": "get", "path": ["bad_field"]},
            },
        }
        with pytest.raises(TransformationError, match="dict/list/str/None"):
            _run([{"bad_field": 42}], [assignment])
