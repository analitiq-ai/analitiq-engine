"""Nested-object and JSON assignments through the vectorized transform.

Covers:
- ``build_output_schema`` produces ``pa.struct``/``pa.list_`` fields from
  ``target.arrow_type: "Object" | "List"`` plus the recursive sub-schema.
- dict/list constants and nested ``get`` drilling flow end-to-end through
  ``compile_transform(...).run(batch)``.
- ``Json`` targets encode a dict const to a JSON string, pass an already-encoded
  string through, and reject a non-dict/list/str/None value loudly.

The transform is one vectorized path: assignments are compiled once and applied
to a ``pa.RecordBatch``. Success returns typed columns; bad data raises
``TransformationError`` for the whole batch.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from src.engine.data_transformer import build_output_schema, compile_transform
from src.engine.exceptions import TransformationError
from src.runner import _translate_assignment


def _run(records, assignments):
    """Compile contract/transformer-shaped *assignments* and run them.

    ``records`` may be a list of dicts (types inferred) or a built
    ``pa.RecordBatch`` when an exact source type matters.
    """
    batch = (
        records
        if isinstance(records, pa.RecordBatch)
        else pa.RecordBatch.from_pylist(records)
    )
    return compile_transform(assignments).run(batch).to_pylist()


class TestBuildOutputSchemaNested:
    def test_object_target_yields_struct(self):
        assignments = [
            {
                "target": {
                    "path": "checkAccount",
                    "arrow_type": "Object",
                    "nullable": False,
                    "properties": {
                        "id": {"arrow_type": "Utf8"},
                        "objectName": {"arrow_type": "Utf8"},
                    },
                },
                "value": {
                    "kind": "const",
                    "const": {"value": {"id": "123456", "objectName": "CheckAccount"}},
                },
            }
        ]
        schema = build_output_schema(assignments)
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
                "value": {"kind": "const", "const": {"value": []}},
            }
        ]
        schema = build_output_schema(assignments)
        f = schema.field("positions")
        assert pa.types.is_list(f.type)
        assert pa.types.is_struct(f.type.value_type)


class TestNestedConstantsEndToEnd:
    """Contract-shaped assignments with dict/list constants flow through the
    translator, the compiler, and Arrow assembly without losing the nesting."""

    CONTRACT_ASSIGNMENTS = [
        {
            "target": {"path": "id", "arrow_type": "Utf8", "nullable": False},
            "value": {"expression": {"op": "get", "path": "id"}},
        },
        {
            "target": {
                "path": "checkAccount",
                "arrow_type": "Object",
                "nullable": False,
                "properties": {
                    "id": {"arrow_type": "Utf8"},
                    "objectName": {"arrow_type": "Utf8"},
                },
            },
            "value": {
                "constant": {
                    "value": {"id": "42", "objectName": "CheckAccount"},
                    "arrow_type": "Object",
                }
            },
        },
    ]

    def test_dict_constant_builds_struct_column(self):
        translated = [_translate_assignment(a) for a in self.CONTRACT_ASSIGNMENTS]
        out = compile_transform(translated).run(
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
                "path": ["tags"],
                "arrow_type": "List",
                "items": {"arrow_type": "Utf8"},
            },
            "value": {"kind": "const", "const": {"value": ["a", "b"]}},
        }
        out = _run([{"x": 1}], [assignment])
        assert out == [{"tags": ["a", "b"]}]

    def test_nested_get_drills_into_struct_source(self):
        batch = pa.record_batch(
            [pa.array([{"inner": "v1"}, {"inner": "v2"}])], names=["outer"]
        )
        assignment = {
            "target": {"path": ["x"], "arrow_type": "Utf8"},
            "value": {
                "kind": "expr",
                "expr": {"op": "get", "path": ["outer", "inner"]},
            },
        }
        assert _run(batch, [assignment]) == [{"x": "v1"}, {"x": "v2"}]

    def test_non_nullable_struct_target_with_missing_source_fails(self):
        """A non-nullable Object target whose source is absent fails the batch:
        the all-null column meets the nullability check loudly."""
        assignment = {
            "target": {
                "path": ["checkAccount"],
                "arrow_type": "Object",
                "nullable": False,
                "properties": {"id": {"arrow_type": "Utf8"}},
            },
            "value": {"kind": "expr", "expr": {"op": "get", "path": ["absent"]}},
        }
        with pytest.raises(TransformationError, match="not nullable"):
            _run([{"id": "r1"}], [assignment])


class TestJsonTarget:
    """A ``Json`` target carries an encoded string column. A dict const is
    json.dumps-ed; an already-encoded string passes through; a non-encodable
    scalar (int) is an author mistake the transform rejects loudly."""

    def test_dict_constant_serialized_to_json_string(self):
        contract = [
            {
                "target": {"path": "id", "arrow_type": "Utf8", "nullable": False},
                "value": {"expression": {"op": "get", "path": "id"}},
            },
            {
                "target": {"path": "metadata", "arrow_type": "Json", "nullable": True},
                "value": {
                    "constant": {
                        "arrow_type": "Json",
                        "value": {"some_key": "some_value", "n": 42},
                    }
                },
            },
        ]
        translated = [_translate_assignment(a) for a in contract]
        out = compile_transform(translated).run(
            pa.RecordBatch.from_pylist([{"id": "r1"}])
        )
        assert pa.types.is_large_string(out.schema.field("metadata").type)
        assert out.to_pylist() == [
            {"id": "r1", "metadata": '{"some_key": "some_value", "n": 42}'}
        ]

    def test_get_expression_string_passes_through(self):
        """A get pulling an already-encoded JSON string from a source column
        passes through so the destination's ``decode_json_columns`` reverses
        it."""
        assignment = _translate_assignment(
            {
                "target": {"path": "metadata", "arrow_type": "Json", "nullable": True},
                "value": {"expression": {"op": "get", "path": "upstream_blob"}},
            }
        )
        out = _run([{"upstream_blob": '{"k": "v"}'}], [assignment])
        assert out == [{"metadata": '{"k": "v"}'}]

    def test_non_dict_non_str_non_none_value_is_rejected(self):
        """A Json target receiving an int is an author mistake: the transform
        rejects it loudly rather than passing it to a vague Arrow error far from
        the source."""
        assignment = _translate_assignment(
            {
                "target": {"path": "metadata", "arrow_type": "Json", "nullable": True},
                "value": {"expression": {"op": "get", "path": "bad_field"}},
            }
        )
        with pytest.raises(TransformationError, match="dict/list/str/None"):
            _run([{"bad_field": 42}], [assignment])
