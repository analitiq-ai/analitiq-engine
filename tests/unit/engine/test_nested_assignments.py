"""End-to-end engine paths for nested-object assignments.

Covers:
- ``build_output_schema`` produces ``pa.struct``/``pa.list_`` fields from
  ``target.arrow_type: "Object" | "List"`` plus the recursive sub-schema.
- ``AssignmentTransformer`` accepts dict / list constants verbatim — the
  Arrow-batch construction downstream handles the type binding.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from src.engine.data_transformer import (
    AssignmentTransformer,
    DataTransformer,
    build_output_schema,
)
from src.engine.pipeline import _translate_assignment


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
                    "const": {
                        "value": {"id": "123456", "objectName": "CheckAccount"}
                    },
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


class TestDictConstantsEndToEnd:
    """Contract-shaped assignments with a dict constant flow through the
    pipeline translator, the transformer, and Arrow-batch construction
    without losing the nested dict."""

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

    @pytest.mark.asyncio
    async def test_dict_constant_passes_through(self):
        translated = [_translate_assignment(a) for a in self.CONTRACT_ASSIGNMENTS]
        record, errors = await AssignmentTransformer().transform_record(
            record={"id": "row-1"}, assignments=translated,
        )
        assert errors == []
        assert record == {
            "id": "row-1",
            "checkAccount": {"id": "42", "objectName": "CheckAccount"},
        }

    @pytest.mark.asyncio
    async def test_json_target_rejects_non_dict_non_str_non_none(self):
        """A Json target receiving an int (or any non-dict/list/str/None)
        is an author mistake — the transformer must collect it as a
        per-record error rather than silently dropping it through to
        ``pa.RecordBatch.from_pylist``, which would raise a vague Arrow
        error far from the source."""
        from src.engine.exceptions import TransformationError

        translated = [
            _translate_assignment({
                "target": {
                    "path": "metadata",
                    "arrow_type": "Json",
                    "nullable": True,
                },
                "value": {
                    "expression": {"op": "get", "path": "bad_field"},
                },
            })
        ]
        with pytest.raises(TransformationError, match="dict/list/str/None"):
            await DataTransformer().apply_transformations(
                [{"bad_field": 42}], {"mapping": {"assignments": translated}},
            )

    @pytest.mark.asyncio
    async def test_json_target_get_expression_string_passes_through(self):
        """A get-expression pulling a value from a Json source column
        yields a string (already encoded upstream). It must pass through
        the transformer unchanged so the destination's
        ``decode_json_columns`` can reverse it."""
        translated = [
            _translate_assignment({
                "target": {
                    "path": "metadata",
                    "arrow_type": "Json",
                    "nullable": True,
                },
                "value": {
                    "expression": {"op": "get", "path": "upstream_blob"},
                },
            })
        ]
        out = await DataTransformer().apply_transformations(
            [{"upstream_blob": '{"k": "v"}'}],
            {"mapping": {"assignments": translated}},
        )
        assert out == [{"metadata": '{"k": "v"}'}]

    @pytest.mark.asyncio
    async def test_json_target_serializes_dict_to_string(self):
        """A target with ``arrow_type: "Json"`` accepts a dict constant and
        the transformer emits a JSON-encoded string the Arrow schema
        (``pa.large_string``) can ingest."""
        contract = [
            {
                "target": {"path": "id", "arrow_type": "Utf8", "nullable": False},
                "value": {"expression": {"op": "get", "path": "id"}},
            },
            {
                "target": {
                    "path": "metadata",
                    "arrow_type": "Json",
                    "nullable": True,
                },
                "value": {
                    "constant": {
                        "arrow_type": "Json",
                        "value": {"some_key": "some_value", "n": 42},
                    }
                },
            },
        ]
        translated = [_translate_assignment(a) for a in contract]
        pylist = await DataTransformer().apply_transformations(
            [{"id": "r1"}], {"mapping": {"assignments": translated}},
        )
        # After transform the dict has been serialized to a JSON string so
        # pa.large_string can hold it.
        assert isinstance(pylist[0]["metadata"], str)
        assert pylist[0]["metadata"] == '{"some_key": "some_value", "n": 42}'

        schema = build_output_schema(translated)
        batch = pa.RecordBatch.from_pylist(pylist, schema=schema)
        assert pa.types.is_large_string(batch.schema.field("metadata").type)

    @pytest.mark.asyncio
    async def test_full_pipeline_builds_struct_column(self):
        """Translator + transformer + Arrow schema: the dict constant
        survives intact and the output column type is ``pa.struct``."""
        translated = [_translate_assignment(a) for a in self.CONTRACT_ASSIGNMENTS]
        pylist = await DataTransformer().apply_transformations(
            [{"id": "r1"}, {"id": "r2"}], {"mapping": {"assignments": translated}},
        )
        schema = build_output_schema(translated)
        batch = pa.RecordBatch.from_pylist(pylist, schema=schema)
        assert pa.types.is_struct(batch.schema.field("checkAccount").type)
        assert batch.to_pylist() == [
            {"id": "r1", "checkAccount": {"id": "42", "objectName": "CheckAccount"}},
            {"id": "r2", "checkAccount": {"id": "42", "objectName": "CheckAccount"}},
        ]
