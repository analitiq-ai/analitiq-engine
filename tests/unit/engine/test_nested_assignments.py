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
