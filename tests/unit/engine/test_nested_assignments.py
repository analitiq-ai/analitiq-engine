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
    async def test_dlq_bound_records_skip_json_encode_pass(self):
        """If the transform loop already produced errors, the json.dumps
        pass must not run — otherwise it could surface a SECOND class of
        error (non-JSON-serializable values) on records that will be
        discarded anyway. The user sees only the actionable, first-class
        failure.

        Setup: one record produces a transform error (non-nullable target
        gets None). A separate record places a non-serializable
        ``datetime`` in a Json target. If the dumps pass ran, the raised
        message would mention both errors; correct behavior surfaces
        only the original transform error."""
        from datetime import datetime as _dt
        from src.engine.exceptions import TransformationError

        translated = [
            _translate_assignment({
                "target": {
                    "path": "required",
                    "arrow_type": "Utf8",
                    "nullable": False,
                },
                "value": {"expression": {"op": "get", "path": "missing_field"}},
            }),
            _translate_assignment({
                "target": {
                    "path": "metadata",
                    "arrow_type": "Json",
                    "nullable": True,
                },
                "value": {"expression": {"op": "get", "path": "blob"}},
            }),
        ]
        batch = [
            {"missing_field": None, "blob": _dt(2026, 5, 12)},
            {"missing_field": "ok", "blob": _dt(2026, 5, 12)},
        ]
        with pytest.raises(TransformationError) as exc_info:
            await DataTransformer().apply_transformations(
                batch, {"mapping": {"assignments": translated}},
            )
        # The first-class (transform) error must surface; the json-encode
        # secondary error must NOT — that's the load-bearing property.
        msg = str(exc_info.value)
        assert "not JSON-serializable" not in msg

    @pytest.mark.asyncio
    async def test_dumps_error_reports_original_source_row(self):
        """When upstream transforms drop a record (return None), the
        source-row index in the dumps-step error must point at the
        ORIGINAL batch position, not the post-filter slot."""
        from datetime import datetime as _dt
        from src.engine.exceptions import TransformationError

        translated = [
            _translate_assignment({
                "target": {
                    "path": "id",
                    "arrow_type": "Utf8",
                    "nullable": True,
                },
                "value": {"expression": {"op": "get", "path": "id"}},
            }),
            _translate_assignment({
                "target": {
                    "path": "metadata",
                    "arrow_type": "Json",
                    "nullable": True,
                },
                "value": {"expression": {"op": "get", "path": "blob"}},
            }),
        ]
        # Patch the assignment transformer to drop the second record so
        # the post-filter index for the third record is 1 while its
        # source-batch index is 2.
        transformer = DataTransformer()
        original = transformer.assignment_transformer.transform_record

        async def _selective(record, assignments, default_on_error="dlq"):
            result, errors = await original(record, assignments, default_on_error)
            if record.get("drop_me"):
                return None, errors
            return result, errors

        transformer.assignment_transformer.transform_record = _selective  # type: ignore[assignment]

        batch = [
            {"id": "r0", "blob": {"ok": 1}},
            {"id": "r1", "blob": {"ok": 2}, "drop_me": True},
            {"id": "r2", "blob": _dt(2026, 5, 12)},
        ]
        with pytest.raises(TransformationError, match=r"\(row 2\)") as exc_info:
            await transformer.apply_transformations(
                batch, {"mapping": {"assignments": translated}},
            )
        msg = str(exc_info.value)
        assert "got datetime" in msg
        # Negative: post-filter index would have reported row 1.
        assert "(row 1)" not in msg

    @pytest.mark.asyncio
    async def test_json_target_rejects_non_dict_non_str_non_none(self):
        """A Json target receiving an int (or any non-dict/list/str/None)
        is an author mistake — the transformer must collect it as a
        per-record error rather than silently passing the value through
        to ``pa.RecordBatch.from_pylist``, which would raise a vague
        Arrow error far from the source."""
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


class TestFnToIntFnToFloat:
    """Unit tests for _fn_to_int and _fn_to_float: None passthrough, valid
    conversions, and TransformationError on unparseable or overflow input."""

    @pytest.mark.asyncio
    async def test_to_int_raises_on_unparseable(self):
        from src.engine.exceptions import TransformationError

        t = AssignmentTransformer()
        with pytest.raises(TransformationError, match="to_int.*abc.*str"):
            await t._fn_to_int("abc")

    @pytest.mark.asyncio
    async def test_to_float_raises_on_unparseable(self):
        from src.engine.exceptions import TransformationError

        t = AssignmentTransformer()
        with pytest.raises(TransformationError, match="to_float.*xyz.*str"):
            await t._fn_to_float("xyz")

    @pytest.mark.asyncio
    async def test_to_int_raises_on_non_numeric_type(self):
        # Any non-numeric complex type triggers the same raise path.
        from src.engine.exceptions import TransformationError

        t = AssignmentTransformer()
        with pytest.raises(TransformationError, match="to_int.*dict"):
            await t._fn_to_int({"a": 1})

    @pytest.mark.asyncio
    async def test_to_float_raises_on_non_numeric_type(self):
        from src.engine.exceptions import TransformationError

        t = AssignmentTransformer()
        with pytest.raises(TransformationError, match="to_float.*list"):
            await t._fn_to_float([1, 2])

    @pytest.mark.asyncio
    async def test_to_int_raises_on_overflow(self):
        # int(float("inf")) raises OverflowError — must become TransformationError.
        from src.engine.exceptions import TransformationError

        t = AssignmentTransformer()
        with pytest.raises(TransformationError, match="to_int.*inf"):
            await t._fn_to_int("inf")

    @pytest.mark.asyncio
    async def test_to_float_raises_on_overflow(self):
        # float(10**400) raises OverflowError — must become TransformationError.
        from src.engine.exceptions import TransformationError

        t = AssignmentTransformer()
        with pytest.raises(TransformationError, match="to_float"):
            await t._fn_to_float(10**400)

    @pytest.mark.asyncio
    async def test_to_int_returns_none_for_none(self):
        assert await AssignmentTransformer()._fn_to_int(None) is None

    @pytest.mark.asyncio
    async def test_to_float_returns_none_for_none(self):
        assert await AssignmentTransformer()._fn_to_float(None) is None

    @pytest.mark.asyncio
    async def test_to_int_converts_valid_inputs(self):
        t = AssignmentTransformer()
        assert await t._fn_to_int("42") == 42
        assert await t._fn_to_int(3.9) == 3  # truncated, not rounded
        assert await t._fn_to_int(0) == 0

    @pytest.mark.asyncio
    async def test_to_float_converts_valid_inputs(self):
        t = AssignmentTransformer()
        assert await t._fn_to_float("3.14") == pytest.approx(3.14)
        assert await t._fn_to_float(2) == pytest.approx(2.0)
        assert await t._fn_to_float("0") == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_to_int_failure_propagates_to_dlq_via_apply_transformations(self):
        """TransformationError from _fn_to_int must reach the batch error
        list through transform_record so DLQ routing fires (not silently None)."""
        from src.engine.exceptions import TransformationError

        # Use transformer-native shape directly (no _translate_assignment needed).
        assignment = {
            "target": {"path": ["amount"], "arrow_type": "Int32", "nullable": True},
            "value": {
                "kind": "expr",
                "expr": {
                    "op": "pipe",
                    "args": [
                        {"op": "get", "path": ["raw_amount"]},
                        {"op": "fn", "name": "to_int", "version": 1, "args": []},
                    ],
                },
            },
        }
        with pytest.raises(TransformationError):
            await DataTransformer().apply_transformations(
                [{"raw_amount": "not-a-number"}],
                {"mapping": {"assignments": [assignment]}},
            )


class TestAssignmentTransformerBadInputs:
    """Verify that unknown ops, function names, and value kinds raise errors
    instead of silently returning None or passing values through."""

    def _assignment(self, value_spec: dict) -> dict:
        return {
            "target": {"path": ["out"], "type": "string", "nullable": True},
            "value": value_spec,
        }

    @pytest.mark.asyncio
    async def test_unknown_expression_op_errors(self):
        assignment = self._assignment(
            {"kind": "expr", "expr": {"op": "frobnicate", "args": []}}
        )
        _, errors = await AssignmentTransformer().transform_record(
            record={}, assignments=[assignment]
        )
        assert errors, "expected an error for unknown op"
        assert "frobnicate" in errors[0]["error"]

    @pytest.mark.asyncio
    async def test_unknown_function_name_errors(self):
        assignment = self._assignment(
            {"kind": "expr", "expr": {"op": "fn", "name": "iso_to_dat", "version": 1, "args": []}}
        )
        _, errors = await AssignmentTransformer().transform_record(
            record={}, assignments=[assignment]
        )
        assert errors, "expected an error for unknown function name"
        assert "iso_to_dat" in errors[0]["error"]

    @pytest.mark.asyncio
    async def test_non_fn_op_in_pipe_stage_errors(self):
        assignment = self._assignment(
            {
                "kind": "expr",
                "expr": {
                    "op": "pipe",
                    "args": [
                        {"op": "const", "value": "hello"},
                        {"op": "get", "path": ["x"]},
                    ],
                },
            }
        )
        _, errors = await AssignmentTransformer().transform_record(
            record={}, assignments=[assignment]
        )
        assert errors, "expected an error for non-fn op in pipe stage"
        assert "get" in errors[0]["error"]

    @pytest.mark.asyncio
    async def test_unknown_value_kind_errors(self):
        assignment = self._assignment({"kind": "literal", "literal": {"value": "x"}})
        _, errors = await AssignmentTransformer().transform_record(
            record={}, assignments=[assignment]
        )
        assert errors, "expected an error for unknown value kind"
        assert "literal" in errors[0]["error"]

    @pytest.mark.asyncio
    async def test_missing_op_key_errors(self):
        """An expr dict with no 'op' key at all should error (op defaults to None)."""
        assignment = self._assignment({"kind": "expr", "expr": {"args": []}})
        _, errors = await AssignmentTransformer().transform_record(
            record={}, assignments=[assignment]
        )
        assert errors, "expected an error for missing op key"
        assert "None" in errors[0]["error"]

    @pytest.mark.asyncio
    async def test_none_function_name_errors(self):
        """A fn expression with no 'name' key should error (name defaults to None)."""
        assignment = self._assignment(
            {"kind": "expr", "expr": {"op": "fn", "version": 1, "args": []}}
        )
        _, errors = await AssignmentTransformer().transform_record(
            record={}, assignments=[assignment]
        )
        assert errors, "expected an error for missing function name"
        assert "None" in errors[0]["error"]

    @pytest.mark.asyncio
    async def test_error_isolated_per_assignment(self):
        """A bad first assignment produces an error entry but the second assignment
        still runs — transform_record continues accumulating rather than short-circuiting."""
        assignments = [
            self._assignment({"kind": "expr", "expr": {"op": "frobnicate"}}),
            {
                "target": {"path": ["name"], "type": "string", "nullable": True},
                "value": {"kind": "const", "const": {"value": "alice"}},
            },
        ]
        result, errors = await AssignmentTransformer().transform_record(
            record={}, assignments=assignments
        )
        assert len(errors) == 1
        assert errors[0]["field"] == "out"
        assert result.get("name") == "alice"
