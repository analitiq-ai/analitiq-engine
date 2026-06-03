"""Parser-helper coverage for the typed resolved-runtime boundary.

These tests exercise the *real* parse chain in
``src/engine/pipeline_config_prep.py`` -- raw stream JSON dict ->
``_parse_*`` helper -> typed dataclass -> ``model_dump()`` ->
``_translate_assignment`` -> ``build_output_schema`` / transformer.

``test_nested_assignments.py`` feeds raw dicts straight into the transformer
and so bypasses ``_parse_mapping_config`` / ``Assignment.model_dump()``; the
two regressions PR #114 fixed (dropped nested ``properties``/``items`` and a
dropped ``validate`` block) slipped through exactly because no test crossed
the parser. These tests close that gap.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from src.engine.data_transformer import (
    AssignmentTransformer,
    _translate_assignment,
    build_output_schema,
)
from src.engine.pipeline_config_prep import (
    _parse_batching_config,
    _parse_destination_config,
    _parse_error_handling_config,
    _parse_mapping_config,
    _parse_replication_config,
    _parse_source_config,
    _parse_stream_filter,
    _parse_write_config,
)
from src.models.stream import ReplicationMethod, WriteMode


def _schema_from_parsed_mapping(raw_mapping: dict) -> pa.Schema:
    """Run a raw mapping dict through the full production chain."""
    mapping = _parse_mapping_config(raw_mapping)
    assignments = [_translate_assignment(a.model_dump()) for a in mapping.assignments]
    return build_output_schema(assignments)


class TestNestedTargetsThroughParser:
    """Object/List targets must survive the parser, not just direct dicts."""

    def test_object_target_yields_struct(self):
        mapping = {
            "assignments": [
                {
                    "target": {
                        "path": "checkAccount",
                        "arrow_type": "Object",
                        "nullable": False,
                        "properties": {
                            "id": {"arrow_type": "Utf8"},
                            "name": {"arrow_type": "Utf8"},
                        },
                    },
                    "value": {"constant": {"value": {"id": "1", "name": "x"}}},
                }
            ]
        }
        # properties survive parsing into the typed AssignmentTarget
        parsed = _parse_mapping_config(mapping)
        assert parsed.assignments[0].target.properties == {
            "id": {"arrow_type": "Utf8"},
            "name": {"arrow_type": "Utf8"},
        }
        # ...and through model_dump -> translate -> schema build
        schema = _schema_from_parsed_mapping(mapping)
        field = schema.field("checkAccount")
        assert pa.types.is_struct(field.type)
        assert {sub.name for sub in field.type} == {"id", "name"}
        assert not field.nullable

    def test_list_target_yields_list_of_struct(self):
        mapping = {
            "assignments": [
                {
                    "target": {
                        "path": "positions",
                        "arrow_type": "List",
                        "items": {
                            "arrow_type": "Object",
                            "properties": {"sku": {"arrow_type": "Utf8"}},
                        },
                    },
                    "value": {"constant": {"value": []}},
                }
            ]
        }
        parsed = _parse_mapping_config(mapping)
        assert parsed.assignments[0].target.items is not None
        schema = _schema_from_parsed_mapping(mapping)
        field = schema.field("positions")
        assert pa.types.is_list(field.type)
        assert pa.types.is_struct(field.type.value_type)

    def test_scalar_target_unaffected(self):
        mapping = {
            "assignments": [
                {
                    "target": {"path": "id", "arrow_type": "Int64"},
                    "value": {"constant": {"value": 1}},
                }
            ]
        }
        parsed = _parse_mapping_config(mapping)
        assert parsed.assignments[0].target.properties is None
        assert parsed.assignments[0].target.items is None
        schema = _schema_from_parsed_mapping(mapping)
        assert schema.field("id").type == pa.int64()


class TestValidateBlockThroughParser:
    """The validate block must reach the transformer via the parser."""

    @pytest.mark.asyncio
    async def test_validate_block_carried_and_enforced_dlq(self):
        mapping = {
            "assignments": [
                {
                    "target": {"path": "email", "arrow_type": "Utf8"},
                    "value": {"constant": {"value": None}},
                    "validate": {
                        "rules": [{"type": "not_null", "message": "email required"}],
                        "on_error": "dlq",
                    },
                }
            ]
        }
        parsed = _parse_mapping_config(mapping)
        # block carried verbatim onto the typed Assignment
        assert parsed.assignments[0].validate == mapping["assignments"][0]["validate"]

        assignments = [_translate_assignment(a.model_dump()) for a in parsed.assignments]
        _, errors = await AssignmentTransformer().transform_record({}, assignments)
        assert len(errors) == 1
        assert errors[0]["action"] == "dlq"
        assert errors[0]["error"] == "email required"

    @pytest.mark.asyncio
    async def test_validate_default_value_substituted(self):
        mapping = {
            "assignments": [
                {
                    "target": {"path": "status", "arrow_type": "Utf8"},
                    "value": {"constant": {"value": None}},
                    "validate": {
                        "rules": [{"type": "not_null"}],
                        "on_error": "default_value",
                        "default": "unknown",
                    },
                }
            ]
        }
        parsed = _parse_mapping_config(mapping)
        assignments = [_translate_assignment(a.model_dump()) for a in parsed.assignments]
        result, _ = await AssignmentTransformer().transform_record({}, assignments)
        assert result["status"] == "unknown"

    def test_no_validate_block_is_none(self):
        mapping = {
            "assignments": [
                {
                    "target": {"path": "id", "arrow_type": "Int64"},
                    "value": {"constant": {"value": 1}},
                }
            ]
        }
        parsed = _parse_mapping_config(mapping)
        assert parsed.assignments[0].validate is None


class TestAssignmentValueGuard:
    """A present-but-null value block must not become a silent null-fill."""

    def test_value_with_neither_expression_nor_constant_raises(self):
        with pytest.raises(ValueError, match="non-null 'expression' or 'constant'"):
            _parse_mapping_config(
                {"assignments": [{"target": {"path": "x"}, "value": {}}]}
            )

    def test_value_with_null_expression_raises(self):
        with pytest.raises(ValueError, match="non-null 'expression' or 'constant'"):
            _parse_mapping_config(
                {
                    "assignments": [
                        {"target": {"path": "x"}, "value": {"expression": None}}
                    ]
                }
            )

    def test_value_with_null_constant_raises(self):
        with pytest.raises(ValueError, match="non-null 'expression' or 'constant'"):
            _parse_mapping_config(
                {
                    "assignments": [
                        {"target": {"path": "x"}, "value": {"constant": None}}
                    ]
                }
            )

    def test_constant_with_null_value_is_valid(self):
        # A null *literal* constant ({"value": null}) is a real value block.
        parsed = _parse_mapping_config(
            {
                "assignments": [
                    {
                        "target": {"path": "x", "arrow_type": "Utf8"},
                        "value": {"constant": {"value": None}},
                    }
                ]
            }
        )
        assert parsed.assignments[0].value.constant is not None
        assert parsed.assignments[0].value.constant.value is None


class TestParseHelperGuards:
    """Each parse guard raises a precise ValueError on a contract violation."""

    def test_unknown_replication_method_raises(self):
        with pytest.raises(ValueError, match="Unknown replication method"):
            _parse_replication_config({"method": "bogus"})

    def test_unknown_write_mode_raises(self):
        with pytest.raises(ValueError, match="Unknown write mode"):
            _parse_write_config({"mode": "bogus"})

    def test_stream_filter_missing_field_raises(self):
        with pytest.raises(ValueError, match="missing required 'field'"):
            _parse_stream_filter({"operator": "eq", "value": 1})


class TestNonDefaultRoundTrip:
    """Non-default source/destination blocks parse into the right typed shape."""

    def test_incremental_source_with_filters(self):
        cfg = _parse_source_config(
            {
                "endpoint_ref": {
                    "scope": "connection",
                    "connection_id": "c",
                    "endpoint_id": "e",
                },
                "replication": {
                    "method": "incremental",
                    "cursor_field": ["updated_at"],
                    "safety_window_seconds": 60,
                },
                "filters": [{"field": "status", "operator": "eq", "value": "active"}],
                "primary_keys": ["id"],
            }
        )
        assert cfg.replication.method == ReplicationMethod.INCREMENTAL
        assert cfg.replication.cursor_field == ["updated_at"]
        assert cfg.replication.safety_window_seconds == 60
        assert cfg.filters[0].field == "status"
        assert cfg.primary_keys == ["id"]

    def test_upsert_destination_with_conflict_keys(self):
        cfg = _parse_destination_config(
            {
                "endpoint_ref": {
                    "scope": "connection",
                    "connection_id": "c",
                    "endpoint_id": "e",
                },
                "write": {"mode": "upsert", "conflict_keys": ["id"]},
            }
        )
        assert cfg.write.mode == WriteMode.UPSERT
        assert cfg.write.conflict_keys == ["id"]


class TestRuntimeSubConfigs:
    """Typed batching / error-handling configs own their defaults once."""

    def test_batching_defaults_and_overrides(self):
        assert _parse_batching_config({}) .batch_size == 1000
        assert _parse_batching_config({}).max_concurrent_batches == 3
        over = _parse_batching_config({"batch_size": 50, "max_concurrent_batches": 2})
        assert (over.batch_size, over.max_concurrent_batches) == (50, 2)

    def test_error_handling_defaults_and_overrides(self):
        d = _parse_error_handling_config({})
        assert (d.strategy, d.max_retries, d.retry_delay_seconds) == ("fail", 3, 5)
        over = _parse_error_handling_config(
            {"strategy": "dlq", "max_retries": 7, "retry_delay_seconds": 9}
        )
        assert (over.strategy, over.max_retries, over.retry_delay_seconds) == ("dlq", 7, 9)
