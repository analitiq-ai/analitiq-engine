"""Custom assertion helpers for tests."""

from datetime import datetime
from typing import Any, Dict, List


def assert_timestamp_fields_are_datetime(record: Dict[str, Any], fields: List[str]):
    """Assert that specified fields in a record are datetime objects."""
    for field in fields:
        if field in record and record[field] is not None:
            assert isinstance(record[field], datetime), f"Field {field} should be datetime, got {type(record[field])}"


def assert_timestamp_fields_are_strings(record: Dict[str, Any], fields: List[str]):
    """Assert that specified fields in a record are ISO timestamp strings."""
    for field in fields:
        if field in record and record[field] is not None:
            assert isinstance(record[field], str), f"Field {field} should be string, got {type(record[field])}"
            assert record[field].endswith('+00:00') or record[field].endswith('Z'), f"Field {field} should be ISO format with timezone"


def assert_database_error_contains_type_mismatch(error_msg: str):
    """Assert that database error message indicates timestamp type mismatch."""
    assert "expected a datetime.date or datetime.datetime instance, got 'str'" in error_msg
    assert "invalid input for query argument" in error_msg


def assert_record_has_expected_structure(record: Dict[str, Any], expected_fields: List[str]):
    """Assert that a record contains all expected fields."""
    for field in expected_fields:
        assert field in record, f"Record missing expected field: {field}"


def assert_pipeline_config_valid(config: Dict[str, Any]):
    """Assert that a pipeline configuration has required structure."""
    required_top_level = ["pipeline_id", "name", "version", "src", "dst"]
    for field in required_top_level:
        assert field in config, f"Pipeline config missing required field: {field}"
    
    assert "host_id" in config["src"], "src config missing host_id"
    assert "host_id" in config["dst"], "dst config missing host_id"


def assert_batch_data_types_match_schema(batch: List[Dict[str, Any]], schema_fields: Dict[str, str]):
    """Assert that batch data types match expected schema types."""
    for record in batch:
        for field, expected_type in schema_fields.items():
            if field in record and record[field] is not None:
                actual_type = type(record[field]).__name__
                if expected_type == "datetime":
                    assert isinstance(record[field], datetime), f"Field {field} should be datetime, got {actual_type}"
                elif expected_type == "str":
                    assert isinstance(record[field], str), f"Field {field} should be string, got {actual_type}"
                elif expected_type == "int":
                    assert isinstance(record[field], int), f"Field {field} should be integer, got {actual_type}"
                elif expected_type == "float":
                    assert isinstance(record[field], (int, float)), f"Field {field} should be numeric, got {actual_type}"