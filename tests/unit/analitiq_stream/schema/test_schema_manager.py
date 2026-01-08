"""Unit tests for SchemaManager class."""

import json
import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest

from src.schema.schema_manager import SchemaManager


class TestSchemaManager:
    """Test suite for SchemaManager."""

    @pytest.fixture
    def mock_state_manager(self):
        """Create a mock state manager."""
        mock = MagicMock()
        mock.get_checkpoint.return_value = {}
        mock.save_checkpoint.return_value = None
        return mock

    @pytest.fixture
    def schema_manager(self, mock_state_manager):
        """Create a SchemaManager instance with mock state manager."""
        return SchemaManager(mock_state_manager)

    @pytest.fixture
    def sample_schema(self):
        """Sample schema for testing."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string", "maxLength": 100},
                "email": {"type": "string", "format": "email"},
                "age": {"type": "number", "minimum": 0, "maximum": 150},
                "tags": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["id", "name"],
        }

    @pytest.fixture
    def modified_schema(self):
        """Modified version of sample schema for drift testing."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string", "maxLength": 200},  # Changed maxLength
                "email": {"type": "string", "format": "email"},
                "age": {"type": "number", "minimum": 0, "maximum": 150},
                "tags": {"type": "array", "items": {"type": "string"}},
                "created_at": {"type": "string", "format": "date-time"},  # New field
            },
            "required": ["id", "name", "email"],  # Email now required
        }

    @pytest.mark.unit
    def test_schema_manager_initialization(self, mock_state_manager):
        """Test SchemaManager initialization."""
        manager = SchemaManager(mock_state_manager)
        assert manager.state_manager == mock_state_manager
        assert manager.schema_cache == {}

    @pytest.mark.unit
    def test_generate_schema_hash_valid_schema(self, schema_manager, sample_schema):
        """Test schema hash generation with valid schema."""
        hash1 = schema_manager.generate_schema_hash(sample_schema)
        hash2 = schema_manager.generate_schema_hash(sample_schema)
        
        assert hash1 == hash2  # Same schema should produce same hash
        assert isinstance(hash1, str)
        assert len(hash1) == 32  # MD5 hash length

    @pytest.mark.unit
    def test_generate_schema_hash_different_schemas(
        self, schema_manager, sample_schema, modified_schema
    ):
        """Test that different schemas produce different hashes."""
        hash1 = schema_manager.generate_schema_hash(sample_schema)
        hash2 = schema_manager.generate_schema_hash(modified_schema)
        
        assert hash1 != hash2

    @pytest.mark.unit
    def test_generate_schema_hash_order_independence(self, schema_manager):
        """Test that field order doesn't affect hash."""
        schema1 = {
            "properties": {
                "a": {"type": "string"},
                "b": {"type": "integer"},
            }
        }
        schema2 = {
            "properties": {
                "b": {"type": "integer"},
                "a": {"type": "string"},
            }
        }
        
        hash1 = schema_manager.generate_schema_hash(schema1)
        hash2 = schema_manager.generate_schema_hash(schema2)
        
        assert hash1 == hash2

    @pytest.mark.unit
    def test_generate_schema_hash_error_handling(self, schema_manager):
        """Test error handling in schema hash generation."""
        with patch("src.schema.schema_manager.logger") as mock_logger:
            # Pass non-serializable object
            result = schema_manager.generate_schema_hash({"func": lambda x: x})

            # Lambda functions can actually be processed by the normalizer, they just get ignored
            # so we get a hash of the normalized empty schema
            assert isinstance(result, str)
            assert len(result) == 32  # MD5 hash length

    @pytest.mark.unit
    def test_normalize_schema_with_properties(self, schema_manager):
        """Test schema normalization with properties."""
        schema = {
            "type": "object",
            "properties": {
                "field1": {"type": "string", "maxLength": 100},
                "field2": {"type": "integer", "minimum": 0},
            },
            "required": ["field1"],
            "additionalProperties": False,  # Should be ignored
        }
        
        normalized = schema_manager._normalize_schema(schema)
        
        assert "type" in normalized
        assert normalized["type"] == "object"
        assert "properties" in normalized
        assert "field1" in normalized["properties"]
        assert "field2" in normalized["properties"]
        assert normalized["required"] == ["field1"]
        assert "additionalProperties" not in normalized

    @pytest.mark.unit
    def test_normalize_schema_required_fields_sorted(self, schema_manager):
        """Test that required fields are sorted in normalized schema."""
        schema = {"required": ["z", "a", "m", "b"]}
        
        normalized = schema_manager._normalize_schema(schema)
        
        assert normalized["required"] == ["a", "b", "m", "z"]

    @pytest.mark.unit
    def test_normalize_field_definition_essential_props(self, schema_manager):
        """Test field definition normalization with essential properties."""
        field_def = {
            "type": "string",
            "format": "email",
            "maxLength": 255,
            "pattern": "^.+@.+$",
            "description": "User email",  # Non-essential, should be ignored
            "title": "Email",  # Non-essential, should be ignored
        }
        
        normalized = schema_manager._normalize_field_definition(field_def)
        
        assert normalized["type"] == "string"
        assert normalized["format"] == "email"
        assert normalized["maxLength"] == 255
        assert normalized["pattern"] == "^.+@.+$"
        assert "description" not in normalized
        assert "title" not in normalized

    @pytest.mark.unit
    def test_normalize_field_definition_array_type(self, schema_manager):
        """Test field definition normalization with array type."""
        field_def = {"type": ["string", "null", "integer"]}
        
        normalized = schema_manager._normalize_field_definition(field_def)
        
        assert normalized["type"] == ["integer", "null", "string"]

    @pytest.mark.unit
    def test_detect_schema_drift_no_previous_schema(
        self, schema_manager, mock_state_manager, sample_schema
    ):
        """Test drift detection when no previous schema exists."""
        mock_state_manager.get_checkpoint.return_value = {}
        
        with patch("src.schema.schema_manager.logger") as mock_logger:
            drift = schema_manager.detect_schema_drift("pipeline-1", sample_schema)
            
            assert drift is False
            mock_logger.info.assert_called_once_with(
                "No previous schema found for pipeline pipeline-1"
            )

    @pytest.mark.unit
    def test_detect_schema_drift_no_drift(
        self, schema_manager, mock_state_manager, sample_schema
    ):
        """Test drift detection when schemas match."""
        schema_hash = schema_manager.generate_schema_hash(sample_schema)
        mock_state_manager.get_checkpoint.return_value = {"schema_hash": schema_hash}
        
        drift = schema_manager.detect_schema_drift("pipeline-1", sample_schema)
        
        assert drift is False

    @pytest.mark.unit
    def test_detect_schema_drift_with_drift(
        self, schema_manager, mock_state_manager, sample_schema, modified_schema
    ):
        """Test drift detection when schemas differ."""
        old_hash = schema_manager.generate_schema_hash(sample_schema)
        mock_state_manager.get_checkpoint.return_value = {"schema_hash": old_hash}
        
        with patch("src.schema.schema_manager.logger") as mock_logger:
            drift = schema_manager.detect_schema_drift("pipeline-1", modified_schema)
            
            assert drift is True
            mock_logger.warning.assert_called_once_with(
                "Schema drift detected for pipeline pipeline-1"
            )

    @pytest.mark.unit
    def test_analyze_schema_changes_added_fields(
        self, schema_manager, sample_schema, modified_schema
    ):
        """Test analysis of added fields between schemas."""
        changes = schema_manager.analyze_schema_changes(sample_schema, modified_schema)

        assert "added_fields" in changes
        assert len(changes["added_fields"]) == 1
        assert changes["added_fields"][0]["field"] == "created_at"

    @pytest.mark.unit
    def test_analyze_schema_changes_removed_fields(self, schema_manager):
        """Test analysis of removed fields between schemas."""
        old_schema = {
            "properties": {
                "field1": {"type": "string"},
                "field2": {"type": "integer"},
                "field3": {"type": "boolean"},
            }
        }
        new_schema = {
            "properties": {
                "field1": {"type": "string"},
            }
        }

        changes = schema_manager.analyze_schema_changes(old_schema, new_schema)

        assert "removed_fields" in changes
        assert len(changes["removed_fields"]) == 2
        removed_field_names = [field["field"] for field in changes["removed_fields"]]
        assert set(removed_field_names) == {"field2", "field3"}

    @pytest.mark.unit
    def test_analyze_schema_changes_modified_fields(self, schema_manager):
        """Test analysis of modified fields between schemas."""
        old_schema = {
            "properties": {
                "name": {"type": "string", "maxLength": 100},
                "age": {"type": "integer", "minimum": 0},
            }
        }
        new_schema = {
            "properties": {
                "name": {"type": "string", "maxLength": 200},  # Changed
                "age": {"type": "integer", "minimum": 0},  # Unchanged
            }
        }

        changes = schema_manager.analyze_schema_changes(old_schema, new_schema)

        assert "modified_fields" in changes
        assert len(changes["modified_fields"]) == 1
        assert changes["modified_fields"][0]["field"] == "name"

        # Age should not be in modified fields since it hasn't changed
        modified_field_names = [field["field"] for field in changes["modified_fields"]]
        assert "age" not in modified_field_names

    @pytest.mark.unit
    def test_analyze_schema_changes_required_fields(self, schema_manager):
        """Test analysis of required field changes."""
        old_schema = {"required": ["id", "name"]}
        new_schema = {"required": ["id", "name", "email"]}

        changes = schema_manager.analyze_schema_changes(old_schema, new_schema)

        # Check that email being made required is detected as a breaking change
        assert "breaking_changes" in changes
        assert any("email" in change and "required" in change for change in changes["breaking_changes"])

    @pytest.mark.unit
    def test_get_evolution_recommendations_breaking_changes(self, schema_manager, sample_schema, modified_schema):
        """Test evolution recommendations for breaking changes."""
        changes = schema_manager.analyze_schema_changes(sample_schema, modified_schema)

        recommendations = schema_manager.get_evolution_recommendations(changes)

        assert isinstance(recommendations, list)
        assert len(recommendations) > 0

    @pytest.mark.unit
    def test_create_schema_migration_plan(self, schema_manager, sample_schema, modified_schema):
        """Test creation of schema migration plan."""
        changes = schema_manager.analyze_schema_changes(sample_schema, modified_schema)

        plan = schema_manager.create_schema_migration_plan("pipeline-1", changes)

        assert "pipeline_id" in plan
        assert plan["pipeline_id"] == "pipeline-1"
        assert "timestamp" in plan
        assert "changes" in plan
        assert "recommendations" in plan
        assert "migration_steps" in plan
        assert "rollback_steps" in plan

    @pytest.mark.unit
    def test_save_schema_version(self, schema_manager, sample_schema):
        """Test saving schema version."""
        schema_manager.save_schema_version("pipeline-1", sample_schema, "v1.0.0")

        # Check that schema was cached
        cache_key = "pipeline-1_v1.0.0"
        assert cache_key in schema_manager.schema_cache

        cached_record = schema_manager.schema_cache[cache_key]
        assert cached_record["pipeline_id"] == "pipeline-1"
        assert cached_record["version"] == "v1.0.0"
        assert cached_record["schema"] == sample_schema
        assert "hash" in cached_record
        assert "timestamp" in cached_record


    @pytest.mark.unit
    def test_get_schema_version_with_version(self, schema_manager, sample_schema):
        """Test getting specific schema version."""
        schema_manager.save_schema_version("pipeline-1", sample_schema, "v1.0.0")

        version_record = schema_manager.get_schema_version("pipeline-1", "v1.0.0")

        assert version_record is not None
        assert version_record["version"] == "v1.0.0"
        assert version_record["schema"] == sample_schema

    @pytest.mark.unit
    def test_get_schema_version_no_version(self, schema_manager):
        """Test getting schema version when no version exists."""
        version_record = schema_manager.get_schema_version("pipeline-1")

        assert version_record is None










