"""
Unit tests for incremental replication functionality in APIConnector.
"""

import json
import pytest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock
from analitiq_stream.connectors.api import APIConnector, ReadError


class TestAPIIncrementalReplication:
    """Test suite for incremental replication functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.connector = APIConnector("test_api")
        self.sample_endpoint_schema = {
            "filters": {
                "updatedAt": {
                    "type": "string",
                    "required": False,
                    "operators": ["gte"],
                    "description": "Filter by update timestamp"
                },
                "createdDateStart": {
                    "type": "string", 
                    "required": False,
                    "operators": ["gte"],
                    "description": "Starting date filter"
                },
                "id": {
                    "type": "integer",
                    "required": False,
                    "operators": ["gte", "lte"],
                    "description": "Filter by ID"
                }
            }
        }

    def test_load_state_from_pipeline_config(self):
        """Test loading state from pipeline configuration."""
        pipeline_config = {
            "bookmarks": [{"partition": {}, "cursor": "2025-08-14T11:58:03Z"}],
            "run": {"run_id": "test-run"},
            "replication_method": "incremental",
            "replication_key": "updatedAt",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120
        }

        state = self.connector._load_state_from_pipeline_config(pipeline_config)

        assert state["replication_method"] == "incremental"
        assert state["replication_key"] == "updatedAt"
        assert state["cursor_mode"] == "inclusive"
        assert state["safety_window_seconds"] == 120
        assert len(state["bookmarks"]) == 1

    def test_load_state_with_defaults(self):
        """Test loading state with default values."""
        pipeline_config = {}

        state = self.connector._load_state_from_pipeline_config(pipeline_config)

        assert state["replication_method"] == "full"
        assert state["replication_key"] is None
        assert state["cursor_mode"] == "inclusive"
        assert state["safety_window_seconds"] == 120
        assert state["bookmarks"] == []

    @patch('pathlib.Path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_get_endpoint_schema_filters_success(self, mock_file, mock_exists):
        """Test successful loading of endpoint schema filters."""
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = json.dumps(self.sample_endpoint_schema)

        filters = self.connector._get_endpoint_schema_filters("test-endpoint-id")

        assert "updatedAt" in filters
        assert "createdDateStart" in filters
        assert "id" in filters

    @patch('pathlib.Path.exists')
    def test_get_endpoint_schema_filters_not_found(self, mock_exists):
        """Test handling when endpoint schema file is not found."""
        mock_exists.return_value = False

        filters = self.connector._get_endpoint_schema_filters("missing-endpoint-id")

        assert filters == {}

    def test_validate_replication_key_against_schema_valid(self):
        """Test validation of valid replication key."""
        with patch.object(self.connector, '_get_endpoint_schema_filters') as mock_get_filters:
            mock_get_filters.return_value = self.sample_endpoint_schema["filters"]

            is_valid = self.connector._validate_replication_key_against_schema("updatedAt", "test-endpoint")

            assert is_valid is True

    def test_validate_replication_key_against_schema_invalid(self):
        """Test validation of invalid replication key."""
        with patch.object(self.connector, '_get_endpoint_schema_filters') as mock_get_filters:
            mock_get_filters.return_value = self.sample_endpoint_schema["filters"]

            is_valid = self.connector._validate_replication_key_against_schema("invalidKey", "test-endpoint")

            assert is_valid is False

    def test_validate_replication_key_no_key(self):
        """Test validation when no replication key is provided."""
        is_valid = self.connector._validate_replication_key_against_schema(None, "test-endpoint")

        assert is_valid is True  # No validation needed for full replication

    def test_compute_effective_start_time_timestamp(self):
        """Test computing effective start time for timestamp cursor."""
        cursor = "2025-08-14T11:58:03Z"
        safety_window = 120

        effective_start = self.connector._compute_effective_start_time(cursor, safety_window)

        # Should subtract 120 seconds
        expected_dt = datetime.fromisoformat("2025-08-14T11:58:03+00:00") - timedelta(seconds=120)
        expected = expected_dt.isoformat().replace('+00:00', 'Z')
        assert effective_start == expected

    def test_compute_effective_start_time_numeric_id(self):
        """Test computing effective start time for numeric ID cursor."""
        cursor = "1000"
        safety_window = 50

        effective_start = self.connector._compute_effective_start_time(cursor, safety_window)

        assert effective_start == "950"  # 1000 - 50

    def test_compute_effective_start_time_numeric_id_minimum(self):
        """Test computing effective start time for numeric ID with minimum boundary."""
        cursor = "30"
        safety_window = 50

        effective_start = self.connector._compute_effective_start_time(cursor, safety_window)

        assert effective_start == "0"  # max(0, 30 - 50)

    def test_compute_effective_start_time_invalid_cursor(self):
        """Test computing effective start time for invalid cursor format."""
        cursor = "invalid-cursor-format"
        safety_window = 120

        effective_start = self.connector._compute_effective_start_time(cursor, safety_window)

        assert effective_start == cursor  # Return original if parsing fails

    def test_build_replication_filter_inclusive(self):
        """Test building replication filter in inclusive mode."""
        replication_key = "updatedAt"
        cursor_mode = "inclusive"
        effective_start = "2025-08-14T11:56:03Z"

        filter_params = self.connector._build_replication_filter(replication_key, cursor_mode, effective_start)

        assert filter_params == {"updatedAt": "2025-08-14T11:56:03Z"}

    def test_build_replication_filter_exclusive(self):
        """Test building replication filter in exclusive mode."""
        replication_key = "id"
        cursor_mode = "exclusive"
        effective_start = "950"

        filter_params = self.connector._build_replication_filter(replication_key, cursor_mode, effective_start)

        assert filter_params == {"id": "950"}

    @pytest.mark.asyncio
    async def test_setup_incremental_replication_success(self):
        """Test successful setup of incremental replication."""
        config = {"filters": {}}
        state = {
            "replication_method": "incremental",
            "replication_key": "updatedAt",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "bookmarks": [{"partition": {}, "cursor": "2025-08-14T11:58:03Z"}]
        }
        src_config = {"endpoint_id": "test-endpoint"}

        with patch.object(self.connector, '_validate_replication_key_against_schema') as mock_validate:
            with patch.object(self.connector, '_compute_effective_start_time') as mock_compute:
                mock_validate.return_value = True
                mock_compute.return_value = "2025-08-14T11:56:03Z"

                await self.connector._setup_incremental_replication(config, state, src_config)

                assert "updatedAt" in config["filters"]
                assert config["filters"]["updatedAt"]["value"] == "2025-08-14T11:56:03Z"
                assert config["filters"]["updatedAt"]["required"] is True

    @pytest.mark.asyncio
    async def test_setup_incremental_replication_invalid_key(self):
        """Test setup of incremental replication with invalid replication key."""
        config = {"filters": {}}
        state = {
            "replication_method": "incremental",
            "replication_key": "invalidKey",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "bookmarks": [{"partition": {}, "cursor": "2025-08-14T11:58:03Z"}]
        }
        src_config = {"endpoint_id": "test-endpoint"}

        with patch.object(self.connector, '_validate_replication_key_against_schema') as mock_validate:
            mock_validate.return_value = False

            with pytest.raises(ReadError, match="Replication key 'invalidKey' not found"):
                await self.connector._setup_incremental_replication(config, state, src_config)

    @pytest.mark.asyncio
    async def test_setup_incremental_replication_no_bookmarks(self):
        """Test setup of incremental replication with no bookmarks."""
        config = {"filters": {}}
        state = {
            "replication_method": "incremental",
            "replication_key": "updatedAt",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "bookmarks": []
        }
        src_config = {"endpoint_id": "test-endpoint"}

        with patch.object(self.connector, '_validate_replication_key_against_schema') as mock_validate:
            mock_validate.return_value = True

            await self.connector._setup_incremental_replication(config, state, src_config)

            # Should not modify config filters when no bookmarks
            assert config["filters"] == {}

    @pytest.mark.asyncio
    async def test_setup_incremental_replication_no_cursor(self):
        """Test setup of incremental replication with bookmark but no cursor."""
        config = {"filters": {}}
        state = {
            "replication_method": "incremental",
            "replication_key": "updatedAt",
            "cursor_mode": "inclusive", 
            "safety_window_seconds": 120,
            "bookmarks": [{"partition": {}}]  # No cursor field
        }
        src_config = {"endpoint_id": "test-endpoint"}

        with patch.object(self.connector, '_validate_replication_key_against_schema') as mock_validate:
            mock_validate.return_value = True

            await self.connector._setup_incremental_replication(config, state, src_config)

            # Should not modify config filters when no cursor
            assert config["filters"] == {}

    @pytest.mark.asyncio
    async def test_read_batches_with_incremental_replication(self):
        """Test read_batches method with incremental replication enabled."""
        config = {
            "endpoint": "/api/data",
            "method": "GET",
            "pipeline_config": {
                "src": {
                    "replication_method": "incremental",
                    "replication_key": "updatedAt",
                    "cursor_mode": "inclusive",
                    "safety_window_seconds": 120,
                    "bookmarks": [{"partition": {}, "cursor": "2025-08-14T11:58:03Z"}],
                    "endpoint_id": "test-endpoint"
                }
            }
        }

        # Mock the connector setup
        self.connector.base_url = "https://api.example.com"
        self.connector.is_connected = True

        with patch.object(self.connector, '_setup_incremental_replication') as mock_setup:
            with patch.object(self.connector, '_read_single_request') as mock_read:
                mock_setup.return_value = None
                mock_read.return_value = [{"id": 1, "name": "test"}]

                batches = []
                async for batch in self.connector.read_batches(config):
                    batches.append(batch)

                assert len(batches) == 1
                assert batches[0] == [{"id": 1, "name": "test"}]
                mock_setup.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_batches_with_full_replication(self):
        """Test read_batches method with full replication (no incremental)."""
        config = {
            "endpoint": "/api/data",
            "method": "GET",
            "pipeline_config": {
                "src": {
                    "replication_method": "full"
                }
            }
        }

        # Mock the connector setup
        self.connector.base_url = "https://api.example.com"
        self.connector.is_connected = True

        with patch.object(self.connector, '_setup_incremental_replication') as mock_setup:
            with patch.object(self.connector, '_read_single_request') as mock_read:
                mock_read.return_value = [{"id": 1, "name": "test"}]

                batches = []
                async for batch in self.connector.read_batches(config):
                    batches.append(batch)

                assert len(batches) == 1
                assert batches[0] == [{"id": 1, "name": "test"}]
                mock_setup.assert_not_called()  # Should not be called for full replication

    @pytest.mark.asyncio
    async def test_setup_uses_cursor_not_example_values(self):
        """Test that setup uses actual cursor values and NOT example values from schema."""
        config = {"filters": {}}
        state = {
            "replication_method": "incremental",
            "replication_key": "createdDateStart",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "bookmarks": [{"partition": {}, "cursor": "2025-08-14T11:58:03Z"}]  # Actual cursor
        }
        src_config = {"endpoint_id": "test-endpoint"}

        # Mock schema that has example values (like the real wise schema)
        schema_with_examples = {
            "createdDateStart": {
                "type": "string",
                "required": False,
                "operators": ["gte"],
                "description": "Starting date to filter transfers, inclusive of the provided date",
                "example": "2018-12-15T00:00:00.000Z"  # This should NOT be used
            }
        }

        with patch.object(self.connector, '_validate_replication_key_against_schema') as mock_validate:
            with patch.object(self.connector, '_get_endpoint_schema_filters') as mock_get_schema:
                mock_validate.return_value = True
                mock_get_schema.return_value = schema_with_examples

                await self.connector._setup_incremental_replication(config, state, src_config)

                # Verify the actual cursor value is used, NOT the example value
                assert "createdDateStart" in config["filters"]
                actual_value = config["filters"]["createdDateStart"]["value"]
                
                # Should be the effective start time (cursor - safety window), NOT the example
                assert actual_value == "2025-08-14T11:56:03Z"  # 11:58:03 - 120s = 11:56:03
                assert actual_value != "2018-12-15T00:00:00.000Z"  # NOT the example value

    def test_extract_value_ignores_examples(self):
        """Test that _extract_value_from_schema_filter ignores example values."""
        # Schema config with example but no explicit value
        filter_config_with_example = {
            "type": "string",
            "required": False,
            "example": "2018-12-15T00:00:00.000Z",
            "description": "Some filter"
        }
        
        # Should return None (no explicit value), NOT the example
        result = self.connector._extract_value_from_schema_filter(filter_config_with_example)
        assert result is None  # Should NOT use example value
        
        # Schema config with explicit value
        filter_config_with_value = {
            "type": "string",
            "required": False,
            "value": "2025-08-14T11:56:03Z",  # Explicit value
            "example": "2018-12-15T00:00:00.000Z",  # Should be ignored
            "description": "Some filter"
        }
        
        # Should return the explicit value, NOT the example
        result = self.connector._extract_value_from_schema_filter(filter_config_with_value)
        assert result == "2025-08-14T11:56:03Z"  # Explicit value
        assert result != "2018-12-15T00:00:00.000Z"  # NOT the example


class TestAPITieBreakerDeduplication:
    """Test suite for tie-breaker based deduplication functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.connector = APIConnector("test_api")

    def test_get_nested_field_value_simple(self):
        """Test getting nested field values with simple dot notation."""
        record = {
            "id": 123,
            "details": {
                "reference": "TXN-456",
                "amount": 100.50
            }
        }

        # Simple field
        assert self.connector._get_nested_field_value(record, "id") == 123
        
        # Nested field
        assert self.connector._get_nested_field_value(record, "details.reference") == "TXN-456"
        assert self.connector._get_nested_field_value(record, "details.amount") == 100.50

    def test_get_nested_field_value_missing_field(self):
        """Test getting nested field values when field is missing."""
        record = {"id": 123, "details": {}}

        # Missing simple field
        assert self.connector._get_nested_field_value(record, "missing") is None
        
        # Missing nested field
        assert self.connector._get_nested_field_value(record, "details.missing") is None
        
        # Missing parent field
        assert self.connector._get_nested_field_value(record, "missing.reference") is None

    def test_get_nested_field_value_invalid_input(self):
        """Test getting nested field values with invalid input."""
        # None record
        assert self.connector._get_nested_field_value(None, "id") is None
        
        # Non-dict record
        assert self.connector._get_nested_field_value("not-a-dict", "id") is None

    def test_compare_tie_breakers_single_field_equal(self):
        """Test tie-breaker comparison with single field that is equal."""
        record = {"id": 123, "created": "2025-08-14T12:00:00Z"}
        tie_breaker_fields = ["id"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "id", "value": "123", "inclusive": True}
                ]
            }
        }

        # In inclusive mode, equal values should be treated as duplicate
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is False  # Equal in inclusive mode = duplicate

        # In exclusive mode, equal values should be treated as new
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=False)
        assert result is True  # Equal in exclusive mode = new

    def test_compare_tie_breakers_single_field_greater(self):
        """Test tie-breaker comparison with single field that is greater."""
        record = {"id": 125, "created": "2025-08-14T12:00:00Z"}
        tie_breaker_fields = ["id"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "id", "value": "123", "inclusive": True}
                ]
            }
        }

        # Greater value should always be treated as new
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is True

        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=False)
        assert result is True

    def test_compare_tie_breakers_single_field_lesser(self):
        """Test tie-breaker comparison with single field that is lesser."""
        record = {"id": 120, "created": "2025-08-14T12:00:00Z"}
        tie_breaker_fields = ["id"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "id", "value": "123", "inclusive": True}
                ]
            }
        }

        # Lesser value should always be treated as duplicate
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is False

        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=False)
        assert result is False

    def test_compare_tie_breakers_multiple_fields(self):
        """Test tie-breaker comparison with multiple fields."""
        record = {
            "account_id": "ACC123",
            "transaction_id": "TXN456",
            "created": "2025-08-14T12:00:00Z"
        }
        tie_breaker_fields = ["account_id", "transaction_id"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "account_id", "value": "ACC123", "inclusive": True},
                    {"field": "transaction_id", "value": "TXN455", "inclusive": True}
                ]
            }
        }

        # Same account_id, but greater transaction_id = new record
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is True

    def test_compare_tie_breakers_multiple_fields_all_equal(self):
        """Test tie-breaker comparison with multiple fields all equal."""
        record = {
            "account_id": "ACC123",
            "transaction_id": "TXN456", 
            "created": "2025-08-14T12:00:00Z"
        }
        tie_breaker_fields = ["account_id", "transaction_id"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "account_id", "value": "ACC123", "inclusive": True},
                    {"field": "transaction_id", "value": "TXN456", "inclusive": True}
                ]
            }
        }

        # All tie-breakers equal in inclusive mode = duplicate
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is False

        # All tie-breakers equal in exclusive mode = new
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=False)
        assert result is True

    def test_compare_tie_breakers_numeric_string_comparison(self):
        """Test tie-breaker comparison with numeric strings."""
        record = {"id": 1000, "created": "2025-08-14T12:00:00Z"}
        tie_breaker_fields = ["id"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "id", "value": "999", "inclusive": True}
                ]
            }
        }

        # 1000 > 999 should be treated as new
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is True

    def test_compare_tie_breakers_nested_fields(self):
        """Test tie-breaker comparison with nested fields."""
        record = {
            "details": {"reference": "TXN-789"},
            "created": "2025-08-14T12:00:00Z"
        }
        tie_breaker_fields = ["details.reference"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "details.reference", "value": "TXN-788", "inclusive": True}
                ]
            }
        }

        # String comparison: "TXN-789" > "TXN-788"
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is True

    def test_compare_tie_breakers_missing_record_value(self):
        """Test tie-breaker comparison when record is missing tie-breaker value."""
        record = {"created": "2025-08-14T12:00:00Z"}  # Missing 'id' field
        tie_breaker_fields = ["id"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "id", "value": "123", "inclusive": True}
                ]
            }
        }

        # Missing record value should be treated as new
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is True

    def test_compare_tie_breakers_no_stored_tiebreakers(self):
        """Test tie-breaker comparison when no stored tie-breakers exist."""
        record = {"id": 123, "created": "2025-08-14T12:00:00Z"}
        tie_breaker_fields = ["id"]
        bookmark = {"cursor": "2025-08-14T12:00:00Z"}  # No aux data

        # No stored tie-breakers should be treated as new
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is True

    def test_compare_tie_breakers_backward_compatibility(self):
        """Test tie-breaker comparison with single tie-breaker (backward compatibility)."""
        record = {"id": 125, "created": "2025-08-14T12:00:00Z"}
        tie_breaker_fields = ["id"]
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreaker_field": "id",
                "tiebreaker_value": "123"
            }
        }

        # Greater value should be new in both modes
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=True)
        assert result is True

        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark, inclusive=False)
        assert result is True

    def test_is_record_new_newer_cursor(self):
        """Test record newness check with newer cursor value."""
        record = {"id": 123, "created": "2025-08-14T12:30:00Z"}
        bookmark = {"cursor": "2025-08-14T12:00:00Z"}
        cursor_field = "created"
        tie_breaker_fields = ["id"]

        result = self.connector._is_record_new(record, bookmark, cursor_field, tie_breaker_fields, "inclusive")
        assert result is True

    def test_is_record_new_older_cursor(self):
        """Test record newness check with older cursor value."""
        record = {"id": 123, "created": "2025-08-14T11:30:00Z"}
        bookmark = {"cursor": "2025-08-14T12:00:00Z"}
        cursor_field = "created"
        tie_breaker_fields = ["id"]

        result = self.connector._is_record_new(record, bookmark, cursor_field, tie_breaker_fields, "inclusive")
        assert result is False

    def test_is_record_new_same_cursor_with_tie_breaker(self):
        """Test record newness check with same cursor but different tie-breaker."""
        record = {"id": 125, "created": "2025-08-14T12:00:00Z"}
        bookmark = {
            "cursor": "2025-08-14T12:00:00Z",
            "aux": {
                "tiebreakers": [
                    {"field": "id", "value": "123", "inclusive": True}
                ]
            }
        }
        cursor_field = "created"
        tie_breaker_fields = ["id"]

        result = self.connector._is_record_new(record, bookmark, cursor_field, tie_breaker_fields, "inclusive")
        assert result is True  # Greater tie-breaker value = new

    def test_is_record_new_missing_cursor_field(self):
        """Test record newness check when record is missing cursor field."""
        record = {"id": 123}  # Missing 'created' field
        bookmark = {"cursor": "2025-08-14T12:00:00Z"}
        cursor_field = "created"
        tie_breaker_fields = ["id"]

        result = self.connector._is_record_new(record, bookmark, cursor_field, tie_breaker_fields, "inclusive")
        assert result is True  # Missing cursor field = treat as new

    def test_deduplicate_records_empty_batch(self):
        """Test deduplication with empty batch."""
        batch = []
        state = {"bookmarks": [{"cursor": "2025-08-14T12:00:00Z"}]}
        config = {"tie_breaker_fields": ["id"]}

        result = self.connector._deduplicate_records(batch, state, config)
        assert result == []

    def test_deduplicate_records_no_bookmarks(self):
        """Test deduplication with no stored bookmarks."""
        batch = [{"id": 123, "created": "2025-08-14T12:00:00Z"}]
        state = {"bookmarks": []}
        config = {"tie_breaker_fields": ["id"]}

        result = self.connector._deduplicate_records(batch, state, config)
        assert result == batch  # All records should be kept

    def test_deduplicate_records_mixed_batch(self):
        """Test deduplication with mixed new and duplicate records."""
        batch = [
            {"id": 120, "created": "2025-08-14T12:00:00Z"},  # Older, duplicate
            {"id": 125, "created": "2025-08-14T12:00:00Z"},  # Same cursor, but newer tie-breaker
            {"id": 130, "created": "2025-08-14T12:30:00Z"},  # Newer cursor
        ]
        state = {
            "bookmarks": [{"cursor": "2025-08-14T12:00:00Z"}],
            "cursor_field": "created"
        }
        config = {
            "tie_breaker_fields": ["id"]
        }

        with patch.object(self.connector, '_is_record_new') as mock_is_new:
            mock_is_new.side_effect = [False, True, True]  # First is duplicate, rest are new

            result = self.connector._deduplicate_records(batch, state, config)

            assert len(result) == 2
            assert result[0]["id"] == 125
            assert result[1]["id"] == 130

    def test_deduplicate_records_all_duplicates(self):
        """Test deduplication when all records are duplicates."""
        batch = [
            {"id": 120, "created": "2025-08-14T11:30:00Z"},
            {"id": 121, "created": "2025-08-14T11:45:00Z"},
        ]
        state = {
            "bookmarks": [{"cursor": "2025-08-14T12:00:00Z"}],
            "cursor_field": "created"
        }
        config = {
            "tie_breaker_fields": ["id"]
        }

        with patch.object(self.connector, '_is_record_new') as mock_is_new:
            mock_is_new.return_value = False  # All are duplicates

            result = self.connector._deduplicate_records(batch, state, config)
            assert result == []