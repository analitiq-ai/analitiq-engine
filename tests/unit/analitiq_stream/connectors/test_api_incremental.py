"""
Unit tests for incremental replication functionality in APIConnector.
"""

import json
import pytest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock, AsyncMock
from src.source.connectors.api import APIConnector, ReadError
from src.state.state_manager import StateManager


class TestAPIIncrementalReplication:
    """Test suite for incremental replication functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.connector = APIConnector("test_api")

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

        # Mock dateutil to raise ValueError to trigger numeric parsing
        with patch('dateutil.parser.isoparse', side_effect=ValueError("Invalid date")):
            effective_start = self.connector._compute_effective_start_time(cursor, safety_window)

        assert effective_start == "950"  # 1000 - 50

    def test_compute_effective_start_time_numeric_id_minimum(self):
        """Test computing effective start time for numeric ID with minimum boundary."""
        cursor = "30"
        safety_window = 50

        # Mock dateutil to raise ValueError to trigger numeric parsing
        with patch('dateutil.parser.isoparse', side_effect=ValueError("Invalid date")):
            effective_start = self.connector._compute_effective_start_time(cursor, safety_window)

        assert effective_start == "0"  # max(0, 30 - 50)

    def test_compute_effective_start_time_invalid_cursor(self):
        """Test computing effective start time for invalid cursor format."""
        cursor = "invalid-cursor-format"
        safety_window = 120

        effective_start = self.connector._compute_effective_start_time(cursor, safety_window)

        assert effective_start == cursor  # Return original if parsing fails

    def test_build_replication_filter_inclusive(self):
        """Test building replication filter (always inclusive mode)."""
        filter_param = "updatedAt"
        effective_start = "2025-08-14T11:56:03Z"

        filter_params = self.connector._build_replication_filter(filter_param, effective_start)

        assert filter_params == {"updatedAt": "2025-08-14T11:56:03Z"}

    def test_build_replication_filter_numeric(self):
        """Test building replication filter with numeric cursor."""
        filter_param = "id"
        effective_start = "950"

        filter_params = self.connector._build_replication_filter(filter_param, effective_start)

        assert filter_params == {"id": "950"}

    def test_get_filter_param_for_cursor_field(self):
        """Test getting filter parameter name for cursor field."""
        # Test with replication filter mapping
        src_config = {
            "replication_filter_mapping": {"updated_at": "updatedAt"},
            "filters": {"updatedAt": {"type": "string"}}
        }

        result = self.connector._get_filter_param_for_cursor_field("updated_at", src_config)
        assert result == "updatedAt"

        # Test with direct filter match
        src_config = {"filters": {"created_at": {"type": "string"}}}
        result = self.connector._get_filter_param_for_cursor_field("created_at", src_config)
        assert result == "created_at"

        # Test with no mapping - should return None
        src_config = {}
        result = self.connector._get_filter_param_for_cursor_field("missing_field", src_config)
        assert result is None

    def test_extract_value_from_schema_filter(self):
        """Test extracting values from schema filter configurations."""
        # Test with explicit value
        filter_config_with_value = {
            "type": "string",
            "value": "2025-08-14T11:56:03Z",
            "example": "2018-12-15T00:00:00.000Z"  # Should be ignored
        }

        result = self.connector._extract_value_from_schema_filter(filter_config_with_value)
        assert result == "2025-08-14T11:56:03Z"

        # Test with no explicit value (should return None, not example)
        filter_config_with_example = {
            "type": "string",
            "example": "2018-12-15T00:00:00.000Z"
        }

        result = self.connector._extract_value_from_schema_filter(filter_config_with_example)
        assert result is None

    @pytest.mark.asyncio
    async def test_setup_incremental_replication_success(self):
        """Test successful setup of incremental replication."""
        config = {"filters": {}}
        state = {
            "replication_method": "incremental",
            "replication_key": "updatedAt",
            "cursor_field": "updatedAt",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "bookmarks": [{"partition": {}, "cursor": "2025-08-14T11:58:03Z"}]
        }
        src_config = {"endpoint_id": "test-endpoint"}

        with patch.object(self.connector, '_get_filter_param_for_cursor_field') as mock_get_param:
            with patch.object(self.connector, '_compute_effective_start_time') as mock_compute:
                mock_get_param.return_value = "updatedAt"
                mock_compute.return_value = "2025-08-14T11:56:03Z"

                await self.connector._setup_incremental_replication(config, state, src_config)

                assert "updatedAt" in config["filters"]
                assert config["filters"]["updatedAt"]["value"] == "2025-08-14T11:56:03Z"
                assert config["filters"]["updatedAt"]["required"] is True

    @pytest.mark.asyncio
    async def test_setup_incremental_replication_no_bookmarks(self):
        """Test setup of incremental replication with no bookmarks."""
        config = {"filters": {}}
        state = {
            "replication_method": "incremental",
            "replication_key": "updatedAt",
            "cursor_field": "updatedAt",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "bookmarks": []
        }
        src_config = {"endpoint_id": "test-endpoint"}

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
            "cursor_field": "updatedAt",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "bookmarks": [{"partition": {}}]  # No cursor field
        }
        src_config = {"endpoint_id": "test-endpoint"}

        await self.connector._setup_incremental_replication(config, state, src_config)

        # Should not modify config filters when no cursor
        assert config["filters"] == {}

    def test_load_state_from_manager(self, tmp_path):
        """Test loading state from state manager."""
        state_manager = StateManager(pipeline_id="test-pipeline", base_dir=str(tmp_path))
        state_manager.start_run({}, run_id="test-run")

        config = {
            "replication_method": "incremental",
            "cursor_field": "updated_at",
            "safety_window_seconds": 300,
        }

        state = self.connector._load_state_from_state_manager(
            state_manager, "test_stream", {}, config
        )

        assert state["replication_method"] == "incremental"
        assert state["cursor_field"] == "updated_at"
        assert state["safety_window_seconds"] == 300
        assert state["bookmarks"] == []
        assert state["run"] == {"run_id": "test-run"}


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

        # In inclusive mode (always used now), equal values should be treated as duplicate
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
        assert result is False  # Equal in inclusive mode = duplicate

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

        # Greater value should be treated as new
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
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

        # Lesser value should be treated as duplicate
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
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
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
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

        # All tie-breakers equal in inclusive mode (always used now) = duplicate
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
        assert result is False

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
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
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
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
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
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
        assert result is True

    def test_compare_tie_breakers_no_stored_tiebreakers(self):
        """Test tie-breaker comparison when no stored tie-breakers exist."""
        record = {"id": 123, "created": "2025-08-14T12:00:00Z"}
        tie_breaker_fields = ["id"]
        bookmark = {"cursor": "2025-08-14T12:00:00Z"}  # No aux data

        # No stored tie-breakers in inclusive mode (always used now) should be treated as duplicate
        result = self.connector._compare_tie_breakers(record, tie_breaker_fields, bookmark)
        assert result is False

    def test_is_record_new_newer_cursor(self):
        """Test record newness check with newer cursor value."""
        record = {"id": 123, "created": "2025-08-14T12:30:00Z"}
        bookmark = {"cursor": "2025-08-14T12:00:00Z"}
        cursor_field = "created"
        tie_breaker_fields = ["id"]

        result = self.connector._is_record_new(record, bookmark, cursor_field, tie_breaker_fields)
        assert result is True

    def test_is_record_new_older_cursor(self):
        """Test record newness check with older cursor value."""
        record = {"id": 123, "created": "2025-08-14T11:30:00Z"}
        bookmark = {"cursor": "2025-08-14T12:00:00Z"}
        cursor_field = "created"
        tie_breaker_fields = ["id"]

        result = self.connector._is_record_new(record, bookmark, cursor_field, tie_breaker_fields)
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

        result = self.connector._is_record_new(record, bookmark, cursor_field, tie_breaker_fields)
        assert result is True  # Greater tie-breaker value = new

    def test_is_record_new_missing_cursor_field(self):
        """Test record newness check when record is missing cursor field."""
        record = {"id": 123}  # Missing 'created' field
        bookmark = {"cursor": "2025-08-14T12:00:00Z"}
        cursor_field = "created"
        tie_breaker_fields = ["id"]

        result = self.connector._is_record_new(record, bookmark, cursor_field, tie_breaker_fields)
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