"""Unit tests for PipelineMetrics Pydantic model."""

from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from analitiq_stream.models.metrics import PipelineMetrics


class TestPipelineMetrics:
    """Test suite for PipelineMetrics model."""

    def test_default_initialization(self):
        """Test default initialization of metrics."""
        metrics = PipelineMetrics()
        
        # Check default values
        assert metrics.records_processed == 0
        assert metrics.records_failed == 0
        assert metrics.batches_processed == 0
        assert metrics.batches_failed == 0
        assert metrics.streams_processed == 0
        assert metrics.streams_failed == 0
        
        # Check timestamps are set
        assert isinstance(metrics.pipeline_start_time, datetime)
        assert isinstance(metrics.last_update_time, datetime)
        
        # Check computed fields
        assert metrics.records_success_rate == 0.0
        assert metrics.batches_success_rate == 0.0
        assert metrics.streams_success_rate == 0.0
        assert metrics.total_records == 0
        assert metrics.total_batches == 0
        assert metrics.total_streams == 0
        assert not metrics.has_failures
        assert metrics.records_per_second == 0.0

    def test_increment_methods(self):
        """Test increment methods update metrics correctly."""
        metrics = PipelineMetrics()
        
        # Test record increments
        metrics.increment_records_processed(10)
        assert metrics.records_processed == 10
        
        metrics.increment_records_failed(2)
        assert metrics.records_failed == 2
        
        # Test batch increments
        metrics.increment_batches_processed(3)
        assert metrics.batches_processed == 3
        
        metrics.increment_batches_failed(1)
        assert metrics.batches_failed == 1
        
        # Test stream increments
        metrics.increment_streams_processed(2)
        assert metrics.streams_processed == 2
        
        metrics.increment_streams_failed(1)
        assert metrics.streams_failed == 1

    def test_computed_success_rates(self):
        """Test computed success rate calculations."""
        metrics = PipelineMetrics()
        
        # Add some metrics
        metrics.increment_records_processed(80)
        metrics.increment_records_failed(20)  # 80% success rate
        
        metrics.increment_batches_processed(9)
        metrics.increment_batches_failed(1)   # 90% success rate
        
        metrics.increment_streams_processed(7)
        metrics.increment_streams_failed(3)   # 70% success rate
        
        # Check computed rates
        assert metrics.records_success_rate == 80.0
        assert metrics.batches_success_rate == 90.0
        assert metrics.streams_success_rate == 70.0
        
        # Check totals
        assert metrics.total_records == 100
        assert metrics.total_batches == 10
        assert metrics.total_streams == 10
        
        # Has failures
        assert metrics.has_failures

    def test_validation_negative_values(self):
        """Test validation prevents negative values."""
        # Direct assignment should fail
        with pytest.raises(ValidationError):
            PipelineMetrics(records_processed=-1)
        
        # Increment with negative values should fail
        metrics = PipelineMetrics()
        with pytest.raises(ValueError):
            metrics.increment_records_processed(-1)
        
        with pytest.raises(ValueError):
            metrics.increment_batches_failed(-5)

    def test_timestamp_updates(self):
        """Test timestamp updates on metric changes."""
        metrics = PipelineMetrics()
        original_time = metrics.last_update_time
        
        # Small delay to ensure timestamp difference
        import time
        time.sleep(0.01)
        
        metrics.increment_records_processed(1)
        assert metrics.last_update_time > original_time

    def test_reset_functionality(self):
        """Test metrics reset to initial state."""
        metrics = PipelineMetrics()
        
        # Add some metrics
        metrics.increment_records_processed(100)
        metrics.increment_batches_failed(5)
        metrics.increment_streams_processed(3)
        
        # Verify metrics are not zero
        assert metrics.total_records > 0
        assert metrics.total_batches > 0
        assert metrics.total_streams > 0
        
        # Reset metrics
        metrics.reset()
        
        # Verify all metrics are back to zero
        assert metrics.records_processed == 0
        assert metrics.records_failed == 0
        assert metrics.batches_processed == 0
        assert metrics.batches_failed == 0
        assert metrics.streams_processed == 0
        assert metrics.streams_failed == 0
        assert metrics.total_records == 0
        assert metrics.total_batches == 0
        assert metrics.total_streams == 0
        assert not metrics.has_failures

    def test_execution_duration_calculation(self):
        """Test execution duration calculation."""
        metrics = PipelineMetrics()
        
        # Duration should be very small initially
        assert metrics.execution_duration_seconds >= 0
        
        # After some time, duration should increase
        import time
        time.sleep(0.01)
        metrics.increment_records_processed(1)  # Updates timestamp
        
        assert metrics.execution_duration_seconds > 0

    def test_records_per_second_calculation(self):
        """Test records per second calculation."""
        metrics = PipelineMetrics()
        
        # Initially should be 0
        assert metrics.records_per_second == 0.0
        
        # Add records and wait a bit
        import time
        time.sleep(0.01)
        metrics.increment_records_processed(10)
        
        # Should now have a rate
        assert metrics.records_per_second > 0

    def test_model_serialization(self):
        """Test Pydantic model serialization."""
        metrics = PipelineMetrics()
        metrics.increment_records_processed(42)
        metrics.increment_batches_processed(3)
        
        # Test dict conversion
        metrics_dict = metrics.model_dump()
        assert metrics_dict['records_processed'] == 42
        assert metrics_dict['batches_processed'] == 3
        assert 'records_success_rate' in metrics_dict  # Computed field included
        
        # Test JSON serialization
        metrics_json = metrics.model_dump_json()
        assert isinstance(metrics_json, str)
        assert '42' in metrics_json

    def test_extra_fields_forbidden(self):
        """Test that extra fields are forbidden."""
        with pytest.raises(ValidationError):
            PipelineMetrics(extra_field="not allowed")

    def test_field_descriptions(self):
        """Test that model has proper field descriptions."""
        schema = PipelineMetrics.model_json_schema()
        
        # Check that descriptions exist for key fields
        props = schema['properties']
        assert 'description' in props['records_processed']
        assert 'description' in props['batches_processed']
        assert 'description' in props['streams_processed']
        assert 'description' in props['pipeline_start_time']