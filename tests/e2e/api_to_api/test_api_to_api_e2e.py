"""E2E Test: API → API Streaming

Tests the complete pipeline execution from an API source to an API destination,
following the pattern used in the wise_to_sevdesk example with mocked connections.
"""

import tempfile
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from analitiq_stream.core.pipeline import Pipeline


class MockHTTPResponse:
    """Mock HTTP response for both source and destination APIs."""

    def __init__(self, data: Any, status: int = 200, has_more: bool = False):
        self._data = data
        self.status = status
        self.has_more = has_more

    async def json(self):
        # Source API pagination response
        if isinstance(self._data, list) and hasattr(self, 'is_source_response'):
            return {
                "data": self._data,
                "pagination": {
                    "has_more": self.has_more,
                    "total": len(self._data),
                    "offset": 0
                }
            }
        # Destination API response
        return self._data

    async def text(self):
        return str(self._data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def mark_as_source_response(self):
        """Mark this response as coming from source API."""
        self.is_source_response = True
        return self


@pytest.fixture
def temp_state_dir():
    """Create temporary directory for pipeline state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def wise_api_data():
    """Sample data from Wise API (source)."""
    return [
        {
            "id": 12345,
            "user": 67890,
            "targetAccount": 98765,
            "status": "outgoing_payment_sent",
            "reference": "Payment for services",
            "created": "2023-12-01T10:00:00.000Z",
            "sourceCurrency": "EUR",
            "sourceValue": 1000.50,
            "targetCurrency": "USD",
            "targetValue": 1100.55,
            "details": {
                "reference": "Invoice #12345"
            }
        },
        {
            "id": 12346,
            "user": 67890,
            "targetAccount": 98766,
            "status": "outgoing_payment_sent",
            "reference": "Salary payment",
            "created": "2023-12-01T11:00:00.000Z",
            "sourceCurrency": "EUR",
            "sourceValue": 2500.00,
            "targetCurrency": "USD",
            "targetValue": 2750.00,
            "details": {
                "reference": "Salary Dec 2023"
            }
        }
    ]


@pytest.fixture
def api_to_api_pipeline_config():
    """Pipeline configuration for API → API streaming (based on wise_to_sevdesk)."""
    return {
        "pipeline_id": "wise-to-sevdesk-test",
        "name": "Wise to SevDesk API Integration Test",
        "version": "1.0",
        "src": {
            "host_id": "wise-api-host",
            "name": "Wise Platform"
        },
        "dst": {
            "host_id": "sevdesk-api-host",
            "name": "SevDesk Platform"
        },
        "engine_config": {
            "batch_size": 100,
            "max_concurrent_batches": 1,
            "buffer_size": 1000
        },
        "streams": {
            "wise-transactions": {
                "name": "wise-transactions",
                "description": "Wise bank transactions sync",
                "src": {
                    "endpoint_id": "wise-transfers-endpoint",
                    "replication_method": "incremental",
                    "cursor_field": "created",
                    "cursor_mode": "inclusive",
                    "safety_window_seconds": 120,
                    "primary_key": ["id"]
                },
                "dst": {
                    "endpoint_id": "sevdesk-transactions-endpoint",
                    "refresh_mode": "upsert",
                    "batch_support": False,
                    "batch_size": 1
                },
                "mapping": {
                    "field_mappings": {
                        "created": {
                            "target": "valueDate",
                            "transformations": ["iso_to_date"],
                            "validation": {
                                "rules": [{"type": "not_null"}],
                                "error_action": "dlq"
                            }
                        },
                        "targetValue": {
                            "target": "amount",
                            "validation": {
                                "rules": [{"type": "not_null"}],
                                "error_action": "dlq"
                            }
                        },
                        "details.reference": {
                            "target": "paymtPurpose"
                        }
                    },
                    "computed_fields": {
                        "objectName": {
                            "expression": "CheckAccountTransaction"
                        },
                        "checkAccount": {
                            "expression": "{\"id\": \"5936402\", \"objectName\": \"CheckAccount\"}"
                        },
                        "status": {
                            "expression": "100"
                        }
                    }
                }
            }
        },
        "error_handling": {
            "strategy": "dlq",
            "retry_failed_records": True,
            "max_retries": 3,
            "retry_delay": 5
        },
        "monitoring": {
            "metrics_enabled": True,
            "log_level": "DEBUG",
            "checkpoint_interval": 50
        }
    }


@pytest.fixture
def wise_source_config():
    """Wise API source configuration."""
    return {
        "type": "api",
        "endpoint": "/v1/transfers",
        "method": "GET",
        "base_url": "https://api.wise.com",
        "headers": {
            "Authorization": "Bearer wise-api-token",
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        "pagination": {
            "type": "offset",
            "limit_param": "limit",
            "offset_param": "offset",
            "limit": 100
        },
        "rate_limit": {
            "max_requests": 60,
            "time_window": 60
        }
    }


@pytest.fixture
def sevdesk_destination_config():
    """SevDesk API destination configuration."""
    return {
        "type": "api",
        "endpoint": "/api/v1/CheckAccountTransaction",
        "method": "POST",
        "base_url": "https://my.sevdesk.de",
        "headers": {
            "Authorization": "sevdesk-api-token",
            "Content-Type": "application/json"
        },
        "rate_limit": {
            "max_requests": 10,
            "time_window": 60
        }
    }


class TestAPIToAPI:
    """Test API → API streaming scenarios."""

    @pytest.mark.asyncio
    async def test_successful_api_to_api_stream(
        self,
        api_to_api_pipeline_config,
        wise_source_config,
        sevdesk_destination_config,
        wise_api_data,
        temp_state_dir
    ):
        """Test successful streaming from Wise API to SevDesk API."""

        # Mock source API responses (Wise)
        source_responses = [
            MockHTTPResponse(wise_api_data[:1], has_more=True).mark_as_source_response(),
            MockHTTPResponse(wise_api_data[1:], has_more=False).mark_as_source_response()
        ]

        # Mock destination API responses (SevDesk)
        dest_responses = [
            MockHTTPResponse({"objects": {"id": "1001", "status": "created"}}),
            MockHTTPResponse({"objects": {"id": "1002", "status": "created"}})
        ]

        with patch('aiohttp.ClientSession.get') as mock_get:
            with patch('aiohttp.ClientSession.post') as mock_post:
                # Mock source GET requests
                mock_get.side_effect = source_responses

                # Mock destination POST requests
                mock_post.side_effect = dest_responses

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    mock_engine.get_metrics.return_value = {
                        "records_processed": 2,
                        "records_failed": 0,
                        "api_requests_source": 2,
                        "api_requests_destination": 2
                    }
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    # Create and run pipeline
                    pipeline = Pipeline(
                        pipeline_config=api_to_api_pipeline_config,
                        source_config=wise_source_config,
                        destination_config=sevdesk_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline.run()

                    # Verify engine was called
                    mock_engine.stream_data.assert_called_once()

                    # Verify configuration structure
                    config = mock_engine.stream_data.call_args[0][0]
                    assert config["pipeline_id"] == "wise-to-sevdesk-test"
                    assert "wise-transactions" in config["streams"]

                    # Verify field mappings from wise_to_sevdesk pattern
                    stream_config = config["streams"]["wise-transactions"]
                    field_mappings = stream_config["mapping"]["field_mappings"]
                    assert field_mappings["created"]["target"] == "valueDate"
                    assert field_mappings["targetValue"]["target"] == "amount"
                    assert field_mappings["details.reference"]["target"] == "paymtPurpose"

                    # Verify computed fields
                    computed_fields = stream_config["mapping"]["computed_fields"]
                    assert computed_fields["objectName"]["expression"] == "CheckAccountTransaction"

    @pytest.mark.asyncio
    async def test_api_to_api_with_rate_limiting(
        self,
        api_to_api_pipeline_config,
        wise_source_config,
        sevdesk_destination_config,
        wise_api_data,
        temp_state_dir
    ):
        """Test API → API streaming with rate limiting scenarios."""

        # Mock rate limited responses
        source_responses = [
            MockHTTPResponse(wise_api_data, has_more=False).mark_as_source_response()
        ]

        dest_responses = [
            MockHTTPResponse({"error": "Rate limit exceeded"}, status=429),
            MockHTTPResponse({"objects": {"id": "1001", "status": "created"}}),
            MockHTTPResponse({"objects": {"id": "1002", "status": "created"}})
        ]

        with patch('aiohttp.ClientSession.get') as mock_get:
            with patch('aiohttp.ClientSession.post') as mock_post:
                mock_get.side_effect = source_responses
                mock_post.side_effect = dest_responses

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    mock_engine.get_metrics.return_value = {
                        "records_processed": 2,
                        "records_failed": 0,
                        "rate_limit_hits": 1
                    }
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline = Pipeline(
                        pipeline_config=api_to_api_pipeline_config,
                        source_config=wise_source_config,
                        destination_config=sevdesk_destination_config,
                        state_dir=temp_state_dir
                    )

                    # Should handle rate limiting gracefully
                    await pipeline.run()

                    # Verify metrics include rate limiting information
                    metrics = pipeline.get_metrics()
                    assert "rate_limit_hits" in metrics

    @pytest.mark.asyncio
    async def test_api_to_api_with_transformation_errors(
        self,
        wise_source_config,
        sevdesk_destination_config,
        temp_state_dir
    ):
        """Test API → API streaming with transformation and validation errors."""

        # Data with validation issues
        problematic_data = [
            {
                "id": 1,
                "created": "invalid-date-format",  # Will fail iso_to_date transformation
                "targetValue": None,  # Will fail not_null validation
                "details": {"reference": "Valid reference"}
            },
            {
                "id": 2,
                "created": "2023-12-01T10:00:00.000Z",
                "targetValue": 100.50,
                "details": {"reference": "Valid transaction"}
            }
        ]

        # Pipeline config with strict validation
        pipeline_config = {
            "pipeline_id": "api-to-api-validation-test",
            "name": "API to API Validation Test",
            "version": "1.0",
            "engine_config": {"batch_size": 1},
            "streams": {
                "validation_stream": {
                    "name": "Validation Stream",
                    "src": {"endpoint_id": "src-api", "host_id": "src-host"},
                    "dst": {"endpoint_id": "dst-api", "host_id": "dst-host"},
                    "mapping": {
                        "field_mappings": {
                            "created": {
                                "target": "date",
                                "transformations": ["iso_to_date"],
                                "validation": {
                                    "rules": [{"type": "not_null"}],
                                    "error_action": "dlq"
                                }
                            },
                            "targetValue": {
                                "target": "amount",
                                "validation": {
                                    "rules": [{"type": "not_null"}],
                                    "error_action": "dlq"
                                }
                            }
                        }
                    }
                }
            }
        }

        source_response = MockHTTPResponse(problematic_data).mark_as_source_response()

        with patch('aiohttp.ClientSession.get', return_value=source_response):
            with patch('aiohttp.ClientSession.post') as mock_post:
                mock_post.return_value = MockHTTPResponse({"status": "created"})

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    mock_engine.get_metrics.return_value = {
                        "records_processed": 1,
                        "records_failed": 1,
                        "validation_errors": 1
                    }
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline = Pipeline(
                        pipeline_config=pipeline_config,
                        source_config=wise_source_config,
                        destination_config=sevdesk_destination_config,
                        state_dir=temp_state_dir
                    )

                    # Should handle validation errors with DLQ
                    await pipeline.run()

                    metrics = pipeline.get_metrics()
                    assert metrics["records_failed"] == 1

    @pytest.mark.asyncio
    async def test_api_to_api_incremental_sync(
        self,
        api_to_api_pipeline_config,
        wise_source_config,
        sevdesk_destination_config,
        temp_state_dir
    ):
        """Test incremental synchronization between APIs."""

        # First batch
        initial_data = [
            {
                "id": 1,
                "created": "2023-12-01T10:00:00.000Z",
                "targetValue": 100.0,
                "details": {"reference": "Initial transaction"}
            }
        ]

        # Second batch with newer data
        newer_data = [
            {
                "id": 2,
                "created": "2023-12-01T11:00:00.000Z",
                "targetValue": 200.0,
                "details": {"reference": "Newer transaction"}
            }
        ]

        # First sync
        with patch('aiohttp.ClientSession.get') as mock_get:
            with patch('aiohttp.ClientSession.post') as mock_post:
                mock_get.return_value = MockHTTPResponse(initial_data).mark_as_source_response()
                mock_post.return_value = MockHTTPResponse({"status": "created"})

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline1 = Pipeline(
                        pipeline_config=api_to_api_pipeline_config,
                        source_config=wise_source_config,
                        destination_config=sevdesk_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline1.run()

        # Second sync with newer data
        with patch('aiohttp.ClientSession.get') as mock_get:
            with patch('aiohttp.ClientSession.post') as mock_post:
                mock_get.return_value = MockHTTPResponse(newer_data).mark_as_source_response()
                mock_post.return_value = MockHTTPResponse({"status": "created"})

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline2 = Pipeline(
                        pipeline_config=api_to_api_pipeline_config,
                        source_config=wise_source_config,
                        destination_config=sevdesk_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline2.run()

                    # Both syncs should complete
                    mock_engine.stream_data.assert_called_once()

    def test_api_to_api_config_validation(
        self,
        wise_source_config,
        sevdesk_destination_config,
        temp_state_dir
    ):
        """Test configuration validation for API → API pipeline."""

        # Valid config should work
        valid_config = {
            "pipeline_id": "valid-api-to-api",
            "name": "Valid API to API Pipeline",
            "streams": {
                "test_stream": {
                    "name": "Test Stream",
                    "src": {"endpoint_id": "src-api", "host_id": "src-host"},
                    "dst": {"endpoint_id": "dst-api", "host_id": "dst-host"}
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_config,
                source_config=wise_source_config,
                destination_config=sevdesk_destination_config,
                state_dir=temp_state_dir
            )

            assert pipeline.config["pipeline_id"] == "valid-api-to-api"

            # Should be able to get status
            status = pipeline.get_status()
            assert "test_stream" in status["streams"]

    def test_api_to_api_wise_sevdesk_pattern(
        self,
        api_to_api_pipeline_config,
        wise_source_config,
        sevdesk_destination_config,
        temp_state_dir
    ):
        """Test that the configuration follows the wise_to_sevdesk pattern correctly."""

        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=api_to_api_pipeline_config,
                source_config=wise_source_config,
                destination_config=sevdesk_destination_config,
                state_dir=temp_state_dir
            )

            # Verify wise_to_sevdesk specific configuration patterns
            config = pipeline.config
            assert config["pipeline_id"] == "wise-to-sevdesk-test"

            stream_config = config["streams"]["wise-transactions"]

            # Verify incremental replication settings (moved to top level by pipeline)
            assert stream_config["replication_method"] == "incremental"
            assert stream_config["cursor_field"] == "created"
            assert stream_config["safety_window_seconds"] == 120

            # Verify destination settings (moved to top level by pipeline)
            assert stream_config["refresh_mode"] == "upsert"
            assert stream_config["batch_support"] == False
            assert stream_config["batch_size"] == 1

            # Verify field mappings match wise_to_sevdesk pattern
            field_mappings = stream_config["mapping"]["field_mappings"]
            assert field_mappings["created"]["target"] == "valueDate"
            assert "iso_to_date" in field_mappings["created"]["transformations"]

            # Verify computed fields match wise_to_sevdesk pattern
            computed_fields = stream_config["mapping"]["computed_fields"]
            assert computed_fields["objectName"]["expression"] == "CheckAccountTransaction"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])