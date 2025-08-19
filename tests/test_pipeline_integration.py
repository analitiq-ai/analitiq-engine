"""End-to-end pipeline integration tests."""

import asyncio
import json
import tempfile
from unittest.mock import AsyncMock, Mock, patch
import pytest

from analitiq_stream.core.engine import StreamingEngine
from analitiq_stream.core.exceptions import ConfigurationError


class TestPipelineIntegration:
    """End-to-end tests for complete pipeline functionality."""

    @pytest.fixture
    def mock_wise_api_connector(self):
        """Mock Wise API connector that returns sample data."""
        connector = AsyncMock()
        connector.configure = AsyncMock()
        connector.health_check = AsyncMock(return_value=True)
        
        # Mock data similar to Wise API response
        sample_data = [
            {
                "id": 123456,
                "created": "2025-08-16T10:30:00Z",
                "targetValue": 100.50,
                "targetCurrency": "EUR",
                "status": "outgoing_payment_sent",
                "details": {"reference": "Payment 1"}
            },
            {
                "id": 789012, 
                "created": "2025-08-16T11:00:00Z",
                "targetValue": 250.75,
                "targetCurrency": "USD", 
                "status": "outgoing_payment_sent",
                "details": {"reference": "Payment 2"}
            }
        ]
        
        async def mock_read_batches(*args, **kwargs):
            yield sample_data
        
        connector.read_batches = mock_read_batches
        connector.close = AsyncMock()
        return connector

    @pytest.fixture
    def mock_sevdesk_api_connector(self):
        """Mock SevDesk API connector."""
        connector = AsyncMock()
        connector.configure = AsyncMock()
        connector.health_check = AsyncMock(return_value=True)
        connector.write_batch = AsyncMock()
        connector.close = AsyncMock()
        return connector

    @pytest.mark.asyncio
    async def test_complete_wise_to_sevdesk_pipeline(self, temp_dir, mock_wise_api_connector, 
                                                   mock_sevdesk_api_connector):
        """Test complete end-to-end Wise to SevDesk pipeline."""
        
        # Create engine
        engine = StreamingEngine(
            pipeline_id="wise-to-sevdesk-test",
            batch_size=10,
            dlq_path=temp_dir
        )
        
        # Pipeline configuration
        config = {
            "pipeline_id": "wise-to-sevdesk-test",
            "name": "Wise to SevDesk Integration Test",
            "version": "1.0",
            "src": {
                "host_id": "wise-host",
                "name": "Wise Platform"
            },
            "dst": {
                "host_id": "sevdesk-host", 
                "name": "SevDesk Platform"
            },
            "engine_config": {
                "batch_size": 10,
                "max_concurrent_batches": 1,
                "buffer_size": 100
            },
            "streams": {
                "wise-transactions": {
                    "name": "wise-transactions",
                    "src": {
                        "endpoint_id": "wise-endpoint",
                        "type": "api"
                    },
                    "dst": {
                        "endpoint_id": "sevdesk-endpoint",
                        "type": "api"
                    },
                    "mapping": {
                        "field_mappings": {
                            "created": {
                                "target": "valueDate",
                                "transformations": ["iso_to_date"]
                            },
                            "targetValue": {
                                "target": "amount"
                            },
                            "id": {
                                "target": "paymtPurpose"
                            }
                        },
                        "computed_fields": {
                            "objectName": {
                                "expression": "CheckAccountTransaction"
                            },
                            "checkAccount": {
                                "expression": '{"id": "5936402", "objectName": "CheckAccount"}'
                            },
                            "status": {
                                "expression": "100"
                            }
                        }
                    }
                }
            }
        }
        
        # Mock connector creation
        def mock_get_connector(connector_config):
            if connector_config.get("type") == "api":
                # Return source or destination connector based on context
                return mock_wise_api_connector
            return mock_sevdesk_api_connector
        
        with patch.object(engine, '_get_connector', side_effect=mock_get_connector):
            with patch.object(engine.sharded_state_manager, 'start_run', return_value="test-run"):
                    
                # Run the pipeline
                await engine.stream_data(config)
                
                # Verify connectors were called correctly
                mock_wise_api_connector.configure.assert_called()
                mock_wise_api_connector.health_check.assert_called()
                
                mock_sevdesk_api_connector.configure.assert_called()
                mock_sevdesk_api_connector.write_batch.assert_called()
                
                # Verify transformation was applied
                write_calls = mock_sevdesk_api_connector.write_batch.call_args_list
                assert len(write_calls) > 0
                
                # Check the transformed data structure
                written_batch = write_calls[0][0][0]  # First call, first argument (batch)
                assert len(written_batch) == 2
                
                # Verify first record transformation
                record1 = written_batch[0]
                assert record1["valueDate"] == "2025-08-16"
                assert record1["amount"] == 100.50
                assert record1["paymtPurpose"] == 123456
                assert record1["objectName"] == "CheckAccountTransaction"
                assert record1["checkAccount"] == {"id": "5936402", "objectName": "CheckAccount"}
                assert record1["status"] == "100"

    @pytest.mark.asyncio
    async def test_pipeline_error_handling(self, temp_dir):
        """Test pipeline error handling and recovery."""
        engine = StreamingEngine(
            pipeline_id="error-test-pipeline",
            dlq_path=temp_dir
        )
        
        # Configuration with problematic stream
        config = {
            "pipeline_id": "error-test-pipeline",
            "name": "Error Test Pipeline",
            "version": "1.0",
            "src": {"host_id": "test-src"},
            "dst": {"host_id": "test-dst"},
            "engine_config": {"batch_size": 10},
            "streams": {
                "error-stream": {
                    "name": "error-stream",
                    "src": {"endpoint_id": "src", "type": "api"},
                    "dst": {"endpoint_id": "dst", "type": "api"}
                }
            }
        }
        
        # Mock connector that fails
        failing_connector = AsyncMock()
        failing_connector.configure.side_effect = RuntimeError("Connector configuration failed")
        
        with patch.object(engine, '_get_connector', return_value=failing_connector):
            with patch.object(engine.sharded_state_manager, 'start_run', return_value="error-run"):
                
                # Should raise ExceptionGroup for stream processing errors
                with pytest.raises(ExceptionGroup) as exc_info:
                    await engine.stream_data(config)
                
                # Verify error handling
                assert len(exc_info.value.exceptions) == 1
                assert engine.metrics.streams_failed == 1
                assert engine.metrics.streams_processed == 0

    @pytest.mark.asyncio
    async def test_pipeline_with_malicious_data(self, temp_dir):
        """Test pipeline handles malicious data safely."""
        engine = StreamingEngine(
            pipeline_id="security-test-pipeline",
            dlq_path=temp_dir
        )
        
        # Mock connector that returns potentially malicious data
        malicious_connector = AsyncMock()
        malicious_connector.configure = AsyncMock()
        malicious_connector.health_check = AsyncMock(return_value=True)
        
        # Malicious data that could be injection attempts
        malicious_data = [
            {
                "id": "__import__('os').system('rm -rf /')",
                "created": "${PATH}; echo injected",
                "targetValue": "eval('1+1')",
                "malicious_field": "exec('print(\"pwned\")')"
            }
        ]
        
        async def mock_read_malicious(*args, **kwargs):
            yield malicious_data
        
        malicious_connector.read_batches = mock_read_malicious
        malicious_connector.close = AsyncMock()
        
        safe_destination = AsyncMock()
        safe_destination.configure = AsyncMock()
        safe_destination.write_batch = AsyncMock()
        safe_destination.close = AsyncMock()
        
        config = {
            "pipeline_id": "security-test-pipeline",
            "name": "Security Test Pipeline",
            "version": "1.0",
            "src": {"host_id": "malicious-src"},
            "dst": {"host_id": "safe-dst"},
            "engine_config": {"batch_size": 10},
            "streams": {
                "security-stream": {
                    "name": "security-stream", 
                    "src": {"endpoint_id": "malicious", "type": "api"},
                    "dst": {"endpoint_id": "safe", "type": "api"},
                    "mapping": {
                        "field_mappings": {
                            "id": {"target": "safe_id"},
                            "created": {"target": "safe_created"},
                            "targetValue": {"target": "safe_value"}
                        },
                        "computed_fields": {
                            "dangerous_expression": {
                                "expression": "eval('print(\"should not execute\")')"
                            }
                        }
                    }
                }
            }
        }
        
        def connector_factory(config_data):
            if "malicious" in str(config_data):
                return malicious_connector
            return safe_destination
        
        with patch.object(engine, '_get_connector', side_effect=connector_factory):
            with patch.object(engine.sharded_state_manager, 'start_run', return_value="security-run"):
                
                # Run the pipeline - should not execute malicious code
                await engine.stream_data(config)
                
                # Verify data was processed safely
                write_calls = safe_destination.write_batch.call_args_list
                assert len(write_calls) > 0
                
                written_data = write_calls[0][0][0][0]  # First batch, first record
                
                # Verify malicious data was treated as safe strings
                assert written_data["safe_id"] == "__import__('os').system('rm -rf /')"
                assert written_data["safe_created"] == "${PATH}; echo injected"
                assert written_data["safe_value"] == "eval('1+1')"
                assert written_data["dangerous_expression"] == "eval('print(\"should not execute\")')"

    @pytest.mark.asyncio
    async def test_pipeline_metrics_tracking(self, temp_dir, mock_connector):
        """Test that pipeline metrics are tracked correctly."""
        engine = StreamingEngine(
            pipeline_id="metrics-test-pipeline",
            dlq_path=temp_dir
        )
        
        config = {
            "pipeline_id": "metrics-test-pipeline",
            "name": "Metrics Test Pipeline",
            "version": "1.0",
            "src": {"host_id": "test-src"},
            "dst": {"host_id": "test-dst"},
            "engine_config": {"batch_size": 5},
            "streams": {
                "metrics-stream": {
                    "name": "metrics-stream",
                    "src": {"endpoint_id": "src", "type": "api"},
                    "dst": {"endpoint_id": "dst", "type": "api"}
                }
            }
        }
        
        with patch.object(engine, '_get_connector', return_value=mock_connector):
            with patch.object(engine.sharded_state_manager, 'start_run', return_value="metrics-run"):
                
                await engine.stream_data(config)
                
                # Verify metrics were updated
                assert engine.metrics.streams_processed == 1
                assert engine.metrics.streams_failed == 0

    @pytest.mark.asyncio
    async def test_pipeline_state_management(self, temp_dir):
        """Test pipeline state management integration."""
        engine = StreamingEngine(
            pipeline_id="state-test-pipeline",
            dlq_path=temp_dir
        )
        
        config = {
            "pipeline_id": "state-test-pipeline",
            "name": "State Test Pipeline",
            "version": "1.0", 
            "src": {"host_id": "test-src"},
            "dst": {"host_id": "test-dst"},
            "engine_config": {"batch_size": 10},
            "streams": {
                "state-stream": {
                    "name": "state-stream",
                    "src": {"endpoint_id": "src", "type": "api"},
                    "dst": {"endpoint_id": "dst", "type": "api"}
                }
            }
        }
        
        mock_connector = AsyncMock()
        mock_connector.configure = AsyncMock()
        mock_connector.health_check = AsyncMock(return_value=True)
        mock_connector.read_batches = AsyncMock(return_value=iter([[]]))  # Empty batch
        mock_connector.write_batch = AsyncMock()
        mock_connector.close = AsyncMock()
        
        with patch.object(engine, '_get_connector', return_value=mock_connector):
            # Verify state manager interactions
            with patch.object(engine.sharded_state_manager, 'start_run', return_value="state-run-123") as mock_start:
                
                await engine.stream_data(config)
                
                # Verify state management calls
                mock_start.assert_called_once_with(config)