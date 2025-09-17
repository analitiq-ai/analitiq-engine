"""E2E Test: Data Quality and Schema Edge Cases

Tests data quality scenarios including schema drift, type mismatches,
corrupt records, validation failures, and data transformation edge cases.
"""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from analitiq_stream.core.pipeline import Pipeline


class SchemaEvolutionSimulator:
    """Simulates schema evolution scenarios for testing."""

    @staticmethod
    def create_v1_schema():
        """Original schema version."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
                "created_at": {"type": "string", "format": "date-time"},
                "status": {"type": "string", "enum": ["active", "inactive"]}
            },
            "required": ["id", "name", "email"]
        }

    @staticmethod
    def create_v2_schema():
        """Evolved schema with new fields and changed types."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "string"},  # Changed from integer to string
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
                "created_at": {"type": "string", "format": "date-time"},
                "status": {"type": "string", "enum": ["active", "inactive", "pending"]},  # Added enum value
                "age": {"type": "integer", "minimum": 0},  # New field
                "metadata": {"type": "object"},  # New nested object
                "tags": {"type": "array", "items": {"type": "string"}}  # New array field
            },
            "required": ["id", "name", "email", "age"]  # Added required field
        }

    @staticmethod
    def create_incompatible_schema():
        """Schema with breaking changes."""
        return {
            "type": "object",
            "properties": {
                "user_id": {"type": "integer"},  # Renamed from 'id'
                "full_name": {"type": "string"},  # Renamed from 'name'
                "contact_email": {"type": "string", "format": "email"},  # Renamed from 'email'
                "registration_date": {"type": "string", "format": "date"},  # Changed format from date-time to date
                "account_status": {"type": "boolean"}  # Changed from string enum to boolean
            },
            "required": ["user_id", "full_name"]
        }


class TestDataQualityE2E:
    """Test data quality and schema evolution scenarios."""

    @pytest.mark.asyncio
    async def test_schema_drift_detection_and_handling(
        self,
        temp_dirs,
        mock_pipeline_id,
        data_generator,
        source_config,
        destination_config
    ):
        """Test detection and handling of schema drift."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Schema Drift Detection Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 5,
                "schema_validation": True,
                "schema_evolution_strategy": "auto_evolve"
            },
            "streams": {
                "evolving_stream": {
                    "name": "Schema Evolution Stream",
                    "src": {"endpoint_id": "evolving-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "evolving-dst", "host_id": "dst-host"},
                    "schema_config": {
                        "drift_detection": True,
                        "compatibility_mode": "forward"
                    }
                }
            }
        }

        # Create records with evolving schema
        v1_records = [
            {
                "id": 1,
                "name": "John Doe",
                "email": "john@example.com",
                "created_at": "2023-12-01T10:00:00Z",
                "status": "active"
            }
        ]

        v2_records = [
            {
                "id": "2",  # Changed to string
                "name": "Jane Smith",
                "email": "jane@example.com",
                "created_at": "2023-12-01T11:00:00Z",
                "status": "pending",  # New enum value
                "age": 30,  # New field
                "metadata": {"department": "Engineering"},  # New nested field
                "tags": ["developer", "senior"]  # New array field
            }
        ]

        class SchemaEvolvingConnector:
            def __init__(self, records_sequence: List[List[Dict]]):
                self.records_sequence = records_sequence
                self.batch_index = 0
                self.is_closed = False

            async def read_batch(self, batch_size: int) -> List[Dict[str, Any]]:
                if self.batch_index < len(self.records_sequence):
                    batch = self.records_sequence[self.batch_index]
                    self.batch_index += 1
                    return batch
                return []

            async def close(self):
                self.is_closed = True

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "schema_changes_detected": 5,  # id type, status enum, 3 new fields
                "schema_evolution_applied": 1,
                "records_processed": 2,
                "validation_errors": 0,
                "schema_versions": ["v1.0", "v2.0"]
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["schema_changes_detected"] > 0
            assert metrics["schema_evolution_applied"] > 0
            assert metrics["records_processed"] == 2

    @pytest.mark.asyncio
    async def test_data_type_coercion_and_validation(
        self,
        temp_dirs,
        mock_pipeline_id,
        data_generator,
        source_config,
        destination_config
    ):
        """Test data type coercion and validation edge cases."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Data Type Validation Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 10,
                "strict_validation": False,
                "type_coercion": True
            },
            "streams": {
                "validation_stream": {
                    "name": "Type Validation Stream",
                    "src": {"endpoint_id": "validation-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "validation-dst", "host_id": "dst-host"},
                    "mapping": {
                        "field_mappings": {
                            "id": {
                                "target": "user_id",
                                "type": "integer",
                                "validation": {
                                    "rules": [{"type": "range", "min": 1, "max": 999999}],
                                    "error_action": "coerce"
                                }
                            },
                            "age": {
                                "target": "user_age",
                                "type": "integer",
                                "validation": {
                                    "rules": [{"type": "range", "min": 0, "max": 150}],
                                    "error_action": "dlq"
                                }
                            },
                            "score": {
                                "target": "rating",
                                "type": "float",
                                "validation": {
                                    "rules": [{"type": "range", "min": 0.0, "max": 10.0}],
                                    "error_action": "default",
                                    "default_value": 5.0
                                }
                            },
                            "email": {
                                "target": "email_address",
                                "type": "string",
                                "validation": {
                                    "rules": [{"type": "email"}, {"type": "not_null"}],
                                    "error_action": "dlq"
                                }
                            }
                        }
                    }
                }
            },
            "error_handling": {
                "strategy": "dlq",
                "validation_errors": "isolate"
            }
        }

        # Records with various type issues
        problematic_records = [
            # Valid record
            {
                "id": 1,
                "age": 30,
                "score": 8.5,
                "email": "valid@example.com"
            },
            # Type coercion cases
            {
                "id": "2",          # String that can be coerced to int
                "age": "25",        # String that can be coerced to int
                "score": "7.2",     # String that can be coerced to float
                "email": "coerced@example.com"
            },
            # Range violations
            {
                "id": 3,
                "age": 200,         # Above max age (150)
                "score": 15.0,      # Above max score (10.0) - should use default
                "email": "range@example.com"
            },
            # Invalid email
            {
                "id": 4,
                "age": 35,
                "score": 6.0,
                "email": "invalid-email"  # Invalid email format
            },
            # Null values
            {
                "id": 5,
                "age": None,        # Null age
                "score": None,      # Null score - should use default
                "email": None       # Null email - should go to DLQ
            },
            # Extreme type mismatches
            {
                "id": [1, 2, 3],    # Array instead of int
                "age": {"value": 30},  # Object instead of int
                "score": "not_a_number",  # Non-numeric string
                "email": 12345      # Number instead of string
            }
        ]

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 2,  # Only first 2 records processed successfully
                "records_failed": 4,     # Last 4 records failed validation
                "type_coercions": 3,     # Successful coercions in record 2
                "validation_errors": {
                    "range_violations": 2,      # Records 3 and 5 age issues
                    "email_format_errors": 2,   # Records 4 and 5 email issues
                    "type_mismatches": 4,       # Record 6 multiple type issues
                    "null_violations": 1        # Record 5 email null
                },
                "default_value_applications": 2,  # Records 3 and 5 score defaults
                "dlq_records": 4
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["type_coercions"] > 0
            assert metrics["validation_errors"]["range_violations"] > 0
            assert metrics["validation_errors"]["email_format_errors"] > 0
            assert metrics["default_value_applications"] > 0
            assert metrics["dlq_records"] > 0

    @pytest.mark.asyncio
    async def test_corrupt_and_malformed_data_handling(
        self,
        temp_dirs,
        mock_pipeline_id,
        data_generator,
        source_config,
        destination_config
    ):
        """Test handling of corrupt and malformed data."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Malformed Data Handling Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 5,
                "malformed_data_strategy": "isolate_and_continue"
            },
            "streams": {
                "malformed_stream": {
                    "name": "Malformed Data Stream",
                    "src": {"endpoint_id": "malformed-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "malformed-dst", "host_id": "dst-host"},
                    "data_quality": {
                        "max_field_length": 1000,
                        "allow_null_required_fields": False,
                        "unicode_handling": "preserve",
                        "binary_data_handling": "base64_encode"
                    }
                }
            }
        }

        # Generate malformed records
        malformed_records = data_generator.generate_malformed_records(15)

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 6,   # Some records could be cleaned/processed
                "records_failed": 9,      # Majority failed due to malformed data
                "data_quality_issues": {
                    "missing_required_fields": 3,     # Every 5th record
                    "invalid_data_types": 3,          # Records with type mismatches
                    "null_required_fields": 3,        # Records with null values
                    "oversized_fields": 3,            # Records with very long values
                    "special_characters": 3,          # Records with special chars
                    "unicode_issues": 0,              # Handled by unicode_handling
                    "binary_data_detected": 0         # Handled by base64 encoding
                },
                "data_cleaning_applied": {
                    "field_truncation": 3,            # Long fields truncated
                    "special_char_escaping": 3,       # Special chars escaped
                    "unicode_normalization": 3,       # Unicode normalized
                    "binary_encoding": 0              # No binary data in this test
                },
                "dlq_records": 9
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["data_quality_issues"]["missing_required_fields"] > 0
            assert metrics["data_quality_issues"]["invalid_data_types"] > 0
            assert metrics["data_cleaning_applied"]["field_truncation"] > 0
            assert metrics["dlq_records"] > 0

    @pytest.mark.asyncio
    async def test_complex_nested_data_validation(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test validation of complex nested data structures."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Nested Data Validation Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 3,
                "deep_validation": True
            },
            "streams": {
                "nested_stream": {
                    "name": "Nested Data Stream",
                    "src": {"endpoint_id": "nested-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "nested-dst", "host_id": "dst-host"},
                    "mapping": {
                        "field_mappings": {
                            "user.profile.name": {
                                "target": "full_name",
                                "validation": {
                                    "rules": [{"type": "not_null"}, {"type": "min_length", "value": 2}],
                                    "error_action": "dlq"
                                }
                            },
                            "user.profile.contacts.email": {
                                "target": "primary_email",
                                "validation": {
                                    "rules": [{"type": "email"}],
                                    "error_action": "dlq"
                                }
                            },
                            "user.preferences.notifications[]": {
                                "target": "notification_types",
                                "validation": {
                                    "rules": [{"type": "array_not_empty"}],
                                    "error_action": "default",
                                    "default_value": ["email"]
                                }
                            },
                            "metadata.scores.overall": {
                                "target": "total_score",
                                "type": "float",
                                "validation": {
                                    "rules": [{"type": "range", "min": 0.0, "max": 100.0}],
                                    "error_action": "coerce"
                                }
                            }
                        }
                    }
                }
            }
        }

        # Complex nested records with various issues
        nested_records = [
            # Valid nested record
            {
                "user": {
                    "profile": {
                        "name": "John Doe",
                        "contacts": {
                            "email": "john@example.com",
                            "phone": "+1234567890"
                        }
                    },
                    "preferences": {
                        "notifications": ["email", "sms", "push"]
                    }
                },
                "metadata": {
                    "scores": {
                        "overall": 85.5,
                        "individual": [80, 90, 86]
                    }
                }
            },
            # Missing nested fields
            {
                "user": {
                    "profile": {
                        "name": "",  # Empty name (min_length violation)
                        "contacts": {
                            "phone": "+1234567890"
                            # Missing email
                        }
                    },
                    "preferences": {
                        "notifications": []  # Empty array
                    }
                },
                "metadata": {
                    "scores": {
                        "overall": 150.0,  # Out of range (should be coerced to 100.0)
                    }
                }
            },
            # Invalid nested data types
            {
                "user": {
                    "profile": {
                        "name": None,  # Null name
                        "contacts": {
                            "email": "invalid-email-format"
                        }
                    },
                    "preferences": {
                        "notifications": "not_an_array"  # Wrong type
                    }
                },
                "metadata": {
                    "scores": {
                        "overall": "not_a_number"  # Wrong type
                    }
                }
            },
            # Completely malformed nested structure
            {
                "user": "not_an_object",
                "metadata": {
                    "scores": []  # Array instead of object
                }
            }
        ]

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 1,  # Only first record valid
                "records_failed": 3,     # Last 3 records failed validation
                "nested_validation_errors": {
                    "missing_nested_fields": 2,       # Records 2 and 3 missing email
                    "null_nested_values": 1,          # Record 3 null name
                    "nested_type_mismatches": 3,      # Various type issues
                    "nested_validation_failures": 4,  # Email format, array type, etc.
                },
                "nested_coercions": 1,         # Record 2 overall score coerced
                "default_applications": 1,     # Record 2 notifications default applied
                "dlq_records": 3
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["nested_validation_errors"]["missing_nested_fields"] > 0
            assert metrics["nested_validation_errors"]["nested_type_mismatches"] > 0
            assert metrics["nested_coercions"] > 0
            assert metrics["default_applications"] > 0

    @pytest.mark.asyncio
    async def test_schema_compatibility_modes(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test different schema compatibility modes."""

        # Test backward compatibility
        backward_compatible_config = {
            "pipeline_id": f"{mock_pipeline_id}-backward",
            "name": "Backward Compatibility Pipeline",
            "version": "1.0",
            "engine_config": {"batch_size": 5},
            "streams": {
                "backward_stream": {
                    "name": "Backward Compatible Stream",
                    "src": {"endpoint_id": "backward-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "backward-dst", "host_id": "dst-host"},
                    "schema_config": {
                        "compatibility_mode": "backward",
                        "allow_field_removal": True,
                        "allow_type_widening": True,
                        "allow_type_narrowing": False
                    }
                }
            }
        }

        # Test forward compatibility
        forward_compatible_config = {
            "pipeline_id": f"{mock_pipeline_id}-forward",
            "name": "Forward Compatibility Pipeline",
            "version": "1.0",
            "engine_config": {"batch_size": 5},
            "streams": {
                "forward_stream": {
                    "name": "Forward Compatible Stream",
                    "src": {"endpoint_id": "forward-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "forward-dst", "host_id": "dst-host"},
                    "schema_config": {
                        "compatibility_mode": "forward",
                        "allow_field_addition": True,
                        "allow_type_widening": False,
                        "allow_type_narrowing": True
                    }
                }
            }
        }

        # Test both compatibility modes
        configs_and_expected_results = [
            (backward_compatible_config, {
                "compatibility_mode": "backward",
                "schema_compatibility_checks": 5,
                "compatible_changes": 3,      # Field removal, type widening allowed
                "incompatible_changes": 2,    # Type narrowing blocked
                "records_processed": 8
            }),
            (forward_compatible_config, {
                "compatibility_mode": "forward",
                "schema_compatibility_checks": 5,
                "compatible_changes": 3,      # Field addition, type narrowing allowed
                "incompatible_changes": 2,    # Type widening blocked
                "records_processed": 8
            })
        ]

        for config, expected_metrics in configs_and_expected_results:
            with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                mock_engine = MagicMock()
                mock_engine.stream_data = AsyncMock()
                mock_engine.get_metrics.return_value = expected_metrics
                # Mock state manager to return None (no previous state)
                mock_state_manager = MagicMock()
                mock_state_manager.get_run_info.return_value = None
                mock_engine.get_state_manager.return_value = mock_state_manager
                mock_engine_cls.return_value = mock_engine

                pipeline = Pipeline(
                    pipeline_config=config,
                    source_config=source_config,
                    destination_config=destination_config,
                    state_dir=str(temp_dirs["state"])
                )

                await pipeline.run()

                metrics = pipeline.get_metrics()
                assert metrics["compatibility_mode"] == expected_metrics["compatibility_mode"]
                assert metrics["compatible_changes"] > 0
                assert metrics["incompatible_changes"] > 0

    @pytest.mark.asyncio
    async def test_data_lineage_tracking(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test data lineage tracking through transformations."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Data Lineage Tracking Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 5,
                "lineage_tracking": True
            },
            "streams": {
                "lineage_stream": {
                    "name": "Lineage Tracking Stream",
                    "src": {"endpoint_id": "lineage-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "lineage-dst", "host_id": "dst-host"},
                    "mapping": {
                        "field_mappings": {
                            "first_name": {
                                "target": "full_name",
                                "transformations": ["strip", "title_case"],
                                "lineage_tracking": True
                            },
                            "last_name": {
                                "target": "full_name",
                                "transformations": ["strip", "title_case"],
                                "lineage_tracking": True,
                                "merge_strategy": "concat_space"
                            },
                            "birth_date": {
                                "target": "age",
                                "transformations": ["date_to_age"],
                                "lineage_tracking": True
                            }
                        },
                        "computed_fields": {
                            "data_source": {"expression": "api_v2"},
                            "processing_timestamp": {"expression": "now()"},
                            "record_hash": {"expression": "hash(id, first_name, last_name)"}
                        }
                    }
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 10,
                "lineage_tracking": {
                    "fields_tracked": 3,           # first_name, last_name, birth_date
                    "transformations_applied": 15, # 2 transforms * 2 name fields + 1 date transform * 10 records
                    "field_merges": 10,            # first_name + last_name merged 10 times
                    "computed_fields": 30,         # 3 computed fields * 10 records
                    "lineage_records_created": 10
                },
                "transformation_success_rate": 1.0,
                "data_quality_score": 0.95
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["lineage_tracking"]["fields_tracked"] > 0
            assert metrics["lineage_tracking"]["transformations_applied"] > 0
            assert metrics["lineage_tracking"]["field_merges"] > 0
            assert metrics["data_quality_score"] > 0.8