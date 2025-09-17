"""Tests for field mapping processor."""

import os
import pytest
from unittest.mock import patch

from analitiq_stream.mapping.processor import (
    FieldMappingProcessor,
    MappingError,
    ValidationError,
)


class TestFieldMappingProcessor:
    """Test field mapping processor functionality."""
    
    def test_simple_field_mapping(self):
        """Test basic field mapping without transformations."""
        config = {
            "field_mappings": {
                "source_field": {
                    "target": "target_field"
                },
                "direct_mapping": {}  # No target specified, should use source name
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        record = {
            "source_field": "test_value",
            "direct_mapping": "direct_value"
        }
        
        result = processor.process_record(record)
        
        assert result["target_field"] == "test_value"
        assert result["direct_mapping"] == "direct_value"
    
    def test_field_mapping_with_transformations(self):
        """Test field mapping with transformations."""
        config = {
            "field_mappings": {
                "amount": {
                    "target": "clean_amount",
                    "transformations": ["abs", "number_format"]
                },
                "name": {
                    "target": "clean_name",
                    "transformations": ["strip", "upper"]
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        record = {
            "amount": -123.456,
            "name": "  john doe  "
        }
        
        result = processor.process_record(record)
        
        assert result["clean_amount"] == "123.46"  # abs + number_format(2 decimals default)
        assert result["clean_name"] == "JOHN DOE"  # strip + upper
    
    def test_nested_field_access(self):
        """Test accessing nested fields with dot notation."""
        config = {
            "field_mappings": {
                "user.profile.name": {
                    "target": "full_name"
                },
                "metadata.created_at": {
                    "target": "creation_date",
                    "transformations": ["iso_to_date"]
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        record = {
            "user": {
                "profile": {
                    "name": "Jane Smith"
                }
            },
            "metadata": {
                "created_at": "2023-12-25T10:30:00Z"
            }
        }
        
        result = processor.process_record(record)
        
        assert result["full_name"] == "Jane Smith"
        assert result["creation_date"] == "2023-12-25"
    
    def test_computed_fields(self):
        """Test computed fields with expressions."""
        config = {
            "computed_fields": {
                "system_id": {
                    "expression": "test_system"
                },
                "record_key": {
                    "expression": "record_${id}_${timestamp}"
                },
                "current_time": {
                    "expression": "now()"
                },
                "unique_id": {
                    "expression": "uuid()"
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        record = {
            "id": "123",
            "timestamp": "20231225"
        }
        
        result = processor.process_record(record)
        
        assert result["system_id"] == "test_system"
        assert result["record_key"] == "record_123_20231225"
        assert "T" in result["current_time"]  # ISO format timestamp
        assert len(result["unique_id"]) == 36  # UUID format
    
    def test_environment_variable_expansion(self):
        """Test environment variable expansion in computed fields."""
        config = {
            "computed_fields": {
                "account_id": {
                    "expression": "${TEST_ACCOUNT_ID}"
                },
                "api_endpoint": {
                    "expression": "${API_BASE_URL}/api/v1"
                }
            }
        }
        
        with patch.dict(os.environ, {
            "TEST_ACCOUNT_ID": "12345",
            "API_BASE_URL": "https://api.example.com"
        }):
            processor = FieldMappingProcessor(config)
            
            result = processor.process_record({"dummy": "data"})
            
            assert result["account_id"] == "12345"
            assert result["api_endpoint"] == "https://api.example.com/api/v1"
    
    def test_field_validation_not_null(self):
        """Test not_null validation rule."""
        config = {
            "field_mappings": {
                "required_field": {
                    "target": "output_field",
                    "validation": {
                        "rules": [{"type": "not_null"}],
                        "error_action": "fail"
                    }
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Valid record should pass
        valid_record = {"required_field": "some_value"}
        result = processor.process_record(valid_record)
        assert result["output_field"] == "some_value"
        
        # Invalid records should raise error
        invalid_records = [
            {"required_field": None},
            {"required_field": ""},
            {}  # Missing field
        ]
        
        for invalid_record in invalid_records:
            with pytest.raises(MappingError):
                processor.process_record(invalid_record)
    
    def test_field_validation_enum(self):
        """Test enum validation rule."""
        config = {
            "field_mappings": {
                "status": {
                    "validation": {
                        "rules": [{"type": "enum", "values": ["active", "inactive", "pending"]}],
                        "error_action": "fail"
                    }
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Valid values should pass
        for valid_status in ["active", "inactive", "pending"]:
            result = processor.process_record({"status": valid_status})
            assert result["status"] == valid_status
        
        # Invalid value should fail
        with pytest.raises(MappingError):
            processor.process_record({"status": "invalid"})
    
    def test_field_validation_range(self):
        """Test range validation rule."""
        config = {
            "field_mappings": {
                "score": {
                    "validation": {
                        "rules": [{"type": "range", "min": 0, "max": 100}],
                        "error_action": "fail"
                    }
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Valid values should pass
        for valid_score in [0, 50, 100, "75"]:  # String numbers should work
            result = processor.process_record({"score": valid_score})
            assert result["score"] == valid_score
        
        # Invalid values should fail
        for invalid_score in [-1, 101, "150", "not_a_number"]:
            with pytest.raises(MappingError):
                processor.process_record({"score": invalid_score})
    
    def test_field_validation_regex(self):
        """Test regex validation rule."""
        config = {
            "field_mappings": {
                "email": {
                    "validation": {
                        "rules": [{"type": "regex", "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}],
                        "error_action": "fail"
                    }
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Valid emails should pass
        valid_emails = ["test@example.com", "user.name+tag@domain.co.uk"]
        for valid_email in valid_emails:
            result = processor.process_record({"email": valid_email})
            assert result["email"] == valid_email
        
        # Invalid emails should fail
        invalid_emails = ["invalid", "test@", "@example.com", "test@example"]
        for invalid_email in invalid_emails:
            with pytest.raises(MappingError):
                processor.process_record({"email": invalid_email})
    
    def test_error_actions_skip(self):
        """Test skip error action."""
        config = {
            "field_mappings": {
                "optional_field": {
                    "validation": {
                        "rules": [{"type": "not_null"}],
                        "error_action": "skip"
                    }
                },
                "valid_field": {
                    "target": "output_field"
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Record with invalid field should skip that field but include others
        record = {
            "optional_field": None,  # Should be skipped
            "valid_field": "valid_value"
        }
        
        result = processor.process_record(record)
        
        # optional_field should be excluded, valid_field should be included
        assert "optional_field" not in result
        assert result["output_field"] == "valid_value"
    
    def test_error_actions_dlq(self):
        """Test DLQ error action."""
        config = {
            "field_mappings": {
                "dlq_field": {
                    "validation": {
                        "rules": [{"type": "not_null"}],
                        "error_action": "dlq"
                    }
                },
                "valid_field": {
                    "target": "output_field"
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Record with invalid field should be processed but field excluded
        record = {
            "dlq_field": None,  # Should trigger DLQ
            "valid_field": "valid_value"
        }
        
        # Should not raise exception (DLQ is non-fatal)
        result = processor.process_record(record)
        
        # DLQ field should be excluded, valid field should be included
        assert "dlq_field" not in result
        assert result["output_field"] == "valid_value"
    
    def test_multiple_validation_rules(self):
        """Test multiple validation rules on same field."""
        config = {
            "field_mappings": {
                "score": {
                    "validation": {
                        "rules": [
                            {"type": "not_null"},
                            {"type": "range", "min": 0, "max": 100}
                        ],
                        "error_action": "fail"
                    }
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Valid value should pass both rules
        result = processor.process_record({"score": 85})
        assert result["score"] == 85
        
        # Should fail on first rule (not_null)
        with pytest.raises(MappingError):
            processor.process_record({"score": None})
        
        # Should fail on second rule (range)
        with pytest.raises(MappingError):
            processor.process_record({"score": 150})
    
    def test_transformation_with_validation(self):
        """Test combining transformations with validation."""
        config = {
            "field_mappings": {
                "amount": {
                    "target": "positive_amount",
                    "transformations": ["abs"],  # Make positive
                    "validation": {
                        "rules": [{"type": "range", "min": 0, "max": 1000}],
                        "error_action": "fail"
                    }
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Negative amount should be transformed to positive and pass validation
        result = processor.process_record({"amount": -500})
        assert result["positive_amount"] == 500
        
        # Large negative amount should be transformed but fail validation
        with pytest.raises(MappingError):
            processor.process_record({"amount": -2000})
    
    def test_computed_field_validation(self):
        """Test validation on computed fields."""
        config = {
            "computed_fields": {
                "account_id": {
                    "expression": "${ACCOUNT_ID}",
                    "validation": {
                        "rules": [{"type": "not_null"}],
                        "error_action": "fail"
                    }
                }
            }
        }
        
        # Test with environment variable set
        with patch.dict(os.environ, {"ACCOUNT_ID": "12345"}):
            processor = FieldMappingProcessor(config)
            result = processor.process_record({})
            assert result["account_id"] == "12345"
        
        # Test with environment variable missing (should fail validation)
        processor = FieldMappingProcessor(config)
        with pytest.raises(MappingError):
            processor.process_record({})
    
    def test_get_target_schema(self):
        """Test getting target schema from mapping configuration."""
        config = {
            "field_mappings": {
                "source1": {"target": "target1"},
                "source2": {}  # Direct mapping
            },
            "computed_fields": {
                "computed1": {"expression": "value"},
                "computed2": {"expression": "now()"}
            }
        }
        
        processor = FieldMappingProcessor(config)
        schema = processor.get_target_schema()
        
        expected_schema = {
            "target1": "mapped",
            "source2": "mapped",
            "computed1": "computed",
            "computed2": "computed"
        }
        
        assert schema == expected_schema
    
    def test_invalid_transformation_name(self):
        """Test error handling for invalid transformation names."""
        config = {
            "field_mappings": {
                "test_field": {
                    "transformations": ["invalid_transform"]
                }
            }
        }
        
        with pytest.raises(MappingError, match="Invalid transformation"):
            FieldMappingProcessor(config)
    
    def test_missing_nested_field(self):
        """Test handling of missing nested fields."""
        config = {
            "field_mappings": {
                "user.profile.missing": {
                    "target": "missing_field"
                }
            }
        }
        
        processor = FieldMappingProcessor(config)
        
        # Record without nested structure should result in None
        record = {"user": {"other": "data"}}
        result = processor.process_record(record)
        
        # Field should be mapped but value should be None
        assert result["missing_field"] is None
    
    def test_default_error_action(self):
        """Test default error action configuration."""
        config = {
            "field_mappings": {
                "test_field": {
                    "validation": {
                        "rules": [{"type": "not_null"}]
                        # No error_action specified
                    }
                }
            },
            "default_error_action": "skip"
        }
        
        processor = FieldMappingProcessor(config)
        
        # Should use default error action (skip)
        result = processor.process_record({"test_field": None})
        assert "test_field" not in result


if __name__ == "__main__":
    pytest.main([__file__])