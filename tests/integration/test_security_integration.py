"""End-to-end security integration tests."""

import json
import os
import pytest

from src.engine.data_transformer import DataTransformer
from src.engine.expression_evaluator import SecureExpressionEvaluator


class TestSecurityIntegration:
    """End-to-end security tests for the full transformation pipeline."""

    @pytest.mark.asyncio
    async def test_wise_to_sevdesk_transformation_security(self, sample_wise_record):
        """Test the complete Wise to SevDesk transformation with security checks."""
        transformer = DataTransformer()
        
        # Configuration matching the actual pipeline
        config = {
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
        
        batch = [sample_wise_record]
        result = await transformer.apply_transformations(batch, config)
        
        # Verify transformation results
        assert len(result) == 1
        transformed = result[0]
        
        # Check field mappings
        assert transformed["valueDate"] == "2025-08-16"
        assert transformed["amount"] == 100.50
        assert transformed["paymtPurpose"] == 123456
        
        # Check computed fields
        assert transformed["objectName"] == "CheckAccountTransaction"
        assert transformed["checkAccount"] == {"id": "5936402", "objectName": "CheckAccount"}
        assert transformed["status"] == "100"

    @pytest.mark.asyncio
    async def test_malicious_expression_injection_prevention(self):
        """Test that malicious expressions are safely handled in real pipeline."""
        transformer = DataTransformer()
        
        # Malicious configuration attempts
        malicious_config = {
            "mapping": {
                "computed_fields": {
                    # Various code injection attempts
                    "malicious1": {
                        "expression": "__import__('os').system('rm -rf /')"
                    },
                    "malicious2": {
                        "expression": "eval('1+1')"
                    },
                    "malicious3": {
                        "expression": "${PATH}; echo 'injected'"
                    },
                    "malicious4": {
                        "expression": "concat(__import__('subprocess').call(['echo', 'pwned']))"
                    },
                    "malicious5": {
                        "expression": '{"__class__": {"__init__": {"__globals__": {"os": "system"}}}}'
                    }
                }
            }
        }
        
        batch = [{"id": 1, "name": "test"}]
        result = await transformer.apply_transformations(batch, malicious_config)
        
        # All malicious expressions should be treated as static strings
        # or safely parsed JSON - no code execution should occur
        transformed = result[0]
        
        # Verify no code execution occurred (values are static strings or safe JSON)
        assert transformed["malicious1"] == "__import__('os').system('rm -rf /')"
        assert transformed["malicious2"] == "eval('1+1')"
        assert transformed["malicious3"] == "${PATH}; echo 'injected'"
        # malicious4 gets processed by concat function - it treats arguments as literals
        assert "__import__('subprocess').call(['echopwned'])" in transformed["malicious4"]
        # malicious5 should be valid JSON but safe
        assert transformed["malicious5"] == {"__class__": {"__init__": {"__globals__": {"os": "system"}}}}

    @pytest.mark.asyncio
    async def test_environment_variable_security(self):
        """Test secure environment variable handling."""
        evaluator = SecureExpressionEvaluator()
        
        # Set up test environment
        os.environ["SAFE_VAR"] = "safe_value"
        
        # Test safe patterns
        result = await evaluator.evaluate("${SAFE_VAR}", {}, {})
        assert result == "safe_value"
        
        # Test unsafe patterns that should be treated as static
        unsafe_patterns = [
            "${PATH}/../../../etc/passwd",
            "${HOME}; rm -rf /",
            "${USER}; echo injected",
            "${}",
            "${123}",
            "${invalid-name}"
        ]
        
        for pattern in unsafe_patterns:
            result = await evaluator.evaluate(pattern, {}, {})
            # Should return the pattern as-is (static string)
            assert result == pattern

    @pytest.mark.asyncio
    async def test_json_injection_prevention(self):
        """Test prevention of JSON-based injection attacks."""
        evaluator = SecureExpressionEvaluator()
        
        # Safe JSON should work
        safe_json = '{"id": "123", "name": "test"}'
        result = await evaluator.evaluate(safe_json, {}, {})
        assert result == {"id": "123", "name": "test"}
        
        # Invalid JSON should raise ValueError
        invalid_json = '{"invalid": json syntax}'
        with pytest.raises(ValueError):
            await evaluator.evaluate(invalid_json, {}, {})

    @pytest.mark.asyncio
    async def test_concat_function_security(self):
        """Test security of concat function with various inputs."""
        evaluator = SecureExpressionEvaluator()
        
        record = {"safe_field": "safe_value", "user_id": 123}
        
        # Safe concat operations
        result = await evaluator.evaluate("concat(user_id, '-', safe_field)", {}, record)
        assert result == "123-safe_value"
        
        # Concat with missing fields (should use literals)
        result = await evaluator.evaluate("concat(missing_field, '-', safe_field)", {}, record)
        assert result == "missing_field-safe_value"
        
        # Empty concat
        result = await evaluator.evaluate("concat()", {}, record)
        assert result == ""

    @pytest.mark.asyncio
    async def test_function_isolation(self):
        """Test that only predefined functions are available."""
        evaluator = SecureExpressionEvaluator()
        
        # Allowed functions should work
        allowed_functions = ["now()", "today()", "uuid()"]
        
        for func in allowed_functions:
            result = await evaluator.evaluate(func, {}, {})
            assert result is not None
            assert isinstance(result, str)
        
        # Disallowed functions should be treated as static
        disallowed_functions = [
            "eval()", "exec()", "import()", "open()", 
            "system()", "__import__()", "subprocess()",
            "compile()", "globals()", "locals()"
        ]
        
        for func in disallowed_functions:
            result = await evaluator.evaluate(func, {}, {})
            # Should return the function string as-is
            assert result == func

    @pytest.mark.asyncio
    async def test_real_world_pipeline_security(self, sample_wise_record):
        """Test security with real-world pipeline configuration."""
        transformer = DataTransformer()
        
        # Load actual pipeline configuration
        pipeline_config = {
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
                    },
                    "timestamp": {
                        "expression": "now()"
                    },
                    "environment_value": {
                        "expression": "${SEVDESK_BANK_ACCOUNT_ID}"
                    }
                }
            }
        }
        
        batch = [sample_wise_record]
        result = await transformer.apply_transformations(batch, pipeline_config)
        
        transformed = result[0]
        
        # Verify all expected fields are present and secure
        assert transformed["valueDate"] == "2025-08-16"
        assert transformed["amount"] == 100.50
        assert transformed["paymtPurpose"] == 123456
        assert transformed["objectName"] == "CheckAccountTransaction"
        assert transformed["checkAccount"] == {"id": "5936402", "objectName": "CheckAccount"}
        assert transformed["status"] == "100"
        assert "T" in transformed["timestamp"]  # ISO timestamp format
        assert transformed["environment_value"] == "5936402"  # From test env

    @pytest.mark.asyncio
    async def test_batch_processing_security(self):
        """Test security across batch processing."""
        transformer = DataTransformer()
        
        # Create a batch with mixed safe and potentially malicious data
        batch = [
            {"id": 1, "data": "normal_data"},
            {"id": 2, "data": "__import__('os').system('echo hack')"},
            {"id": 3, "data": "${PATH}/../../etc/passwd"},
            {"id": 4, "data": '{"malicious": "json"}'}
        ]
        
        config = {
            "mapping": {
                "field_mappings": {
                    "id": {"target": "record_id"},
                    "data": {"target": "safe_data"}
                },
                "computed_fields": {
                    "malicious_expression": {
                        "expression": "eval('print(\"hacked\")')"
                    }
                }
            }
        }
        
        result = await transformer.apply_transformations(batch, config)
        
        # All records should be processed safely
        assert len(result) == 4
        
        for i, record in enumerate(result):
            assert record["record_id"] == i + 1
            assert record["malicious_expression"] == "eval('print(\"hacked\")')"  # Static string
            
        # Verify specific potentially dangerous inputs are handled safely
        assert result[1]["safe_data"] == "__import__('os').system('echo hack')"
        assert result[2]["safe_data"] == "${PATH}/../../etc/passwd"
        assert result[3]["safe_data"] == '{"malicious": "json"}'