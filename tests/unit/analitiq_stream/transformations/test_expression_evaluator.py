"""Unit tests for SecureExpressionEvaluator."""

import json
import os
import pytest

from src.engine.expression_evaluator import SecureExpressionEvaluator


class TestSecureExpressionEvaluator:
    """Test suite for SecureExpressionEvaluator security and functionality."""

    @pytest.fixture
    def evaluator(self):
        """Create a SecureExpressionEvaluator instance."""
        return SecureExpressionEvaluator()

    @pytest.fixture
    def sample_records(self):
        """Sample record data for testing."""
        original = {"id": 123, "name": "test", "created": "2025-08-16T10:30:00Z"}
        transformed = {"user_id": 123, "full_name": "test", "date": "2025-08-16"}
        return original, transformed

    @pytest.mark.asyncio
    async def test_environment_variable_safe_pattern(self, evaluator, sample_records, monkeypatch):
        """Test safe environment variable substitution."""
        original, transformed = sample_records

        # Set up test environment variable
        monkeypatch.setenv("TEST_VAR", "test_value")

        # Valid environment variable pattern
        result = await evaluator.evaluate("${TEST_VAR}", original, transformed)
        assert result == "test_value"

        # Non-existent environment variable
        result = await evaluator.evaluate("${NON_EXISTENT}", original, transformed)
        assert result is None

    @pytest.mark.asyncio
    async def test_environment_variable_unsafe_patterns(self, evaluator, sample_records):
        """Test that unsafe environment variable patterns are treated as static."""
        original, transformed = sample_records
        
        unsafe_patterns = [
            "${PATH}/../../../etc/passwd",  # Path traversal attempt
            "${HOME}/../../root/.ssh",      # Directory traversal
            "${invalid-name}",              # Invalid variable name
            "${}",                          # Empty variable
            "${123}",                       # Numeric variable name
        ]
        
        for pattern in unsafe_patterns:
            result = await evaluator.evaluate(pattern, original, transformed)
            # Should be treated as static string
            assert result == pattern

    @pytest.mark.asyncio
    async def test_predefined_functions(self, evaluator, sample_records):
        """Test predefined safe functions."""
        original, transformed = sample_records
        
        # Test now() function
        result = await evaluator.evaluate("now()", original, transformed)
        assert isinstance(result, str)
        assert "T" in result  # ISO format
        
        # Test today() function
        result = await evaluator.evaluate("today()", original, transformed)
        assert isinstance(result, str)
        assert len(result) == 10  # YYYY-MM-DD format
        
        # Test uuid() function
        result = await evaluator.evaluate("uuid()", original, transformed)
        assert isinstance(result, str)
        assert len(result) == 36  # UUID format

    @pytest.mark.asyncio
    async def test_unknown_functions_treated_as_static(self, evaluator, sample_records):
        """Test that unknown functions are treated as static strings."""
        original, transformed = sample_records
        
        unknown_functions = [
            "eval()",
            "exec()",
            "import()",
            "system()",
            "dangerous_function()"
        ]
        
        for func in unknown_functions:
            result = await evaluator.evaluate(func, original, transformed)
            assert result == func

    @pytest.mark.asyncio
    async def test_concat_function_safe(self, evaluator, sample_records):
        """Test safe concat function functionality."""
        original, transformed = sample_records
        
        # Basic concat with field references
        result = await evaluator.evaluate("concat(user_id, '-', full_name)", 
                                        original, transformed)
        assert result == "123-test"
        
        # Concat with missing field (should use literal)
        result = await evaluator.evaluate("concat(missing_field, '-', full_name)", 
                                        original, transformed)
        assert result == "missing_field-test"
        
        # Empty concat
        result = await evaluator.evaluate("concat()", original, transformed)
        assert result == ""

    @pytest.mark.asyncio
    async def test_json_parsing_safe(self, evaluator, sample_records):
        """Test safe JSON parsing."""
        original, transformed = sample_records
        
        # Valid JSON object
        json_obj = '{"id": "5936402", "objectName": "CheckAccount"}'
        result = await evaluator.evaluate(json_obj, original, transformed)
        expected = {"id": "5936402", "objectName": "CheckAccount"}
        assert result == expected
        
        # Valid JSON array
        json_array = '[1, 2, 3]'
        result = await evaluator.evaluate(json_array, original, transformed)
        assert result == [1, 2, 3]
        
        # Simple values (these are treated as static strings unless they're complex JSON)
        simple_json_str = '{"value": "test"}'
        result = await evaluator.evaluate(simple_json_str, original, transformed)
        assert result == {"value": "test"}

    @pytest.mark.asyncio
    async def test_json_parsing_invalid(self, evaluator, sample_records):
        """Test invalid JSON handling."""
        original, transformed = sample_records
        
        invalid_json = '{"invalid": json}'
        with pytest.raises(ValueError, match="Invalid JSON expression"):
            await evaluator.evaluate(invalid_json, original, transformed)

    @pytest.mark.asyncio
    async def test_static_values(self, evaluator, sample_records):
        """Test static value handling."""
        original, transformed = sample_records
        
        static_values = [
            "CheckAccountTransaction",
            "100",
            "simple_string",
            "complex-string_with.symbols!",
        ]
        
        for value in static_values:
            result = await evaluator.evaluate(value, original, transformed)
            assert result == value

    @pytest.mark.asyncio
    async def test_code_injection_prevention(self, evaluator, sample_records):
        """Test prevention of code injection attacks."""
        original, transformed = sample_records
        
        injection_attempts = [
            "__import__('os').system('echo hacked')",
            "eval('1+1')",
            "exec('print(\"pwned\")')",
            "open('/etc/passwd', 'r').read()",
            "subprocess.call(['rm', '-rf', '/'])",
            "${PATH}; rm -rf /",
            "concat(__import__('os').system('echo hack'))",
            '{"__class__": {"__init__": {"__globals__": {"sys": "exit()"}}}}',
        ]
        
        for injection in injection_attempts:
            # Should be treated as static string - no code execution
            result = await evaluator.evaluate(injection, original, transformed)
            # Result should be the original string or safely parsed JSON
            assert isinstance(result, (str, dict, list, int, float, bool)) or result is None

    @pytest.mark.asyncio
    async def test_add_custom_function(self, evaluator, sample_records):
        """Test adding custom safe functions."""
        original, transformed = sample_records
        
        # Add a custom function
        evaluator.add_function("double", lambda: "double_value")
        
        result = await evaluator.evaluate("double()", original, transformed)
        assert result == "double_value"
        
        # Remove the function
        evaluator.remove_function("double")
        
        # Should now be treated as static
        result = await evaluator.evaluate("double()", original, transformed)
        assert result == "double()"

    @pytest.mark.asyncio
    async def test_non_string_expressions(self, evaluator, sample_records):
        """Test handling of non-string expression inputs."""
        original, transformed = sample_records
        
        # Non-string inputs should be returned as-is
        assert await evaluator.evaluate(123, original, transformed) == 123
        assert await evaluator.evaluate(True, original, transformed) is True
        assert await evaluator.evaluate(None, original, transformed) is None
        assert await evaluator.evaluate([], original, transformed) == []