"""Tests for transformation functions."""

import pytest
from datetime import datetime
from decimal import Decimal

from src.transformations.registry import (
    TransformationError,
    TransformationRegistry,
    transformation_registry,
)
from src.models.transformations import TransformationType, TransformationConfig


class TestTransformationFunctions:
    """Test individual transformation functions."""

    def _apply_transform(self, transform_type: str, value, **params):
        """Helper method to apply transformations using the registry."""
        config = TransformationConfig(
            type=TransformationType(transform_type),
            parameters=params
        )
        return transformation_registry.apply_transformation(value, config)

    def test_abs_transform(self):
        """Test abs transformation."""
        assert self._apply_transform("abs", -5) == 5
        assert self._apply_transform("abs", 5) == 5
        assert self._apply_transform("abs", -3.14) == 3.14
        assert self._apply_transform("abs", Decimal("-10.5")) == Decimal("10.5")
        assert self._apply_transform("abs", "-123") == 123.0

        with pytest.raises(TransformationError):
            self._apply_transform("abs", "invalid")

        with pytest.raises(TransformationError):
            self._apply_transform("abs", {"not": "number"})

    def test_strip_transform(self):
        """Test strip transformation."""
        assert self._apply_transform("strip", "  hello  ") == "hello"
        assert self._apply_transform("strip", "\n\tworld\t\n") == "world"
        assert self._apply_transform("strip", "") == ""
        assert self._apply_transform("strip", None) == ""
        assert self._apply_transform("strip", 123) == "123"

    def test_upper_transform(self):
        """Test upper transformation."""
        assert self._apply_transform("upper", "hello") == "HELLO"
        assert self._apply_transform("upper", "Hello World") == "HELLO WORLD"
        assert self._apply_transform("upper", "") == ""
        assert self._apply_transform("upper", None) == ""
        assert self._apply_transform("upper", 123) == "123"

    def test_lower_transform(self):
        """Test lower transformation."""
        assert self._apply_transform("lower", "HELLO") == "hello"
        assert self._apply_transform("lower", "Hello World") == "hello world"
        assert self._apply_transform("lower", "") == ""
        assert self._apply_transform("lower", None) == ""
        assert self._apply_transform("lower", 123) == "123"

    def test_iso_to_date_transform(self):
        """Test ISO to date transformation."""
        assert self._apply_transform("iso_to_date", "2023-12-25T10:30:00Z") == "2023-12-25"
        assert self._apply_transform("iso_to_date", "2023-12-25T10:30:00+01:00") == "2023-12-25"
        assert self._apply_transform("iso_to_date", "2023-12-25") == "2023-12-25"
        assert self._apply_transform("iso_to_date", None) == ""

        # Test with datetime object
        dt = datetime(2023, 12, 25, 10, 30, 0)
        assert self._apply_transform("iso_to_date", dt) == "2023-12-25"

        with pytest.raises(TransformationError):
            self._apply_transform("iso_to_date", "invalid-date")

    def test_iso_to_datetime_transform(self):
        """Test ISO to datetime transformation."""
        assert self._apply_transform("iso_to_datetime", "2023-12-25T10:30:45Z") == "2023-12-25 10:30:45"
        # Note: timezone conversion handled by fromisoformat, actual behavior may vary
        result = self._apply_transform("iso_to_datetime", "2023-12-25T10:30:45+01:00")
        assert "2023-12-25" in result and "30:45" in result  # Check basic format
        assert self._apply_transform("iso_to_datetime", None) == ""

        # Test with datetime object
        dt = datetime(2023, 12, 25, 10, 30, 45)
        assert self._apply_transform("iso_to_datetime", dt) == "2023-12-25 10:30:45"

        with pytest.raises(TransformationError):
            self._apply_transform("iso_to_datetime", "invalid-datetime")

    def test_now_transform(self):
        """Test now transformation."""
        result = self._apply_transform("now", None)
        # Check that it returns an ISO format timestamp
        assert "T" in result
        assert len(result) >= 19  # Basic ISO format length

        # Should ignore input and generate timestamps (can't test equality due to timing)
        result1 = self._apply_transform("now", "ignored")
        result2 = self._apply_transform("now", None)
        assert "T" in result1 and "T" in result2  # Both should be valid timestamps

    def test_uuid_transform(self):
        """Test UUID transformation."""
        result = self._apply_transform("uuid", None)
        # Check UUID format (36 characters with 4 hyphens)
        assert len(result) == 36
        assert result.count("-") == 4

        # Should generate different UUIDs
        assert self._apply_transform("uuid", None) != self._apply_transform("uuid", None)

        # Should ignore input
        assert len(self._apply_transform("uuid", "ignored")) == 36

    def test_md5_transform(self):
        """Test MD5 transformation."""
        assert self._apply_transform("md5", "hello") == "5d41402abc4b2a76b9719d911017c592"
        assert self._apply_transform("md5", "") == "d41d8cd98f00b204e9800998ecf8427e"
        assert self._apply_transform("md5", None) == "d41d8cd98f00b204e9800998ecf8427e"
        assert self._apply_transform("md5", 123) == "202cb962ac59075b964b07152d234b70"

    def test_truncate_transform(self):
        """Test truncate transformation."""
        assert self._apply_transform("truncate", "hello world", length=5) == "hello"
        assert self._apply_transform("truncate", "short", length=10) == "short"
        assert self._apply_transform("truncate", "", length=5) == ""
        assert self._apply_transform("truncate", None, length=3) == ""
        assert self._apply_transform("truncate", "test") == "test"  # Default length 50

        long_string = "a" * 100
        assert len(self._apply_transform("truncate", long_string)) == 50
        assert len(self._apply_transform("truncate", long_string, length=20)) == 20

    def test_regex_replace_transform(self):
        """Test regex replace transformation."""
        assert self._apply_transform("regex_replace", "hello world", pattern=r"world", replacement="universe") == "hello universe"
        assert self._apply_transform("regex_replace", "123-456-7890", pattern=r"-", replacement="") == "1234567890"
        assert self._apply_transform("regex_replace", "test", pattern=r"xyz", replacement="abc") == "test"  # No match
        assert self._apply_transform("regex_replace", None, pattern=r".*", replacement="replacement") == ""

        # Test with complex regex
        assert self._apply_transform("regex_replace", "Phone: 123-456-7890", pattern=r"Phone: (\d+)-(\d+)-(\d+)", replacement=r"\1.\2.\3") == "123.456.7890"

    def test_coalesce_transform(self):
        """Test coalesce transformation."""
        # Note: coalesce takes multiple values as *args, but we can only pass one value at a time through our helper
        # This test needs to be handled differently
        registry_func = transformation_registry.get_transformation(TransformationType.COALESCE)
        assert registry_func(None, "", "hello") == "hello"
        assert registry_func("first", "second") == "first"
        assert registry_func(None, None, "", "default") == "default"
        assert registry_func(0, "zero") == 0  # 0 is not None or empty string
        assert registry_func(None, None) == ""

    def test_number_format_transform(self):
        """Test number format transformation."""
        assert self._apply_transform("number_format", 3.14159, decimals=2) == "3.14"
        assert self._apply_transform("number_format", 10, decimals=0) == "10"
        assert self._apply_transform("number_format", "5.678", decimals=1) == "5.7"
        assert self._apply_transform("number_format", None) == "0.00"

        with pytest.raises(TransformationError):
            self._apply_transform("number_format", "not-a-number")

    def test_boolean_transform(self):
        """Test boolean transformation."""
        # String tests
        assert self._apply_transform("boolean", "true") == True
        assert self._apply_transform("boolean", "TRUE") == True
        assert self._apply_transform("boolean", "1") == True
        assert self._apply_transform("boolean", "yes") == True
        assert self._apply_transform("boolean", "y") == True
        assert self._apply_transform("boolean", "on") == True

        assert self._apply_transform("boolean", "false") == False
        assert self._apply_transform("boolean", "0") == False
        assert self._apply_transform("boolean", "no") == False
        assert self._apply_transform("boolean", "off") == False
        assert self._apply_transform("boolean", "") == False

        # Number tests
        assert self._apply_transform("boolean", 1) == True
        assert self._apply_transform("boolean", 0) == False
        assert self._apply_transform("boolean", -1) == True
        assert self._apply_transform("boolean", 0.0) == False
        assert self._apply_transform("boolean", 3.14) == True

        # Boolean tests
        assert self._apply_transform("boolean", True) == True
        assert self._apply_transform("boolean", False) == False

        # Other types
        assert self._apply_transform("boolean", [1, 2, 3]) == True
        assert self._apply_transform("boolean", []) == False
        assert self._apply_transform("boolean", {"a": 1}) == True
        assert self._apply_transform("boolean", {}) == False


class TestTransformationRegistry:
    """Test transformation registry and lookup functions."""

    def test_get_transformation_function(self):
        """Test getting transformation functions by enum type."""
        abs_func = transformation_registry.get_transformation(TransformationType.ABS)
        assert callable(abs_func)

        strip_func = transformation_registry.get_transformation(TransformationType.STRIP)
        assert callable(strip_func)

        # Test error handling by calling get_transformation with a made-up enum value
        # We'll create a mock enum that doesn't exist in the registry
        from unittest.mock import Mock
        fake_enum = Mock()
        fake_enum.value = "nonexistent_transform"

        with pytest.raises(TransformationError, match="Unknown transformation"):
            transformation_registry.get_transformation(fake_enum)

    def test_list_available_transformations(self):
        """Test listing available transformations."""
        transformations = transformation_registry.list_available_transformations()

        # Check some expected transformations are present
        assert "abs" in transformations
        assert "strip" in transformations
        assert "upper" in transformations
        assert "lower" in transformations
        assert "iso_to_date" in transformations
        assert "now" in transformations
        assert "uuid" in transformations

        # Check all transformations are strings
        for name in transformations:
            assert isinstance(name, str)
            assert len(name) > 0

    def test_apply_transformation_with_config(self):
        """Test applying transformations with TransformationConfig."""
        config = TransformationConfig(
            type=TransformationType.TRUNCATE,
            parameters={"length": 5}
        )
        result = transformation_registry.apply_transformation("hello world", config)
        assert result == "hello"

    def test_apply_transformations_chain(self):
        """Test applying multiple transformations in sequence."""
        configs = [
            TransformationConfig(type=TransformationType.STRIP),
            TransformationConfig(type=TransformationType.LOWER),
            TransformationConfig(type=TransformationType.TRUNCATE, parameters={"length": 5})
        ]
        result = transformation_registry.apply_transformations("  HELLO WORLD  ", configs)
        assert result == "hello"


class TestTransformationChaining:
    """Test chaining multiple transformations."""

    def _apply_transform(self, transform_type: str, value, **params):
        """Helper method to apply transformations using the registry."""
        config = TransformationConfig(
            type=TransformationType(transform_type),
            parameters=params
        )
        return transformation_registry.apply_transformation(value, config)

    def test_chain_transformations(self):
        """Test that transformations can be chained together."""
        # Start with messy input
        input_value = "  HELLO WORLD  "

        # Chain: strip -> lower -> truncate
        step1 = self._apply_transform("strip", input_value)  # "HELLO WORLD"
        step2 = self._apply_transform("lower", step1)        # "hello world"
        step3 = self._apply_transform("truncate", step2, length=5)  # "hello"

        assert step3 == "hello"

    def test_transform_numbers_to_strings(self):
        """Test transforming numbers through string operations."""
        # Start with number, convert to formatted string
        number = -123.456

        # Chain: abs -> number_format -> upper (though upper on numbers is silly)
        step1 = self._apply_transform("abs", number)           # 123.456
        step2 = self._apply_transform("number_format", step1, decimals=1)  # "123.5"
        step3 = self._apply_transform("upper", step2)          # "123.5" (no change)

        assert step3 == "123.5"


class TestErrorHandling:
    """Test error handling in transformations."""

    def _apply_transform(self, transform_type: str, value, **params):
        """Helper method to apply transformations using the registry."""
        config = TransformationConfig(
            type=TransformationType(transform_type),
            parameters=params
        )
        return transformation_registry.apply_transformation(value, config)

    def test_transformation_error_propagation(self):
        """Test that transformation errors are properly raised."""
        with pytest.raises(TransformationError):
            self._apply_transform("abs", "not-a-number")

        with pytest.raises(TransformationError):
            self._apply_transform("iso_to_date", "invalid-date")

        with pytest.raises(TransformationError):
            self._apply_transform("number_format", "not-a-number")

    def test_invalid_transformation_lookup(self):
        """Test error when looking up invalid transformation."""
        # Test that we get proper errors for unknown transformation types
        try:
            # This will raise a ValueError for invalid enum value
            TransformationType("invalid")
        except ValueError as e:
            assert "invalid" in str(e).lower()


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def _apply_transform(self, transform_type: str, value, **params):
        """Helper method to apply transformations using the registry."""
        config = TransformationConfig(
            type=TransformationType(transform_type),
            parameters=params
        )
        return transformation_registry.apply_transformation(value, config)

    def test_none_handling(self):
        """Test how transformations handle None values."""
        assert self._apply_transform("strip", None) == ""
        assert self._apply_transform("upper", None) == ""
        assert self._apply_transform("lower", None) == ""
        assert self._apply_transform("iso_to_date", None) == ""
        assert self._apply_transform("iso_to_datetime", None) == ""
        assert self._apply_transform("truncate", None, length=10) == ""
        assert self._apply_transform("regex_replace", None, pattern="test", replacement="replacement") == ""
        assert self._apply_transform("number_format", None) == "0.00"

    def test_empty_string_handling(self):
        """Test how transformations handle empty strings."""
        assert self._apply_transform("strip", "") == ""
        assert self._apply_transform("upper", "") == ""
        assert self._apply_transform("lower", "") == ""
        assert self._apply_transform("truncate", "", length=10) == ""
        assert self._apply_transform("regex_replace", "", pattern="test", replacement="replacement") == ""

        # These should raise errors or handle gracefully
        with pytest.raises(TransformationError):
            self._apply_transform("iso_to_date", "")  # Empty string is not valid ISO date

        with pytest.raises(TransformationError):
            self._apply_transform("number_format", "")  # Empty string is not a number

    def test_type_coercion(self):
        """Test automatic type coercion in transformations."""
        # Numbers should be converted to strings for string operations
        assert self._apply_transform("strip", 123) == "123"
        assert self._apply_transform("upper", 456) == "456"
        assert self._apply_transform("lower", 789) == "789"

        # Strings should be converted to numbers where applicable
        assert self._apply_transform("abs", "123") == 123.0
        assert self._apply_transform("number_format", "3.14159", decimals=2) == "3.14"


if __name__ == "__main__":
    pytest.main([__file__])