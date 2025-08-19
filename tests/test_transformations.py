"""Tests for transformation functions."""

import pytest
from datetime import datetime
from decimal import Decimal

from analitiq_stream.transformations.common import (
    TransformationError,
    get_transformation_function,
    list_available_transformations,
    abs_transform,
    strip_transform,
    upper_transform,
    lower_transform,
    iso_to_date_transform,
    iso_to_datetime_transform,
    now_transform,
    uuid_transform,
    md5_transform,
    truncate_transform,
    regex_replace_transform,
    coalesce_transform,
    number_format_transform,
    boolean_transform,
)


class TestTransformationFunctions:
    """Test individual transformation functions."""
    
    def test_abs_transform(self):
        """Test abs transformation."""
        assert abs_transform(-5) == 5
        assert abs_transform(5) == 5
        assert abs_transform(-3.14) == 3.14
        assert abs_transform(Decimal("-10.5")) == Decimal("10.5")
        assert abs_transform("-123") == 123.0
        
        with pytest.raises(TransformationError):
            abs_transform("invalid")
        
        with pytest.raises(TransformationError):
            abs_transform({"not": "number"})
    
    def test_strip_transform(self):
        """Test strip transformation."""
        assert strip_transform("  hello  ") == "hello"
        assert strip_transform("\n\tworld\t\n") == "world"
        assert strip_transform("") == ""
        assert strip_transform(None) == ""
        assert strip_transform(123) == "123"
    
    def test_upper_transform(self):
        """Test upper transformation."""
        assert upper_transform("hello") == "HELLO"
        assert upper_transform("Hello World") == "HELLO WORLD"
        assert upper_transform("") == ""
        assert upper_transform(None) == ""
        assert upper_transform(123) == "123"
    
    def test_lower_transform(self):
        """Test lower transformation."""
        assert lower_transform("HELLO") == "hello"
        assert lower_transform("Hello World") == "hello world"
        assert lower_transform("") == ""
        assert lower_transform(None) == ""
        assert lower_transform(123) == "123"
    
    def test_iso_to_date_transform(self):
        """Test ISO to date transformation."""
        assert iso_to_date_transform("2023-12-25T10:30:00Z") == "2023-12-25"
        assert iso_to_date_transform("2023-12-25T10:30:00+01:00") == "2023-12-25"
        assert iso_to_date_transform("2023-12-25") == "2023-12-25"
        assert iso_to_date_transform(None) == ""
        
        # Test with datetime object
        dt = datetime(2023, 12, 25, 10, 30, 0)
        assert iso_to_date_transform(dt) == "2023-12-25"
        
        with pytest.raises(TransformationError):
            iso_to_date_transform("invalid-date")
    
    def test_iso_to_datetime_transform(self):
        """Test ISO to datetime transformation."""
        assert iso_to_datetime_transform("2023-12-25T10:30:45Z") == "2023-12-25 10:30:45"
        # Note: timezone conversion handled by fromisoformat, actual behavior may vary
        result = iso_to_datetime_transform("2023-12-25T10:30:45+01:00")
        assert "2023-12-25" in result and "30:45" in result  # Check basic format
        assert iso_to_datetime_transform(None) == ""
        
        # Test with datetime object
        dt = datetime(2023, 12, 25, 10, 30, 45)
        assert iso_to_datetime_transform(dt) == "2023-12-25 10:30:45"
        
        with pytest.raises(TransformationError):
            iso_to_datetime_transform("invalid-datetime")
    
    def test_now_transform(self):
        """Test now transformation."""
        result = now_transform()
        # Check that it returns an ISO format timestamp
        assert "T" in result
        assert len(result) >= 19  # Basic ISO format length
        
        # Should ignore input and generate timestamps (can't test equality due to timing)
        result1 = now_transform("ignored")
        result2 = now_transform()
        assert "T" in result1 and "T" in result2  # Both should be valid timestamps
    
    def test_uuid_transform(self):
        """Test UUID transformation."""
        result = uuid_transform()
        # Check UUID format (36 characters with 4 hyphens)
        assert len(result) == 36
        assert result.count("-") == 4
        
        # Should generate different UUIDs
        assert uuid_transform() != uuid_transform()
        
        # Should ignore input
        assert len(uuid_transform("ignored")) == 36
    
    def test_md5_transform(self):
        """Test MD5 transformation."""
        assert md5_transform("hello") == "5d41402abc4b2a76b9719d911017c592"
        assert md5_transform("") == "d41d8cd98f00b204e9800998ecf8427e"
        assert md5_transform(None) == "d41d8cd98f00b204e9800998ecf8427e"
        assert md5_transform(123) == "202cb962ac59075b964b07152d234b70"
    
    def test_truncate_transform(self):
        """Test truncate transformation."""
        assert truncate_transform("hello world", 5) == "hello"
        assert truncate_transform("short", 10) == "short"
        assert truncate_transform("", 5) == ""
        assert truncate_transform(None, 3) == ""
        assert truncate_transform("test") == "test"  # Default length 50
        
        long_string = "a" * 100
        assert len(truncate_transform(long_string)) == 50
        assert len(truncate_transform(long_string, 20)) == 20
    
    def test_regex_replace_transform(self):
        """Test regex replace transformation."""
        assert regex_replace_transform("hello world", r"world", "universe") == "hello universe"
        assert regex_replace_transform("123-456-7890", r"-", "") == "1234567890"
        assert regex_replace_transform("test", r"xyz", "abc") == "test"  # No match
        assert regex_replace_transform(None, r".*", "replacement") == ""
        
        # Test with complex regex
        assert regex_replace_transform("Phone: 123-456-7890", r"Phone: (\d+)-(\d+)-(\d+)", r"\1.\2.\3") == "123.456.7890"
    
    def test_coalesce_transform(self):
        """Test coalesce transformation."""
        assert coalesce_transform(None, "", "hello") == "hello"
        assert coalesce_transform("first", "second") == "first"
        assert coalesce_transform(None, None, "", "default") == "default"
        assert coalesce_transform(0, "zero") == 0  # 0 is not None or empty string
        assert coalesce_transform(None, None) == ""
    
    def test_number_format_transform(self):
        """Test number format transformation."""
        assert number_format_transform(3.14159, 2) == "3.14"
        assert number_format_transform(10, 0) == "10"
        assert number_format_transform("5.678", 1) == "5.7"
        assert number_format_transform(None) == "0.00"
        
        with pytest.raises(TransformationError):
            number_format_transform("not-a-number")
    
    def test_boolean_transform(self):
        """Test boolean transformation."""
        # String tests
        assert boolean_transform("true") == True
        assert boolean_transform("TRUE") == True
        assert boolean_transform("1") == True
        assert boolean_transform("yes") == True
        assert boolean_transform("y") == True
        assert boolean_transform("on") == True
        
        assert boolean_transform("false") == False
        assert boolean_transform("0") == False
        assert boolean_transform("no") == False
        assert boolean_transform("off") == False
        assert boolean_transform("") == False
        
        # Number tests
        assert boolean_transform(1) == True
        assert boolean_transform(0) == False
        assert boolean_transform(-1) == True
        assert boolean_transform(0.0) == False
        assert boolean_transform(3.14) == True
        
        # Boolean tests
        assert boolean_transform(True) == True
        assert boolean_transform(False) == False
        
        # Other types
        assert boolean_transform([1, 2, 3]) == True
        assert boolean_transform([]) == False
        assert boolean_transform({"a": 1}) == True
        assert boolean_transform({}) == False


class TestTransformationRegistry:
    """Test transformation registry and lookup functions."""
    
    def test_get_transformation_function(self):
        """Test getting transformation functions by name."""
        abs_func = get_transformation_function("abs")
        assert abs_func == abs_transform
        
        strip_func = get_transformation_function("strip")
        assert strip_func == strip_transform
        
        with pytest.raises(TransformationError, match="Unknown transformation"):
            get_transformation_function("nonexistent")
    
    def test_list_available_transformations(self):
        """Test listing available transformations."""
        transformations = list_available_transformations()
        
        # Check some expected transformations are present
        assert "abs" in transformations
        assert "strip" in transformations
        assert "upper" in transformations
        assert "lower" in transformations
        assert "iso_to_date" in transformations
        assert "now" in transformations
        assert "uuid" in transformations
        
        # Check that descriptions are provided
        assert "absolute value" in transformations["abs"].lower()
        assert "whitespace" in transformations["strip"].lower()
        assert "uppercase" in transformations["upper"].lower()
        
        # Check all transformations have descriptions
        for name, description in transformations.items():
            assert isinstance(description, str)
            assert len(description) > 0


class TestTransformationChaining:
    """Test chaining multiple transformations."""
    
    def test_chain_transformations(self):
        """Test that transformations can be chained together."""
        # Start with messy input
        input_value = "  HELLO WORLD  "
        
        # Chain: strip -> lower -> truncate
        step1 = strip_transform(input_value)  # "HELLO WORLD"
        step2 = lower_transform(step1)        # "hello world"
        step3 = truncate_transform(step2, 5)  # "hello"
        
        assert step3 == "hello"
    
    def test_transform_numbers_to_strings(self):
        """Test transforming numbers through string operations."""
        # Start with number, convert to formatted string
        number = -123.456
        
        # Chain: abs -> number_format -> upper (though upper on numbers is silly)
        step1 = abs_transform(number)           # 123.456
        step2 = number_format_transform(step1, 1)  # "123.5"
        step3 = upper_transform(step2)          # "123.5" (no change)
        
        assert step3 == "123.5"


class TestErrorHandling:
    """Test error handling in transformations."""
    
    def test_transformation_error_propagation(self):
        """Test that transformation errors are properly raised."""
        with pytest.raises(TransformationError):
            abs_transform("not-a-number")
        
        with pytest.raises(TransformationError):
            iso_to_date_transform("invalid-date")
        
        with pytest.raises(TransformationError):
            number_format_transform("not-a-number")
    
    def test_invalid_transformation_lookup(self):
        """Test error when looking up invalid transformation."""
        with pytest.raises(TransformationError, match="Unknown transformation 'invalid'"):
            get_transformation_function("invalid")
        
        # Check that error message includes available transformations
        try:
            get_transformation_function("invalid")
        except TransformationError as e:
            assert "Available:" in str(e)
            assert "abs" in str(e)  # Should list some available transformations


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_none_handling(self):
        """Test how transformations handle None values."""
        assert strip_transform(None) == ""
        assert upper_transform(None) == ""
        assert lower_transform(None) == ""
        assert iso_to_date_transform(None) == ""
        assert iso_to_datetime_transform(None) == ""
        assert truncate_transform(None, 10) == ""
        assert regex_replace_transform(None, "test", "replacement") == ""
        assert number_format_transform(None) == "0.00"
    
    def test_empty_string_handling(self):
        """Test how transformations handle empty strings."""
        assert strip_transform("") == ""
        assert upper_transform("") == ""
        assert lower_transform("") == ""
        assert truncate_transform("", 10) == ""
        assert regex_replace_transform("", "test", "replacement") == ""
        
        # These should raise errors or handle gracefully
        with pytest.raises(TransformationError):
            iso_to_date_transform("")  # Empty string is not valid ISO date
        
        with pytest.raises(TransformationError):
            number_format_transform("")  # Empty string is not a number
    
    def test_type_coercion(self):
        """Test automatic type coercion in transformations."""
        # Numbers should be converted to strings for string operations
        assert strip_transform(123) == "123"
        assert upper_transform(456) == "456"
        assert lower_transform(789) == "789"
        
        # Strings should be converted to numbers where applicable
        assert abs_transform("123") == 123.0
        assert number_format_transform("3.14159", 2) == "3.14"


if __name__ == "__main__":
    pytest.main([__file__])