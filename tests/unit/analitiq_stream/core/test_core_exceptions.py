"""Unit tests for core exceptions module."""


from src.engine.exceptions import (
    ConfigurationError,
    StreamProcessingError,
    TransformationError,
)


class TestStreamProcessingError:
    """Test StreamProcessingError exception."""

    def test_basic_exception(self):
        """Test basic exception creation and message."""
        error = StreamProcessingError("Test error")

        assert str(error) == "Test error"
        assert error.stream_id is None
        assert error.original_error is None

    def test_exception_with_stream_id(self):
        """Test exception with stream ID."""
        error = StreamProcessingError("Test error", stream_id="stream_123")

        assert str(error) == "Stream stream_123: Test error"
        assert error.stream_id == "stream_123"

    def test_exception_with_original_error(self):
        """Test exception with original error."""
        original = ValueError("Original error")
        error = StreamProcessingError("Test error", original_error=original)

        assert str(error) == "Test error (caused by: Original error)"
        assert error.original_error is original

    def test_exception_with_all_fields(self):
        """Test exception with all fields."""
        original = ValueError("Original error")
        error = StreamProcessingError(
            "Test error", stream_id="stream_123", original_error=original
        )

        expected = "Stream stream_123: Test error (caused by: Original error)"
        assert str(error) == expected
        assert error.stream_id == "stream_123"
        assert error.original_error is original

    def test_exception_inheritance(self):
        """Test that StreamProcessingError inherits from Exception."""
        error = StreamProcessingError("Test error")

        assert isinstance(error, Exception)
        assert isinstance(error, StreamProcessingError)


class TestTransformationError:
    """Test TransformationError exception."""

    def test_inheritance(self):
        """Test TransformationError inheritance."""
        error = TransformationError("Transformation failed")

        assert isinstance(error, StreamProcessingError)
        assert isinstance(error, TransformationError)

    def test_with_context(self):
        """Test TransformationError with context."""
        original = TypeError("Invalid type")
        error = TransformationError(
            "Field transformation failed",
            stream_id="transform_stream",
            original_error=original,
        )

        expected = (
            "Stream transform_stream: Field transformation failed "
            "(caused by: Invalid type)"
        )
        assert str(error) == expected


class TestConfigurationError:
    """Test ConfigurationError exception."""

    def test_basic_exception(self):
        """Test basic configuration error."""
        error = ConfigurationError("Invalid configuration")

        assert str(error) == "Invalid configuration"
        assert isinstance(error, Exception)


class TestExceptionInteroperability:
    """Test how exceptions work together and with Python's exception system."""

    def test_exception_chaining(self):
        """Test exception chaining with raise from."""
        original = ValueError("Original issue")

        try:
            try:
                raise original
            except ValueError as e:
                raise StreamProcessingError("Processing failed") from e
        except StreamProcessingError as spe:
            assert spe.__cause__ is original
            assert "Processing failed" in str(spe)

    def test_exception_isinstance_checks(self):
        """Test isinstance checks work correctly."""
        error = TransformationError("Test")

        assert isinstance(error, TransformationError)
        assert isinstance(error, StreamProcessingError)
        assert isinstance(error, Exception)
        assert not isinstance(error, ConfigurationError)

    def test_exception_attributes_preserved(self):
        """Test that exception attributes are preserved when caught."""
        original = RuntimeError("Runtime issue")
        error = TransformationError(
            "Transformation failed",
            stream_id="test_stream",
            original_error=original,
        )

        try:
            raise error
        except TransformationError as caught:
            assert caught.stream_id == "test_stream"
            assert caught.original_error is original

    def test_exception_repr(self):
        """Test exception repr for debugging."""
        error = StreamProcessingError("Test error", stream_id="test")

        repr_str = repr(error)
        assert "StreamProcessingError" in repr_str
        assert "Test error" in repr_str or "test" in repr_str.lower()


class TestExceptionEdgeCases:
    """Test edge cases and special scenarios."""

    def test_empty_message(self):
        """Test exceptions with empty messages."""
        error = StreamProcessingError("")

        assert str(error) == ""
        assert isinstance(error, StreamProcessingError)

    def test_none_message(self):
        """Test exceptions with None message (should be converted to string)."""
        error = StreamProcessingError(None)

        # Exception converts None to "None"
        assert str(error) == "None"

    def test_unicode_message(self):
        """Test exceptions with unicode messages."""
        error = StreamProcessingError("Test with unicode: áéíóú 🚀")

        assert "áéíóú 🚀" in str(error)

    def test_complex_nested_errors(self):
        """Test complex nested error scenarios."""
        # Create a chain of errors
        config_error = ConfigurationError(
            "Config failed"
        )  # ConfigurationError doesn't have original_error param
        stream_error = StreamProcessingError(
            "Stream failed", stream_id="nested", original_error=config_error
        )

        error_str = str(stream_error)
        assert "Stream nested:" in error_str
        assert "Stream failed" in error_str
        assert "Config failed" in error_str
