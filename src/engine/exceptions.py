"""Custom exceptions for the streaming engine."""

from cdk.types import FailureCategory

from .batch_policy import ErrorStrategy


class StreamProcessingError(Exception):
    """Exception for stream processing errors with context.

    ``failure_category`` carries the destination's machine-readable failure
    category from the batch ack into classification (issue #351): the raise
    site that consumed the ack stamps it here so
    ``classify_destination_failure`` reads the declared category instead of
    re-deriving it from the message text. UNSPECIFIED means nothing was
    declared and classification falls back to text matching.
    """

    def __init__(
        self,
        message: str,
        stream_id: str | None = None,
        original_error: Exception | None = None,
        failure_category: FailureCategory = (
            FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
        ),
    ):
        self.stream_id = stream_id
        self.original_error = original_error
        self.failure_category = failure_category
        super().__init__(message)

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.stream_id:
            base_msg = f"Stream {self.stream_id}: {base_msg}"
        if self.original_error:
            base_msg = f"{base_msg} (caused by: {self.original_error})"
        return base_msg


class TransformationError(StreamProcessingError):
    """Exception for data transformation errors."""

    pass


class ValidationFailure(TransformationError):
    """A batch with rows that fail a validation rule.

    Raised by :meth:`src.engine.mapping.CompiledTransform.run` only when the
    batch is otherwise sound -- every column built and converted, every
    non-nullable column filled. ``strategy`` is the strictest effective
    strategy among the rules that failed, and is what the stream disposes of
    the batch under.
    """

    def __init__(self, message: str, *, strategy: ErrorStrategy) -> None:
        super().__init__(message)
        self.strategy = strategy


class ConfigurationError(Exception):
    """Exception for pipeline configuration errors."""
