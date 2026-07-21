"""Custom exceptions for the streaming engine."""

from cdk.types import FailureCategory


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


class ConnectorError(StreamProcessingError):
    """Exception for connector-related errors."""

    pass


class ConfigurationError(Exception):
    """Base exception for pipeline configuration errors."""

    def __init__(
        self,
        message: str,
        field_path: str | None = None,
        validation_errors: list[str] | None = None,
    ):
        self.field_path = field_path
        self.validation_errors = validation_errors or []
        super().__init__(message)


class StreamConfigurationError(ConfigurationError):
    """Exception for stream-specific configuration errors."""

    def __init__(
        self,
        message: str,
        stream_id: str | None = None,
        field_path: str | None = None,
        validation_errors: list[str] | None = None,
    ):
        self.stream_id = stream_id
        super().__init__(message, field_path, validation_errors)


class PipelineValidationError(ConfigurationError):
    """Exception for pipeline-level validation failures."""

    def __init__(self, message: str, errors: dict[str, list[str]] | None = None):
        self.errors = errors or {}
        super().__init__(message)


class StreamExecutionError(StreamProcessingError):
    """Exception for stream execution failures with detailed context."""

    def __init__(
        self,
        message: str,
        stream_id: str | None = None,
        stage: str | None = None,
        batch_id: int | None = None,
        original_error: Exception | None = None,
    ):
        self.stage = stage
        self.batch_id = batch_id
        super().__init__(message, stream_id, original_error)

    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.stage:
            base_msg = f"{base_msg} [Stage: {self.stage}]"
        if self.batch_id is not None:
            base_msg = f"{base_msg} [Batch: {self.batch_id}]"
        return base_msg


class StageConfigurationError(ConfigurationError):
    """Exception for pipeline stage configuration errors."""

    def __init__(self, message: str, stage_name: str, stream_id: str | None = None):
        self.stage_name = stage_name
        self.stream_id = stream_id
        super().__init__(message)
