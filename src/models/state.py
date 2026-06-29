"""Dataclass models for stream state management."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class CursorField:
    """Cursor field definition with type safety."""

    field: str = ""
    value: Any = None
    inclusive: bool = True


@dataclass
class StreamCursor:
    """Cursor state for a stream."""

    primary: CursorField = field(default_factory=CursorField)
    tiebreakers: list[CursorField] | None = None


@dataclass
class HttpConditionals:
    """HTTP conditional headers for efficient fetching."""

    etag: str | None = None
    last_modified: str | None = None


@dataclass
class PageState:
    """Pagination state for resumable API requests."""

    next_token: str | None = None
    request: dict[str, Any] | None = None


@dataclass
class StreamStats:
    """Statistics for stream processing."""

    records_synced: int = 0
    batches_written: int = 0
    last_checkpoint_at: datetime | None = None
    errors_since_checkpoint: int = 0


@dataclass
class StreamState:
    """Complete state for a stream (no partitions)."""

    version: int = 1
    stream_id: str = ""
    cursor: StreamCursor | None = None
    hwm: str = ""
    page_state: PageState | None = None
    http_conditionals: HttpConditionals | None = None
    stats: StreamStats | None = None
    last_updated: datetime | None = None


@dataclass
class ReplicationConfig:
    """Configuration for incremental replication."""

    replication_method: str = "incremental"
    cursor_field: str | None = None
    safety_window_seconds: int = 120
    tie_breaker_fields: list[str] | None = None
    primary_key: list[str] | None = None

    @classmethod
    def get_replication_field_names(cls) -> list[str]:
        """Field names that should be copied from stream config to source config."""
        return [
            "cursor_field",
            "safety_window_seconds",
            "replication_method",
            "tie_breaker_fields",
            "primary_key",
        ]
