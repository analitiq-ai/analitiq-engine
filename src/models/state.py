"""Dataclass models for stream state management."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


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
    tiebreakers: Optional[List[CursorField]] = None


@dataclass
class HttpConditionals:
    """HTTP conditional headers for efficient fetching."""
    etag: Optional[str] = None
    last_modified: Optional[str] = None


@dataclass
class PageState:
    """Pagination state for resumable API requests."""
    next_token: Optional[str] = None
    request: Optional[Dict[str, Any]] = None


@dataclass
class StreamStats:
    """Statistics for stream processing."""
    records_synced: int = 0
    batches_written: int = 0
    last_checkpoint_at: Optional[datetime] = None
    errors_since_checkpoint: int = 0


@dataclass
class StreamState:
    """Complete state for a stream (no partitions)."""
    version: int = 1
    stream_id: str = ""
    cursor: Optional[StreamCursor] = None
    hwm: str = ""
    page_state: Optional[PageState] = None
    http_conditionals: Optional[HttpConditionals] = None
    stats: Optional[StreamStats] = None
    last_updated: Optional[datetime] = None


@dataclass
class ReplicationConfig:
    """Configuration for incremental replication."""
    replication_method: str = "incremental"
    cursor_field: Optional[str] = None
    safety_window_seconds: int = 120
    tie_breaker_fields: Optional[List[str]] = None
    primary_key: Optional[List[str]] = None

    @classmethod
    def get_replication_field_names(cls) -> List[str]:
        """Get list of field names that should be copied from stream config to source config."""
        return [
            "cursor_field",
            "safety_window_seconds",
            "replication_method",
            "tie_breaker_fields",
            "primary_key",
        ]


@dataclass
class SourceConfig:
    """Source configuration."""
    endpoint_id: str = ""
    host_id: str = ""
    replication_method: str = "incremental"
    replication_key: Optional[str] = None
    safety_window_seconds: int = 120


@dataclass
class DestinationConfig:
    """Destination configuration."""
    endpoint_id: str = ""
    host_id: str = ""
    refresh_mode: str = "upsert"
    batch_support: bool = False
    batch_size: int = 1
