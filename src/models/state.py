"""Pydantic models for stream state management."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class CursorField(BaseModel):
    """Cursor field definition with type safety."""

    field: str = Field(..., description="Field name for cursor")
    value: Any = Field(..., description="Current cursor value")
    inclusive: bool = Field(True, description="Whether cursor is inclusive")


class StreamCursor(BaseModel):
    """Cursor state for a stream."""

    primary: CursorField = Field(..., description="Primary cursor field")
    tiebreakers: Optional[List[CursorField]] = Field(
        None,
        description="Tiebreaker fields for deterministic ordering (0+ items)",
    )


class HttpConditionals(BaseModel):
    """HTTP conditional headers for efficient fetching."""

    etag: Optional[str] = Field(None, description="Entity tag for resource version")
    last_modified: Optional[str] = Field(None, description="Last-Modified header value")


class PageState(BaseModel):
    """Pagination state for resumable API requests."""

    next_token: Optional[str] = Field(None, description="Next page token")
    request: Optional[Dict[str, Any]] = Field(None, description="Last request parameters")


class StreamStats(BaseModel):
    """Statistics for stream processing."""

    records_synced: int = Field(0, description="Total records synchronized")
    batches_written: int = Field(0, description="Number of batches written")
    last_checkpoint_at: datetime = Field(..., description="Last checkpoint timestamp")
    errors_since_checkpoint: int = Field(0, description="Error count since last checkpoint")


class StreamState(BaseModel):
    """Complete state for a stream (no partitions)."""

    version: int = Field(1, description="State format version")
    stream_id: str = Field(..., description="Stream identifier")
    cursor: StreamCursor = Field(..., description="Current cursor position")
    hwm: str = Field(..., description="High-water mark for this sync run")
    page_state: Optional[PageState] = Field(None, description="Pagination state")
    http_conditionals: Optional[HttpConditionals] = Field(None, description="HTTP conditional headers")
    stats: StreamStats = Field(..., description="Processing statistics")
    last_updated: datetime = Field(..., description="When this state was last updated")


class ReplicationConfig(BaseModel):
    """Configuration for incremental replication."""

    replication_method: str = Field("incremental", description="Replication method")
    cursor_field: Optional[str] = Field(None, description="Field for cursor-based replication")
    safety_window_seconds: int = Field(120, description="Safety window for late-arriving data")
    tie_breaker_fields: Optional[List[str]] = Field(None, description="Fields for deterministic ordering")
    primary_key: Optional[List[str]] = Field(None, description="Primary key fields for record identification")

    @field_validator("replication_method")
    @classmethod
    def validate_replication_method(cls, v):
        if v not in ["full", "incremental"]:
            raise ValueError("replication_method must be 'full' or 'incremental'")
        return v

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


class SourceConfig(BaseModel):
    """Source configuration with validation."""

    endpoint_id: str = Field(..., description="UUID of endpoint schema")
    host_id: str = Field(..., description="UUID of host credentials")
    replication_method: str = Field("incremental", description="Replication method")
    replication_key: Optional[str] = Field(None, description="Field for incremental sync")
    safety_window_seconds: int = Field(120, description="Safety window in seconds")

    @field_validator("replication_method")
    @classmethod
    def validate_replication_method(cls, v):
        if v not in ["full", "incremental"]:
            raise ValueError("replication_method must be 'full' or 'incremental'")
        return v


class DestinationConfig(BaseModel):
    """Destination configuration with validation."""

    endpoint_id: str = Field(..., description="UUID of endpoint schema")
    host_id: str = Field(..., description="UUID of host credentials")
    refresh_mode: str = Field("upsert", description="Write strategy")
    batch_support: bool = Field(False, description="Whether destination supports batching")
    batch_size: int = Field(1, description="Batch size for destination")

    @field_validator("refresh_mode")
    @classmethod
    def validate_refresh_mode(cls, v):
        if v not in ["insert", "upsert", "truncate_insert"]:
            raise ValueError("refresh_mode must be 'insert', 'upsert', or 'truncate_insert'")
        return v


