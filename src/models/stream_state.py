"""Pydantic models for Stream State (STREAM_STATE.yaml specification)."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StreamStatus(str, Enum):
    """Stream execution status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"


class RunOutcome(str, Enum):
    """Pipeline run outcome."""
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIAL = "partial"


class CursorState(BaseModel):
    """Cursor state for incremental replication."""
    model_config = ConfigDict(extra="forbid")

    field_path: List[str] = Field(..., description="Token array path to cursor field")
    mode: str = Field("inclusive", description="Cursor mode (inclusive/exclusive)")
    value: Any = Field(..., description="Last committed cursor value")
    tie_breakers: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Tie-breaker field values for deterministic ordering"
    )

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        if v not in ("inclusive", "exclusive"):
            raise ValueError("mode must be 'inclusive' or 'exclusive'")
        return v


class CheckpointState(BaseModel):
    """Checkpoint state during a run."""
    model_config = ConfigDict(extra="forbid")

    last_checkpoint_at: datetime = Field(..., description="Last checkpoint timestamp")
    records_processed_since_checkpoint: int = Field(
        0,
        ge=0,
        description="Records processed since last checkpoint"
    )
    last_seen_record_key: Optional[List[str]] = Field(
        None,
        description="Primary key of last processed record"
    )


class ReplicationState(BaseModel):
    """Complete replication state for a stream."""
    model_config = ConfigDict(extra="forbid")

    method: str = Field("incremental", description="Replication method")
    cursor: CursorState = Field(..., description="Cursor state")
    safety_window_seconds: int = Field(120, ge=0, description="Safety window for late data")
    checkpoint: Optional[CheckpointState] = Field(None, description="Current checkpoint state")

    @field_validator("method")
    @classmethod
    def validate_method(cls, v: str) -> str:
        if v not in ("incremental", "full"):
            raise ValueError("method must be 'incremental' or 'full'")
        return v


class DestinationCounters(BaseModel):
    """Counters for a destination."""
    model_config = ConfigDict(extra="forbid")

    upserts: int = Field(0, ge=0, description="Successful upsert count")
    inserts: int = Field(0, ge=0, description="Successful insert count")
    updates: int = Field(0, ge=0, description="Successful update count")
    failures: int = Field(0, ge=0, description="Failed record count")
    dlq: int = Field(0, ge=0, description="Records sent to DLQ")


class DestinationDeliveryState(BaseModel):
    """Delivery state for a single destination."""
    model_config = ConfigDict(extra="forbid")

    endpoint_id: str = Field(..., description="Destination endpoint UUID")
    last_success_at: Optional[datetime] = Field(None, description="Last successful write timestamp")
    counters: DestinationCounters = Field(
        default_factory=DestinationCounters,
        description="Delivery counters"
    )


class DeliveryState(BaseModel):
    """Delivery state for all destinations."""
    model_config = ConfigDict(extra="forbid")

    destinations: List[DestinationDeliveryState] = Field(
        default_factory=list,
        description="Per-destination delivery state"
    )


class RetryState(BaseModel):
    """Retry/backoff state for transient failures."""
    model_config = ConfigDict(extra="forbid")

    consecutive_failures: int = Field(0, ge=0, description="Consecutive failure count")
    next_retry_at: Optional[datetime] = Field(None, description="Next scheduled retry time")
    last_error: Optional[str] = Field(None, description="Last error message")
    last_error_at: Optional[datetime] = Field(None, description="Last error timestamp")


class LastRunSummary(BaseModel):
    """Summary of the last pipeline run."""
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(..., description="Unique run identifier")
    started_at: datetime = Field(..., description="Run start timestamp")
    finished_at: Optional[datetime] = Field(None, description="Run finish timestamp")
    outcome: RunOutcome = Field(..., description="Run outcome")
    error_summary: Optional[str] = Field(None, description="Error summary if failed")
    records_processed: int = Field(0, ge=0, description="Total records processed")
    records_failed: int = Field(0, ge=0, description="Total records failed")


class StreamState(BaseModel):
    """
    Complete Stream State model based on STREAM_STATE.yaml specification.

    Tracks the runtime state of a stream including:
    - Current status (idle, running, paused, error)
    - Replication state (cursor, checkpoints)
    - Delivery state (per-destination progress)
    - Retry state (failure tracking)
    - Last run summary
    """
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    version: str = Field("1.0", description="State format version")
    pipeline_id: str = Field(..., description="Parent pipeline identifier")
    stream_id: str = Field(..., description="Stream identifier")

    # Status and control
    status: StreamStatus = Field(StreamStatus.IDLE, description="Current stream status")
    is_enabled: bool = Field(True, description="Whether the stream is enabled")

    # Version tracking
    stream_version: Optional[str] = Field(None, description="Stream config version that produced this state")
    source_endpoint_id: str = Field("", description="Source endpoint UUID")
    destinations_endpoints_ids: List[str] = Field(
        default_factory=list,
        description="Destination endpoint UUIDs"
    )

    # Replication state
    replication_state: Optional[ReplicationState] = Field(None, description="Replication state")

    # Delivery state
    delivery_state: Optional[DeliveryState] = Field(None, description="Delivery state")

    # Retry state
    retry_state: RetryState = Field(default_factory=RetryState, description="Retry/backoff state")

    # Last run summary
    last_run: Optional[LastRunSummary] = Field(None, description="Last run summary")

    def mark_running(self, run_id: str) -> None:
        """Mark stream as running with a new run."""
        self.status = StreamStatus.RUNNING
        self.last_run = LastRunSummary(
            run_id=run_id,
            started_at=datetime.utcnow(),
            outcome=RunOutcome.PARTIAL,  # Will be updated on completion
        )

    def mark_completed(
        self,
        outcome: RunOutcome,
        records_processed: int = 0,
        records_failed: int = 0,
        error_summary: Optional[str] = None
    ) -> None:
        """Mark stream run as completed."""
        self.status = StreamStatus.IDLE if outcome == RunOutcome.SUCCESS else StreamStatus.ERROR
        if self.last_run:
            self.last_run.finished_at = datetime.utcnow()
            self.last_run.outcome = outcome
            self.last_run.records_processed = records_processed
            self.last_run.records_failed = records_failed
            self.last_run.error_summary = error_summary

        if outcome == RunOutcome.SUCCESS:
            self.retry_state.consecutive_failures = 0
            self.retry_state.next_retry_at = None
        else:
            self.retry_state.consecutive_failures += 1
            self.retry_state.last_error = error_summary
            self.retry_state.last_error_at = datetime.utcnow()

    def update_cursor(self, cursor_value: Any, tie_breakers: Optional[List[Dict[str, Any]]] = None) -> None:
        """Update the cursor position."""
        if self.replication_state and self.replication_state.cursor:
            self.replication_state.cursor.value = cursor_value
            if tie_breakers is not None:
                self.replication_state.cursor.tie_breakers = tie_breakers

    def update_checkpoint(self, records_since: int, last_record_key: Optional[List[str]] = None) -> None:
        """Update checkpoint state."""
        if self.replication_state:
            self.replication_state.checkpoint = CheckpointState(
                last_checkpoint_at=datetime.utcnow(),
                records_processed_since_checkpoint=records_since,
                last_seen_record_key=last_record_key,
            )

    def update_destination_counters(
        self,
        endpoint_id: str,
        upserts: int = 0,
        inserts: int = 0,
        updates: int = 0,
        failures: int = 0,
        dlq: int = 0
    ) -> None:
        """Update counters for a specific destination."""
        if not self.delivery_state:
            self.delivery_state = DeliveryState()

        # Find or create destination state
        dest_state = None
        for ds in self.delivery_state.destinations:
            if ds.endpoint_id == endpoint_id:
                dest_state = ds
                break

        if dest_state is None:
            dest_state = DestinationDeliveryState(endpoint_id=endpoint_id)
            self.delivery_state.destinations.append(dest_state)

        # Update counters
        dest_state.counters.upserts += upserts
        dest_state.counters.inserts += inserts
        dest_state.counters.updates += updates
        dest_state.counters.failures += failures
        dest_state.counters.dlq += dlq

        if upserts > 0 or inserts > 0 or updates > 0:
            dest_state.last_success_at = datetime.utcnow()

    @classmethod
    def create_initial(
        cls,
        pipeline_id: str,
        stream_id: str,
        source_endpoint_id: str,
        destination_endpoint_ids: List[str],
        cursor_field_path: List[str],
        initial_cursor_value: Any = None,
        replication_method: str = "incremental",
    ) -> "StreamState":
        """Create initial stream state for a new stream."""
        replication_state = None
        if replication_method == "incremental":
            replication_state = ReplicationState(
                method=replication_method,
                cursor=CursorState(
                    field_path=cursor_field_path,
                    mode="inclusive",  # Always use inclusive mode
                    value=initial_cursor_value,
                ),
            )

        delivery_state = DeliveryState(
            destinations=[
                DestinationDeliveryState(endpoint_id=ep_id)
                for ep_id in destination_endpoint_ids
            ]
        )

        return cls(
            version="1.0",
            pipeline_id=pipeline_id,
            stream_id=stream_id,
            status=StreamStatus.IDLE,
            source_endpoint_id=source_endpoint_id,
            destinations_endpoints_ids=destination_endpoint_ids,
            replication_state=replication_state,
            delivery_state=delivery_state,
        )


