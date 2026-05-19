"""Resolved-runtime types.

Frozen dataclasses produced by `build_resolved_pipeline` from raw JSON. The
engine, the source/destination connectors, and the gRPC layer all consume
these instead of untyped dicts. Field names match the published JSON-Schema
contracts at https://schemas.analitiq.ai/ — no legacy aliases.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class EndpointRef:
    """Reference from a stream into an endpoint definition."""

    scope: Literal["connector", "connection"]
    connection_id: str
    endpoint_id: str

    def __str__(self) -> str:
        return f"{self.scope}:{self.connection_id}/{self.endpoint_id}"


@dataclass(frozen=True)
class Column:
    name: str
    arrow_type: str
    native_type: str | None
    nullable: bool
    default: str | None
    ordinal_position: int | None


@dataclass(frozen=True)
class DatabaseObject:
    schema: str
    name: str
    object_type: str  # table | view | materialized_view


# --- API endpoints --------------------------------------------------------


@dataclass(frozen=True)
class ParamSpec:
    name: str
    location: str  # query | path | header
    type: str
    required: bool
    default: Any
    controlled_by: str | None
    operators: tuple[str, ...]
    format: str | None


@dataclass(frozen=True)
class HttpRequest:
    method: str
    path: str
    query: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, Any] = field(default_factory=dict)
    path_params: dict[str, Any] = field(default_factory=dict)
    body: Any = None


@dataclass(frozen=True)
class OffsetPagination:
    limit_param: str
    offset_param: str
    offset_initial: int = 0
    limit_default: Any = None
    limit_max: int | None = None
    stop_when_empty_ref: str | None = None
    type: Literal["offset"] = "offset"


@dataclass(frozen=True)
class CursorPagination:
    cursor_param: str
    cursor_response_ref: str
    initial_cursor: Any = None
    stop_when_empty_ref: str | None = None
    type: Literal["cursor"] = "cursor"


@dataclass(frozen=True)
class PagePagination:
    page_param: str
    page_initial: int = 1
    size_param: str | None = None
    size_default: Any = None
    stop_when_empty_ref: str | None = None
    type: Literal["page"] = "page"


@dataclass(frozen=True)
class KeysetPagination:
    key_field: str
    key_param: str
    direction: str = "asc"
    stop_when_empty_ref: str | None = None
    type: Literal["keyset"] = "keyset"


@dataclass(frozen=True)
class LinkPagination:
    next_link_ref: str
    stop_when_empty_ref: str | None = None
    type: Literal["link"] = "link"


PaginationSpec = (
    OffsetPagination | CursorPagination | PagePagination | KeysetPagination | LinkPagination
)


@dataclass(frozen=True)
class CursorMapping:
    cursor_field: str
    param: str
    operator: str
    format: str | None


@dataclass(frozen=True)
class Replication:
    supported_methods: tuple[str, ...]
    cursor_mappings: tuple[CursorMapping, ...]


@dataclass(frozen=True)
class ResponseExtraction:
    records_ref: str
    metadata_ref: str | None
    schema: dict | None


@dataclass(frozen=True)
class ApiReadEndpoint:
    endpoint_id: str
    request: HttpRequest
    params: dict[str, ParamSpec]
    pagination: PaginationSpec | None
    replication: Replication
    response: ResponseExtraction
    kind: Literal["api_read"] = "api_read"


@dataclass(frozen=True)
class WriteBatching:
    max_records: int | None


@dataclass(frozen=True)
class ApiWriteEndpoint:
    endpoint_id: str
    write_mode: str  # insert | upsert | update
    request: HttpRequest
    input_schema: dict | None
    batching: WriteBatching | None
    kind: Literal["api_write"] = "api_write"


# --- Database endpoints ---------------------------------------------------


@dataclass(frozen=True)
class DatabaseReadEndpoint:
    endpoint_id: str
    database_object: DatabaseObject
    columns: tuple[Column, ...]
    primary_keys: tuple[str, ...]
    kind: Literal["db_read"] = "db_read"


@dataclass(frozen=True)
class DatabaseWriteEndpoint:
    endpoint_id: str
    database_object: DatabaseObject
    columns: tuple[Column, ...]
    primary_keys: tuple[str, ...]
    kind: Literal["db_write"] = "db_write"


ReadEndpoint = ApiReadEndpoint | DatabaseReadEndpoint
WriteEndpoint = ApiWriteEndpoint | DatabaseWriteEndpoint


# --- Transport / Connector / Connection -----------------------------------


@dataclass(frozen=True)
class HttpTransport:
    base_url: Any  # may contain ${refs}
    headers: dict[str, Any]
    timeout_seconds: int | None
    type: Literal["http"] = "http"


@dataclass(frozen=True)
class SqlAlchemyTransport:
    driver: str
    dsn_template: str
    dsn_bindings: dict[str, dict]
    tls: dict | None
    type: Literal["sqlalchemy"] = "sqlalchemy"


TransportSpec = HttpTransport | SqlAlchemyTransport


@dataclass(frozen=True)
class ResolvedConnector:
    connector_id: str
    kind: str  # api | database | file | stdout
    display_name: str
    default_transport: str
    transports: dict[str, TransportSpec]
    type_map_path: str | None = None  # optional, on disk only


@dataclass(frozen=True)
class ResolvedConnection:
    connection_id: str
    connector: ResolvedConnector
    parameters: dict[str, Any]
    selections: dict[str, Any]
    secret_refs: dict[str, str]
    type_map_path: str | None = None


# --- Stream ---------------------------------------------------------------


@dataclass(frozen=True)
class ReplicationConfig:
    method: str  # full_refresh | incremental
    cursor_field: str | None
    safety_window_seconds: int | None
    tie_breaker_fields: tuple[tuple[str, ...], ...] | None


@dataclass(frozen=True)
class Filter:
    field: str
    op: str
    value: Any


@dataclass(frozen=True)
class ResolvedSource:
    connection: ResolvedConnection
    endpoint: ReadEndpoint
    endpoint_ref: EndpointRef
    replication: ReplicationConfig
    filters: tuple[Filter, ...]
    primary_keys: tuple[str, ...]
    parameters: dict[str, Any]


@dataclass(frozen=True)
class WriteSpec:
    mode: str  # insert | upsert | update
    conflict_keys: tuple[tuple[str, ...], ...] | None


@dataclass(frozen=True)
class ExecutionConfig:
    batch_size: int | None
    max_concurrent_batches: int | None


@dataclass(frozen=True)
class ResolvedDestination:
    connection: ResolvedConnection
    endpoint: WriteEndpoint
    endpoint_ref: EndpointRef
    write: WriteSpec
    execution: ExecutionConfig


@dataclass(frozen=True)
class AssignmentTarget:
    path: str
    arrow_type: str
    nullable: bool


@dataclass(frozen=True)
class Assignment:
    target: AssignmentTarget
    value: dict
    validate: dict | None


@dataclass(frozen=True)
class StreamMapping:
    assignments: tuple[Assignment, ...]


@dataclass(frozen=True)
class ResolvedStream:
    stream_id: str
    display_name: str
    pipeline_id: str
    status: str
    source: ResolvedSource
    destinations: tuple[ResolvedDestination, ...]
    mapping: StreamMapping | None


# --- Pipeline -------------------------------------------------------------


@dataclass(frozen=True)
class Schedule:
    type: str  # manual | cron | interval
    cron: str | None = None
    interval_minutes: int | None = None
    timezone: str | None = None


@dataclass(frozen=True)
class BatchingPolicy:
    batch_size: int
    max_concurrent_batches: int


@dataclass(frozen=True)
class LoggingPolicy:
    log_level: str
    metrics_enabled: bool


@dataclass(frozen=True)
class ErrorHandlingPolicy:
    strategy: str
    max_retries: int
    retry_delay_seconds: int


@dataclass(frozen=True)
class RuntimeConfig:
    buffer_size: int
    batching: BatchingPolicy
    logging: LoggingPolicy
    error_handling: ErrorHandlingPolicy


@dataclass(frozen=True)
class EngineSizing:
    vcpu: int | None
    memory: int | None


@dataclass(frozen=True)
class ResolvedPipeline:
    pipeline_id: str
    display_name: str
    status: str
    schedule: Schedule | None
    runtime: RuntimeConfig
    engine: EngineSizing | None
    streams: tuple[ResolvedStream, ...]
    source_connection_id: str
    destination_connection_ids: tuple[str, ...]
