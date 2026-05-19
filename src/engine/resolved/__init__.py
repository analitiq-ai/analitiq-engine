"""Resolved-runtime layer.

Single typed boundary between raw JSON (loaded from disk or fetched from
the cloud) and every other engine subsystem. Build via the factories in
`build.py` or load directly from disk via `loader.load_resolved_pipeline`.
"""

from .build import (
    build_api_read_endpoint,
    build_api_write_endpoint,
    build_connection,
    build_connector,
    build_db_read_endpoint,
    build_db_write_endpoint,
    build_endpoint_ref,
    build_read_endpoint,
    build_resolved_pipeline,
    build_resolved_stream,
    build_write_endpoint,
)
from .errors import ResolveError
from .loader import discover_pipeline_ids, load_resolved_pipeline
from .types import (
    ApiReadEndpoint,
    ApiWriteEndpoint,
    Assignment,
    AssignmentTarget,
    BatchingPolicy,
    Column,
    CursorMapping,
    CursorPagination,
    DatabaseObject,
    DatabaseReadEndpoint,
    DatabaseWriteEndpoint,
    EndpointRef,
    EngineSizing,
    ErrorHandlingPolicy,
    ExecutionConfig,
    Filter,
    HttpRequest,
    HttpTransport,
    KeysetPagination,
    LinkPagination,
    LoggingPolicy,
    OffsetPagination,
    PagePagination,
    PaginationSpec,
    ParamSpec,
    ReadEndpoint,
    Replication,
    ReplicationConfig,
    ResolvedConnection,
    ResolvedConnector,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
    ResponseExtraction,
    RuntimeConfig,
    Schedule,
    SqlAlchemyTransport,
    StreamMapping,
    TransportSpec,
    WriteBatching,
    WriteEndpoint,
    WriteSpec,
)

__all__ = [
    # types
    "ApiReadEndpoint", "ApiWriteEndpoint", "Assignment", "AssignmentTarget",
    "BatchingPolicy", "Column", "CursorMapping", "CursorPagination",
    "DatabaseObject", "DatabaseReadEndpoint", "DatabaseWriteEndpoint",
    "EndpointRef", "EngineSizing", "ErrorHandlingPolicy", "ExecutionConfig",
    "Filter", "HttpRequest", "HttpTransport", "KeysetPagination",
    "LinkPagination", "LoggingPolicy", "OffsetPagination", "PagePagination",
    "PaginationSpec", "ParamSpec", "ReadEndpoint", "Replication",
    "ReplicationConfig", "ResolvedConnection", "ResolvedConnector",
    "ResolvedDestination", "ResolvedPipeline", "ResolvedSource",
    "ResolvedStream", "ResponseExtraction", "RuntimeConfig", "Schedule",
    "SqlAlchemyTransport", "StreamMapping", "TransportSpec", "WriteBatching",
    "WriteEndpoint", "WriteSpec",
    # builders
    "build_api_read_endpoint", "build_api_write_endpoint",
    "build_connection", "build_connector", "build_db_read_endpoint",
    "build_db_write_endpoint", "build_endpoint_ref", "build_read_endpoint",
    "build_resolved_pipeline", "build_resolved_stream", "build_write_endpoint",
    # loader
    "discover_pipeline_ids", "load_resolved_pipeline",
    # errors
    "ResolveError",
]
