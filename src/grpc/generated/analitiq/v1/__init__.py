"""Analitiq v1 protobuf messages and gRPC services."""

from .stream_pb2 import (
    PayloadFormat,
    Cursor,
    WriteMode,
    SchemaMessage,
    RecordBatch,
    AckStatus,
    BatchAck,
    FailureCategory,
    StreamRequest,
    StreamResponse,
    SchemaAck,
    RetrySemantics,
)

from .destination_service_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
    ConnectionStatus,
    GetCapabilitiesRequest,
    GetCapabilitiesResponse,
    ShutdownRequest,
    ShutdownAck,
)

from .destination_service_pb2_grpc import (
    DestinationServiceStub,
    DestinationServiceServicer,
    add_DestinationServiceServicer_to_server,
)

from .source_service_pb2 import (
    ReadRequest,
    ReadResponse,
    ReadBatchChunk,
    ReadComplete,
    ReadError,
    CursorSave,
)

from .source_service_pb2_grpc import (
    SourceServiceStub,
    SourceServiceServicer,
    add_SourceServiceServicer_to_server,
)

__all__ = [
    # stream.proto messages
    "PayloadFormat",
    "Cursor",
    "WriteMode",
    "SchemaMessage",
    "RecordBatch",
    "AckStatus",
    "BatchAck",
    "FailureCategory",
    "StreamRequest",
    "StreamResponse",
    "SchemaAck",
    "RetrySemantics",
    # destination_service.proto messages
    "HealthCheckRequest",
    "HealthCheckResponse",
    "ConnectionStatus",
    "GetCapabilitiesRequest",
    "GetCapabilitiesResponse",
    "ShutdownRequest",
    "ShutdownAck",
    # source_service.proto messages
    "ReadRequest",
    "ReadResponse",
    "ReadBatchChunk",
    "ReadComplete",
    "ReadError",
    "CursorSave",
    # gRPC services
    "DestinationServiceStub",
    "DestinationServiceServicer",
    "add_DestinationServiceServicer_to_server",
    "SourceServiceStub",
    "SourceServiceServicer",
    "add_SourceServiceServicer_to_server",
]
