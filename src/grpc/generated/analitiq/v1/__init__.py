"""Analitiq v1 protobuf messages and gRPC services."""

from .stream_pb2 import (
    PayloadFormat,
    Cursor,
    WriteMode,
    SchemaMessage,
    DestinationConfig,
    DatabaseConfig,
    ConflictResolution,
    AutoConfigureOptions,
    IndexDefinition,
    ApiConfig,
    RecordBatch,
    AckStatus,
    BatchAck,
    StreamRequest,
    StreamResponse,
    SchemaAck,
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

__all__ = [
    # stream.proto messages
    "PayloadFormat",
    "Cursor",
    "WriteMode",
    "SchemaMessage",
    "DestinationConfig",
    "DatabaseConfig",
    "ConflictResolution",
    "AutoConfigureOptions",
    "IndexDefinition",
    "ApiConfig",
    "RecordBatch",
    "AckStatus",
    "BatchAck",
    "StreamRequest",
    "StreamResponse",
    "SchemaAck",
    # destination_service.proto messages
    "HealthCheckRequest",
    "HealthCheckResponse",
    "ConnectionStatus",
    "GetCapabilitiesRequest",
    "GetCapabilitiesResponse",
    "ShutdownRequest",
    "ShutdownAck",
    # gRPC service
    "DestinationServiceStub",
    "DestinationServiceServicer",
    "add_DestinationServiceServicer_to_server",
]
