"""Destination service for gRPC streaming data ingestion.

This module provides:
- BaseDestinationHandler: Abstract interface for destination handlers
- GenericSQLConnector: Unified SQL connector (SQLAlchemy or ADBC) for all SQL databases
- DestinationGRPCServer: gRPC server for receiving streamed data
"""

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from .server import DestinationGRPCServer

__all__ = [
    "BaseDestinationHandler",
    "BatchWriteResult",
    "DestinationGRPCServer",
]
