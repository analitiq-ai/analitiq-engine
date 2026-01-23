"""Destination service for gRPC streaming data ingestion.

This module provides:
- BaseDestinationHandler: Abstract interface for destination handlers
- DatabaseDestinationHandler: Unified SQLAlchemy-based database handler (supports PostgreSQL, MySQL, etc.)
- DestinationGRPCServer: gRPC server for receiving streamed data
"""

from .base_handler import BaseDestinationHandler, BatchWriteResult
from .server import DestinationGRPCServer

__all__ = [
    "BaseDestinationHandler",
    "BatchWriteResult",
    "DestinationGRPCServer",
]
