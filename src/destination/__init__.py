"""Destination service for gRPC streaming data ingestion.

The connector classes live in the CDK; what remains here is the engine's
own orchestration:

- DestinationGRPCServer: gRPC server for receiving streamed data
- BaseDestinationHandler: the CDK's abstract handler, re-exported because
  the server is typed against it
"""

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult

from .server import DestinationGRPCServer

__all__ = [
    "BaseDestinationHandler",
    "BatchWriteResult",
    "DestinationGRPCServer",
]
