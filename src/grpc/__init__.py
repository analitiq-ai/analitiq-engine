"""gRPC module for Analitiq Stream destination communication."""

from src.config import settings

# One message-size ceiling for every channel that carries Arrow batches:
# destination client/server (TCP) and the worker hops (UDS). Client and
# server limits must match or batches between the two limits are rejected
# with RESOURCE_EXHAUSTED on one side only. Value lives in src.config.settings.
DEFAULT_MAX_MESSAGE_SIZE = settings.GRPC_MAX_MESSAGE_SIZE
