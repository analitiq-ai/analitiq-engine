"""gRPC module for Analitiq Stream destination communication."""

# One message-size ceiling for every channel that carries Arrow batches:
# destination client/server (TCP) and the worker hops (UDS). Client and
# server limits must match or batches between the two limits are rejected
# with RESOURCE_EXHAUSTED on one side only.
DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024  # 16MB
