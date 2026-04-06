#!/usr/bin/env python3
"""
gRPC health check for destination container.

Checks if gRPC server is ready to accept connections.
Used by ECS task definition healthCheck for destination container.

Exit codes:
    0 - Healthy (gRPC server serving)
    1 - Unhealthy
"""

import asyncio
import os
import sys


async def check_grpc_health(port: int) -> bool:
    """Check if gRPC destination server is healthy."""
    try:
        from src.grpc.generated.analitiq.v1 import (
            HealthCheckRequest,
            HealthCheckResponse,
            DestinationServiceStub,
        )
        from grpc import aio as grpc_aio

        address = f"localhost:{port}"
        async with grpc_aio.insecure_channel(address) as channel:
            stub = DestinationServiceStub(channel)
            response = await asyncio.wait_for(
                stub.HealthCheck(HealthCheckRequest()),
                timeout=5.0,
            )
            is_healthy = response.status == HealthCheckResponse.ServingStatus.SERVING
            print(f"gRPC health: {'OK' if is_healthy else 'FAILED'} - {response.message}")
            return is_healthy

    except Exception as e:
        print(f"gRPC health check failed: {e}")
        return False


if __name__ == "__main__":
    port = int(os.getenv("GRPC_PORT", "50051"))
    sys.exit(0 if asyncio.run(check_grpc_health(port)) else 1)
