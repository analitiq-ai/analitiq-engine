"""The engine's channel to a destination carries no keepalive options.

PR #85 removed client-side keepalives that tripped the destination's HTTP/2
ping-flood policy and produced "Too many pings" GOAWAY mid-batch. This pins
the absence at the one production call site so they cannot be quietly
re-added; the incident is recorded in docs/grpc-streaming-architecture.md.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.grpc.client import DestinationGRPCClient
from src.grpc.generated.analitiq.v1 import HealthCheckResponse

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_connect_channel_has_no_keepalive_options():
    """Capture the options the client hands grpc.aio and assert none of them
    is a keepalive knob."""
    client = DestinationGRPCClient()
    captured_options: list[tuple[str, object]] = []

    def fake_channel(address, options=None):
        captured_options.extend(options or [])
        return MagicMock()

    serving = HealthCheckResponse(
        status=HealthCheckResponse.ServingStatus.SERVING, message="ok"
    )
    mock_stub = MagicMock()
    mock_stub.HealthCheck = AsyncMock(return_value=serving)

    with patch(
        "src.grpc.client.grpc_aio.insecure_channel", side_effect=fake_channel
    ), patch("src.grpc.client.DestinationServiceStub", return_value=mock_stub):
        await client.connect(max_connect_retries=1)

    keepalive_keys = {k for k, _ in captured_options if "keepalive" in k.lower()}
    assert (
        not keepalive_keys
    ), f"Channel must not use keepalive options; found: {keepalive_keys}"
