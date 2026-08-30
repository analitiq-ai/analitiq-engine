"""Connect and disconnect, the part both roles share.

The runtime owns transport materialization; the connector adopts what it
built. What is the connector's own is the loudness -- a materialization
that fails must not escape as whatever the transport happened to raise --
and releasing the runtime exactly once.
"""

from __future__ import annotations

from typing import Any

import pytest
from contract_documents import connection_document

from cdk.api import GenericAPIConnector
from cdk.api.exceptions import ConnectorConnectionError
from cdk.connection_runtime import ConnectionRuntime
from cdk.rate_limiter import RateLimiter
from cdk.secrets import InMemorySecretsResolver

from .fakes import BASE_URL, FakeSession, runtime_with

pytestmark = pytest.mark.unit


def _unmaterialized(parameters: dict[str, Any]) -> ConnectionRuntime:
    return ConnectionRuntime(
        connection=connection_document(parameters=parameters),
        connection_id="test-conn",
        connector_id="test-connector",
        connector_type="api",
        driver=None,
        resolver=InMemorySecretsResolver({}),
    )


@pytest.mark.asyncio
class TestConnect:
    async def test_it_adopts_the_runtimes_transport(self) -> None:
        connector = GenericAPIConnector()
        await connector.connect(runtime_with(FakeSession()))
        assert connector.base_url == BASE_URL
        assert connector._http is not None

    async def test_the_transports_rate_limiter_is_the_one_used(self) -> None:
        # The declared ceiling is a fact about the transport block, so the
        # sender opened over a transport paces with that transport's limiter
        # -- a second transport has its own, or none.
        limiter = RateLimiter(max_requests=10, time_window=60)
        runtime = runtime_with(FakeSession(), rate_limiter=limiter)
        connector = GenericAPIConnector()
        await connector.connect(runtime)
        assert connector._http._rate_limiter is limiter

    @pytest.mark.parametrize(
        "config", [{"invalid": "config"}, {}], ids=["junk", "no-host"]
    )
    async def test_a_failed_materialization_is_named_as_a_connection_failure(
        self, config: dict[str, Any]
    ) -> None:
        # The write role used to let the transport's own error escape raw,
        # so the same misconfiguration reported two different things
        # depending on which direction the stream ran.
        connector = GenericAPIConnector()
        with pytest.raises(ConnectorConnectionError, match="API connection failed"):
            await connector.connect(_unmaterialized(config))


@pytest.mark.asyncio
class TestDisconnect:
    async def test_it_closes_the_session_and_clears_the_state(self) -> None:
        session = FakeSession()
        connector = GenericAPIConnector()
        await connector.connect(runtime_with(session))
        await connector.disconnect()
        assert session.closed is True
        assert connector._http is None
        assert connector._connected is False

    async def test_it_is_safe_to_call_twice(self) -> None:
        connector = GenericAPIConnector()
        await connector.connect(runtime_with(FakeSession()))
        await connector.disconnect()
        await connector.disconnect()

    async def test_disconnecting_before_connecting_does_nothing(self) -> None:
        await GenericAPIConnector().disconnect()
