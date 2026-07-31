"""Shutdown runs the handler's run-completion cleanup while still connected.

The worker is SIGTERM'd right after the Shutdown RPC returns, so this is the
last reliable point for connection-dependent cleanup (``finalize_run``). The
servicer method is called directly here -- it takes a request and a context,
never a channel.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.destination.server import DestinationServicer

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_success_reason_finalizes_with_succeeded_true():
    handler = MagicMock()
    handler.finalize_run = AsyncMock()
    server = MagicMock()
    servicer = DestinationServicer(handler, server=server)

    ack = await servicer.Shutdown(MagicMock(reason="pipeline_completed"), MagicMock())

    handler.finalize_run.assert_awaited_once_with(succeeded=True)
    server.signal_shutdown.assert_called_once()
    assert ack.acknowledged is True


@pytest.mark.asyncio
async def test_non_success_reason_finalizes_with_succeeded_false():
    # A failed/aborted (or generic teardown) reason must not signal success,
    # so the handler can skip success-only cleanup for a possible resume.
    handler = MagicMock()
    handler.finalize_run = AsyncMock()
    server = MagicMock()
    servicer = DestinationServicer(handler, server=server)

    ack = await servicer.Shutdown(MagicMock(reason="pipeline_failed"), MagicMock())

    handler.finalize_run.assert_awaited_once_with(succeeded=False)
    server.signal_shutdown.assert_called_once()
    assert ack.acknowledged is True


@pytest.mark.asyncio
async def test_finalize_failure_does_not_block_shutdown():
    handler = MagicMock()
    handler.finalize_run = AsyncMock(side_effect=RuntimeError("prune failed"))
    server = MagicMock()
    servicer = DestinationServicer(handler, server=server)

    ack = await servicer.Shutdown(MagicMock(reason="x"), MagicMock())

    server.signal_shutdown.assert_called_once()
    assert ack.acknowledged is True


@pytest.mark.asyncio
async def test_finalize_cancelled_still_signals_shutdown():
    # The client's send_shutdown deadline can fire while the prune is still
    # running, cancelling this handler. CancelledError is not an Exception, so
    # without the finally, signal_shutdown would be skipped and the server
    # would keep running after the engine has finished.
    handler = MagicMock()
    handler.finalize_run = AsyncMock(side_effect=asyncio.CancelledError())
    server = MagicMock()
    servicer = DestinationServicer(handler, server=server)

    with pytest.raises(asyncio.CancelledError):
        await servicer.Shutdown(MagicMock(reason="pipeline_completed"), MagicMock())

    server.signal_shutdown.assert_called_once()
