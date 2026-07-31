"""The declaration reaches the transport, and OSError never outranks it.

Two layers act on a status before any verdict exists: the retry client
re-attempts it, and the base handler's errno table judges it. Both run
early enough to make a declared category irrelevant -- the retry client by
hammering a status the connector called fatal until the ack deadline
passes, the errno table by claiming the client's failures before the
declared map is ever consulted (the client's connection errors derive from
``OSError``, and ``asyncio.TimeoutError`` *is* the builtin ``TimeoutError``).

The verdict tables themselves live in ``test_verdicts.py``; these pin that
the wiring in between actually carries them.
"""

from __future__ import annotations

import asyncio
import errno
from unittest.mock import MagicMock

import aiohttp
import pytest

from cdk.api import GenericAPIConnector
from cdk.types import AckStatus, FailureCategory

from .fakes import FakeSession, runtime_with

pytestmark = pytest.mark.unit


async def _connected(error_map: dict | None = None) -> GenericAPIConnector:
    connector = GenericAPIConnector()
    await connector.connect(runtime_with(FakeSession(), error_map=error_map))
    return connector


@pytest.mark.asyncio
class TestConnectAppliesTheDeclaredRetryPolicy:
    async def test_a_declared_fatal_status_leaves_the_retry_client(self) -> None:
        connector = await _connected({"http": {"503": "auth"}})
        options = connector._http._client._retry_options
        assert 503 not in options.statuses

    async def test_a_declared_retryable_status_joins_it(self) -> None:
        connector = await _connected({"http": {"402": "transient"}})
        assert 402 in connector._http._client._retry_options.statuses

    async def test_server_errors_are_not_blanket_retried(self) -> None:
        # The retry client retries every 5xx by default, which re-attempts a
        # declared-fatal status no matter what the set says.
        connector = await _connected({"http": {"503": "auth"}})
        assert connector._http._client._retry_options.retry_all_server_errors is False


@pytest.mark.asyncio
class TestTransportErrorsReachTheDeclaredMap:
    async def test_a_declared_connection_failure_is_the_config_defect_it_says(
        self,
    ) -> None:
        # A bad host or port is deterministic: retrying it burns the whole
        # budget to arrive at the same place.
        connector = await _connected({"exception": {"ClientConnectorError": "config"}})
        result = connector.os_error_failure(
            aiohttp.ClientConnectorError(MagicMock(), OSError(111, "refused")),
            run_id="run-1",
            stream_id="items",
            batch_seq=1,
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        assert "OSError[" not in result.failure_summary

    async def test_a_timeout_is_judged_as_http_not_as_a_file_descriptor(self) -> None:
        connector = await _connected()
        result = connector.os_error_failure(
            asyncio.TimeoutError(), run_id="run-1", stream_id="items", batch_seq=1
        )
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        # The status alone does not discriminate -- an errno-less OSError is
        # retryable too. The summary shape does: an errno verdict reads
        # "OSError[unknown]", which would mean the HTTP taxonomy never ran.
        assert "OSError[" not in result.failure_summary
        assert "TimeoutError" in result.failure_summary

    async def test_a_mid_request_disconnect_stays_retryable(self) -> None:
        # ClientOSError carries a real errno; judged by an errno table it
        # would read as a fatal broken pipe and dead-letter the batch.
        connector = await _connected()
        result = connector.os_error_failure(
            aiohttp.ClientOSError(errno.EPIPE, "broken pipe"),
            run_id="run-1",
            stream_id="items",
            batch_seq=1,
        )
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
