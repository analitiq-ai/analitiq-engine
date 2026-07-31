"""Who owns a failed handshake survives the hops between shell and engine.

The engine classifies a rejected schema from the category on the ``SchemaAck``,
never from the rejection wording. That makes the category load-bearing on a
path with three participants: a connector worker refuses or dies, the shell's
``WorkerProxyHandler`` decides who owns that, the destination servicer puts it
on the engine-facing ack, and the engine's client reads it back.

Only the shell can tell "the worker refused this schema" from "the worker never
answered", and the two mean opposite things to a customer -- one is their
configuration, the other is not. Before the category existed that distinction
survived only as reason text a phrase list matched.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from cdk.types import FailureCategory, RetrySemantics, RetryVerdict
from src.destination.server import DestinationServicer
from src.grpc.client import DestinationGRPCClient
from src.grpc.generated.analitiq.v1 import SchemaAck, SchemaMessage
from src.state.error_classification import (
    ErrorCode,
    SchemaHandshakeOutcome,
    classify_handshake_failure,
)
from src.worker import proxy as proxy_module
from src.worker.proxy import _forwarded_schema_category

pytestmark = pytest.mark.unit


class _StubClient:
    """The two properties ``_forwarded_schema_category`` reads off a client."""

    def __init__(
        self,
        outcome: SchemaHandshakeOutcome,
        declared: FailureCategory = FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
    ) -> None:
        self.schema_handshake_outcome = outcome
        self.schema_failure_category = declared


class TestShellDecidesWhoOwnsTheRejection:
    def test_a_worker_that_never_answered_is_not_ready(self) -> None:
        # Nothing about the customer's configuration was established: the
        # worker never got far enough to have an opinion on the schema.
        category = _forwarded_schema_category(
            _StubClient(SchemaHandshakeOutcome.TRANSPORT_FAILURE)
        )
        assert category is FailureCategory.FAILURE_CATEGORY_NOT_READY

    def test_a_worker_that_answered_nonsense_is_internal(self) -> None:
        # A reply that is not a SchemaAck is a defective worker or a
        # protocol bug. Nobody but us owns that, and without a wire member
        # for it the failure would have to impersonate a user-owned one.
        category = _forwarded_schema_category(
            _StubClient(SchemaHandshakeOutcome.PROTOCOL_VIOLATION)
        )
        assert category is FailureCategory.FAILURE_CATEGORY_INTERNAL

    def test_a_worker_that_refused_owns_it_as_a_config_defect(self) -> None:
        # It was reachable and said no, and declared nothing more specific.
        category = _forwarded_schema_category(
            _StubClient(SchemaHandshakeOutcome.REJECTED)
        )
        assert category is FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT

    def test_a_workers_own_declaration_is_forwarded_verbatim(self) -> None:
        # The worker knows more than the hop does; the shell must not
        # overwrite a declaration with its own default.
        category = _forwarded_schema_category(
            _StubClient(
                SchemaHandshakeOutcome.REJECTED,
                declared=FailureCategory.FAILURE_CATEGORY_NOT_READY,
            )
        )
        assert category is FailureCategory.FAILURE_CATEGORY_NOT_READY

    def test_an_accepted_handshake_is_a_caller_defect(self) -> None:
        with pytest.raises(ValueError):
            _forwarded_schema_category(_StubClient(SchemaHandshakeOutcome.ACCEPTED))


class TestTheCategoryChangesTheCustomerFacingCode:
    def test_an_unreachable_worker_does_not_read_as_bad_configuration(self) -> None:
        # The regression this whole path exists to prevent. The shell sends
        # an ordinary rejection either way, so without the category both
        # cases reach the engine identically and a customer whose
        # destination worker failed to start is told to fix their config.
        unreachable = _forwarded_schema_category(
            _StubClient(SchemaHandshakeOutcome.TRANSPORT_FAILURE)
        )
        refused = _forwarded_schema_category(
            _StubClient(SchemaHandshakeOutcome.REJECTED)
        )
        # Both arrive at the engine as a rejection -- the shell was reachable.
        assert (
            classify_handshake_failure(
                SchemaHandshakeOutcome.REJECTED, declared=unreachable
            )
            is ErrorCode.INTERNAL
        )
        assert (
            classify_handshake_failure(
                SchemaHandshakeOutcome.REJECTED, declared=refused
            )
            is ErrorCode.CONFIG_INVALID
        )


class TestTheProxyRecordsItForTheServicer:
    """``_open_stream`` is the wiring: the verdict has to be stored, not just
    computed. The servicer reads it off the handler by attribute."""

    async def _open_against(self, monkeypatch, worker_client) -> object:
        handler = proxy_module.WorkerProxyHandler(
            connectors_dir=Path("/nonexistent"),
            connections_dir=Path("/nonexistent"),
        )
        handler._handle = MagicMock(target="unix:///tmp/worker.sock")
        monkeypatch.setattr(
            proxy_module, "DestinationGRPCClient", lambda **kw: worker_client
        )
        worker_client.connect = AsyncMock(return_value=True)
        worker_client.disconnect = AsyncMock()
        assert await handler._open_stream("s1", {"ack_timeout_seconds": 30}) is None
        return handler

    @pytest.mark.asyncio
    async def test_a_worker_that_never_answered_is_recorded_not_ready(
        self, monkeypatch
    ) -> None:
        client = _StubClient(SchemaHandshakeOutcome.TRANSPORT_FAILURE)
        client.start_stream = AsyncMock(return_value=False)
        client.schema_rejection_message = "destination closed stream"
        handler = await self._open_against(monkeypatch, client)
        assert (
            handler.last_schema_failure_category
            is FailureCategory.FAILURE_CATEGORY_NOT_READY
        )

    @pytest.mark.asyncio
    async def test_a_worker_that_refused_is_recorded_as_a_config_defect(
        self, monkeypatch
    ) -> None:
        client = _StubClient(SchemaHandshakeOutcome.REJECTED)
        client.start_stream = AsyncMock(return_value=False)
        client.schema_rejection_message = "unsupported write mode"
        handler = await self._open_against(monkeypatch, client)
        assert (
            handler.last_schema_failure_category
            is FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        )

    @pytest.mark.asyncio
    async def test_a_worker_channel_that_never_opened_is_not_ready(
        self, monkeypatch
    ) -> None:
        handler = proxy_module.WorkerProxyHandler(
            connectors_dir=Path("/nonexistent"),
            connections_dir=Path("/nonexistent"),
        )
        handler._handle = MagicMock(target="unix:///tmp/worker.sock")
        client = _StubClient(SchemaHandshakeOutcome.TRANSPORT_FAILURE)
        client.connect = AsyncMock(return_value=False)
        monkeypatch.setattr(proxy_module, "DestinationGRPCClient", lambda **kw: client)
        assert await handler._open_stream("s1", {"ack_timeout_seconds": 30}) is None
        assert (
            handler.last_schema_failure_category
            is FailureCategory.FAILURE_CATEGORY_NOT_READY
        )


class TestTheEngineReadsItBackOffTheAck:
    """The last hop: what the destination declared reaches the raise site."""

    async def _reject_with(self, declared: int):
        client = DestinationGRPCClient()
        client._connected = True
        client._stub = MagicMock()
        client._stub.StreamRecords = MagicMock(return_value=MagicMock())
        client._teardown_stream = AsyncMock()

        async def _push_rejection() -> None:
            await client._response_queue.put(
                SchemaAck(
                    stream_id="s1",
                    accepted=False,
                    message="Schema configuration failed",
                    failure_category=declared,
                )
            )

        client._read_responses = _push_rejection
        client._write_requests = AsyncMock()
        accepted = await client.start_stream(
            run_id="r1", stream_id="s1", schema_config={}
        )
        assert not accepted
        return client

    @pytest.mark.asyncio
    async def test_a_declared_category_is_read_off_the_ack(self) -> None:
        client = await self._reject_with(
            FailureCategory.FAILURE_CATEGORY_NOT_READY.value
        )
        assert client.schema_handshake_outcome is SchemaHandshakeOutcome.REJECTED
        assert (
            client.schema_failure_category is FailureCategory.FAILURE_CATEGORY_NOT_READY
        )
        # And it is what the raise site reports, not the "Schema
        # configuration failed" wording on the same ack.
        assert (
            classify_handshake_failure(
                client.schema_handshake_outcome,
                declared=client.schema_failure_category,
            )
            is ErrorCode.INTERNAL
        )

    @pytest.mark.asyncio
    async def test_an_unreadable_declaration_degrades_at_this_hop(self) -> None:
        # proto3 enums are open and the sender may be an untrusted connector
        # process, so a value this build cannot name must not reach the
        # classifier or abort the handshake with a second failure.
        client = await self._reject_with(99)
        assert (
            client.schema_failure_category
            is FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
        )
        assert (
            classify_handshake_failure(
                client.schema_handshake_outcome,
                declared=client.schema_failure_category,
            )
            is ErrorCode.CONFIG_INVALID
        )


class _StubHandler:
    """The slice of the handler contract the schema handshake drives.

    Declared once: the servicer calls the same three members whichever way
    the handshake ends, and only the ending differs per test below.
    """

    last_schema_rejection: str | None = None

    def set_statement_timeout(self, seconds: int) -> None:
        """No SQL runs behind a stub, so there is no statement to bound."""

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Read only on an accepted handshake."""
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE,
            reason="dedups by primary key",
        )


async def _ack_from(handler: _StubHandler) -> SchemaAck:
    return await DestinationServicer(handler, MagicMock())._handle_schema_message(
        SchemaMessage(stream_id="s1", version=1, ack_timeout_seconds=30)
    )


class TestTheServicerPutsItOnTheEngineFacingAck:
    @pytest.mark.asyncio
    async def test_a_proxying_handlers_category_reaches_the_ack(self) -> None:
        class _RejectingProxy(_StubHandler):
            last_schema_rejection = "destination worker channel did not connect"
            last_schema_failure_category = FailureCategory.FAILURE_CATEGORY_NOT_READY

            async def configure_schema(self, schema_spec: object) -> bool:
                return False

        ack = await _ack_from(_RejectingProxy())
        assert not ack.accepted
        assert ack.failure_category == FailureCategory.FAILURE_CATEGORY_NOT_READY

    @pytest.mark.asyncio
    async def test_a_deterministic_config_error_declares_config_defect(self) -> None:
        class _RaisingHandler(_StubHandler):
            async def configure_schema(self, schema_spec: object) -> bool:
                raise ValueError("endpoint document names no target table")

        ack = await _ack_from(_RaisingHandler())
        assert not ack.accepted
        assert ack.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT

    @pytest.mark.asyncio
    async def test_an_accepted_handshake_declares_nothing(self) -> None:
        class _AcceptingHandler(_StubHandler):
            async def configure_schema(self, schema_spec: object) -> bool:
                return True

        ack = await _ack_from(_AcceptingHandler())
        assert ack.accepted
        assert ack.failure_category == FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
