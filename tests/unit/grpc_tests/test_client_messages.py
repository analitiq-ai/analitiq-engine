"""What the client puts on the wire and reads back off it, with no channel.

Both surfaces are pure functions of a message: ``_build_schema_message``
stamps the handshake the destination derives its statement bound from
(issues #231, #234), and ``_process_ack`` is the single reading site for an
ack on either hop. A client can be constructed and both called without ever
connecting, so neither needs a faked channel to be pinned.
"""

from __future__ import annotations

import pytest

from cdk.types import FailureCategory
from src.grpc.client import DestinationGRPCClient
from src.grpc.generated.analitiq.v1 import AckStatus, BatchAck, Cursor
from src.grpc.generated.analitiq.v1 import FailureCategory as WireFailureCategory

pytestmark = pytest.mark.unit


class TestProcessAckFailureCategory:
    """``_process_ack`` is where the failure category comes off the wire (#351).

    On the worker-proxy hop the sender is the untrusted connector process, so
    the value must be bounds-checked: an unrecognised integer degrades to
    UNSPECIFIED (text-matching fallback) instead of aborting the stream, and a
    category paired with a success status is zeroed so the field is only ever
    meaningful on a failure result.
    """

    def test_declared_category_passes_through(self):
        ack = BatchAck(
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            failure_summary="type-map: no rule",
            failure_category=WireFailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
        )
        result = DestinationGRPCClient()._process_ack(ack)
        assert result.failure_category is FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT

    def test_unknown_category_degrades_to_unspecified(self):
        # A worker sending an integer this engine build does not know:
        # proto3 enums are open, so 99 survives assignment and the wire.
        ack = BatchAck(status=AckStatus.ACK_STATUS_FATAL_FAILURE, failure_category=99)
        assert ack.failure_category == 99
        result = DestinationGRPCClient()._process_ack(ack)
        assert result.failure_category is FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

    def test_category_on_success_ack_is_zeroed(self):
        ack = BatchAck(
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=1,
            failure_category=WireFailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
        )
        result = DestinationGRPCClient()._process_ack(ack)
        assert result.success is True
        assert result.failure_category is FailureCategory.FAILURE_CATEGORY_UNSPECIFIED


class TestSchemaMessageAckBudget:
    """The sender stamps the ack budget it will actually wait, and the
    destination derives its statement timeout from that stamp (issue #234).
    Whichever waiter on the path gives up soonest must own the number, or the
    destination outlives its caller and orphans a statement.
    """

    def test_schema_message_stamps_clients_own_ack_budget(self):
        """The stamped budget is the wait this client applies (self.timeout),
        so the destination's statement timeout cannot drift from it."""
        client = DestinationGRPCClient(timeout_seconds=42)
        schema_msg = client._build_schema_message(
            "s", {"write_mode": "upsert", "schema_version": 1}
        )
        assert schema_msg.ack_timeout_seconds == 42

    def test_schema_message_forwards_smaller_upstream_budget(self):
        """A forwarding hop (the worker proxy) passes the engine's budget in
        schema_config; when it is tighter than this client's own wait, the
        engine's value wins -- the worker must stay below every waiter on the
        path."""
        client = DestinationGRPCClient(timeout_seconds=300)
        schema_msg = client._build_schema_message(
            "s",
            {"write_mode": "upsert", "schema_version": 1, "ack_timeout_seconds": 30},
        )
        assert schema_msg.ack_timeout_seconds == 30

    def test_schema_message_caps_upstream_budget_at_own_wait(self):
        """An upstream budget larger than this client's own wait must not
        widen the stamp: this hop gives up sooner, so the statement bound
        derives from the tighter wait."""
        client = DestinationGRPCClient(timeout_seconds=30)
        schema_msg = client._build_schema_message(
            "s",
            {"write_mode": "upsert", "schema_version": 1, "ack_timeout_seconds": 300},
        )
        assert schema_msg.ack_timeout_seconds == 30


class TestUnrecognizedAckStatus:
    """A status this build cannot name must not abort the RPC.

    proto3 enums are open, so a newer or untrusted peer can send a value
    with no name here. Naming it is only ever for a diagnostic string, so
    an unnameable value degrades to its number -- the same treatment
    ``failure_category`` already gets (raised on PR #445).
    """

    def test_unnameable_status_is_reported_as_its_number(self):
        from src.grpc.client import _ack_status_name

        assert _ack_status_name(99) == "unrecognized status 99"

    def test_known_status_keeps_its_name(self):
        from src.grpc.client import _ack_status_name

        assert (
            _ack_status_name(AckStatus.ACK_STATUS_FATAL_FAILURE)
            == "ACK_STATUS_FATAL_FAILURE"
        )

    def test_contract_violation_on_an_unnameable_status_returns_a_result(self):
        # The contract-violation branch (a cursor on a failed ack) runs before
        # the unknown-status path, so it is where an unnameable status first
        # reaches a name lookup. It must still return a bounded BatchResult
        # for BatchPolicy rather than raising out of the RPC.
        ack = BatchAck(committed_cursor=Cursor(token=b"\x01"))
        ack.status = 99
        result = DestinationGRPCClient()._process_ack(ack)

        assert result.success is False
        assert result.status is AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.committed_cursor is None
        assert "unrecognized status 99" in result.failure_summary
