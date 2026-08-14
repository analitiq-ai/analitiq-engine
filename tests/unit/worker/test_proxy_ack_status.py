"""An ack status this build cannot name survives the worker-proxy hop.

proto3 enums are open, so a newer or defective connector can ack with a
value the engine has no member for. The hop used to narrow the wire value
into the closed :class:`AckStatus`, which raised and surfaced as a
retryable transport failure -- the wrong verdict, and one that hid the
right one, since ``BatchPolicy`` already answers an unrecognised status
with a bounded terminal disposition (raised on PR #445).
"""

from __future__ import annotations

import pytest

from cdk.types import AckStatus
from src.worker.proxy import _known_ack_status

pytestmark = pytest.mark.unit


class TestKnownAckStatus:
    @pytest.mark.parametrize("member", list(AckStatus))
    def test_a_declared_status_narrows_to_its_member(self, member: AckStatus) -> None:
        narrowed = _known_ack_status(int(member))
        assert narrowed is member

    def test_an_unrecognized_status_is_carried_through_unchanged(self) -> None:
        assert _known_ack_status(99) == 99

    def test_an_unrecognized_status_does_not_raise(self) -> None:
        # The whole point: narrowing raised here, and the raise became a
        # retryable transport failure the engine could not tell from a real
        # one. Constructing the enum directly still raises, which is what
        # makes the guard load-bearing rather than decorative.
        with pytest.raises(ValueError):
            AckStatus(99)
        assert _known_ack_status(99) == 99
