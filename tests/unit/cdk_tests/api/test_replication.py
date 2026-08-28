"""Binding an incremental cursor to the param the endpoint declares."""

from __future__ import annotations

import pytest
from analitiq.contracts.endpoints import Replication
from pydantic import ValidationError

from cdk.api.replication import cursor_param_for, effective_start
from cdk.exceptions import ReadError

pytestmark = pytest.mark.unit

_SINGLE = {"cursor_field": "updated_at", "param": "since", "operator": "gte"}
_WINDOW = {
    "cursor_field": "updated_at",
    "start_param": "from",
    "end_param": "to",
    "start_operator": "gte",
    "end_operator": "lt",
}


def _replication(*mappings: dict[str, object]) -> Replication:
    """The declared block, parsed the way the read path receives it.

    Which mapping form each entry is comes out of the parse as a class, so
    a test that handed over a bare dict would be asking a question the
    caller never asks.
    """
    return Replication.model_validate(
        {"supported_methods": ["incremental"], "cursor_mappings": list(mappings)}
    )


class TestCursorParam:
    def test_a_single_mapping_names_the_param(self) -> None:
        assert cursor_param_for(_replication(_SINGLE), "updated_at") == "since"

    def test_a_window_mapping_does_not_drive_the_filter(self) -> None:
        # Half-binding a window would send a lower bound with no upper one
        # and read a different range than the author declared.
        assert cursor_param_for(_replication(_WINDOW), "updated_at") is None

    def test_a_window_mapping_ahead_of_it_does_not_hide_the_single_one(self) -> None:
        assert cursor_param_for(_replication(_WINDOW, _SINGLE), "updated_at") == "since"

    def test_an_undeclared_block_answers_nothing(self) -> None:
        assert cursor_param_for(None, "updated_at") is None

    def test_a_block_declaring_no_mapping_at_all_is_unrepresentable(self) -> None:
        # ``None`` is the only "nothing declared" state left. A parsed block
        # with an empty ``cursor_mappings`` used to be the other one, and it
        # is what the contract refuses here -- so the loop above can never
        # meet it. Pinned rather than dropped: if the contract relaxes
        # ``min_length``, that state comes back untested.
        with pytest.raises(ValidationError):
            Replication(supported_methods=["incremental"], cursor_mappings=[])

    def test_another_fields_mapping_is_not_borrowed(self) -> None:
        assert cursor_param_for(_replication(_SINGLE), "created_at") is None


class TestEffectiveStart:
    def test_a_timestamp_moves_back_by_seconds(self) -> None:
        assert effective_start("2026-07-31T12:00:00Z", 120) == "2026-07-31T11:58:00Z"

    def test_a_naive_timestamp_is_read_as_utc(self) -> None:
        assert effective_start("2026-07-31T12:00:00", 60) == "2026-07-31T11:59:00Z"

    def test_an_integer_cursor_moves_back_by_ids(self) -> None:
        assert effective_start("987654321", 120) == "987654201"

    def test_an_integer_cursor_floors_at_zero(self) -> None:
        assert effective_start(50, 120) == "0"

    def test_an_id_that_reads_as_a_date_is_read_as_one(self) -> None:
        # The ISO parser claims a bare 4- or 8-digit string as a year or a
        # date, so an id shaped like one is bounded as a timestamp. Pinned
        # rather than fixed: narrowing what parses would start failing
        # timestamp cursors that work today, and the fix belongs in the
        # contract declaring what a cursor field is.
        assert effective_start("1000", 120) == "0999-12-31T23:58:00Z"

    def test_a_cursor_in_neither_vocabulary_fails_loud(self) -> None:
        with pytest.raises(ReadError, match="neither an ISO timestamp nor an integer"):
            effective_start("not-a-cursor", 120)
