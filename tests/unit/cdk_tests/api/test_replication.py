"""Binding an incremental cursor to the params the endpoint declares."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from analitiq.contracts.endpoints import (
    Replication,
    SingleCursorMapping,
    WindowCursorMapping,
)
from pydantic import ValidationError

from cdk.api.replication import cursor_bounds, cursor_mapping_for
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
_NOW = datetime(2026, 8, 1, 9, 30, 15, tzinfo=timezone.utc)


def _replication(*mappings: dict[str, object]) -> Replication:
    """The declared block, parsed the way the read path receives it.

    Which mapping form each entry is comes out of the parse as a class, so
    a test that handed over a bare dict would be asking a question the
    caller never asks.
    """
    return Replication.model_validate(
        {"supported_methods": ["incremental"], "cursor_mappings": list(mappings)}
    )


def _single(**overrides: object) -> SingleCursorMapping:
    return SingleCursorMapping.model_validate({**_SINGLE, **overrides})


def _window(**overrides: object) -> WindowCursorMapping:
    return WindowCursorMapping.model_validate({**_WINDOW, **overrides})


class TestCursorMapping:
    def test_a_single_mapping_is_found_by_its_cursor_field(self) -> None:
        mapping = cursor_mapping_for(_replication(_SINGLE), "updated_at")
        assert isinstance(mapping, SingleCursorMapping)
        assert mapping.param == "since"

    def test_a_window_mapping_is_found_by_its_cursor_field(self) -> None:
        mapping = cursor_mapping_for(_replication(_WINDOW), "updated_at")
        assert isinstance(mapping, WindowCursorMapping)
        assert (mapping.start_param, mapping.end_param) == ("from", "to")

    def test_the_first_mapping_for_the_field_wins(self) -> None:
        mapping = cursor_mapping_for(_replication(_WINDOW, _SINGLE), "updated_at")
        assert isinstance(mapping, WindowCursorMapping)

    def test_an_undeclared_block_answers_nothing(self) -> None:
        assert cursor_mapping_for(None, "updated_at") is None

    def test_a_block_declaring_no_mapping_at_all_is_unrepresentable(self) -> None:
        # ``None`` is the only "nothing declared" state left. A parsed block
        # with an empty ``cursor_mappings`` used to be the other one, and it
        # is what the contract refuses here -- so the loop above can never
        # meet it. Pinned rather than dropped: if the contract relaxes
        # ``min_length``, that state comes back untested.
        with pytest.raises(ValidationError):
            Replication(supported_methods=["incremental"], cursor_mappings=[])

    def test_another_fields_mapping_is_not_borrowed(self) -> None:
        assert cursor_mapping_for(_replication(_SINGLE), "created_at") is None


def _bounds(
    mapping: SingleCursorMapping | WindowCursorMapping,
    cursor: object,
    safety_window_seconds: int,
    *,
    field_type: str = "string",
    now: datetime = _NOW,
) -> dict[str, str | int]:
    """``cursor_bounds`` with the cursor field's declared JSON type spelled out.

    The type is what the endpoint document declares for the record field
    the cursor came from; a string field holds an ISO moment, an integer
    field holds ticks or an id.
    """
    return cursor_bounds(
        mapping, cursor, safety_window_seconds, cursor_field_type=field_type, now=now
    )


class TestSingleBound:
    def test_a_timestamp_moves_back_by_seconds(self) -> None:
        bounds = _bounds(_single(), "2026-07-31T12:00:00Z", 120)
        assert bounds == {"since": "2026-07-31T11:58:00Z"}

    def test_a_naive_timestamp_is_read_as_utc(self) -> None:
        bounds = _bounds(_single(), "2026-07-31T12:00:00", 60)
        assert bounds == {"since": "2026-07-31T11:59:00Z"}

    def test_an_integer_cursor_moves_back_by_ids(self) -> None:
        assert _bounds(_single(), "987654321", 120, field_type="integer") == {
            "since": 987654201
        }

    def test_an_integer_cursor_floors_at_zero(self) -> None:
        assert _bounds(_single(), 50, 120, field_type="integer") == {"since": 0}

    def test_an_id_shaped_like_a_year_is_still_an_id(self) -> None:
        # The record schema, not the value's shape, says what the cursor
        # is: an integer field holds an id even when the ISO parser would
        # claim the digits as a year.
        assert _bounds(_single(), "1000", 120, field_type="integer") == {"since": 880}

    def test_a_basic_iso_date_on_a_string_field_is_a_moment(self) -> None:
        # Eight digits on a string field are the basic-format date, never
        # an epoch, whatever format the request param takes.
        bounds = _bounds(_single(format="epoch_seconds"), "20260731", 0)
        epoch = int(datetime(2026, 7, 31, tzinfo=timezone.utc).timestamp())
        assert bounds == {"since": epoch}

    def test_a_string_cursor_that_is_no_timestamp_fails_loud(self) -> None:
        with pytest.raises(ReadError, match="not an ISO timestamp"):
            _bounds(_single(), "not-a-cursor", 120)

    def test_an_integer_field_with_a_non_integer_cursor_fails_loud(self) -> None:
        with pytest.raises(ReadError, match="not an integer"):
            _bounds(_single(), "2026-07-31T12:00:00Z", 120, field_type="integer")

    @pytest.mark.parametrize("field_type", ["number", "boolean", "object", "array"])
    def test_a_field_type_that_cannot_hold_a_cursor_is_refused(
        self, field_type: str
    ) -> None:
        with pytest.raises(ReadError, match=f"declared as type {field_type!r}"):
            _bounds(_single(), "1", 0, field_type=field_type)

    def test_gt_and_gte_send_the_same_value(self) -> None:
        # Inclusiveness is the provider's fact; the safety window already
        # re-reads the boundary.
        gt = _bounds(_single(operator="gt"), "2026-07-31T12:00:00Z", 0)
        gte = _bounds(_single(operator="gte"), "2026-07-31T12:00:00Z", 0)
        assert gt == gte == {"since": "2026-07-31T12:00:00Z"}

    @pytest.mark.parametrize("operator", ["lt", "lte"])
    def test_an_upper_bound_alone_cannot_resume_a_read(self, operator: str) -> None:
        with pytest.raises(ReadError, match="an upper bound"):
            _bounds(_single(operator=operator), "2026-07-31T12:00:00Z", 0)


class TestFormat:
    @pytest.mark.parametrize(
        ("fmt", "cursor", "field_type", "expected"),
        [
            ("date-time", "2026-07-31T12:00:00Z", "string", "2026-07-31T11:58:00Z"),
            ("date", "2026-08-01T00:01:00Z", "string", "2026-07-31"),
            ("epoch_seconds", 1722427200, "integer", 1722427080),
            ("epoch_seconds", "1722427200", "integer", 1722427080),
            ("epoch_milliseconds", 1722427200000, "integer", 1722427080000),
        ],
    )
    def test_the_bound_is_rendered_in_the_declared_format(
        self, fmt: str, cursor: object, field_type: str, expected: str | int
    ) -> None:
        assert _bounds(_single(format=fmt), cursor, 120, field_type=field_type) == {
            "since": expected
        }

    def test_a_zero_cursor_is_the_first_id_not_an_absent_one(self) -> None:
        assert _bounds(_single(), 0, 0, field_type="integer") == {"since": 0}

    def test_an_epoch_bound_is_a_number_not_its_spelling(self) -> None:
        # A JSON body typed ``integer`` refuses ``"1722427080"``; the
        # request builder spells a query value itself.
        bounds = _bounds(
            _single(format="epoch_seconds"), 1722427200, 120, field_type="integer"
        )
        assert bounds == {"since": 1722427080}
        assert isinstance(bounds["since"], int)

    def test_an_epoch_format_reads_an_integer_as_a_moment_not_an_id(self) -> None:
        # 1722427200 - 120 as an id would be 1722427080 too; the window
        # end proves the integer became a moment.
        bounds = _bounds(
            _window(format="epoch_seconds"), 1722427200, 120, field_type="integer"
        )
        assert bounds == {"from": 1722427080, "to": int(_NOW.timestamp())}

    def test_an_epoch_format_renders_an_iso_cursor_as_epoch(self) -> None:
        # The format is the request param's vocabulary, not the record
        # field's: a provider may answer ISO timestamps and take an epoch
        # ``since``.
        moment = datetime(2026, 7, 31, 12, 0, 0, tzinfo=timezone.utc)
        bounds = _bounds(
            _single(format="epoch_milliseconds"), "2026-07-31T12:00:00Z", 0
        )
        assert bounds == {"since": int(moment.timestamp()) * 1000}

    @pytest.mark.parametrize("fmt", ["epoch_seconds", "epoch_milliseconds"])
    def test_an_epoch_bound_truncates_toward_the_past(self, fmt: str) -> None:
        # .9996 must not round up: a lower bound after the cursor opens a
        # gap, one before it only widens the replay.
        bounds = _bounds(_single(format=fmt), "2026-07-31T12:00:00.9996Z", 0)
        whole = int(datetime(2026, 7, 31, 12, 0, 0, tzinfo=timezone.utc).timestamp())
        expected = whole if fmt == "epoch_seconds" else whole * 1000 + 999
        assert bounds == {"since": expected}

    @pytest.mark.parametrize("fmt", ["epoch_seconds", "epoch_milliseconds"])
    def test_an_epoch_cursor_reads_back_to_the_moment_it_rendered(
        self, fmt: str
    ) -> None:
        # One unit table serves both directions, so a rendered bound stored
        # as the next cursor is the same moment when it is read again.
        moment = "2026-07-31T12:00:00.5Z"
        rendered = _bounds(_single(format=fmt), moment, 0)["since"]
        assert isinstance(rendered, int)
        again = _bounds(_single(format=fmt), rendered, 0, field_type="integer")
        assert again == {"since": rendered}

    @pytest.mark.parametrize("cursor", [10**20, -(10**20)])
    def test_an_epoch_cursor_out_of_range_is_a_read_error(self, cursor: int) -> None:
        with pytest.raises(ReadError, match="outside the range a moment can hold"):
            _bounds(_single(format="epoch_seconds"), cursor, 0, field_type="integer")

    @pytest.mark.parametrize("fmt", ["date", "date-time"])
    def test_an_integer_field_under_a_calendar_format_is_refused(
        self, fmt: str
    ) -> None:
        # An integer is a moment only under an epoch format, which is the
        # only place its unit is declared.
        with pytest.raises(ReadError, match="only under an epoch format"):
            _bounds(_single(format=fmt), 1722427200, 0, field_type="integer")

    def test_a_date_is_taken_from_the_cursor_in_utc(self) -> None:
        # The window end is rendered in UTC; a cursor carrying an offset
        # must be too, or the range can face backwards across midnight.
        bounds = _bounds(
            _window(format="date"),
            "2026-08-01T00:30:00+02:00",
            0,
            now=datetime(2026, 7, 31, 23, 0, 0, tzinfo=timezone.utc),
        )
        assert bounds == {"from": "2026-07-31", "to": "2026-07-31"}

    def test_a_date_format_refuses_a_non_timestamp_cursor(self) -> None:
        with pytest.raises(ReadError, match="not an ISO timestamp"):
            _bounds(_single(format="date"), "not-a-cursor", 0)

    def test_every_contract_format_renders(self) -> None:
        # The vocabulary is read off the contract's published schema, so a
        # format the contract adds and this module cannot render fails here.
        declared = SingleCursorMapping.model_json_schema()["properties"]["format"]
        seen = {fmt for branch in declared["anyOf"] for fmt in branch.get("enum", [])}
        assert seen
        for fmt in seen:
            assert _bounds(_single(format=fmt), "2026-07-31T12:00:00Z", 0)["since"]


class TestWindowBounds:
    def test_a_window_binds_start_and_end(self) -> None:
        bounds = _bounds(_window(), "2026-07-31T12:00:00Z", 120)
        assert bounds == {"from": "2026-07-31T11:58:00Z", "to": "2026-08-01T09:30:15Z"}

    def test_the_end_is_now_in_utc(self) -> None:
        local_now = _NOW.astimezone(timezone(timedelta(hours=2)))
        bounds = _bounds(
            _window(format="date-time"), "2026-07-31T12:00:00Z", 0, now=local_now
        )
        assert bounds["to"] == "2026-08-01T09:30:15Z"

    def test_both_ends_share_the_declared_format(self) -> None:
        bounds = _bounds(_window(format="date"), "2026-07-31T12:00:00Z", 0)
        assert bounds == {"from": "2026-07-31", "to": "2026-08-01"}

    @pytest.mark.parametrize(
        "overrides",
        [{"start_operator": "lt"}, {"end_operator": "gte"}],
        ids=["start-faces-up", "end-faces-down"],
    )
    def test_a_window_facing_the_wrong_way_is_refused(
        self, overrides: dict[str, str]
    ) -> None:
        with pytest.raises(ReadError, match="start must be gt/gte"):
            _bounds(_window(**overrides), "2026-07-31T12:00:00Z", 0)

    def test_a_window_over_an_id_cursor_is_refused(self) -> None:
        # An id has no "now" to bound the window at.
        with pytest.raises(ReadError, match="needs a timestamp cursor"):
            _bounds(_window(), "987654321", 0, field_type="integer")
