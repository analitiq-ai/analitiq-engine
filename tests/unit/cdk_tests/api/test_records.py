"""Where a page's records live, and what happens when the ref addresses nothing.

Extraction used to answer zero records three separate ways, and under the
loop's empty-page rule zero records ends the traversal -- so each of them
reported a truncated read as a complete one.
"""

from __future__ import annotations

from typing import Any

import pytest

from cdk.api.page_loop import Page
from cdk.api.records import extract_records, page_scope, split_records_ref, walk_path
from cdk.exceptions import ReadError

pytestmark = pytest.mark.unit


class TestSplitRecordsRef:
    def test_the_body_itself_is_an_empty_path(self) -> None:
        assert split_records_ref("response.body") == []

    def test_a_dotted_ref_is_the_fields_below_the_body(self) -> None:
        assert split_records_ref("response.body.data.items") == ["data", "items"]

    @pytest.mark.parametrize("ref", ["", None, "body.records", "records", "$.records"])
    def test_an_unanchored_ref_raises_naming_it(self, ref: Any) -> None:
        # The contract anchors records at the response body. Reading an
        # unanchored ref as "nothing found" is what let a mistyped path pass
        # for an empty stream.
        with pytest.raises(ReadError, match="response.body"):
            split_records_ref(ref)


class TestExtractRecords:
    def test_a_list_of_objects_is_the_records(self) -> None:
        payload = {"records": [{"id": 1}, {"id": 2}]}
        assert extract_records(payload, "response.body.records") == [
            {"id": 1},
            {"id": 2},
        ]

    def test_a_single_object_is_one_record(self) -> None:
        assert extract_records({"id": 1}, "response.body") == [{"id": 1}]

    def test_a_walk_landing_on_a_scalar_raises_naming_the_ref_and_the_type(
        self,
    ) -> None:
        with pytest.raises(ReadError, match=r"records\.ref .*int"):
            extract_records({"records": 7}, "response.body.records")

    def test_a_ref_that_addresses_nothing_raises_rather_than_ending_the_read(
        self,
    ) -> None:
        # Answering [] here would end the traversal at page one and report a
        # read of zero rows as a complete one.
        with pytest.raises(ReadError, match="records.ref"):
            extract_records({"data": [{"id": 1}]}, "response.body.records")

    def test_non_object_items_are_dropped_with_a_warning(self, caplog) -> None:
        # One null in a provider's array is not a reason to fail a stream,
        # but the loss has to be visible.
        with caplog.at_level("WARNING"):
            records = extract_records(
                {"records": [{"id": 1}, None, "x"]}, "response.body.records"
            )
        assert records == [{"id": 1}]
        assert "dropped 2 non-object item(s)" in caplog.text


class TestWalkPath:
    def test_it_returns_the_terminal_value(self) -> None:
        assert walk_path({"a": {"b": {"c": 3}}}, ["a", "b", "c"]) == 3

    @pytest.mark.parametrize(
        "path", [["a", "missing"], ["missing"], ["a", "b", "c", "deeper"]]
    )
    def test_any_miss_answers_none(self, path: list[str]) -> None:
        assert walk_path({"a": {"b": {"c": 3}}}, path) is None


class TestPageScope:
    def test_it_carries_the_body_and_the_record_count(self) -> None:
        page = Page(records=[{"id": 1}, {"id": 2}], payload={"records": "..."})
        assert page_scope(page) == {"body": {"records": "..."}, "record_count": 2}
