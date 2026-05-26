"""Direct unit tests for the API connector's dedup pure functions.

``_is_record_new`` and ``_deduplicate_records`` are how the API
connector decides which records on a resumed page are *new* (strictly
past the stored bookmark) versus already-seen duplicates. The dedup
path is reachable from ``read_batches`` only when ``state["bookmarks"]``
is non-empty — today the connector initialises it empty (see
``src/source/connectors/api.py:162``), so this file exercises the
comparators directly. If a future change wires bookmarks back through
``read_batches``, these tests guarantee the comparator's contract
hasn't drifted.

Contract under test:

* Record cursor strictly past stored cursor -> new (returned).
* Record cursor strictly before stored cursor -> duplicate (filtered).
* Cursor tie + tie-breaker strictly greater -> new.
* Cursor tie + exact tie-breaker -> duplicate (inclusive mode).
* Record missing the cursor field -> conservatively kept as new.
"""

from __future__ import annotations

from typing import Any, Dict, List

from src.source.connectors.api import APIConnector, _is_record_new


def _bookmark(cursor: str, tiebreakers: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    return {
        "cursor": cursor,
        "aux": {"tiebreakers": list(tiebreakers or [])},
    }


# ---------------------------------------------------------------------------
# _is_record_new — cursor comparison
# ---------------------------------------------------------------------------


class TestIsRecordNewCursor:
    def test_strictly_after_bookmark_is_new(self):
        record = {"updated_at": "2024-01-02T00:00:00Z", "id": 1}
        bookmark = _bookmark("2024-01-01T00:00:00Z")
        assert _is_record_new(record, bookmark, "updated_at", []) is True

    def test_strictly_before_bookmark_is_duplicate(self):
        record = {"updated_at": "2023-12-31T00:00:00Z", "id": 1}
        bookmark = _bookmark("2024-01-01T00:00:00Z")
        assert _is_record_new(record, bookmark, "updated_at", []) is False

    def test_missing_cursor_field_is_kept_as_new(self):
        """Conservative: if we can't compare, keep the record so we never
        silently drop data on a schema regression."""
        record = {"id": 1}
        bookmark = _bookmark("2024-01-01T00:00:00Z")
        assert _is_record_new(record, bookmark, "updated_at", []) is True

    def test_missing_stored_cursor_is_kept_as_new(self):
        record = {"updated_at": "2024-01-01T00:00:00Z", "id": 1}
        bookmark = {"cursor": None}
        assert _is_record_new(record, bookmark, "updated_at", []) is True

    def test_non_iso_cursor_falls_back_to_string_compare(self):
        record = {"updated_at": "zzz", "id": 1}
        bookmark = _bookmark("aaa")
        assert _is_record_new(record, bookmark, "updated_at", []) is True


# ---------------------------------------------------------------------------
# _is_record_new — tie-breaker comparison
# ---------------------------------------------------------------------------


class TestIsRecordNewTieBreakers:
    def test_cursor_tie_with_greater_tie_breaker_is_new(self):
        record = {"updated_at": "2024-01-01T00:00:00Z", "id": "42"}
        bookmark = _bookmark(
            "2024-01-01T00:00:00Z",
            tiebreakers=[{"field": "id", "value": "10"}],
        )
        assert _is_record_new(record, bookmark, "updated_at", ["id"]) is True

    def test_cursor_tie_with_smaller_tie_breaker_is_duplicate(self):
        record = {"updated_at": "2024-01-01T00:00:00Z", "id": "5"}
        bookmark = _bookmark(
            "2024-01-01T00:00:00Z",
            tiebreakers=[{"field": "id", "value": "10"}],
        )
        assert _is_record_new(record, bookmark, "updated_at", ["id"]) is False

    def test_cursor_tie_with_exact_tie_breaker_is_duplicate_inclusive(self):
        """Inclusive mode: exact tie-breaker match means we've seen this
        exact row. Drop it so resume doesn't re-emit."""
        record = {"updated_at": "2024-01-01T00:00:00Z", "id": "10"}
        bookmark = _bookmark(
            "2024-01-01T00:00:00Z",
            tiebreakers=[{"field": "id", "value": "10"}],
        )
        assert _is_record_new(record, bookmark, "updated_at", ["id"]) is False

    def test_cursor_tie_with_no_stored_tie_breakers_is_new(self):
        """If the stored bookmark has no tie-breakers at all, we have no
        basis to call a cursor-tied record a duplicate."""
        record = {"updated_at": "2024-01-01T00:00:00Z", "id": "10"}
        bookmark = _bookmark("2024-01-01T00:00:00Z")
        assert _is_record_new(record, bookmark, "updated_at", ["id"]) is True


# ---------------------------------------------------------------------------
# _deduplicate_records — wrapper using _is_record_new under bookmarks state
# ---------------------------------------------------------------------------


class TestDeduplicateRecords:
    def test_empty_bookmarks_returns_batch_unchanged(self):
        connector = APIConnector("t")
        batch = [{"updated_at": "2024-01-01T00:00:00Z", "id": "1"}]
        state: Dict[str, Any] = {"bookmarks": []}
        assert connector._deduplicate_records(batch, state, "updated_at", ["id"]) is batch

    def test_no_cursor_field_returns_batch_unchanged(self):
        connector = APIConnector("t")
        batch = [{"id": "1"}]
        state: Dict[str, Any] = {"bookmarks": [_bookmark("2024-01-01T00:00:00Z")]}
        assert connector._deduplicate_records(batch, state, None, []) is batch

    def test_filters_duplicates_at_cursor_boundary(self):
        """At a cursor boundary the helper must keep records strictly past
        the bookmark and drop those already represented by tie-breakers."""
        connector = APIConnector("t")
        bookmark_cursor = "2024-01-01T00:00:00Z"
        batch = [
            {"updated_at": bookmark_cursor, "id": "5"},   # tie + smaller id -> dup
            {"updated_at": bookmark_cursor, "id": "10"},  # tie + exact id -> dup
            {"updated_at": bookmark_cursor, "id": "15"},  # tie + greater id -> new
            {"updated_at": "2024-01-02T00:00:00Z", "id": "1"},  # strictly after -> new
        ]
        state: Dict[str, Any] = {
            "bookmarks": [_bookmark(bookmark_cursor, [{"field": "id", "value": "10"}])],
        }
        kept = connector._deduplicate_records(batch, state, "updated_at", ["id"])
        assert [r["id"] for r in kept] == ["15", "1"]
