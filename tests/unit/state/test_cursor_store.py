"""Unit tests for the filesystem cursor checkpoint store.

The E2E suite covers the happy round-trip (incremental resumes from the
bookmark), but it cannot reach the corruption / write-failure branches — you
can't easily torn-write a checkpoint mid-Docker-run. These tests lock those
branches: a datetime cursor must round-trip with its type intact, and any
unreadable or unwritable checkpoint must degrade to a logged full re-scan
rather than crash the run.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import pytest

from src.state.store import CursorStore

_PIPELINE = "pipe-1"
_STREAM = "stream-1"


def test_datetime_cursor_round_trips_with_microseconds(tmp_path):
    store = CursorStore(tmp_path)
    value = datetime(2026, 2, 5, 0, 1, 0, 10010)

    store.set(_PIPELINE, _STREAM, value)
    loaded = store.get(_PIPELINE, _STREAM)

    assert isinstance(loaded, datetime)
    assert loaded == value


def test_date_cursor_round_trips_as_date(tmp_path):
    store = CursorStore(tmp_path)
    value = date(2026, 2, 5)

    store.set(_PIPELINE, _STREAM, value)
    loaded = store.get(_PIPELINE, _STREAM)

    assert isinstance(loaded, date) and not isinstance(loaded, datetime)
    assert loaded == value


@pytest.mark.parametrize("value", [42, "2026-02-05", 3.5])
def test_json_native_cursors_pass_through(tmp_path, value):
    store = CursorStore(tmp_path)

    store.set(_PIPELINE, _STREAM, value)

    assert store.get(_PIPELINE, _STREAM) == value


def test_unknown_type_tag_passes_through_unchanged(tmp_path):
    # A dict carrying an unrecognized __type__ (e.g. a tag written by a future
    # version) is returned verbatim rather than crashing the decode.
    import json

    store = CursorStore(tmp_path)
    path = tmp_path / _PIPELINE / f"{_STREAM}.json"
    path.parent.mkdir(parents=True)
    payload = {"__type__": "future-kind", "value": "x"}
    path.write_text(json.dumps({"cursor": payload}))

    assert store.get(_PIPELINE, _STREAM) == payload


def test_get_missing_returns_none(tmp_path):
    assert CursorStore(tmp_path).get(_PIPELINE, _STREAM) is None


def test_set_none_writes_nothing(tmp_path):
    store = CursorStore(tmp_path)
    store.set(_PIPELINE, _STREAM, None)
    assert store.get(_PIPELINE, _STREAM) is None


def test_set_leaves_no_temp_file(tmp_path):
    store = CursorStore(tmp_path)
    store.set(_PIPELINE, _STREAM, datetime(2026, 2, 5, 0, 1, 0))

    pipeline_dir = tmp_path / _PIPELINE
    assert (pipeline_dir / f"{_STREAM}.json").is_file()
    assert not list(pipeline_dir.glob("*.tmp"))


def test_corrupt_json_reverts_to_full_scan_loudly(tmp_path, caplog):
    store = CursorStore(tmp_path)
    path = tmp_path / _PIPELINE / f"{_STREAM}.json"
    path.parent.mkdir(parents=True)
    path.write_text("{not valid json")

    with caplog.at_level(logging.WARNING, logger="src.state.store"):
        assert store.get(_PIPELINE, _STREAM) is None
    assert any("unreadable" in r.message for r in caplog.records)


@pytest.mark.parametrize("bad_value", ["not-a-date", 123, None])
def test_bad_tagged_datetime_reverts_to_full_scan_loudly(tmp_path, caplog, bad_value):
    # Valid JSON, valid tag, but a value datetime.fromisoformat cannot parse
    # (ValueError for the string, TypeError for the non-string). Both must
    # converge on the same soft revert, not a hard crash out of get().
    store = CursorStore(tmp_path)
    path = tmp_path / _PIPELINE / f"{_STREAM}.json"
    path.parent.mkdir(parents=True)
    import json

    path.write_text(json.dumps({"cursor": {"__type__": "datetime", "value": bad_value}}))

    with caplog.at_level(logging.WARNING, logger="src.state.store"):
        assert store.get(_PIPELINE, _STREAM) is None
    assert any("unreadable" in r.message for r in caplog.records)


def test_write_failure_is_best_effort_and_warned_once(tmp_path, caplog):
    store = CursorStore(tmp_path)
    # Plant a file where the pipeline directory needs to be, so mkdir/write fail.
    blocker = tmp_path / _PIPELINE
    blocker.write_text("not a directory")

    with caplog.at_level(logging.WARNING, logger="src.state.store"):
        # Must not raise despite the write being impossible.
        store.set(_PIPELINE, _STREAM, datetime(2026, 2, 5, 0, 1, 0))
        store.set(_PIPELINE, _STREAM, datetime(2026, 2, 6, 0, 1, 0))

    warnings = [r for r in caplog.records if "failed to persist" in r.message]
    assert len(warnings) == 1  # warned once per path, not per call


def test_distinct_failing_paths_each_warn(tmp_path, caplog):
    store = CursorStore(tmp_path)
    (tmp_path / "pipe-a").write_text("blocker")
    (tmp_path / "pipe-b").write_text("blocker")

    with caplog.at_level(logging.WARNING, logger="src.state.store"):
        store.set("pipe-a", _STREAM, datetime(2026, 2, 5, 0, 1, 0))
        store.set("pipe-b", _STREAM, datetime(2026, 2, 5, 0, 1, 0))

    warnings = [r for r in caplog.records if "failed to persist" in r.message]
    assert len(warnings) == 2  # a distinct path's failure is not masked
