"""Unit tests for the filesystem cursor checkpoint store.

The E2E suite covers the happy round-trip (incremental resumes from the
bookmark), but it cannot reach the corruption / write-failure branches — you
can't easily torn-write a checkpoint mid-Docker-run. These tests lock those
branches: a datetime cursor must round-trip with its type intact, and any
unreadable or unwritable checkpoint must degrade to a logged full re-scan
rather than crash the run.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

import pytest

from src.state.store import (
    CursorStore,
    load_resume_file,
    parse_resume_state,
    write_resume_file,
)

_PIPELINE = "pipe-1"
_STREAM = "stream-1"


def test_datetime_cursor_round_trips_with_microseconds(tmp_path):
    store = CursorStore(tmp_path)
    value = datetime(2026, 2, 5, 0, 1, 0, 10010, tzinfo=timezone.utc)

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
    store.set(_PIPELINE, _STREAM, datetime(2026, 2, 5, 0, 1, 0, tzinfo=timezone.utc))

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

    path.write_text(
        json.dumps({"cursor": {"__type__": "datetime", "value": bad_value}})
    )

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
        store.set(
            _PIPELINE, _STREAM, datetime(2026, 2, 5, 0, 1, 0, tzinfo=timezone.utc)
        )
        store.set(
            _PIPELINE, _STREAM, datetime(2026, 2, 6, 0, 1, 0, tzinfo=timezone.utc)
        )

    warnings = [r for r in caplog.records if "failed to persist" in r.message]
    assert len(warnings) == 1  # warned once per path, not per call


def test_distinct_failing_paths_each_warn(tmp_path, caplog):
    store = CursorStore(tmp_path)
    (tmp_path / "pipe-a").write_text("blocker")
    (tmp_path / "pipe-b").write_text("blocker")

    with caplog.at_level(logging.WARNING, logger="src.state.store"):
        store.set("pipe-a", _STREAM, datetime(2026, 2, 5, 0, 1, 0, tzinfo=timezone.utc))
        store.set("pipe-b", _STREAM, datetime(2026, 2, 5, 0, 1, 0, tzinfo=timezone.utc))

    warnings = [r for r in caplog.records if "failed to persist" in r.message]
    assert len(warnings) == 2  # a distinct path's failure is not masked


class TestParseResumeState:
    """Decoding the resume-state payload a fresh container restores from.

    On Fargate the local per-stream ``state/`` checkpoints are empty, so the
    delivered resume file is the only bookmark an incremental stream can resume
    from. Each value carries its type the same way the on-disk checkpoint does:
    a ``datetime``/``date`` is the tagged ``{"__type__": ..., "value": ...}``
    form and decodes back to the real type (asyncpg rejects a string for a
    timestamp bind); a JSON-native ``int``/``str`` passes through untouched -- a
    string is never coerced from its shape.
    """

    @pytest.mark.parametrize("raw", [None, ""])
    def test_missing_payload_is_empty(self, raw):
        assert parse_resume_state(raw) == {}

    def test_numeric_cursor_passes_through_as_int(self):
        restored = parse_resume_state(json.dumps({"s1": 100}))
        assert restored == {"s1": 100}
        assert isinstance(restored["s1"], int)

    def test_tagged_timestamp_decodes_as_datetime(self):
        ts = "2024-06-01T12:00:00+00:00"
        tagged = {"__type__": "datetime", "value": ts}
        restored = parse_resume_state(json.dumps({"s1": tagged}))
        assert restored["s1"] == datetime.fromisoformat(ts)
        assert isinstance(restored["s1"], datetime)

    def test_tagged_date_decodes_as_date(self):
        tagged = {"__type__": "date", "value": "2024-06-01"}
        restored = parse_resume_state(json.dumps({"s1": tagged}))
        assert restored["s1"] == date(2024, 6, 1)
        assert isinstance(restored["s1"], date) and not isinstance(
            restored["s1"], datetime
        )

    def test_iso_looking_string_cursor_is_not_coerced(self):
        # The P2 fix: an untagged string is a string, even if it looks like a
        # timestamp. A varchar cursor whose values resemble dates must not be
        # turned into a datetime (which would mis-bind as varchar > timestamp).
        restored = parse_resume_state(json.dumps({"s1": "2024-06-01T12:00:00"}))
        assert restored["s1"] == "2024-06-01T12:00:00"
        assert isinstance(restored["s1"], str)

    def test_opaque_string_cursor_stays_a_string(self):
        restored = parse_resume_state(json.dumps({"s1": "v2.1.0"}))
        assert restored["s1"] == "v2.1.0"

    def test_null_cursor_value_preserved_as_none(self):
        # A stream the deployment harvested before it ever emitted a cursor
        # arrives as JSON null. It must decode to None (not a fabricated value)
        # so _restore_durable_cursors can skip it and leave the stream to a
        # full re-scan rather than seeding a useless {"cursor": None}.
        restored = parse_resume_state(json.dumps({"orders": None}))
        assert restored == {"orders": None}

    def test_multiple_streams_decoded_independently(self):
        restored = parse_resume_state(
            json.dumps(
                {
                    "num": 42,
                    "ts": {
                        "__type__": "datetime",
                        "value": "2024-06-01T00:00:00+00:00",
                    },
                }
            )
        )
        assert restored == {
            "num": 42,
            "ts": datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
        }

    def test_invalid_json_degrades_to_empty_loudly(self, caplog):
        with caplog.at_level(logging.WARNING, logger="src.state.store"):
            assert parse_resume_state("{not json") == {}
        assert any("not valid JSON" in r.message for r in caplog.records)

    def test_non_object_payload_degrades_to_empty_loudly(self, caplog):
        with caplog.at_level(logging.WARNING, logger="src.state.store"):
            assert parse_resume_state(json.dumps([1, 2, 3])) == {}
        assert any("must be a JSON object" in r.message for r in caplog.records)

    def test_malformed_tagged_value_skips_stream_loudly(self, caplog):
        # A corrupt tagged datetime (bad durable state or manual injection) must
        # not abort StateManager construction: skip the bad stream (full
        # re-scan), keep the good ones, and log it -- like CursorStore.get does
        # for an unreadable on-disk checkpoint.
        payload = {
            "good": 100,
            "bad": {"__type__": "datetime", "value": "not-a-real-timestamp"},
        }
        with caplog.at_level(logging.WARNING, logger="src.state.store"):
            restored = parse_resume_state(json.dumps(payload))
        assert restored == {"good": 100}
        assert any("unreadable" in r.message for r in caplog.records)


class TestResumeFile:
    """The on-disk resume file a local run writes and every run reads.

    A local run writes the consolidated ``{stream_id: cursor}`` map at the end of
    a run; the cloud deployment delivers the byte-identical file in the config
    bundle. Both are read back the same way, so write_resume_file and
    load_resume_file must round-trip the tagged-value contract and degrade a
    missing/unreadable file to an empty mapping (a full re-scan) rather than
    crashing the run.
    """

    def test_round_trips_numeric_and_timestamp_cursors(self, tmp_path):
        path = tmp_path / "resume.json"
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        write_resume_file(path, {"orders": 100, "events": ts})

        # The timestamp is tagged on disk, so it comes back a datetime, not a
        # string -- the same shape the deployment-delivered file uses.
        on_disk = json.loads(path.read_text())
        assert on_disk == {
            "orders": 100,
            "events": {"__type__": "datetime", "value": ts.isoformat()},
        }
        assert load_resume_file(path) == {"orders": 100, "events": ts}

    def test_missing_file_is_empty(self, tmp_path):
        assert load_resume_file(tmp_path / "absent.json") == {}

    def test_write_creates_parent_directory(self, tmp_path):
        path = tmp_path / "nested" / "dir" / "resume.json"
        write_resume_file(path, {"orders": 5})
        assert load_resume_file(path) == {"orders": 5}

    def test_unreadable_file_degrades_to_empty_loudly(self, tmp_path, caplog):
        # A directory in place of the file makes read_text raise OSError; it must
        # degrade to a full re-scan, not crash StateManager construction.
        path = tmp_path / "resume.json"
        path.mkdir()
        with caplog.at_level(logging.WARNING, logger="src.state.store"):
            assert load_resume_file(path) == {}
        assert any("unreadable" in r.message for r in caplog.records)

    def test_write_failure_does_not_raise(self, tmp_path, caplog):
        # A path whose parent cannot be created (a file sits where a dir must go)
        # must warn and move on, never abort an otherwise-successful run.
        blocker = tmp_path / "blocker"
        blocker.write_text("not a dir")
        path = blocker / "resume.json"
        with caplog.at_level(logging.WARNING, logger="src.state.store"):
            write_resume_file(path, {"orders": 1})  # must not raise
        assert any(
            "failed to write resume-state file" in r.message for r in caplog.records
        )
