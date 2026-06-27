"""Unit tests for resume-cursor encoding and the consolidated resume file.

The E2E suite covers the happy round-trip (an incremental stream resumes from
the committed bookmark), but it cannot reach the corruption / write-failure
branches -- you can't easily torn-write a file mid-Docker-run. These tests lock
those branches: tagged cursor values must round-trip with their type intact, and
any unreadable or unwritable resume file must degrade to a logged full re-scan
rather than crash the run.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from src.state.store import load_resume_file, parse_resume_state, write_resume_file


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
        # re-scan), keep the good ones, and log it.
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
    a run; the cloud deployment delivers the same file in the config bundle. Both
    decode back the same way, so write_resume_file and load_resume_file must
    round-trip the tagged-value contract and degrade a missing/unreadable file to
    an empty mapping (a full re-scan) rather than crashing the run.
    """

    def test_round_trips_native_and_tagged_cursors(self, tmp_path):
        # Every cursor type the engine can present: JSON-native int/float, and
        # the tagged datetime/date/decimal forms that must come back as their
        # real type (a decimal/date resuming as a bare string would mis-bind the
        # source filter). default=str must never reach a tagged value. An
        # unrecognized tag (a value written by a future version) passes through
        # unchanged rather than crashing the decode.
        path = tmp_path / "resume.json"
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        day = date(2024, 6, 1)
        amount = Decimal("123.45")
        future = {"__type__": "future-kind", "value": "x"}
        write_resume_file(
            path,
            {
                "orders": 100,
                "ratio": 3.5,
                "events": ts,
                "day": day,
                "amount": amount,
                "later": future,
            },
        )

        on_disk = json.loads(path.read_text())
        assert on_disk == {
            "orders": 100,
            "ratio": 3.5,
            "events": {"__type__": "datetime", "value": ts.isoformat()},
            "day": {"__type__": "date", "value": day.isoformat()},
            "amount": {"__type__": "decimal", "value": "123.45"},
            "later": future,
        }
        assert load_resume_file(path) == {
            "orders": 100,
            "ratio": 3.5,
            "events": ts,
            "day": day,
            "amount": amount,
            "later": future,
        }

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

    def test_non_utf8_file_degrades_to_empty_loudly(self, tmp_path, caplog):
        # A corrupt/binary bundle artifact (invalid UTF-8) makes read_text raise
        # UnicodeDecodeError; it must degrade to a re-scan like any other bad
        # resume state, not abort StateManager construction.
        path = tmp_path / "cursors.json"
        path.write_bytes(b"\xff\xfe\x00not utf-8")
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
