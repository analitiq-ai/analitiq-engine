"""Unit tests for the structured log emitter.

``emit_log`` is the single seam every observability record (state, metrics,
dlq) passes through. It stamps two envelope fields on every record: ``org_id``
(for downstream keying) and ``emitted_at`` (a sub-second UTC ISO-8601
emission timestamp the collector uses as a single ordering key across all
categories), plus ambient run-identity defaults ``run_id`` and (in cloud)
``invocation_id``. These tests lock that envelope.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import pytest

from src.state.log_emitter import emit_log


def _emitted_records(caplog):
    """Parse captured ``MARKER::{json}`` INFO lines into (marker, payload).

    The error log a serialisation failure also writes is skipped — only the
    emitted marker records carry a JSON payload.
    """
    out = []
    for record in caplog.records:
        if record.levelno != logging.INFO:
            continue
        marker, sep, payload = record.getMessage().partition("::")
        if not sep:
            continue
        out.append((marker, json.loads(payload)))
    return out


@pytest.mark.parametrize(
    "category,marker",
    [
        ("state", "ANALITIQ_STATE"),
        ("metrics", "ANALITIQ_METRICS"),
        ("dlq", "ANALITIQ_DLQ"),
    ],
)
def test_emitted_at_stamped_on_every_category(caplog, category, marker):
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log(category, {"k": "v"})

    (got_marker, payload), = _emitted_records(caplog)
    assert got_marker == marker
    parsed = datetime.fromisoformat(payload["emitted_at"])
    assert parsed.tzinfo is not None  # timezone-aware UTC
    # Envelope fields ride alongside the caller's payload, not replacing it.
    assert payload["k"] == "v"
    assert "org_id" in payload


def test_emitted_at_is_strictly_increasing_across_calls(caplog):
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        for i in range(10):
            emit_log("state", {"seq": i})

    stamps = [payload["emitted_at"] for _, payload in _emitted_records(caplog)]
    assert len(stamps) == 10
    assert stamps == sorted(stamps)  # non-decreasing
    assert len(set(stamps)) == 10  # clamp keeps them collision-free


def test_emitted_at_clamped_when_clock_steps_back(monkeypatch, caplog):
    # Simulate the host clock stepping backward mid-run: the second now() is
    # older than the first. emitted_at must still advance, so a later
    # checkpoint (newer cursor) never carries an older stamp.
    ticks = iter(
        [
            datetime(2026, 1, 1, 0, 0, 0, 500, tzinfo=timezone.utc),
            datetime(2026, 1, 1, 0, 0, 0, 200, tzinfo=timezone.utc),  # steps back
            datetime(2026, 1, 1, 0, 0, 0, 400, tzinfo=timezone.utc),  # still behind
        ]
    )

    class _FrozenClock:
        @staticmethod
        def now(tz=None):
            return next(ticks)

    monkeypatch.setattr("src.state.log_emitter.datetime", _FrozenClock)
    monkeypatch.setattr("src.state.log_emitter._last_emitted_at", None)

    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        for i in range(3):
            emit_log("state", {"seq": i})

    stamps = [payload["emitted_at"] for _, payload in _emitted_records(caplog)]
    assert stamps == sorted(stamps)  # monotonic despite the backward step
    assert len(set(stamps)) == 3  # and collision-free


def test_caller_field_named_emitted_at_is_overridden_by_envelope(caplog):
    # The envelope owns emitted_at: a stray caller value must not win, so the
    # collector always sees a real emission timestamp.
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log("state", {"emitted_at": "not-a-timestamp"})

    (_, payload), = _emitted_records(caplog)
    assert payload["emitted_at"] != "not-a-timestamp"
    datetime.fromisoformat(payload["emitted_at"])  # raises if not ISO-8601


def test_emitted_at_survives_unserialisable_payload(caplog):
    # A non-serialisable payload falls back to an error record; emitted_at must
    # still be present so the failure line is orderable like any other.
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log("state", {("tuple", "key"): 1})  # non-str key -> json.dumps raises

    (_, payload), = _emitted_records(caplog)
    assert "error" in payload
    datetime.fromisoformat(payload["emitted_at"])  # raises if not ISO-8601


@pytest.mark.parametrize("category", ["state", "metrics", "dlq"])
def test_run_id_stamped_on_every_record(monkeypatch, caplog, category):
    # run_id rides on every record so collectors can attribute it to a run
    # even when the caller payload omits it.
    monkeypatch.setenv("RUN_ID", "run-xyz")
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log(category, {"k": "v"})

    (_, payload), = _emitted_records(caplog)
    assert payload["run_id"] == "run-xyz"


def test_caller_run_id_overrides_ambient_default(monkeypatch, caplog):
    # The ambient run_id is a default, not an envelope: a caller that owns
    # run_id (the typed metrics field) still wins.
    monkeypatch.setenv("RUN_ID", "ambient-run")
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log("metrics", {"run_id": "caller-run"})

    (_, payload), = _emitted_records(caplog)
    assert payload["run_id"] == "caller-run"


def test_invocation_id_present_when_env_set(monkeypatch, caplog):
    # In cloud the control plane injects INVOCATION_ID; echoing it lets the
    # run-status processor key lookups by it directly.
    monkeypatch.setenv("INVOCATION_ID", "inv-123")
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log("state", {"k": "v"})

    (_, payload), = _emitted_records(caplog)
    assert payload["invocation_id"] == "inv-123"


def test_invocation_id_omitted_when_env_absent(monkeypatch, caplog):
    # Local runs have no INVOCATION_ID: the field is omitted, not null or an
    # error.
    monkeypatch.delenv("INVOCATION_ID", raising=False)
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log("state", {"k": "v"})

    (_, payload), = _emitted_records(caplog)
    assert "invocation_id" not in payload


def test_blank_invocation_id_is_treated_as_absent(monkeypatch, caplog):
    # A whitespace-only INVOCATION_ID is not a real id and must not surface.
    monkeypatch.setenv("INVOCATION_ID", "   ")
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log("state", {"k": "v"})

    (_, payload), = _emitted_records(caplog)
    assert "invocation_id" not in payload


def test_run_identity_present_in_error_fallback(monkeypatch, caplog):
    # The serialisation-failure fallback record must still carry run-identity
    # so a dropped payload stays correlatable.
    monkeypatch.setenv("RUN_ID", "run-xyz")
    monkeypatch.setenv("INVOCATION_ID", "inv-123")
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        emit_log("state", {("tuple", "key"): 1})  # non-str key -> json.dumps raises

    (_, payload), = _emitted_records(caplog)
    assert "error" in payload
    assert payload["run_id"] == "run-xyz"
    assert payload["invocation_id"] == "inv-123"
