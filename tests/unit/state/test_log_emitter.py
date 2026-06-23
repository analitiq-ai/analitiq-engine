"""Unit tests for the structured log emitter.

``emit_log`` is the single seam every observability record (state, metrics,
dlq) passes through. It stamps two envelope fields on every record: ``org_id``
(for downstream keying) and ``emitted_at`` (a sub-second UTC ISO-8601
emission timestamp the collector uses as a single ordering key across all
categories). These tests lock that envelope.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

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


def test_emitted_at_is_non_decreasing_across_calls(caplog):
    with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
        for i in range(10):
            emit_log("state", {"seq": i})

    stamps = [payload["emitted_at"] for _, payload in _emitted_records(caplog)]
    assert len(stamps) == 10
    assert stamps == sorted(stamps)


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
