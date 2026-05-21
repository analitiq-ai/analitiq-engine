"""Unit tests for log_emitter emit_log output format."""

import json
import logging
from datetime import datetime

from src.state.log_emitter import emit_log


def _captured_payload(caplog) -> tuple[str, dict]:
    """Return (marker, parsed payload) from the single emitted log record."""
    assert len(caplog.records) == 1, f"expected 1 record, got {len(caplog.records)}"
    message = caplog.records[0].getMessage()
    marker, raw_payload = message.split("::", 1)
    return marker, json.loads(raw_payload)


class TestEmitLog:
    def test_prefix_is_upper_cased(self, caplog):
        with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
            emit_log("dlq", {"count": 3})
        marker, _ = _captured_payload(caplog)
        assert marker == "ANALITIQ_DLQ"

    def test_separator_is_double_colon(self, caplog):
        with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
            emit_log("state", {"run_id": "r1"})
        marker, payload = _captured_payload(caplog)
        assert marker == "ANALITIQ_STATE"
        assert payload["run_id"] == "r1"

    def test_metrics_category_maps_to_metrics_marker(self, caplog):
        with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
            emit_log("metrics", {"x": 1})
        marker, _ = _captured_payload(caplog)
        assert marker == "ANALITIQ_METRICS"

    def test_payload_round_trips_with_org_id(self, caplog):
        with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
            emit_log("dlq", {"pipeline_id": "p1", "added": 5, "total": 10})
        _, payload = _captured_payload(caplog)
        assert payload["pipeline_id"] == "p1"
        assert payload["added"] == 5
        assert payload["total"] == 10
        assert "org_id" in payload

    def test_default_str_handles_datetime(self, caplog):
        with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
            emit_log("state", {"ts": datetime(2026, 1, 1, 12, 0, 0)})
        marker, payload = _captured_payload(caplog)
        assert marker == "ANALITIQ_STATE"
        assert payload["ts"].startswith("2026-01-01")

    def test_unknown_category_falls_back_to_uppercased_name(self, caplog):
        with caplog.at_level(logging.INFO, logger="src.state.log_emitter"):
            emit_log("custom", {"k": "v"})
        marker, payload = _captured_payload(caplog)
        assert marker == "CUSTOM"
        assert payload["k"] == "v"
