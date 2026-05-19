"""Unit tests for log_emitter emit_log output format."""

import json
from datetime import datetime

import pytest

from src.state.log_emitter import emit_log


class TestEmitLog:
    def test_prefix_is_upper_cased(self, capsys):
        emit_log("dlq", {"count": 3})
        out = capsys.readouterr().out
        assert out.startswith("ANALITIQ_DLQ::")

    def test_separator_is_double_colon(self, capsys):
        emit_log("state", {"run_id": "r1"})
        out = capsys.readouterr().out.strip()
        prefix, payload = out.split("::", 1)
        assert prefix == "ANALITIQ_STATE"
        assert json.loads(payload)["run_id"] == "r1"

    def test_lowercase_category_is_uppercased(self, capsys):
        emit_log("metrics", {"x": 1})
        out = capsys.readouterr().out
        assert out.startswith("ANALITIQ_METRICS::")

    def test_payload_round_trips(self, capsys):
        emit_log("dlq", {"pipeline_id": "p1", "added": 5, "total": 10})
        out = capsys.readouterr().out.strip()
        payload = json.loads(out.split("::", 1)[1])
        assert payload == {"pipeline_id": "p1", "added": 5, "total": 10}

    def test_default_str_handles_datetime(self, capsys):
        emit_log("state", {"ts": datetime(2026, 1, 1, 12, 0, 0)})
        out = capsys.readouterr().out
        assert "ANALITIQ_STATE::" in out
        assert "2026-01-01" in out

    def test_broken_stdout_does_not_raise(self, capsys, monkeypatch):
        def _bad_print(*args, **kwargs):
            raise OSError("broken pipe")

        import builtins
        monkeypatch.setattr(builtins, "print", _bad_print)
        # Should not raise
        emit_log("dlq", {"count": 1})
