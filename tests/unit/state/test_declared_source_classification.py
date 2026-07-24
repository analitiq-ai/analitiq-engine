"""Declared error_map consumption in the source-extract split (issue #401).

Resolution order: declared map first (structural — live chain attributes or
boundary-collapsed class names), text heuristics last resort only. The
categories that claim no source code (``transient``, ``write_rejected``)
fall through to the text split.
"""

from __future__ import annotations

import pytest

from cdk.declarations import parse_declared_error_map
from src.state.error_classification import ErrorCode, classify_source_extract


@pytest.fixture()
def error_map():
    parsed = parse_declared_error_map(
        {
            "sqlstate": {"28000": "auth"},
            "exception": {
                "AutoReconnect": "transient",
                "OperationalTeapotError": "rate_limited",
                "MisleadingError": "auth",
            },
        }
    )
    assert parsed is not None
    return parsed


class TestDeclaredFirst:
    def test_declared_sqlstate_beats_misleading_text(self, error_map):
        # The message reads like an unreachable host; the declared SQLSTATE
        # says auth. Declared facts win over phrases.
        exc = Exception("connection timed out talking to host")
        exc.sqlstate = "28000"
        assert (
            classify_source_extract(exc, error_map=error_map)
            == ErrorCode.SOURCE_AUTH_FAILED
        )

    def test_declared_name_classifies_unrecognized_text(self, error_map):
        class OperationalTeapotError(Exception):
            pass

        exc = OperationalTeapotError("E418 short and stout")
        assert (
            classify_source_extract(exc, error_map=error_map) == ErrorCode.RATE_LIMITED
        )

    def test_boundary_collapsed_name_matches(self, error_map):
        # Across the worker boundary only the error_type prefix survives;
        # _signature promotes it into the name set and the declared
        # exception family claims it (#245's class: a JSON edit, no code).
        exc = RuntimeError("AutoReconnect: connection pool closed (worker x)")
        # transient claims no source code; the text split then sees no
        # recognizable phrase in this message and stays INTERNAL.
        assert classify_source_extract(exc, error_map=error_map) == ErrorCode.INTERNAL

    def test_transient_falls_through_to_text(self, error_map):
        class AutoReconnect(Exception):
            pass

        # Declared transient speaks to retryability, not the terminal code;
        # the text split still reads the unreachable phrase.
        exc = AutoReconnect("connection refused by peer")
        assert (
            classify_source_extract(exc, error_map=error_map)
            == ErrorCode.SOURCE_UNREACHABLE
        )


class TestFallbackUnchanged:
    def test_no_map_keeps_text_behavior(self):
        exc = Exception("password authentication failed for user x")
        assert classify_source_extract(exc) == ErrorCode.SOURCE_AUTH_FAILED

    def test_unclaimed_exception_uses_text(self, error_map):
        exc = Exception("too many requests")
        assert (
            classify_source_extract(exc, error_map=error_map) == ErrorCode.RATE_LIMITED
        )

    def test_local_io_guard_still_precedes_the_map(self, error_map):
        # A local permission failure is engine infra, never source auth —
        # even a declared map cannot reach past the local-I/O guard.
        exc = PermissionError(13, "Permission denied")
        assert classify_source_extract(exc, error_map=error_map) == ErrorCode.INTERNAL

    def test_heuristic_fallback_logs(self, error_map, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger="src.state.error_classification"):
            classify_source_extract(Exception("gremlins"), error_map=error_map)
        assert any("text heuristic" in r.message for r in caplog.records)
