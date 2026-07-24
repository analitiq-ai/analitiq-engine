"""Declared source categories cross the worker boundary as data (issue #401).

Birth-site architecture: the worker classifies against the declared
``error_map`` where the failure is raised and forwards the category on the
``ReadError`` wire message; the engine derives the published code via
``source_code_for_declared_category`` and never re-matches. The text split
in ``classify_source_extract`` stays the logged last resort for failures
nothing claimed.
"""

from __future__ import annotations

import logging

import pytest

from src.state.error_classification import (
    ErrorCode,
    classify_source_extract,
    source_code_for_declared_category,
)


class TestDeclaredCategoryToCode:
    @pytest.mark.parametrize(
        ("category", "code"),
        [
            ("auth", ErrorCode.SOURCE_AUTH_FAILED),
            ("unreachable", ErrorCode.SOURCE_UNREACHABLE),
            ("rate_limited", ErrorCode.RATE_LIMITED),
            ("config", ErrorCode.CONFIG_INVALID),
        ],
    )
    def test_code_claiming_categories(self, category, code):
        assert source_code_for_declared_category(category) == code

    @pytest.mark.parametrize("category", ["transient", "write_rejected"])
    def test_categories_claiming_no_source_code(self, category):
        # They speak to retryability, not to which published code names
        # the terminal cause.
        assert source_code_for_declared_category(category) is None

    def test_off_vocabulary_wire_value_is_ignored_and_logged(self, caplog):
        with caplog.at_level(logging.WARNING, logger="src.state.error_classification"):
            assert source_code_for_declared_category("weird") is None
        assert any("not in the engine vocabulary" in r.message for r in caplog.records)


class TestTextSplitLastResort:
    def test_unclaimed_failure_uses_text(self):
        exc = Exception("password authentication failed for user x")
        assert classify_source_extract(exc) == ErrorCode.SOURCE_AUTH_FAILED

    def test_local_io_guard_precedes_the_text_split(self):
        # A local permission failure is engine infra, never source auth.
        exc = PermissionError(13, "Permission denied")
        assert classify_source_extract(exc) == ErrorCode.INTERNAL

    def test_heuristic_fallback_logs(self, caplog):
        with caplog.at_level(logging.INFO, logger="src.state.error_classification"):
            classify_source_extract(Exception("gremlins"))
        assert any("text heuristic" in r.message for r in caplog.records)
