"""Declared source categories cross the worker boundary as data (issue #401).

Birth-site architecture: the worker classifies against the declared
``error_map`` where the failure is raised and forwards the category on the
``ReadError`` wire message; the engine derives the published code via
``source_code_for_declared_category`` and never re-matches. Since issue #429
there is no text split behind it -- a source failure nothing declared takes
the extract stage's own default.
"""

from __future__ import annotations

import logging

import pytest

from src.state.error_classification import (
    ErrorCode,
    FailureStage,
    default_code_for_stage,
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


class TestUndeclaredSourceFailure:
    def test_extract_stage_default_is_internal(self):
        # Decision 6.1. The extract stage establishes that the source side
        # broke and nothing more. SOURCE_AUTH_FAILED / SOURCE_UNREACHABLE /
        # RATE_LIMITED each name a mechanism the stage did not observe, so a
        # connector that declared no error_map entry gets the honest verdict
        # rather than the most likely-looking one.
        assert default_code_for_stage(FailureStage.SOURCE_EXTRACT) is ErrorCode.INTERNAL

    def test_no_source_code_is_reachable_without_a_declaration(self):
        # The source-specific codes exist only for connectors that declare.
        # If a stage default ever names one, an undeclared failure starts
        # claiming a mechanism again -- the regression this locks out.
        source_only = {
            ErrorCode.SOURCE_AUTH_FAILED,
            ErrorCode.SOURCE_UNREACHABLE,
            ErrorCode.RATE_LIMITED,
        }
        defaults = {default_code_for_stage(stage) for stage in FailureStage}
        assert not (defaults & source_only)
