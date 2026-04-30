"""Tests for pure helpers in :mod:`src.engine.pipeline`.

The helpers are transitional shims that smooth the gap between legacy
``${name}`` placeholders in endpoint files and the typed ``ref`` /
``template`` expressions the spec calls for. They are tracked for removal
in analitiq-engine#37; until then their precedence and warning contracts
need to stay locked down.
"""

from __future__ import annotations

import logging

from src.engine.pipeline import (
    _flat_connection_lookup,
    _warn_unresolved_placeholders,
)


# ---------------------------------------------------------------------------
# _flat_connection_lookup
# ---------------------------------------------------------------------------


class TestFlatConnectionLookup:
    def test_combines_all_three_scopes(self):
        out = _flat_connection_lookup(
            {
                "parameters": {"region": "eu"},
                "selections": {"profile_id": 99},
                "discovered": {"api_domain": "tenant"},
            }
        )
        assert out == {
            "region": "eu",
            "profile_id": "99",
            "api_domain": "tenant",
        }

    def test_parameters_win_over_selections(self):
        out = _flat_connection_lookup(
            {
                "parameters": {"profile_id": "from-params"},
                "selections": {"profile_id": "from-selections"},
            }
        )
        assert out["profile_id"] == "from-params"

    def test_selections_win_over_discovered(self):
        out = _flat_connection_lookup(
            {
                "selections": {"api_domain": "from-selections"},
                "discovered": {"api_domain": "from-discovered"},
            }
        )
        assert out["api_domain"] == "from-selections"

    def test_inter_scope_conflict_emits_warning(self, caplog):
        """The reviewer flagged the silent-precedence risk. The contract
        is: when a higher-precedence scope overrides a *different* value
        from a lower-precedence scope, log a WARNING that names both
        scopes and both values so the operator sees the surprise."""
        caplog.set_level(logging.WARNING, logger="src.engine.pipeline")
        _flat_connection_lookup(
            {
                "discovered": {"profile_id": "discovered-value"},
                "selections": {"profile_id": "selections-value"},
                "parameters": {"profile_id": "parameters-value"},
            }
        )
        warn_messages = [
            rec.getMessage()
            for rec in caplog.records
            if rec.levelno == logging.WARNING
        ]
        # Two overrides happen: discovered → selections, then selections
        # → parameters. Both should be reported.
        assert len(warn_messages) == 2
        assert all("profile_id" in msg for msg in warn_messages)
        assert any("discovered" in msg and "selections" in msg for msg in warn_messages)
        assert any("selections" in msg and "parameters" in msg for msg in warn_messages)

    def test_same_value_across_scopes_is_silent(self, caplog):
        """A scope with the same value as a higher-precedence scope is
        NOT a conflict — only differing values trigger the warning."""
        caplog.set_level(logging.WARNING, logger="src.engine.pipeline")
        _flat_connection_lookup(
            {
                "selections": {"profile_id": "same-value"},
                "parameters": {"profile_id": "same-value"},
            }
        )
        assert all(rec.levelno < logging.WARNING for rec in caplog.records)

    def test_dict_and_list_values_skipped(self):
        # Non-scalar values are dropped — the legacy ``${name}`` model
        # only supported flat string substitution.
        out = _flat_connection_lookup(
            {
                "parameters": {
                    "host": "h",
                    "headers": {"Authorization": "Bearer x"},
                    "tags": ["a", "b"],
                }
            }
        )
        assert out == {"host": "h"}

    def test_none_values_skipped(self):
        out = _flat_connection_lookup({"parameters": {"host": "h", "port": None}})
        assert out == {"host": "h"}

    def test_non_dict_scope_ignored(self):
        out = _flat_connection_lookup(
            {"parameters": "not-a-dict", "selections": {"x": 1}}
        )
        assert out == {"x": "1"}

    def test_missing_scopes_return_empty(self):
        assert _flat_connection_lookup({}) == {}


# ---------------------------------------------------------------------------
# _warn_unresolved_placeholders
# ---------------------------------------------------------------------------


class TestWarnUnresolvedPlaceholders:
    """The flat-merge shim uses ``ignore_missing=True`` so the engine
    does not crash on legacy unresolved references; this helper is the
    visibility net that keeps the residue from flowing silently into
    downstream provider requests."""

    def test_warning_on_string_with_unresolved_placeholder(self, caplog):
        caplog.set_level(logging.WARNING, logger="src.engine.pipeline")
        _warn_unresolved_placeholders(
            "transfers?profile=${profile_id}", "endpoint", "stream-1"
        )
        assert any(
            "stream 'stream-1'" in rec.getMessage() and "${profile_id}" in rec.getMessage()
            for rec in caplog.records
        )

    def test_warning_on_dict_with_nested_placeholder(self, caplog):
        caplog.set_level(logging.WARNING, logger="src.engine.pipeline")
        _warn_unresolved_placeholders(
            {"profile": {"default": "${profile_id}"}, "status": {"default": "OK"}},
            "filters",
            "stream-2",
        )
        assert any(
            "${profile_id}" in rec.getMessage() and "filters" in rec.getMessage()
            for rec in caplog.records
        )

    def test_warning_on_list_with_nested_placeholder(self, caplog):
        caplog.set_level(logging.WARNING, logger="src.engine.pipeline")
        _warn_unresolved_placeholders(
            ["literal", "${unresolved}"], "filters", "stream-3"
        )
        assert any("${unresolved}" in rec.getMessage() for rec in caplog.records)

    def test_no_warning_when_all_resolved(self, caplog):
        caplog.set_level(logging.WARNING, logger="src.engine.pipeline")
        _warn_unresolved_placeholders(
            {"profile": {"default": "1234"}, "status": {"default": "OK"}},
            "filters",
            "stream-4",
        )
        assert all(rec.levelno < logging.WARNING for rec in caplog.records)

    def test_no_warning_for_non_string_leaves(self, caplog):
        caplog.set_level(logging.WARNING, logger="src.engine.pipeline")
        _warn_unresolved_placeholders(
            {"limit": 100, "active": True, "ratio": 1.5}, "filters", "stream-5"
        )
        assert all(rec.levelno < logging.WARNING for rec in caplog.records)

    def test_each_placeholder_listed_once(self, caplog):
        # Same placeholder in multiple places should be deduplicated in
        # the warning so the message stays readable on big endpoint dicts.
        caplog.set_level(logging.WARNING, logger="src.engine.pipeline")
        _warn_unresolved_placeholders(
            {
                "a": "${profile_id}",
                "b": "${profile_id}",
                "c": "${profile_id}",
            },
            "filters",
            "stream-6",
        )
        warning = next(
            rec.getMessage() for rec in caplog.records if rec.levelno == logging.WARNING
        )
        # The message lists sorted+deduped placeholders, so `${profile_id}`
        # should appear exactly once in the bracket text.
        assert warning.count("${profile_id}") == 1
