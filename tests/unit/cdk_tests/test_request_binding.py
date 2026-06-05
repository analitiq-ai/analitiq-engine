"""Tests for :mod:`cdk.request_binding`.

The binders rewrite ``from_param`` / ``from_input`` nodes into
``{"literal": ...}`` wraps and must treat expression nodes as opaque —
what sits inside a ``literal`` (or any marker node) belongs to the
resolution pass, never to a binding walk. End-to-end behavior through
the connectors is covered in the source/destination handler tests.
"""

from __future__ import annotations

import pytest

from cdk.request_binding import bind_param_refs, bind_record_inputs


class TestBindParamRefs:
    def test_binds_named_param_as_literal(self):
        out = bind_param_refs({"q": {"from_param": "search"}}, {"search": "abc"})
        assert out == {"q": {"literal": "abc"}}

    def test_missing_param_binds_literal_none(self):
        # The expression pass drops literal None per the omit rule; the
        # binder's job is only to never leave the raw binding dict behind.
        out = bind_param_refs({"q": {"from_param": "nope"}}, {})
        assert out == {"q": {"literal": None}}

    def test_expression_nodes_are_opaque(self):
        # An authored literal containing what looks like a binding node is
        # data for the resolution pass, not a binding site.
        spec = {"meta": {"literal": {"from_param": "search"}}}
        assert bind_param_refs(spec, {"search": "abc"}) == spec

    def test_binds_inside_lists(self):
        out = bind_param_refs(
            {"filters": [{"from_param": "a"}, "static"]}, {"a": 1}
        )
        assert out == {"filters": [{"literal": 1}, "static"]}

    def test_siblings_next_to_marker_raise(self):
        with pytest.raises(ValueError, match="only key"):
            bind_param_refs({"x": {"from_param": "a", "extra": 1}}, {"a": 1})

    def test_non_string_name_raises(self):
        with pytest.raises(ValueError, match="non-empty string"):
            bind_param_refs({"x": {"from_param": 7}}, {})


class TestBindRecordInputs:
    def test_expression_nodes_are_opaque(self):
        spec = {"meta": {"literal": {"from_input": "record"}}}
        assert bind_record_inputs(spec, record={"id": 1}) == spec

    def test_dotted_path_to_missing_field_binds_none(self):
        out = bind_record_inputs(
            {"x": {"from_input": "record.absent"}}, record={"id": 1}
        )
        assert out == {"x": {"literal": None}}

    def test_record_bound_verbatim(self):
        record = {"id": 1, "payload": {"ref": "user text"}}
        out = bind_record_inputs({"data": {"from_input": "record"}}, record=record)
        assert out == {"data": {"literal": record}}
