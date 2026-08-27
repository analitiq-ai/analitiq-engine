"""Tests for :mod:`cdk.request_binding`.

The binders rewrite ``from_param`` / ``from_input`` nodes into
``{"literal": ...}`` wraps and must treat expression nodes as opaque —
what sits inside a ``literal`` (or any marker node) belongs to the
resolution pass, never to a binding walk. End-to-end behavior through
the connectors is covered in the source/destination handler tests.
"""

from __future__ import annotations

import pytest
from analitiq.contracts.endpoints import Param

from cdk.request_binding import (
    bind_param_refs,
    bind_record_inputs,
    resolve_param_defaults,
)


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
        out = bind_param_refs({"filters": [{"from_param": "a"}, "static"]}, {"a": 1})
        assert out == {"filters": [{"literal": 1}, "static"]}

    def test_siblings_next_to_marker_raise(self):
        with pytest.raises(ValueError, match="only key"):
            bind_param_refs({"x": {"from_param": "a", "extra": 1}}, {"a": 1})

    def test_non_string_name_raises(self):
        with pytest.raises(ValueError, match="non-empty string"):
            bind_param_refs({"x": {"from_param": 7}}, {})

    def test_binds_inside_function_inputs(self):
        # A function node's input is authored structure, not data: the
        # binding must reach into it so the derived function receives the
        # bound value, never the raw {"from_param": ...} dict.
        out = bind_param_refs(
            {"auth": {"function": "base64_encode", "input": {"from_param": "token"}}},
            {"token": "t-1"},
        )
        assert out == {
            "auth": {"function": "base64_encode", "input": {"literal": "t-1"}}
        }


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

    def test_binds_inside_function_inputs(self):
        out = bind_record_inputs(
            {
                "token": {
                    "function": "base64_encode",
                    "input": {"from_input": "record.id"},
                }
            },
            record={"id": "abc"},
        )
        assert out == {
            "token": {"function": "base64_encode", "input": {"literal": "abc"}}
        }


class TestResolveParamDefaults:
    """Tests for :func:`cdk.request_binding.resolve_param_defaults`."""

    def _param(self, **fields):
        """Return a declared query param carrying *fields*.

        Built through the contract model the callers pass, so a test sees
        the same ``model_fields_set`` the implementation reads.
        """
        return Param.model_validate({"in": "query", "type": "string", **fields})

    def _make_resolver(self, mapping: dict):
        """Return a minimal resolver stub that looks up values in *mapping*."""

        class _Resolver:
            def __init__(self):
                self.seen = []

            def resolve_for_request(self, expr):
                self.seen.append(expr)
                return mapping.get(expr)

        return _Resolver()

    def test_resolves_defaults(self):
        resolver = self._make_resolver({"$page": 1, "$size": 50})
        spec = {
            "page": self._param(required=False, default="$page"),
            "size": self._param(required=False, default="$size"),
        }
        assert resolve_param_defaults(spec, resolver) == {"page": 1, "size": 50}

    def test_omits_none_resolution(self):
        resolver = self._make_resolver({})
        spec = {"page": self._param(required=False, default="$missing")}
        assert resolve_param_defaults(spec, resolver) == {}

    def test_omits_none_resolution_logs_warning(self, caplog):
        import logging

        resolver = self._make_resolver({})
        spec = {"page": self._param(required=False, default="$missing")}
        with caplog.at_level(logging.WARNING, logger="cdk.request_binding"):
            resolve_param_defaults(spec, resolver)
        assert "page" in caplog.text
        assert "did not resolve" in caplog.text

    def test_skips_entries_without_default(self):
        resolver = self._make_resolver({"$p": 1})
        spec = {
            "has_default": self._param(required=False, default="$p"),
            "no_default": self._param(required=True),
        }
        assert resolve_param_defaults(spec, resolver) == {"has_default": 1}

    def test_an_authored_null_default_is_not_an_omitted_one(self, caplog):
        """The contract fills an omitted ``default`` with ``None``.

        So the value alone cannot tell the two apart, and they behave
        differently: an omission is never offered to the resolver, while an
        authored null is a default nothing can resolve and must say so.
        """
        import logging

        resolver = self._make_resolver({})
        spec = {
            "no_default": self._param(required=True),
            "authored_null": self._param(required=False, default=None),
        }
        with caplog.at_level(logging.WARNING, logger="cdk.request_binding"):
            assert resolve_param_defaults(spec, resolver) == {}
        assert resolver.seen == [None]
        assert "authored_null" in caplog.text
        assert "no_default" not in caplog.text

    def test_context_label_in_warning(self, caplog):
        import logging

        resolver = self._make_resolver({})
        spec = {"amount": self._param(required=False, default="$missing")}
        with caplog.at_level(logging.WARNING, logger="cdk.request_binding"):
            resolve_param_defaults(spec, resolver, context="write param")
        assert "write param" in caplog.text

    def test_empty_spec_returns_empty_dict(self):
        resolver = self._make_resolver({})
        assert resolve_param_defaults({}, resolver) == {}

    def test_falsy_resolved_values_are_kept(self):
        resolver = self._make_resolver({"$zero": 0, "$flag": False, "$empty": ""})
        spec = {
            "page": self._param(required=False, default="$zero"),
            "enabled": self._param(required=False, default="$flag"),
            "prefix": self._param(required=False, default="$empty"),
        }
        assert resolve_param_defaults(spec, resolver) == {
            "page": 0,
            "enabled": False,
            "prefix": "",
        }


class TestCollectFromInputSelectors:
    def test_collects_nested_selectors_skipping_literals(self):
        from cdk.request_binding import collect_from_input_selectors

        spec = {
            "a": {"from_input": "record.id"},
            "b": {"items": [{"from_input": "records"}]},
            "c": {"literal": {"from_input": "record"}},  # data, not a binding
            "d": {"function": "base64_encode", "input": {"from_input": "record.key"}},
        }
        assert collect_from_input_selectors(spec) == {
            "record.id",
            "records",
            "record.key",
        }
