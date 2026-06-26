"""Tests for :mod:`cdk.resolver`.

Focuses on edge cases the higher-level transport-factory tests do not
reach: scope traversal failures, expression-marker discipline, template
substitution constraints, and function dispatch.
"""

from __future__ import annotations

import pytest

from cdk.exceptions import TransportSpecError
from cdk.resolver import ResolutionContext, Resolver

# ---------------------------------------------------------------------------
# ResolutionContext.lookup
# ---------------------------------------------------------------------------


class TestResolutionContextLookup:
    def test_lookup_simple_path(self):
        ctx = ResolutionContext(connection={"parameters": {"host": "h"}})
        assert ctx.lookup("connection.parameters.host") == "h"

    def test_lookup_preserves_native_type(self):
        ctx = ResolutionContext(
            connection={"parameters": {"port": 5432, "verify": True}}
        )
        assert ctx.lookup("connection.parameters.port") == 5432
        assert ctx.lookup("connection.parameters.verify") is True

    def test_unknown_scope_raises_keyerror(self):
        ctx = ResolutionContext()
        with pytest.raises(KeyError, match="Unknown resolution scope"):
            ctx.lookup("not_a_scope.foo")

    def test_missing_segment_includes_available_keys(self):
        ctx = ResolutionContext(connection={"parameters": {"host": "h"}})
        with pytest.raises(KeyError, match="missing key 'port'"):
            ctx.lookup("connection.parameters.port")

    def test_traversing_none_raises_keyerror(self):
        ctx = ResolutionContext(connection={"parameters": None})
        with pytest.raises(KeyError, match="encountered None"):
            ctx.lookup("connection.parameters.host")

    def test_indexing_into_scalar_raises_keyerror(self):
        ctx = ResolutionContext(connection={"parameters": {"host": "h"}})
        with pytest.raises(KeyError, match="cannot index str"):
            ctx.lookup("connection.parameters.host.deeper")

    def test_empty_path_raises_transport_spec_error(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="non-empty"):
            ctx.lookup("")

    def test_with_runtime_returns_copy(self):
        original = ResolutionContext(
            connection={"parameters": {"host": "h"}},
            runtime={"batch_size": 10},
        )
        clone = original.with_runtime({"batch_size": 99})
        assert clone is not original
        assert clone.runtime == {"batch_size": 99}
        assert clone.connection == original.connection
        # Original is untouched.
        assert original.runtime == {"batch_size": 10}


# ---------------------------------------------------------------------------
# Resolver: ref / literal / template
# ---------------------------------------------------------------------------


class TestResolverRefAndLiteral:
    def test_ref_returns_native_value(self):
        ctx = ResolutionContext(connection={"parameters": {"port": 5432}})
        assert Resolver(ctx).resolve({"ref": "connection.parameters.port"}) == 5432

    def test_ref_must_be_string(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="`ref` must be a string"):
            Resolver(ctx).resolve({"ref": 42})

    def test_literal_returns_value_unchanged(self):
        ctx = ResolutionContext()
        # Even structures that look like other expressions are returned verbatim.
        assert Resolver(ctx).resolve({"literal": {"ref": "scope.path"}}) == {
            "ref": "scope.path"
        }

    def test_literal_with_sibling_keys_rejected(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="must be the only key"):
            Resolver(ctx).resolve({"literal": "ok", "extra": 1})


class TestResolverTemplate:
    def test_template_substitutes_scalars(self):
        ctx = ResolutionContext(connection={"parameters": {"host": "h", "port": 5432}})
        out = Resolver(ctx).resolve(
            {"template": "${connection.parameters.host}:${connection.parameters.port}"}
        )
        assert out == "h:5432"

    def test_template_no_placeholders_returns_string_as_is(self):
        ctx = ResolutionContext()
        assert (
            Resolver(ctx).resolve({"template": "https://api.example.com"})
            == "https://api.example.com"
        )

    def test_template_must_be_string(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="`template` must be a string"):
            Resolver(ctx).resolve({"template": 42})

    def test_template_unterminated_placeholder_raises(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="Unterminated"):
            Resolver(ctx).resolve({"template": "abc${unterminated"})

    def test_template_placeholder_resolving_to_none_raises(self):
        ctx = ResolutionContext(connection={"parameters": {"host": None}})
        with pytest.raises(KeyError, match="resolved to None"):
            Resolver(ctx).resolve({"template": "${connection.parameters.host}"})

    def test_template_falsy_scalars_are_substituted(self):
        ctx = ResolutionContext(
            connection={"parameters": {"flag": False, "count": 0, "name": ""}}
        )
        r = Resolver(ctx)
        assert r.resolve({"template": "${connection.parameters.flag}"}) == "False"
        assert r.resolve({"template": "${connection.parameters.count}"}) == "0"
        assert r.resolve({"template": "${connection.parameters.name}"}) == ""

    def test_template_rejects_non_scalar_values(self):
        # Common authoring mistake: template referencing an object instead
        # of a leaf scalar. We surface it instead of producing repr garbage.
        ctx = ResolutionContext(
            connection={"parameters": {"headers": {"Authorization": "Bearer t"}}}
        )
        with pytest.raises(TransportSpecError, match="only scalars"):
            Resolver(ctx).resolve({"template": "${connection.parameters.headers}"})


# ---------------------------------------------------------------------------
# Resolver: function dispatch + marker discipline
# ---------------------------------------------------------------------------


def _capture(node, resolver):
    return ("captured", node.get("input"))


class TestResolverFunctions:
    def test_function_dispatch_invokes_registered_callable(self):
        ctx = ResolutionContext()
        resolver = Resolver(ctx, functions={"capture": _capture})
        assert resolver.resolve({"function": "capture", "input": "value"}) == (
            "captured",
            "value",
        )

    def test_unknown_function_raises_transport_spec_error(self):
        # A typo'd function name is an authoring defect against the closed
        # registry — the deterministic-config error type, never KeyError,
        # which the per-request drop policy would absorb as missing data.
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="Unknown derived function"):
            Resolver(ctx, functions={}).resolve({"function": "no_such", "input": "x"})

    def test_function_with_unexpected_sibling_key_rejected(self):
        # Sibling key that is not another expression marker (those are
        # caught by the conflicting-marker rule) and not in the allow-list.
        ctx = ResolutionContext()
        resolver = Resolver(ctx, functions={"capture": _capture})
        with pytest.raises(TransportSpecError, match="unexpected sibling keys"):
            resolver.resolve({"function": "capture", "input": "x", "bogus": 1})

    def test_function_with_allowed_siblings_resolves(self):
        ctx = ResolutionContext()
        resolver = Resolver(ctx, functions={"capture": _capture})
        # `input`, `map`, and `safe` are documented allowed siblings.
        out = resolver.resolve(
            {"function": "capture", "input": "x", "map": {"x": "y"}, "safe": ""}
        )
        assert out == ("captured", "x")

    def test_empty_function_name_raises_transport_spec_error(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="`function` field must name"):
            Resolver(ctx).resolve({"function": ""})

    def test_non_string_function_name_raises_transport_spec_error(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="`function` field must name"):
            Resolver(ctx).resolve({"function": None})

    def test_register_existing_function_rejected(self):
        ctx = ResolutionContext()
        resolver = Resolver(ctx, functions={"capture": _capture})
        with pytest.raises(ValueError, match="already registered"):
            resolver.register("capture", _capture)


class TestResolverMarkerDiscipline:
    """Mixed expression markers should be rejected, not silently
    deconflicted. Connector-author typos (e.g. ``ref`` next to
    ``template``) need to surface."""

    def test_conflicting_markers_rejected(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="conflicting markers"):
            Resolver(ctx).resolve({"ref": "scope.x", "template": "literal"})

    def test_ref_with_extra_sibling_rejected(self):
        ctx = ResolutionContext(connection={"parameters": {"host": "h"}})
        with pytest.raises(TransportSpecError, match="must be the only key"):
            Resolver(ctx).resolve({"ref": "connection.parameters.host", "extra": 1})

    def test_template_with_extra_sibling_rejected(self):
        ctx = ResolutionContext()
        with pytest.raises(TransportSpecError, match="must be the only key"):
            Resolver(ctx).resolve({"template": "x", "extra": 1})


# ---------------------------------------------------------------------------
# Resolver: structural recursion
# ---------------------------------------------------------------------------


class TestResolverStructuralRecursion:
    def test_dict_children_are_resolved(self):
        ctx = ResolutionContext(secrets={"token": "abc"})
        out = Resolver(ctx).resolve(
            {
                "Authorization": {"template": "Bearer ${secrets.token}"},
                "Accept": "application/json",
            }
        )
        assert out == {
            "Authorization": "Bearer abc",
            "Accept": "application/json",
        }

    def test_list_elements_are_resolved(self):
        ctx = ResolutionContext(secrets={"token": "abc"})
        out = Resolver(ctx).resolve([{"template": "Bearer ${secrets.token}"}, "static"])
        assert out == ["Bearer abc", "static"]

    def test_bare_strings_are_literals(self):
        # The spec is explicit: bare JSON strings without ${...} are
        # literal values; only marker objects are expressions.
        ctx = ResolutionContext()
        assert Resolver(ctx).resolve("https://api.example.com") == (
            "https://api.example.com"
        )

    def test_scalars_pass_through(self):
        ctx = ResolutionContext()
        for value in (42, 3.14, True, False, None):
            assert Resolver(ctx).resolve(value) == value


# ---------------------------------------------------------------------------
# Resolver.resolve_for_request: per-request drop-unresolved policy
# ---------------------------------------------------------------------------


def _request_resolver(**scopes) -> Resolver:
    """Resolver wired with the default derived functions, as the
    per-request paths (param defaults, request bodies) use it."""
    from cdk.derived_functions import DEFAULT_FUNCTIONS

    return Resolver(ResolutionContext(**scopes), functions=DEFAULT_FUNCTIONS)


class TestResolveForRequestExpressionForms:
    """All four contract forms resolve through the per-request entry point."""

    def test_literal_resolves(self):
        assert _request_resolver().resolve_for_request({"literal": 42}) == 42

    def test_ref_resolves_preserving_type(self):
        r = _request_resolver(connection={"parameters": {"page_size": 50}})
        assert r.resolve_for_request({"ref": "connection.parameters.page_size"}) == 50

    def test_template_resolves(self):
        r = _request_resolver(connection={"selections": {"org": "acme"}})
        assert (
            r.resolve_for_request({"template": "orgs/${connection.selections.org}"})
            == "orgs/acme"
        )

    def test_function_resolves(self):
        # The gap behind #166: the lightweight resolver passed function
        # nodes through verbatim. The full grammar must evaluate them.
        r = _request_resolver(connection={"parameters": {"token": "secret"}})
        out = r.resolve_for_request(
            {
                "function": "base64_encode",
                "input": {"ref": "connection.parameters.token"},
            }
        )
        import base64

        assert out == base64.b64encode(b"secret").decode("ascii")

    def test_nested_structure_resolves_recursively(self):
        r = _request_resolver(connection={"parameters": {"region": "eu"}})
        out = r.resolve_for_request(
            {
                "filters": [
                    {
                        "field": "region",
                        "value": {"ref": "connection.parameters.region"},
                    },
                ],
                "page": 1,
            }
        )
        assert out == {"filters": [{"field": "region", "value": "eu"}], "page": 1}

    def test_bare_strings_stay_literal(self):
        # Bare strings are literals per the spec; ${...} expansion only
        # happens inside explicit template nodes.
        r = _request_resolver()
        assert r.resolve_for_request("${connection.parameters.x}") == (
            "${connection.parameters.x}"
        )


class TestResolveForRequestDropsUnresolved:
    """Contract rule 7: an unresolved expression omits its field or
    parameter (with a warning) instead of going onto the wire raw."""

    def test_top_level_unresolved_ref_returns_none(self, caplog):
        r = _request_resolver()
        with caplog.at_level("WARNING"):
            out = r.resolve_for_request({"ref": "connection.parameters.missing"})
        assert out is None
        assert "value-expression" in caplog.text

    def test_dict_field_with_unresolved_ref_is_dropped(self, caplog):
        r = _request_resolver(connection={"parameters": {"kept": "v"}})
        with caplog.at_level("WARNING"):
            out = r.resolve_for_request(
                {
                    "kept": {"ref": "connection.parameters.kept"},
                    "gone": {"ref": "connection.parameters.missing"},
                }
            )
        assert out == {"kept": "v"}
        assert "dropping field 'gone'" in caplog.text

    def test_list_item_with_unresolved_ref_is_dropped(self, caplog):
        r = _request_resolver(connection={"parameters": {"kept": "v"}})
        with caplog.at_level("WARNING"):
            out = r.resolve_for_request(
                [
                    {"ref": "connection.parameters.kept"},
                    {"ref": "connection.parameters.missing"},
                ]
            )
        assert out == ["v"]
        assert "dropping list item" in caplog.text

    def test_literal_none_is_dropped(self):
        # {"literal": null} resolves to None, which per-request means
        # "omit the field" — same as the Lambda runtime.
        r = _request_resolver()
        out = r.resolve_for_request({"a": {"literal": None}, "b": {"literal": 1}})
        assert out == {"b": 1}

    def test_structural_none_passes_through(self):
        # A plain JSON null is data, not an unresolved expression.
        r = _request_resolver()
        assert r.resolve_for_request({"a": None}) == {"a": None}

    def test_unknown_function_raises_even_per_request(self):
        # A typo'd function name is an authoring defect, never missing
        # data — the drop policy must not absorb it.
        r = _request_resolver()
        with pytest.raises(TransportSpecError, match="no_such_fn"):
            r.resolve_for_request({"f": {"function": "no_such_fn"}})

    def test_unknown_scope_raises_even_per_request(self):
        # Same boundary for a typo'd scope name: the scope vocabulary is
        # closed, so this is a defect, not an absent optional value.
        r = _request_resolver()
        with pytest.raises(KeyError, match="Unknown resolution scope"):
            r.resolve_for_request({"x": {"ref": "connecton.parameters.a"}})

    def test_derived_function_internal_keyerror_propagates(self):
        # The drop policy absorbs UnresolvedValueError only; a KeyError
        # raised by a function's own internals is a programming bug and
        # must surface, not silently drop the field.
        def _buggy(node, resolver):
            raise KeyError("internal bug")

        r = Resolver(ResolutionContext(), functions={"buggy": _buggy})
        with pytest.raises(KeyError, match="internal bug"):
            r.resolve_for_request({"f": {"function": "buggy"}})

    def test_unknown_lookup_key_is_dropped(self):
        r = _request_resolver(connection={"parameters": {"env": "staging"}})
        out = r.resolve_for_request(
            {
                "url": {
                    "function": "lookup",
                    "input": {"ref": "connection.parameters.env"},
                    "map": {"sandbox": "s", "production": "p"},
                }
            }
        )
        assert out == {}

    def test_nested_dict_fields_are_dropped_too(self):
        r = _request_resolver(connection={"parameters": {"kept": "v"}})
        out = r.resolve_for_request(
            {
                "outer": {
                    "kept": {"ref": "connection.parameters.kept"},
                    "gone": {"ref": "connection.parameters.missing"},
                }
            }
        )
        assert out == {"outer": {"kept": "v"}}


class TestResolveForRequestTemplateLeniency:
    """Plain template nodes resolve leniently per request: a missing
    placeholder renders empty (with a warning) and the field is kept.
    Function inputs stay strict — a partial input drops the whole node."""

    def test_missing_placeholder_renders_empty(self, caplog):
        r = _request_resolver(connection={"parameters": {"org": "acme"}})
        with caplog.at_level("WARNING"):
            out = r.resolve_for_request(
                {
                    "template": (
                        "${connection.parameters.org}/" "${connection.parameters.gone}"
                    )
                }
            )
        assert out == "acme/"
        assert "unresolved placeholder" in caplog.text

    def test_template_field_with_missing_placeholder_is_kept(self):
        r = _request_resolver()
        out = r.resolve_for_request(
            {"path": {"template": "v1/${connection.parameters.gone}/x"}}
        )
        assert out == {"path": "v1//x"}

    def test_template_inside_function_input_stays_strict(self):
        # base64("user:") style garbage must not reach the wire: an
        # unresolved placeholder inside a function input drops the node.
        r = _request_resolver(connection={"parameters": {"user": "u"}})
        out = r.resolve_for_request(
            {
                "auth": {
                    "function": "base64_encode",
                    "input": {
                        "template": (
                            "${connection.parameters.user}:"
                            "${connection.parameters.gone}"
                        )
                    },
                }
            }
        )
        assert out == {}

    def test_strict_resolve_still_raises_on_missing_placeholder(self):
        # The materialization entry point keeps its loud failure mode.
        r = _request_resolver()
        with pytest.raises(KeyError):
            r.resolve({"template": "v1/${connection.parameters.gone}"})


class TestResolveForRequestAuthoringErrorsRaise:
    """Authoring defects are configuration errors in both failure
    policies — rule 7's omission applies to missing values only."""

    def test_conflicting_markers_raise(self):
        r = _request_resolver()
        with pytest.raises(TransportSpecError, match="conflicting markers"):
            r.resolve_for_request({"x": {"ref": "a.b", "template": "c"}})

    def test_unterminated_template_raises(self):
        r = _request_resolver()
        with pytest.raises(TransportSpecError, match="Unterminated"):
            r.resolve_for_request({"template": "v1/${connection.parameters.x"})

    def test_non_scalar_template_substitution_raises(self):
        r = _request_resolver(connection={"parameters": {"obj": {"k": "v"}}})
        with pytest.raises(TransportSpecError, match="only scalars"):
            r.resolve_for_request({"template": "x${connection.parameters.obj}"})
