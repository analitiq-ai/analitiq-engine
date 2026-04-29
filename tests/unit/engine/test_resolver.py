"""Tests for :mod:`src.engine.resolver`.

Focuses on edge cases the higher-level transport-factory tests do not
reach: scope traversal failures, expression-marker discipline, template
substitution constraints, and function dispatch.
"""

from __future__ import annotations

import pytest

from src.engine.resolver import ResolutionContext, Resolver


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

    def test_empty_path_raises_valueerror(self):
        ctx = ResolutionContext()
        with pytest.raises(ValueError, match="non-empty"):
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
        with pytest.raises(TypeError, match="`ref` must be a string"):
            Resolver(ctx).resolve({"ref": 42})

    def test_literal_returns_value_unchanged(self):
        ctx = ResolutionContext()
        # Even structures that look like other expressions are returned verbatim.
        assert Resolver(ctx).resolve(
            {"literal": {"ref": "scope.path"}}
        ) == {"ref": "scope.path"}

    def test_literal_with_sibling_keys_rejected(self):
        ctx = ResolutionContext()
        with pytest.raises(ValueError, match="must be the only key"):
            Resolver(ctx).resolve({"literal": "ok", "extra": 1})


class TestResolverTemplate:
    def test_template_substitutes_scalars(self):
        ctx = ResolutionContext(
            connection={"parameters": {"host": "h", "port": 5432}}
        )
        out = Resolver(ctx).resolve(
            {"template": "${connection.parameters.host}:${connection.parameters.port}"}
        )
        assert out == "h:5432"

    def test_template_no_placeholders_returns_string_as_is(self):
        ctx = ResolutionContext()
        assert Resolver(ctx).resolve({"template": "https://api.example.com"}) == "https://api.example.com"

    def test_template_must_be_string(self):
        ctx = ResolutionContext()
        with pytest.raises(TypeError, match="`template` must be a string"):
            Resolver(ctx).resolve({"template": 42})

    def test_template_unterminated_placeholder_raises(self):
        ctx = ResolutionContext()
        with pytest.raises(ValueError, match="Unterminated"):
            Resolver(ctx).resolve({"template": "abc${unterminated"})

    def test_template_placeholder_resolving_to_none_raises(self):
        ctx = ResolutionContext(connection={"parameters": {"host": None}})
        with pytest.raises(KeyError, match="resolved to None"):
            Resolver(ctx).resolve(
                {"template": "${connection.parameters.host}"}
            )

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
        with pytest.raises(TypeError, match="only scalars"):
            Resolver(ctx).resolve(
                {"template": "${connection.parameters.headers}"}
            )


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

    def test_unknown_function_raises_keyerror(self):
        ctx = ResolutionContext()
        with pytest.raises(KeyError, match="Unknown derived function"):
            Resolver(ctx, functions={}).resolve(
                {"function": "no_such", "input": "x"}
            )

    def test_function_with_unexpected_sibling_key_rejected(self):
        # Sibling key that is not another expression marker (those are
        # caught by the conflicting-marker rule) and not in the allow-list.
        ctx = ResolutionContext()
        resolver = Resolver(ctx, functions={"capture": _capture})
        with pytest.raises(ValueError, match="unexpected sibling keys"):
            resolver.resolve({"function": "capture", "input": "x", "bogus": 1})

    def test_function_with_allowed_siblings_resolves(self):
        ctx = ResolutionContext()
        resolver = Resolver(ctx, functions={"capture": _capture})
        # `input`, `map`, and `safe` are documented allowed siblings.
        out = resolver.resolve(
            {"function": "capture", "input": "x", "map": {"x": "y"}, "safe": ""}
        )
        assert out == ("captured", "x")

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
        with pytest.raises(ValueError, match="conflicting markers"):
            Resolver(ctx).resolve({"ref": "scope.x", "template": "literal"})

    def test_ref_with_extra_sibling_rejected(self):
        ctx = ResolutionContext(connection={"parameters": {"host": "h"}})
        with pytest.raises(ValueError, match="must be the only key"):
            Resolver(ctx).resolve(
                {"ref": "connection.parameters.host", "extra": 1}
            )

    def test_template_with_extra_sibling_rejected(self):
        ctx = ResolutionContext()
        with pytest.raises(ValueError, match="must be the only key"):
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
        out = Resolver(ctx).resolve(
            [{"template": "Bearer ${secrets.token}"}, "static"]
        )
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
