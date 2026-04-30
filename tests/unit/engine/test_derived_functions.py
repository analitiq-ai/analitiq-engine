"""Tests for :mod:`src.engine.derived_functions`.

The registered functions are the only escape hatch from the declarative
connector model. These tests pin down the input/output contract for each
function — particularly the failure paths for malformed connector JSON.
"""

from __future__ import annotations

import base64

import pytest

from src.engine.derived_functions import (
    DEFAULT_FUNCTIONS,
    base64_encode_function,
    basic_auth_function,
    lookup_function,
    url_encode_function,
)
from src.engine.resolver import ResolutionContext, Resolver


def _resolver(ctx: ResolutionContext | None = None) -> Resolver:
    return Resolver(ctx or ResolutionContext(), functions=DEFAULT_FUNCTIONS)


# ---------------------------------------------------------------------------
# lookup
# ---------------------------------------------------------------------------


class TestLookup:
    def test_string_input_returns_mapped_value(self):
        node = {
            "function": "lookup",
            "input": {"literal": "verify-full"},
            "map": {"prefer": True, "verify-full": {"check_hostname": True}},
        }
        out = lookup_function(node, _resolver())
        assert out == {"check_hostname": True}

    def test_boolean_input_canonicalised_to_string_keys(self):
        node = {
            "function": "lookup",
            "input": {"literal": True},
            "map": {"true": "yes", "false": "no"},
        }
        assert lookup_function(node, _resolver()) == "yes"

    def test_integer_input_stringified_for_lookup(self):
        node = {
            "function": "lookup",
            "input": {"literal": 5},
            "map": {"5": "five"},
        }
        assert lookup_function(node, _resolver()) == "five"

    def test_unknown_input_raises_keyerror_with_known_keys(self):
        node = {
            "function": "lookup",
            "input": {"literal": "missing"},
            "map": {"a": 1, "b": 2},
        }
        with pytest.raises(KeyError, match=r"input 'missing' not present"):
            lookup_function(node, _resolver())

    def test_none_input_is_rejected(self):
        node = {
            "function": "lookup",
            "input": {"literal": None},
            "map": {"a": 1},
        }
        with pytest.raises(KeyError, match="resolved to None"):
            lookup_function(node, _resolver())

    def test_dict_input_rejected_with_clear_typeerror(self):
        # The reviewer flagged this: stringifying a dict produced
        # confusing "{'k': 'v'} not present in map" errors. We now reject
        # non-scalar inputs explicitly.
        node = {
            "function": "lookup",
            "input": {"literal": {"a": 1}},
            "map": {"x": "y"},
        }
        with pytest.raises(TypeError, match="must resolve to a scalar"):
            lookup_function(node, _resolver())

    def test_list_input_rejected(self):
        node = {
            "function": "lookup",
            "input": {"literal": [1, 2]},
            "map": {"x": "y"},
        }
        with pytest.raises(TypeError, match="must resolve to a scalar"):
            lookup_function(node, _resolver())

    def test_missing_input_field_rejected(self):
        with pytest.raises(ValueError, match="requires `input`"):
            lookup_function({"function": "lookup", "map": {}}, _resolver())

    def test_missing_map_field_rejected(self):
        with pytest.raises(ValueError, match="requires `map`"):
            lookup_function(
                {"function": "lookup", "input": {"literal": "x"}}, _resolver()
            )

    def test_map_must_be_object(self):
        node = {
            "function": "lookup",
            "input": {"literal": "x"},
            "map": ["not", "an", "object"],
        }
        with pytest.raises(TypeError, match="must be an object"):
            lookup_function(node, _resolver())

    def test_mapped_value_resolved_recursively(self):
        # A lookup can return another expression; it must be evaluated.
        ctx = ResolutionContext(secrets={"api_key": "abc"})
        node = {
            "function": "lookup",
            "input": {"literal": "auth"},
            "map": {"auth": {"template": "Bearer ${secrets.api_key}"}},
        }
        assert lookup_function(node, _resolver(ctx)) == "Bearer abc"


# ---------------------------------------------------------------------------
# basic_auth
# ---------------------------------------------------------------------------


class TestBasicAuth:
    def test_builds_base64_credential(self):
        node = {
            "function": "basic_auth",
            "input": {"literal": {"username": "u", "password": "p"}},
        }
        out = basic_auth_function(node, _resolver())
        assert base64.b64decode(out).decode("ascii") == "u:p"

    def test_missing_input_rejected(self):
        with pytest.raises(ValueError, match="requires `input`"):
            basic_auth_function({"function": "basic_auth"}, _resolver())

    def test_input_must_resolve_to_object(self):
        node = {"function": "basic_auth", "input": {"literal": "not-an-object"}}
        with pytest.raises(TypeError, match="must resolve to an object"):
            basic_auth_function(node, _resolver())

    def test_treats_missing_username_password_as_empty(self):
        node = {"function": "basic_auth", "input": {"literal": {}}}
        out = basic_auth_function(node, _resolver())
        assert base64.b64decode(out).decode("ascii") == ":"


# ---------------------------------------------------------------------------
# base64_encode
# ---------------------------------------------------------------------------


class TestBase64Encode:
    def test_encodes_string(self):
        node = {"function": "base64_encode", "input": {"literal": "hello"}}
        assert base64_encode_function(node, _resolver()) == base64.b64encode(b"hello").decode()

    def test_encodes_bytes(self):
        node = {"function": "base64_encode", "input": {"literal": b"hello"}}
        assert base64_encode_function(node, _resolver()) == base64.b64encode(b"hello").decode()

    def test_rejects_non_string_non_bytes(self):
        node = {"function": "base64_encode", "input": {"literal": 123}}
        with pytest.raises(TypeError, match="string or bytes"):
            base64_encode_function(node, _resolver())


# ---------------------------------------------------------------------------
# url_encode
# ---------------------------------------------------------------------------


class TestUrlEncode:
    def test_encodes_reserved_characters(self):
        node = {
            "function": "url_encode",
            "input": {"literal": "a@b#c%/d:e"},
        }
        out = url_encode_function(node, _resolver())
        assert out == "a%40b%23c%25%2Fd%3Ae"

    def test_safe_chars_pass_through(self):
        node = {
            "function": "url_encode",
            "input": {"literal": "a/b"},
            "safe": "/",
        }
        assert url_encode_function(node, _resolver()) == "a/b"

    def test_none_resolves_to_empty_string(self):
        node = {"function": "url_encode", "input": {"literal": None}}
        assert url_encode_function(node, _resolver()) == ""

    def test_integer_value_stringified_then_encoded(self):
        node = {"function": "url_encode", "input": {"literal": 5432}}
        assert url_encode_function(node, _resolver()) == "5432"

    def test_dict_input_rejected(self):
        node = {"function": "url_encode", "input": {"literal": {"a": 1}}}
        with pytest.raises(TypeError, match="scalar"):
            url_encode_function(node, _resolver())

    def test_safe_must_be_string(self):
        node = {
            "function": "url_encode",
            "input": {"literal": "a/b"},
            "safe": ["/"],
        }
        with pytest.raises(TypeError, match="`safe` must be a string"):
            url_encode_function(node, _resolver())


# ---------------------------------------------------------------------------
# Integration: function dispatch through the resolver
# ---------------------------------------------------------------------------


class TestFunctionsViaResolver:
    """End-to-end check that the registered functions go through the
    resolver's marker discipline and resolve nested expressions."""

    def test_lookup_with_ref_input(self):
        ctx = ResolutionContext(
            connection={"parameters": {"ssl_mode": "verify-full"}}
        )
        spec = {
            "function": "lookup",
            "input": {"ref": "connection.parameters.ssl_mode"},
            "map": {
                "disable": False,
                "verify-full": {"check_hostname": True},
            },
        }
        assert _resolver(ctx).resolve(spec) == {"check_hostname": True}

    def test_basic_auth_with_ref_input(self):
        ctx = ResolutionContext(
            connection={"parameters": {"client_id": "abc"}},
            secrets={"client_secret": "xyz"},
        )
        spec = {
            "function": "basic_auth",
            "input": {
                "username": {"ref": "connection.parameters.client_id"},
                "password": {"ref": "secrets.client_secret"},
            },
        }
        out = _resolver(ctx).resolve(spec)
        assert base64.b64decode(out).decode() == "abc:xyz"
