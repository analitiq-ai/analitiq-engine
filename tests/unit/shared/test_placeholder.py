"""Unit tests for src.shared.placeholder."""

import pytest

from src.shared.placeholder import expand_placeholders, has_placeholders


# ---------------------------------------------------------------------------
# has_placeholders
# ---------------------------------------------------------------------------

class TestHasPlaceholders:
    def test_plain_string(self):
        assert has_placeholders("hello world") is False

    def test_string_with_placeholder(self):
        assert has_placeholders("Bearer ${api_key}") is True

    def test_string_multiple_placeholders(self):
        assert has_placeholders("${host}:${port}") is True

    def test_empty_string(self):
        assert has_placeholders("") is False

    def test_integer(self):
        assert has_placeholders(42) is False

    def test_none(self):
        assert has_placeholders(None) is False

    def test_boolean(self):
        assert has_placeholders(True) is False

    def test_dict_without_placeholders(self):
        assert has_placeholders({"key": "value", "n": 1}) is False

    def test_dict_with_placeholder_in_value(self):
        assert has_placeholders({"auth": "Bearer ${token}"}) is True

    def test_dict_nested(self):
        assert has_placeholders({"a": {"b": "${secret}"}}) is True

    def test_list_without_placeholders(self):
        assert has_placeholders(["a", "b", "c"]) is False

    def test_list_with_placeholder(self):
        assert has_placeholders(["a", "${b}", "c"]) is True

    def test_list_nested_dict(self):
        assert has_placeholders([{"key": "${val}"}]) is True

    def test_dollar_without_braces(self):
        assert has_placeholders("$not_a_placeholder") is False

    def test_empty_dict(self):
        assert has_placeholders({}) is False

    def test_empty_list(self):
        assert has_placeholders([]) is False


# ---------------------------------------------------------------------------
# expand_placeholders — strings
# ---------------------------------------------------------------------------

class TestExpandStrings:
    def test_single_placeholder(self):
        result = expand_placeholders("${api_key}", {"api_key": "abc123"})
        assert result == "abc123"

    def test_placeholder_in_context(self):
        result = expand_placeholders(
            "Bearer ${token}", {"token": "xyz"}
        )
        assert result == "Bearer xyz"

    def test_multiple_placeholders(self):
        result = expand_placeholders(
            "${host}:${port}",
            {"host": "localhost", "port": "5432"},
        )
        assert result == "localhost:5432"

    def test_no_placeholders_passthrough(self):
        result = expand_placeholders("plain text", {"key": "val"})
        assert result == "plain text"

    def test_empty_string(self):
        result = expand_placeholders("", {"key": "val"})
        assert result == ""

    def test_missing_key_raises(self):
        with pytest.raises(KeyError, match="missing"):
            expand_placeholders("${missing}", {})

    def test_missing_key_ignore(self):
        result = expand_placeholders(
            "${missing}", {}, ignore_missing=True
        )
        assert result == "${missing}"

    def test_partial_resolution_ignore_missing(self):
        result = expand_placeholders(
            "${found}/${missing}",
            {"found": "ok"},
            ignore_missing=True,
        )
        assert result == "ok/${missing}"

    def test_lookup_value_cast_to_string(self):
        result = expand_placeholders("${num}", {"num": "42"})
        assert result == "42"


# ---------------------------------------------------------------------------
# expand_placeholders — dicts
# ---------------------------------------------------------------------------

class TestExpandDicts:
    def test_flat_dict(self):
        result = expand_placeholders(
            {"auth": "Bearer ${token}", "name": "test"},
            {"token": "abc"},
        )
        assert result == {"auth": "Bearer abc", "name": "test"}

    def test_nested_dict(self):
        result = expand_placeholders(
            {"outer": {"inner": "${val}"}},
            {"val": "resolved"},
        )
        assert result == {"outer": {"inner": "resolved"}}

    def test_keys_not_expanded(self):
        result = expand_placeholders(
            {"${key}": "value"},
            {"key": "replaced"},
        )
        assert result == {"${key}": "value"}

    def test_mixed_value_types(self):
        result = expand_placeholders(
            {"s": "${x}", "n": 42, "b": True, "null": None},
            {"x": "yes"},
        )
        assert result == {"s": "yes", "n": 42, "b": True, "null": None}

    def test_empty_dict(self):
        assert expand_placeholders({}, {"k": "v"}) == {}


# ---------------------------------------------------------------------------
# expand_placeholders — lists
# ---------------------------------------------------------------------------

class TestExpandLists:
    def test_flat_list(self):
        result = expand_placeholders(
            ["${a}", "static", "${b}"],
            {"a": "1", "b": "2"},
        )
        assert result == ["1", "static", "2"]

    def test_list_with_non_strings(self):
        result = expand_placeholders([1, "${x}", None], {"x": "ok"})
        assert result == [1, "ok", None]

    def test_list_of_dicts(self):
        result = expand_placeholders(
            [{"val": "${p}"}],
            {"p": "resolved"},
        )
        assert result == [{"val": "resolved"}]

    def test_empty_list(self):
        assert expand_placeholders([], {"k": "v"}) == []


# ---------------------------------------------------------------------------
# expand_placeholders — scalars (passthrough)
# ---------------------------------------------------------------------------

class TestExpandScalars:
    def test_integer(self):
        assert expand_placeholders(42, {"k": "v"}) == 42

    def test_float(self):
        assert expand_placeholders(3.14, {"k": "v"}) == 3.14

    def test_boolean(self):
        assert expand_placeholders(True, {"k": "v"}) is True

    def test_none(self):
        assert expand_placeholders(None, {"k": "v"}) is None


# ---------------------------------------------------------------------------
# expand_placeholders — realistic endpoint filter scenario
# ---------------------------------------------------------------------------

class TestEndpointFilterResolution:
    """Tests modelling the actual use case: resolving endpoint filter
    placeholders from connection parameters."""

    def test_filter_default_resolved_from_connection_params(self):
        filters = {
            "profile": {
                "type": "integer",
                "required": True,
                "default": "${profile_id}",
            },
            "status": {
                "type": "string",
                "required": False,
            },
        }
        params = {"profile_id": "2496403"}

        result = expand_placeholders(filters, params, ignore_missing=True)

        assert result["profile"]["default"] == "2496403"
        assert result["status"] == {"type": "string", "required": False}

    def test_endpoint_path_resolved_from_connection_params(self):
        endpoint = "/v1/workspaces/${workspace_id}/transfers"
        params = {"workspace_id": "ws-42"}

        result = expand_placeholders(endpoint, params)
        assert result == "/v1/workspaces/ws-42/transfers"

    def test_unresolvable_filter_left_intact_with_ignore(self):
        filters = {
            "profile": {"default": "${profile_id}"},
            "tenant": {"default": "${tenant_id}"},
        }
        params = {"profile_id": "123"}

        result = expand_placeholders(filters, params, ignore_missing=True)

        assert result["profile"]["default"] == "123"
        assert result["tenant"]["default"] == "${tenant_id}"

    def test_filter_with_no_placeholders_unchanged(self):
        filters = {
            "status": {
                "type": "string",
                "operators": ["eq", "in"],
                "required": False,
            }
        }
        result = expand_placeholders(filters, {"profile_id": "123"})
        assert result == filters