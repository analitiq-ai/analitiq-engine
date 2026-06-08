"""Unit tests for src.shared.dict_path.walk_path."""

from __future__ import annotations

from src.shared.dict_path import walk_path


class TestWalkPath:
    def test_empty_path_returns_data_unchanged(self):
        data = {"a": 1}
        assert walk_path(data, []) is data

    def test_single_key_present(self):
        assert walk_path({"a": 42}, ["a"]) == 42

    def test_single_key_missing_returns_none(self):
        assert walk_path({"a": 1}, ["b"]) is None

    def test_nested_path_present(self):
        data = {"a": {"b": {"c": "deep"}}}
        assert walk_path(data, ["a", "b", "c"]) == "deep"

    def test_nested_path_missing_intermediate(self):
        data = {"a": {"x": 1}}
        assert walk_path(data, ["a", "b", "c"]) is None

    def test_nested_path_missing_leaf(self):
        data = {"a": {"b": {}}}
        assert walk_path(data, ["a", "b", "c"]) is None

    def test_value_is_none(self):
        # A key present with value None should be returned, not treated as missing
        data = {"a": {"b": None}}
        assert walk_path(data, ["a", "b"]) is None

    def test_non_dict_intermediate_returns_none(self):
        data = {"a": "string"}
        assert walk_path(data, ["a", "b"]) is None

    def test_non_dict_data_with_path_returns_none(self):
        assert walk_path("not-a-dict", ["a"]) is None

    def test_value_is_zero(self):
        assert walk_path({"a": 0}, ["a"]) == 0

    def test_value_is_false(self):
        assert walk_path({"a": False}, ["a"]) is False

    def test_value_is_empty_string(self):
        assert walk_path({"a": ""}, ["a"]) == ""

    def test_value_is_list(self):
        assert walk_path({"a": [1, 2, 3]}, ["a"]) == [1, 2, 3]

    def test_non_dict_data_empty_path_returns_data(self):
        assert walk_path("scalar", []) == "scalar"

    def test_none_data_with_path_returns_none(self):
        assert walk_path(None, ["a"]) is None
