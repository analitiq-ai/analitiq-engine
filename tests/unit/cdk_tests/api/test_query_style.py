"""How a collection param lands on the query string.

The schema REQUIRES ``style`` and ``explode`` on a query param typed
``array`` or ``object``, and until this landed the request build refused
every such value with "declare how the container serializes" -- against a
document that had declared exactly that. So the tests below ask what went
on the wire, through the same binder both roles use.
"""

from __future__ import annotations

from typing import Any

import pytest

from cdk.api.exceptions import RequestSpecError
from cdk.api.http import query_pairs
from cdk.api.query_style import QueryStyle, declared_query_styles
from cdk.api.request import bind_query_and_headers, request_block_problem
from cdk.resolver import ResolutionContext, Resolver

pytestmark = pytest.mark.unit


def _param(kind: str, style: str, explode: bool) -> dict[str, Any]:
    return {
        "tags": {
            "in": "query",
            "type": kind,
            "required": False,
            "style": style,
            "explode": explode,
        }
    }


def _sent(
    kind: str,
    style: str,
    explode: bool,
    value: Any,
    *,
    key: str = "tags",
) -> list[tuple[str, Any]]:
    """The name/value pairs one declared query key puts on the wire."""
    declared = {key: {"from_param": "tags"}}
    params = _param(kind, style, explode)
    query, _headers = bind_query_and_headers(
        params={"tags": value},
        declared_query=declared,
        declared_headers=None,
        resolver=Resolver(ResolutionContext()),
        endpoint="items",
        query_styles=declared_query_styles(declared, params),
    )
    return query_pairs(query)


class TestArrays:
    @pytest.mark.parametrize(
        ("style", "explode", "expected"),
        [
            # The one style that repeats the key -- and the reason a built
            # query carries a list at all.
            ("form", True, [("tags", "a"), ("tags", "b")]),
            ("form", False, [("tags", "a,b")]),
            ("spaceDelimited", False, [("tags", "a b")]),
            ("pipeDelimited", False, [("tags", "a|b")]),
        ],
    )
    def test_each_style_spells_the_array_its_own_way(
        self, style: str, explode: bool, expected: list[tuple[str, Any]]
    ) -> None:
        assert _sent("array", style, explode, ["a", "b"]) == expected

    def test_the_wire_key_is_the_declared_one_not_the_param_name(self) -> None:
        # The key map and the style meet here: `country[id]` is what the
        # provider sees, and the style says how many values it carries.
        assert _sent("array", "form", False, ["1", "2"], key="country[id]") == [
            ("country[id]", "1,2")
        ]

    def test_a_single_value_under_a_collection_param_is_untouched(self) -> None:
        """A style says how MANY are spelled; one value is still one value."""
        assert _sent("array", "form", True, "a") == [("tags", "a")]


class TestObjects:
    def test_deep_object_writes_the_property_names(self) -> None:
        assert _sent("object", "deepObject", True, {"status": "open"}) == [
            ("tags[status]", "open")
        ]

    def test_an_exploded_form_object_sends_its_properties_as_keys(self) -> None:
        assert _sent("object", "form", True, {"a": "1", "b": "2"}) == [
            ("a", "1"),
            ("b", "2"),
        ]

    def test_a_non_exploded_form_object_flattens_name_and_value(self) -> None:
        assert _sent("object", "form", False, {"a": "1", "b": "2"}) == [
            ("tags", "a,1,b,2")
        ]


class TestOneRuleForWhatAValueMayBe:
    """A value lands the same way inside a collection as it does alone."""

    def test_a_boolean_entry_takes_its_json_spelling(self) -> None:
        # str() would send Python's `True`, a spelling no JSON document
        # contains -- and the same value sent alone goes out as `true`.
        assert _sent("array", "form", False, [True, False]) == [("tags", "true,false")]

    def test_an_exploded_boolean_entry_takes_it_too(self) -> None:
        assert _sent("array", "form", True, [True]) == [("tags", "true")]

    def test_a_null_entry_is_refused_like_a_declared_null(self) -> None:
        with pytest.raises(RequestSpecError, match="declares null"):
            _sent("array", "form", False, ["a", None])


class TestADelimiterCannotBeData:
    """A structural delimiter inside a value has no encoding that survives.

    yarl sends a joined `a,b,c` verbatim and encodes the whole string as
    one value, so a comma that was data is indistinguishable from one that
    was a separator -- and pre-encoding the component is double-encoded to
    `%252C`. The boundary is unrecoverable, so the value is refused.
    """

    @pytest.mark.parametrize(
        ("style", "entry"),
        [("form", "a,b"), ("spaceDelimited", "a b"), ("pipeDelimited", "a|b")],
    )
    def test_an_entry_carrying_its_own_delimiter_is_refused(
        self, style: str, entry: str
    ) -> None:
        with pytest.raises(RequestSpecError, match="contains that character"):
            _sent("array", style, False, [entry, "c"])

    def test_the_message_names_the_style_that_can_send_it(self) -> None:
        with pytest.raises(RequestSpecError, match="explode=true"):
            _sent("array", "form", False, ["a,b"])

    def test_exploding_the_same_value_sends_it_intact(self) -> None:
        """The remedy works: a repeated name encodes each value on its own."""
        assert _sent("array", "form", True, ["a,b", "c"]) == [
            ("tags", "a,b"),
            ("tags", "c"),
        ]

    def test_an_object_entry_carrying_the_delimiter_is_refused_too(self) -> None:
        with pytest.raises(RequestSpecError, match="contains that character"):
            _sent("object", "form", False, {"a": "x,y"})


class TestWhatIsRefused:
    def test_a_style_the_engine_does_not_serialize_is_named_at_plan_time(
        self,
    ) -> None:
        """The registry is closed engine-side: the schema types style as a string."""
        problem = request_block_problem(
            {"query": {"tags": {"from_param": "tags"}}},
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=Resolver(ResolutionContext()),
            declared_params=_param("array", "matrix", True),
        )
        assert problem is not None
        assert "'matrix'" in problem
        assert "deepObject" in problem, "the message names what it does send"

    def test_an_undefined_style_and_explode_pair_is_named_at_plan_time(self) -> None:
        """OpenAPI leaves deepObject on explode=false with no spelling at all."""
        problem = request_block_problem(
            {"query": {"tags": {"from_param": "tags"}}},
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=Resolver(ResolutionContext()),
            declared_params=_param("object", "deepObject", False),
        )
        assert problem is not None
        assert "undefined" in problem

    def test_a_value_of_the_wrong_shape_for_its_type_is_refused(self) -> None:
        with pytest.raises(RequestSpecError, match="declares type 'object'"):
            _sent("object", "deepObject", True, ["a", "b"])

    @pytest.mark.parametrize(
        ("kind", "explode", "value"),
        [
            ("array", True, {"a": "1"}),
            ("array", False, {"a": "1"}),
            ("object", True, ["a"]),
            ("object", False, ["a"]),
        ],
    )
    def test_form_does_not_let_a_runtime_value_change_the_wire_shape(
        self, kind: str, explode: bool, value: Any
    ) -> None:
        """`form` spells both shapes, so only the DECLARED type may decide.

        A stream filter or a loop supplies the value; accepting whichever
        collection arrives lets an exploded object replace the declared
        wire key with whatever property names it happened to carry.
        """
        with pytest.raises(RequestSpecError, match=f"declares type {kind!r}"):
            _sent(kind, "form", explode, value)

    def test_a_nested_collection_is_refused(self) -> None:
        """OpenAPI stops at one level, so there is nothing to send."""
        with pytest.raises(RequestSpecError, match="nested"):
            _sent("array", "form", True, ["a", ["b"]])

    def test_a_style_writing_a_key_the_request_already_sends_is_refused(
        self,
    ) -> None:
        """One of the two would go out and the other vanish, unremarked."""
        declared = {
            "status": {"literal": "open"},
            "filter": {"from_param": "tags"},
        }
        params = {
            "tags": {
                "in": "query",
                "type": "object",
                "required": False,
                "style": "form",
                "explode": True,
            }
        }
        with pytest.raises(RequestSpecError, match="already carries"):
            bind_query_and_headers(
                params={"tags": {"status": "closed"}},
                declared_query=declared,
                declared_headers=None,
                resolver=Resolver(ResolutionContext()),
                endpoint="items",
                query_styles=declared_query_styles(declared, params),
            )

    def test_the_collision_is_refused_whichever_order_it_is_declared_in(
        self,
    ) -> None:
        """Insertion is one site, so the refusal cannot turn on declaration order."""
        params = {
            "tags": {
                "in": "query",
                "type": "object",
                "required": False,
                "style": "form",
                "explode": True,
            }
        }
        plain_first = {"status": {"literal": "open"}, "filter": {"from_param": "tags"}}
        styled_first = {"filter": {"from_param": "tags"}, "status": {"literal": "open"}}
        for declared in (plain_first, styled_first):
            with pytest.raises(RequestSpecError, match="already carries"):
                bind_query_and_headers(
                    params={"tags": {"status": "closed"}},
                    declared_query=declared,
                    declared_headers=None,
                    resolver=Resolver(ResolutionContext()),
                    endpoint="items",
                    query_styles=declared_query_styles(declared, params),
                )

    def test_a_container_under_a_key_no_param_owns_keeps_the_old_refusal(
        self,
    ) -> None:
        """A template or function output has no param, so it has no style."""
        with pytest.raises(RequestSpecError, match="must be a scalar"):
            bind_query_and_headers(
                params={"tags": ["a", "b"]},
                declared_query={"tags": {"literal": ["a", "b"]}},
                declared_headers=None,
                resolver=Resolver(ResolutionContext()),
                endpoint="items",
            )


class TestAStyleMustSpellTheDeclaredType:
    """A defined pair can still say nothing about the type it is declared on."""

    @pytest.mark.parametrize(
        ("kind", "style", "explode"),
        [
            ("object", "spaceDelimited", False),
            ("object", "pipeDelimited", False),
            ("array", "deepObject", True),
        ],
    )
    def test_a_style_that_does_not_spell_that_type_is_refused_at_plan_time(
        self, kind: str, style: str, explode: bool
    ) -> None:
        problem = request_block_problem(
            {"query": {"tags": {"from_param": "tags"}}},
            endpoint="items",
            reserved_headers=frozenset(),
            resolver=Resolver(ResolutionContext()),
            declared_params=_param(kind, style, explode),
        )
        assert problem is not None
        assert f"typed {kind!r}" in problem
        assert "no spelling" in problem

    @pytest.mark.parametrize(
        ("kind", "style", "explode"),
        [
            ("array", "spaceDelimited", False),
            ("array", "form", True),
            ("object", "form", False),
            ("object", "deepObject", True),
        ],
    )
    def test_a_style_that_does_spell_it_passes(
        self, kind: str, style: str, explode: bool
    ) -> None:
        assert (
            request_block_problem(
                {"query": {"tags": {"from_param": "tags"}}},
                endpoint="items",
                reserved_headers=frozenset(),
                resolver=Resolver(ResolutionContext()),
                declared_params=_param(kind, style, explode),
            )
            is None
        )


class TestStyleLookup:
    def test_only_a_bare_from_param_binding_carries_a_style(self) -> None:
        params = _param("array", "form", True)
        assert declared_query_styles({"tags": {"from_param": "tags"}}, params) == {
            "tags": QueryStyle("array", "form", True)
        }
        # A function over the param produces a value no single param owns.
        assert (
            declared_query_styles(
                {"tags": {"function": "lower", "input": {"from_param": "tags"}}}, params
            )
            == {}
        )
