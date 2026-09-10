"""The one override seam: three hooks, one binding site, two closed routes.

The guards matter more than the hooks: a subclass that settles its own
error map, or replaces the parse, classifies every response by a fact its
connector.json never declared -- and nothing raises when it does.
"""

from __future__ import annotations

from typing import Any

import pytest

from cdk.api.dialects import ApiDialect, dialect_overrides
from cdk.declarations import ErrorMap

pytestmark = pytest.mark.unit


class _Runtime:
    """The two attributes ``for_runtime`` reads, and nothing else."""

    def __init__(self, error_map: dict[str, Any] | None = None):
        self.declared_error_map = error_map
        self.connector_id = "test-connector"


class TestTheBaseAnswersNeutrally:
    def test_unwrap_page_returns_the_body_untouched(self) -> None:
        body = {"records": [1]}
        assert ApiDialect().unwrap_page(body) is body

    def test_sign_request_returns_the_request_untouched(self) -> None:
        from cdk.api.http import SignedRequest

        request = SignedRequest(method="GET", url="https://x/y")
        assert ApiDialect().sign_request(request) is request

    def test_classify_has_no_opinion(self) -> None:
        assert ApiDialect().classify(500, {"error": "x"}) is None


class TestTheOneBindingSite:
    def test_for_runtime_parses_the_declared_map(self) -> None:
        dialect = ApiDialect.for_runtime(_Runtime({"http": {"429": "rate_limited"}}))
        assert isinstance(dialect.error_map, ErrorMap)
        assert dialect.error_map.match_http(429).category == "rate_limited"

    def test_an_undeclared_connector_carries_no_map(self) -> None:
        assert ApiDialect.for_runtime(_Runtime()).error_map is None

    def test_the_map_is_read_only_on_an_instance(self) -> None:
        dialect = ApiDialect.for_runtime(_Runtime())
        with pytest.raises(AttributeError):
            dialect.error_map = "anything"  # type: ignore[misc]


class TestTheRoutesAroundItAreClosed:
    def test_a_class_body_error_map_is_refused_where_it_is_written(self) -> None:
        with pytest.raises(TypeError, match="declares 'error_map'"):

            class Shadowing(ApiDialect):
                error_map = {"http": {"429": "transient"}}

    def test_overriding_the_binding_site_is_refused(self) -> None:
        with pytest.raises(TypeError, match="overrides 'for_runtime'"):

            class Rebinding(ApiDialect):
                @classmethod
                def for_runtime(cls, runtime: Any) -> Any:
                    return cls()

    def test_a_constructor_that_cannot_take_the_declaration_is_refused(self) -> None:
        with pytest.raises(TypeError, match="cannot accept the declared error map"):

            class Deaf(ApiDialect):
                def __init__(self) -> None:  # no error_map parameter
                    super().__init__(None)

    def test_a_conforming_subclass_is_accepted(self) -> None:
        class Provider(ApiDialect):
            name = "provider"

            def unwrap_page(self, body: Any) -> Any:
                return body["result"]

        dialect = Provider.for_runtime(_Runtime())
        assert dialect.unwrap_page({"result": [1, 2]}) == [1, 2]


class TestFieldEncodingHooks:
    """The two hooks backing ``{"encoding"/"encoding_write": {"name": "code"}}``.

    Unlike the other three, their base is not a neutral pass-through: they
    are only reached once a field opts in, so a missing override is a
    config defect, not silence.
    """

    def test_decode_field_base_raises_naming_the_field(self) -> None:
        with pytest.raises(NotImplementedError, match="my_field"):
            ApiDialect().decode_field("my_field", ["a", "b"], None)

    def test_encode_field_base_raises_naming_the_field(self) -> None:
        with pytest.raises(NotImplementedError, match="my_field"):
            ApiDialect().encode_field("my_field", "a", None)

    def test_a_conforming_decode_field_override_is_accepted(self) -> None:
        class Provider(ApiDialect):
            def decode_field(
                self, field_name: str, values: Any, arrow_type: Any
            ) -> Any:
                return [v.upper() for v in values]

        assert Provider(None).decode_field("f", ["a"], None) == ["A"]

    def test_a_conforming_encode_field_override_is_accepted(self) -> None:
        class Provider(ApiDialect):
            def encode_field(self, field_name: str, value: Any, arrow_type: Any) -> Any:
                return value.upper()

        assert Provider(None).encode_field("f", "a", None) == "A"

    def test_a_decode_field_override_with_the_wrong_arity_is_refused(self) -> None:
        # Built via type(), not a `class ... (ApiDialect):` statement: a
        # static override-compatibility scan pattern-matches the latter
        # syntactically and cannot tell this deliberately-malformed
        # fixture from a real bug. type() drives the identical runtime
        # path -- __init_subclass__ fires the same way either way -- so
        # the mechanism under test is unchanged.
        def bad_decode_field(self: object, field_name: str) -> Any:
            return field_name

        with pytest.raises(TypeError, match="decode_field"):
            type("BadDialect", (ApiDialect,), {"decode_field": bad_decode_field})

    def test_an_encode_field_override_with_the_wrong_arity_is_refused(self) -> None:
        def bad_encode_field(self: object, field_name: str, value: Any) -> Any:
            return value

        with pytest.raises(TypeError, match="encode_field"):
            type("BadDialect", (ApiDialect,), {"encode_field": bad_encode_field})

    def test_an_async_decode_field_override_is_refused(self) -> None:
        # Both call sites invoke the hook synchronously -- an async def
        # would hand SchemaContract a coroutine where it expects a
        # pa.Array, failing far from this class-definition-time check.
        async def bad_decode_field(
            self: object, field_name: str, values: Any, arrow_type: Any
        ) -> Any:
            return values

        with pytest.raises(TypeError, match="async"):
            type("BadDialect", (ApiDialect,), {"decode_field": bad_decode_field})

    def test_an_async_encode_field_override_is_refused(self) -> None:
        async def bad_encode_field(
            self: object, field_name: str, value: Any, arrow_type: Any
        ) -> Any:
            return value

        with pytest.raises(TypeError, match="async"):
            type("BadDialect", (ApiDialect,), {"encode_field": bad_encode_field})

    def test_a_conforming_staticmethod_override_is_accepted(self) -> None:
        # inspect.getattr_static returns the raw staticmethod descriptor,
        # not the bound function a real call site would see -- checking
        # the descriptor as if it always took an implicit leading self
        # rejected this correctly-shaped 3-arg staticmethod outright.
        class Provider(ApiDialect):
            @staticmethod
            def encode_field(field_name: str, value: Any, arrow_type: Any) -> Any:
                return value.upper()

        assert Provider(None).encode_field("f", "a", None) == "A"

    def test_a_staticmethod_override_with_a_bogus_leading_self_is_refused(self) -> None:
        # The same unwrap-blind check accepted a 4-arg staticmethod with a
        # bogus unbound leading parameter (a plain function never binds
        # one to a staticmethod) -- unusable, since the real call site
        # supplies only the 3 declared arguments, and the first encoded
        # record would raise TypeError far from this class-definition-time
        # check. Built via type(), like the arity fixtures above: a
        # `class ... (ApiDialect):` statement would raise here too, but at
        # class-body-evaluation time rather than the __init_subclass__
        # path under test.
        def bad_encode_field(
            self: object, field_name: str, value: Any, arrow_type: Any
        ) -> Any:
            return value

        with pytest.raises(TypeError, match="encode_field"):
            type(
                "BadStaticDialect",
                (ApiDialect,),
                {"encode_field": staticmethod(bad_encode_field)},
            )

    def test_an_unrelated_hook_override_is_still_accepted(self) -> None:
        # The new signature check is scoped to decode_field/encode_field
        # only; it must not start rejecting the other three hooks.
        class Provider(ApiDialect):
            def unwrap_page(self, body: Any) -> Any:
                return body

        assert dialect_overrides(Provider, "unwrap_page") is True


class TestOverrideProbe:
    def test_it_reports_which_hooks_a_dialect_implements(self) -> None:
        class Provider(ApiDialect):
            def classify(self, status: int, body: Any) -> str | None:
                return "config" if status == 200 else None

        assert dialect_overrides(Provider, "classify") is True
        assert dialect_overrides(Provider, "unwrap_page") is False
        assert dialect_overrides(ApiDialect, "classify") is False

    def test_it_reports_decode_and_encode_field_overrides(self) -> None:
        class Provider(ApiDialect):
            def decode_field(
                self, field_name: str, values: Any, arrow_type: Any
            ) -> Any:
                return values

        assert dialect_overrides(Provider, "decode_field") is True
        assert dialect_overrides(Provider, "encode_field") is False
        assert dialect_overrides(ApiDialect, "decode_field") is False
