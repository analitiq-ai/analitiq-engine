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


class TestOverrideProbe:
    def test_it_reports_which_hooks_a_dialect_implements(self) -> None:
        class Provider(ApiDialect):
            def classify(self, status: int, body: Any) -> str | None:
                return "config" if status == 200 else None

        assert dialect_overrides(Provider, "classify") is True
        assert dialect_overrides(Provider, "unwrap_page") is False
        assert dialect_overrides(ApiDialect, "classify") is False
