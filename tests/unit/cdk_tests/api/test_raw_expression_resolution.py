"""A raw declared node resolves without an engine-side adapter.

The engine wraps every value expression in ``resolve_contract_expr``, whose
whole body dumps a parsed contract model back to the authored node shape and
hands it to the CDK resolver. Its own docstring says why: "The CDK resolver
reads authored JSON... a model handed straight to the resolver is not an
expression node to it and would sail through as an opaque value."

So the wrapper is not a layer to port. Once a document is navigated raw --
which is what keeps the CDK free of contract models -- the nodes are already
in the shape the resolver wants, and the four expression models the wrapper
imports go with it. These tests are the evidence for deleting it rather than
moving it.
"""

from __future__ import annotations

from typing import Any

import pytest

from cdk.resolver import ResolutionContext, Resolver

pytestmark = pytest.mark.unit


@pytest.fixture()
def page_resolver() -> Resolver:
    return Resolver(ResolutionContext()).with_response(
        {"body": {"next": "abc", "count": 3, "nested": {"cursor": "z9"}}}
    )


class TestTheResolverAlreadySpeaksAuthoredNodes:
    def test_a_ref_reads_the_page_scope(self, page_resolver: Resolver) -> None:
        assert page_resolver.resolve_for_request({"ref": "response.body.next"}) == "abc"

    def test_a_ref_walks_into_the_body(self, page_resolver: Resolver) -> None:
        node = {"ref": "response.body.nested.cursor"}
        assert page_resolver.resolve_for_request(node) == "z9"

    def test_a_template_substitutes_from_the_page_scope(
        self, page_resolver: Resolver
    ) -> None:
        node = {"template": "page=${response.body.count}"}
        assert page_resolver.resolve_for_request(node) == "page=3"

    @pytest.mark.parametrize("value", ["plain", 7, 0, None, True, [1, 2]])
    def test_a_non_expression_operand_passes_through(
        self, page_resolver: Resolver, value: Any
    ) -> None:
        # Comparison operands are frequently literals; the grammar passing
        # them through unchanged is what lets a stop condition mix them with
        # refs without the caller sorting them first.
        assert page_resolver.resolve_for_request(value) == value

    def test_each_page_gets_its_own_scope(self) -> None:
        # with_response is per page, so a resolver built for one page cannot
        # answer with another's body.
        base = Resolver(ResolutionContext())
        first = base.with_response({"body": {"next": "one"}})
        second = base.with_response({"body": {"next": "two"}})
        node = {"ref": "response.body.next"}
        assert first.resolve_for_request(node) == "one"
        assert second.resolve_for_request(node) == "two"
