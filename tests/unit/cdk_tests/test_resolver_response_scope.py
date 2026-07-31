"""The response scope reads through the one resolution vocabulary.

The api read path hands raw declared nodes straight to the resolver, so
these are the semantics the pagination loop and its stop conditions are
built on. They lived beside the engine-side wrapper that used to undo
contract parsing; the wrapper is gone and the semantics are not, so they
are pinned here against the resolver itself.
"""

from __future__ import annotations

import base64
from decimal import Decimal
from typing import Any

import pytest

from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.resolver import ResolutionContext, Resolver

pytestmark = pytest.mark.unit


def _resolver(body: Any, *, parameters: dict[str, Any] | None = None) -> Resolver:
    """Response-scoped resolver over *body*, the shape a page builds."""
    return Resolver(
        ResolutionContext(
            connection={"parameters": parameters or {}},
            response={"body": body},
        ),
        functions=DEFAULT_FUNCTIONS,
    )


class TestResponseScope:
    def test_the_body_ref_addresses_the_payload_itself(self) -> None:
        assert _resolver([1, 2]).resolve_for_request({"ref": "response.body"}) == [1, 2]

    def test_a_missing_path_answers_none(self) -> None:
        # None is the paging loop's own end signal. Raising here -- which
        # ``resolve`` does -- would turn end-of-pages into a read failure,
        # which is why the read path calls ``resolve_for_request``.
        assert (
            _resolver({"a": 1}).resolve_for_request({"ref": "response.body.next"})
            is None
        )

    def test_another_scope_reads_through_the_same_grammar(self) -> None:
        resolver = _resolver({}, parameters={"token": "t-1"})
        value = resolver.resolve_for_request({"ref": "connection.parameters.token"})
        assert value == "t-1"

    def test_an_unknown_scope_raises(self) -> None:
        # The contract's ref pattern rejects this at validation, so it can
        # only arrive from a hand-built node; the resolver is the last line
        # of defence rather than a silent None.
        with pytest.raises(KeyError, match="scope"):
            _resolver({}).resolve_for_request({"ref": "bogus.path"})

    def test_a_decimal_interpolates_exactly(self) -> None:
        # Response numbers arrive as Decimal from the lossless parse; a
        # keyset key rendered through float would lose the digits the
        # parse exists to keep.
        resolver = _resolver({"next_score": Decimal("1234567890.12345678")})
        rendered = resolver.resolve_for_request(
            {"template": "after=${response.body.next_score}"}
        )
        assert rendered == "after=1234567890.12345678"

    def test_a_function_runs_over_a_response_value(self) -> None:
        resolver = _resolver({"token": "abc"})
        encoded = resolver.resolve_for_request(
            {"function": "base64_encode", "input": {"ref": "response.body.token"}}
        )
        assert encoded == base64.b64encode(b"abc").decode("ascii")
