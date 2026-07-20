"""Drift guards pinning the engine's pagination vocabularies to the contract.

The engine hardcodes three vocabularies the published api-endpoint contract
also defines: the predicate operator set, the pagination strategy set, and the
reserved ``response`` scope keys. It has to — ``cdk`` deliberately carries no
dependency on ``analitiq-contract-models`` (the CDK is what connectors build
against; making it depend on the contract package would invert that), and the
contract's own tag lists are private to it.

That leaves a silent-divergence risk of exactly the kind issue #346 was about:
the contract grows an operator or a strategy, documents authored against it
validate clean, and the engine rejects or ignores them at read time. These
tests close it from the one layer that can see both sides. A contract bump that
changes any of the three fails here, naming what moved, instead of surfacing as
a runtime error against a document that "should" work.

The introspection mirrors the contract package's own import-time drift
assertions: read the `Tag` metadata off the published discriminated unions
rather than a private constant.
"""

from __future__ import annotations

from typing import get_args

import pytest
from analitiq.contracts.endpoints import RESERVED_RESPONSE_SCOPES, Pagination, Predicate
from analitiq.contracts.value_expression import RESOLUTION_SCOPES
from pydantic import Tag

from cdk.predicate import PREDICATE_OPERATORS
from cdk.resolver import ResolutionContext, Resolver
from src.source.connectors.api import (
    _ADVANCE_BUILDERS,
    _CONTROL_FIELDS,
    APIConnector,
    _Page,
)

pytestmark = pytest.mark.unit


def _union_tags(annotated_union: object) -> frozenset[str]:
    """Tag values of an ``Annotated[Union[Annotated[..., Tag(...)], ...], ...]``."""
    tags: set[str] = set()
    union_arg, _discriminator = get_args(annotated_union)
    for member in get_args(union_arg):
        for meta in get_args(member)[1:]:
            if isinstance(meta, Tag):
                tags.add(meta.tag)
    return frozenset(tags)


def test_predicate_operators_match_the_contract():
    """Every contract predicate operator is evaluable, and no extras are invented.

    An operator the contract defines but the evaluator does not know is
    rejected as "unknown operator" on a document that validates clean. One the
    evaluator accepts but the contract does not define is a private extension
    the schema will reject at authoring time.
    """
    assert PREDICATE_OPERATORS == _union_tags(Predicate)


def test_pagination_strategies_match_the_contract():
    """Every contract strategy has a builder — the #346 `link` gap, generalized."""
    assert frozenset(_ADVANCE_BUILDERS) == _union_tags(Pagination)


def test_response_scope_keys_match_the_contract():
    """The per-page `response` scope offers exactly the reserved vocabulary.

    A missing key makes a contract-valid expression unresolvable at read time;
    an extra one would shadow a name the contract lets `response.metadata`
    define. Asserted against the scope the connector actually builds rather
    than a mirrored constant, which would be one more thing to drift.
    """
    scope = APIConnector._response_scope(
        _Page(body={}, records=[], status=200, headers={}),
        metadata_spec={},
        resolver=Resolver(ResolutionContext()),
    )
    assert frozenset(scope) == RESERVED_RESPONSE_SCOPES


def test_resolution_scopes_match_the_contract():
    """The engine addresses exactly the scope vocabulary the contract validates.

    Both sides police this list and neither can see the other. A scope the
    contract accepts but the resolver does not is the worst direction: the
    document validates clean, ships, and every ref into that scope dies at
    read time as an unknown-scope defect. One the resolver accepts but the
    contract does not can never appear in a validated document, so it is dead
    vocabulary that reads as supported.
    """
    assert frozenset(ResolutionContext._SCOPES) == frozenset(RESOLUTION_SCOPES)


# Fields of a contract pagination block that carry a plain scalar rather than
# a value expression. Everything else in those models is an expression the
# engine has to classify; see `_CONTROL_FIELDS`.
_SCALAR_PAGINATION_FIELDS = frozenset({"type", "param", "order_by_field", "max"})


def test_every_pagination_expression_field_is_classified():
    """Every value expression in a strategy block is a declared control value.

    This is the #346 defect class stated once: an expression that decides
    what to request must resolve strictly, classify its failures as config
    errors, and land through a type guard. A field the contract adds and the
    engine does not classify gets none of that — it is either ignored or
    resolved leniently, which is how a read silently starts from a value
    nobody declared. Failing here makes a contract bump name the new field
    instead of a connector discovering it in production.
    """
    for strategy_model in get_args(get_args(Pagination)[0]):
        model = get_args(strategy_model)[0]
        strategy = model.model_fields["type"].annotation.__args__[0]
        for name, block in model.model_fields.items():
            if name in ("type", "stop_when"):
                continue
            expression_fields = (
                frozenset(
                    field
                    for field in block.annotation.__args__[0].model_fields
                    if field not in _SCALAR_PAGINATION_FIELDS
                )
                if _optional(block)
                else frozenset(
                    field
                    for field in block.annotation.model_fields
                    if field not in _SCALAR_PAGINATION_FIELDS
                )
            )
            # `limit` is shared by four strategies under one key; the
            # per-strategy blocks are keyed by the strategy name.
            key = name if name == "limit" else strategy
            assert expression_fields == _CONTROL_FIELDS[key], (
                f"{strategy}.{name}: contract expression fields "
                f"{sorted(expression_fields)} do not match the engine's "
                f"classified control fields {sorted(_CONTROL_FIELDS[key])}"
            )


def _optional(field: object) -> bool:
    """True when a model field is declared ``X | None`` rather than ``X``."""
    return type(None) in get_args(getattr(field, "annotation", None))
