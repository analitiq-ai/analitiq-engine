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
from analitiq.contracts.endpoints import (
    RESERVED_RESPONSE_SCOPES,
    ApiEndpointDoc,
    Pagination,
    Predicate,
)
from analitiq.contracts.value_expression import RESOLUTION_SCOPES
from pydantic import Tag, TypeAdapter

from cdk.exceptions import UnresolvedValueError
from cdk.predicate import PREDICATE_OPERATORS
from cdk.resolver import ResolutionContext, Resolver
from src.source.connectors.api import (
    _ADVANCE_BUILDERS,
    _CONTROL_FIELDS,
    APIConnector,
    _Page,
)
from tests.unit.analitiq_stream.connectors.test_api_read_batches import (
    _endpoint_doc_with_records,
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


# Contract scopes the engine knowingly does not serve yet. A ref into one of
# these is reported as an unknown scope, which is a document defect rather
# than missing data — deliberately, because a *known but empty* scope reads as
# missing data and the per-request policy drops that silently. Nothing
# populates `stream`, and what belongs in it is the contract's call, not the
# engine's; listing it here keeps the gap visible instead of implied by a
# vocabulary mismatch.
_SCOPES_AWAITING_ENGINE_SUPPORT = frozenset({"stream"})


def test_resolution_scopes_match_the_contract():
    """The engine addresses the contract's scope vocabulary, minus a named gap.

    Both sides police this list and neither can see the other. A scope the
    contract accepts but the resolver does not is the worst direction: the
    document validates clean, ships, and every ref into that scope dies at
    read time. One the resolver accepts but the contract does not can never
    appear in a validated document, so it is dead vocabulary that reads as
    supported. A *new* contract scope therefore has to be either implemented
    or added to the list above, by hand, with a reason.
    """
    engine = frozenset(ResolutionContext._SCOPES)
    contract = frozenset(RESOLUTION_SCOPES)
    assert engine <= contract, f"engine invents {sorted(engine - contract)}"
    assert contract - engine == _SCOPES_AWAITING_ENGINE_SUPPORT


def test_an_unserved_contract_scope_fails_loudly_rather_than_dropping():
    """The gap above must surface as a defect, never as a quietly absent value.

    This is why the scope is withheld rather than declared empty: `lookup`
    raises a plain `KeyError` for an unknown scope, which the read path
    reports as a document error, while `UnresolvedValueError` is missing data
    that a request param is allowed to drop.
    """
    for scope in _SCOPES_AWAITING_ENGINE_SUPPORT:
        with pytest.raises(KeyError) as caught:
            ResolutionContext().lookup(f"{scope}.anything")
        assert not isinstance(caught.value, UnresolvedValueError)


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


# ---------------------------------------------------------------------------
# The suite's own documents
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "doc",
    [
        pytest.param(_endpoint_doc_with_records(), id="plain"),
        pytest.param(
            _endpoint_doc_with_records(
                pagination={
                    "type": "offset",
                    "offset": {"param": "offset", "initial": 0},
                    "limit": {"param": "limit"},
                    "stop_when": {"empty": {"ref": "response.body.records"}},
                }
            ),
            id="paginated",
        ),
        pytest.param(
            _endpoint_doc_with_records(
                extra_record_properties={
                    "updated_at": {
                        "type": "string",
                        "native_type": "timestamp",
                        "arrow_type": "Timestamp(MICROSECOND)",
                    }
                },
                replication={
                    "cursor_mappings": [
                        {
                            "cursor_field": "updated_at",
                            "param": "since",
                            "operator": "gte",
                        }
                    ]
                },
            ),
            id="incremental",
        ),
    ],
)
def test_the_suites_endpoint_documents_are_contract_valid(doc):
    """The helper claims to emit what the contract accepts; hold it to that.

    Every read test asserts against a document this helper builds. If those
    documents cannot validate, the suite is describing behaviour for shapes
    that could never ship — and the engine's own load-time validation would
    reject them before a single request went out.
    """
    TypeAdapter(ApiEndpointDoc).validate_python(doc)
