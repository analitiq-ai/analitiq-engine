"""One filter vocabulary, two restatements, pinned to the contract.

A stream's ``source.filters[].operator`` is drawn from the contract's
``FilterOperator`` literal, and ``GenericSQLConnector._build_filters`` hands
that value straight to :class:`~cdk.query_builder.QueryBuilder`. So every
operator the contract admits has to be a key in ``OPERATOR_MAP``, or a
contract-valid stream cannot run.

That is not hypothetical: the map spent a release four names short of the
contract (``neq``, ``contains``, ``starts_with``, ``ends_with``), and the
gap was invisible because both sides are hand-written lists that nothing
compared. ``neq`` was the live one -- RULE-STRM-012 admits it on a
connection-scoped source, which is what every database stream declares, so
a contract-valid stream reached the map and failed its read. The three
substring operators are api-scope under that rule and cannot be reached by
a connection-scoped stream; they are still required here because scope,
not kind, picks the vocabulary, and a database connector shipping a
connector-scoped endpoint renders api-vocabulary filters through this
builder.

Reading the vocabulary off the contract is what makes a future addition
fail here instead of at a customer's first filtered read.

Coverage, not equality: the map also accepts SQL symbols (``=``, ``!=``,
``<>``) for a connector that calls the builder directly. Those carry no
contract spelling and are not drift.
"""

from typing import get_args

from analitiq.contracts.stream import FilterOperator

from cdk.query_builder import QueryBuilder


def test_the_map_covers_every_operator_the_contract_admits() -> None:
    """Every contract operator resolves to a builder operator."""
    contract_operators = set(get_args(FilterOperator))
    assert contract_operators, (
        "the contract's FilterOperator is no longer a Literal of strings; this "
        "pin must follow it rather than silently compare two empty sets"
    )
    unmapped = sorted(contract_operators - set(QueryBuilder.OPERATOR_MAP))
    assert not unmapped, (
        f"the stream contract admits {unmapped}, which QueryBuilder.OPERATOR_MAP "
        f"does not map: a stream declaring one of these is contract-valid and "
        f"fails its read with 'Unknown filter operator'"
    )
