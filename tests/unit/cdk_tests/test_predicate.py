"""Unit tests for :mod:`cdk.predicate`, the contract predicate evaluator.

The predicate grammar is what the api-endpoint contract uses to express
pagination stop conditions, so these pin the two operand policies the module
draws a line between: absence-testing operators answer for data that is not
there, comparison operators refuse to.
"""

from __future__ import annotations

import pytest

from cdk.exceptions import TransportSpecError
from cdk.predicate import UNRESOLVED, evaluate_predicate
from cdk.resolver import ResolutionContext, Resolver

pytestmark = pytest.mark.unit


def _resolver(**response) -> Resolver:
    """Resolver over a `response` scope, the way a page evaluates its predicate."""
    return Resolver(ResolutionContext(response=response))


# ---------------------------------------------------------------------------
# Comparison operators
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "operator,left,right,expected",
    [
        ("eq", 3, 3, True),
        ("eq", 3, 4, False),
        ("neq", 3, 4, True),
        ("neq", 3, 3, False),
        ("lt", 1, 2, True),
        ("lt", 2, 2, False),
        ("lte", 2, 2, True),
        ("gt", 3, 2, True),
        ("gt", 2, 3, False),
        ("gte", 2, 2, True),
    ],
)
def test_operator_truth_table(operator, left, right, expected):
    resolver = _resolver(record_count=left)
    node = {operator: [{"ref": "response.record_count"}, {"literal": right}]}
    assert evaluate_predicate(node, resolver) is expected


def test_eq_against_a_boolean_body_field():
    # The `has_more: false` shape, which the short-page heuristic could
    # never express.
    resolver = _resolver(body={"has_more": False})
    node = {"eq": [{"ref": "response.body.has_more"}, {"literal": False}]}
    assert evaluate_predicate(node, resolver) is True


def test_unresolved_comparison_operand_raises():
    # An author comparing against a field the provider never sends has a
    # defect. Inventing a truth value would either truncate the read or
    # spin forever, so it fails loud and names the alternative.
    resolver = _resolver(body={})
    node = {"gte": [{"ref": "response.body.page"}, {"literal": 3}]}
    with pytest.raises(TransportSpecError, match="did not resolve"):
        evaluate_predicate(node, resolver)


def test_unorderable_operands_raise():
    resolver = _resolver(body={"page": "two"})
    node = {"lt": [{"ref": "response.body.page"}, {"literal": 3}]}
    with pytest.raises(TransportSpecError, match="cannot order"):
        evaluate_predicate(node, resolver)


def test_wrong_operand_count_raises():
    resolver = _resolver(record_count=1)
    with pytest.raises(TransportSpecError, match="exactly two operands"):
        evaluate_predicate({"eq": [{"literal": 1}]}, _resolver())
    with pytest.raises(TransportSpecError, match="exactly two operands"):
        evaluate_predicate({"eq": {"literal": 1}}, resolver)


# ---------------------------------------------------------------------------
# Absence-testing operators
# ---------------------------------------------------------------------------


def test_exists_and_missing_are_complementary():
    resolver = _resolver(body={"next": "token"})
    assert evaluate_predicate({"exists": {"ref": "response.body.next"}}, resolver)
    assert not evaluate_predicate({"missing": {"ref": "response.body.next"}}, resolver)

    absent = _resolver(body={})
    assert not evaluate_predicate({"exists": {"ref": "response.body.next"}}, absent)
    assert evaluate_predicate({"missing": {"ref": "response.body.next"}}, absent)


@pytest.mark.parametrize("value", [[], {}, "", None])
def test_empty_values_are_empty(value):
    resolver = _resolver(body={"objects": value})
    assert evaluate_predicate({"empty": {"ref": "response.body.objects"}}, resolver)


def test_absent_field_is_empty():
    # The sevdesk stop condition: a body that stops carrying `objects` at
    # all reads the same as one carrying an empty list.
    resolver = _resolver(body={})
    assert evaluate_predicate({"empty": {"ref": "response.body.objects"}}, resolver)


@pytest.mark.parametrize("value", [0, False, 0.0])
def test_zero_and_false_are_not_empty(value):
    # A count of zero is a value, not an absence. Conflating them would
    # stop a read on a legitimate result.
    resolver = _resolver(body={"total": value})
    assert not evaluate_predicate({"empty": {"ref": "response.body.total"}}, resolver)
    assert evaluate_predicate({"not_empty": {"ref": "response.body.total"}}, resolver)


def test_not_empty_on_a_populated_list():
    resolver = _resolver(body={"objects": [{"id": 1}]})
    assert evaluate_predicate({"not_empty": {"ref": "response.body.objects"}}, resolver)


# ---------------------------------------------------------------------------
# Nesting
# ---------------------------------------------------------------------------


def test_and_requires_every_branch():
    resolver = _resolver(record_count=0, body={"has_more": False})
    node = {
        "and": [
            {"eq": [{"ref": "response.record_count"}, {"literal": 0}]},
            {"eq": [{"ref": "response.body.has_more"}, {"literal": False}]},
        ]
    }
    assert evaluate_predicate(node, resolver) is True


def test_and_is_false_when_one_branch_fails():
    resolver = _resolver(record_count=5, body={"has_more": False})
    node = {
        "and": [
            {"eq": [{"ref": "response.record_count"}, {"literal": 0}]},
            {"eq": [{"ref": "response.body.has_more"}, {"literal": False}]},
        ]
    }
    assert evaluate_predicate(node, resolver) is False


def test_or_needs_only_one_branch():
    resolver = _resolver(record_count=5, body={})
    node = {
        "or": [
            {"missing": {"ref": "response.body.next"}},
            {"eq": [{"ref": "response.record_count"}, {"literal": 0}]},
        ]
    }
    assert evaluate_predicate(node, resolver) is True


def test_not_inverts():
    resolver = _resolver(body={"next": "token"})
    node = {"not": {"missing": {"ref": "response.body.next"}}}
    assert evaluate_predicate(node, resolver) is True


def test_empty_branch_list_raises():
    # An empty `and` is vacuously true (stop at once) and an empty `or`
    # vacuously false (never stop) — opposite silent failures from the
    # same typo, so neither gets a default.
    for operator in ("and", "or"):
        with pytest.raises(TransportSpecError, match="non-empty list"):
            evaluate_predicate({operator: []}, _resolver())


# ---------------------------------------------------------------------------
# Authoring defects
# ---------------------------------------------------------------------------


def test_no_operator_key_raises():
    with pytest.raises(TransportSpecError, match="exactly one operator"):
        evaluate_predicate({}, _resolver())


def test_two_operator_keys_raise():
    node = {
        "eq": [{"literal": 1}, {"literal": 1}],
        "missing": {"ref": "response.body.next"},
    }
    with pytest.raises(TransportSpecError, match="exactly one operator"):
        evaluate_predicate(node, _resolver(body={}))


def test_unknown_operator_is_named():
    with pytest.raises(TransportSpecError, match="unknown operator"):
        evaluate_predicate({"matches": "anything"}, _resolver())


def test_extension_keys_are_not_operators():
    # `x-*` is the contract's extension escape hatch; it must not count
    # toward the exactly-one-operator rule.
    resolver = _resolver(body={})
    node = {"missing": {"ref": "response.body.next"}, "x-note": "why we stop"}
    assert evaluate_predicate(node, resolver) is True


def test_non_object_predicate_raises():
    with pytest.raises(TransportSpecError, match="must be an object"):
        evaluate_predicate("stop", _resolver())


def test_unknown_scope_is_not_swallowed_as_missing():
    # A typo'd scope is an authoring defect, not absent data — `missing`
    # must not report a confident True for it.
    with pytest.raises(KeyError):
        evaluate_predicate({"missing": {"ref": "respones.body.next"}}, _resolver())


def test_unresolved_sentinel_is_a_singleton():
    from cdk.predicate import _Unresolved

    assert _Unresolved() is UNRESOLVED
