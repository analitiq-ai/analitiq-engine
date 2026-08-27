"""Stop conditions evaluated from raw nodes, with the engine's semantics.

The engine-side evaluator selected each operator by ``isinstance`` over
seventeen contract models. Reading the operator off the node keeps the same
vocabulary and the same answers while the CDK imports none of them -- so
these tests pin the semantics that must survive the move, not the mechanism.
"""

from __future__ import annotations

import inspect
from decimal import Decimal
from typing import Any

import pytest

from cdk.api import predicates as predicates_module
from cdk.api.predicates import (
    _COMPARISONS,
    _PRESENCE,
    UnknownPredicate,
    evaluate_predicate,
)

pytestmark = pytest.mark.unit


def _resolve(value: Any) -> Any:
    """Treat a ``{"value": x}`` node as the literal x; anything else is itself."""
    if isinstance(value, dict) and set(value) == {"value"}:
        return value["value"]
    return value


def _ev(pred: Any) -> bool:
    return evaluate_predicate(pred, _resolve)


class TestComparisons:
    @pytest.mark.parametrize(
        "op,left,right,expected",
        [
            ("eq", 1, 1, True),
            ("eq", 1, 2, False),
            ("neq", 1, 2, True),
            ("lt", 1, 2, True),
            ("lt", 2, 1, False),
            ("lte", 2, 2, True),
            ("gt", 3, 2, True),
            ("gte", 2, 2, True),
        ],
    )
    def test_each_operator(
        self, op: str, left: Any, right: Any, expected: bool
    ) -> None:
        assert _ev({op: [{"value": left}, {"value": right}]}) is expected

    def test_a_decimal_compares_by_value_with_an_authored_float(self) -> None:
        # Response numbers parse losslessly to Decimal while authored
        # literals are floats. `Decimal("0.1") == 0.1` is False, but the
        # author meant 0.1 the decimal.
        assert _ev({"eq": [{"value": Decimal("0.1")}, {"value": 0.1}]}) is True
        assert _ev({"eq": [{"value": 0.1}, {"value": Decimal("0.1")}]}) is True

    def test_an_incomparable_pair_raises_rather_than_guessing(self) -> None:
        # Guessing here either truncates the read or runs it forever.
        with pytest.raises(ValueError, match="cannot compare"):
            _ev({"lt": [{"value": None}, {"value": 3}]})


class TestPresence:
    @pytest.mark.parametrize(
        "op,value,expected",
        [
            ("exists", 1, True),
            ("exists", None, False),
            ("missing", None, True),
            ("missing", 0, False),
            ("empty", None, True),
            ("empty", [], True),
            ("empty", {}, True),
            ("empty", "", True),
            ("empty", [1], False),
            ("not_empty", [1], True),
            ("not_empty", [], False),
        ],
    )
    def test_each_operator(self, op: str, value: Any, expected: bool) -> None:
        assert _ev({op: {"value": value}}) is expected

    @pytest.mark.parametrize("value", [0, False, 0.0])
    def test_a_falsy_scalar_is_present_not_empty(self, value: Any) -> None:
        # `empty` asks "is there nothing here", not "is this falsy". A page
        # count of 0 is a value the provider sent.
        assert _ev({"empty": {"value": value}}) is False
        assert _ev({"not_empty": {"value": value}}) is True


class TestComposition:
    def test_and_requires_every_branch(self) -> None:
        assert _ev({"and": [{"eq": [1, 1]}, {"eq": [2, 2]}]}) is True
        assert _ev({"and": [{"eq": [1, 1]}, {"eq": [2, 3]}]}) is False

    def test_or_requires_one_branch(self) -> None:
        assert _ev({"or": [{"eq": [1, 2]}, {"eq": [2, 2]}]}) is True
        assert _ev({"or": [{"eq": [1, 2]}, {"eq": [2, 3]}]}) is False

    def test_not_inverts(self) -> None:
        assert _ev({"not": {"eq": [1, 1]}}) is False

    def test_they_nest(self) -> None:
        pred = {
            "and": [
                {"or": [{"eq": [1, 2]}, {"gt": [5, 3]}]},
                {"not": {"empty": {"value": [1]}}},
            ]
        }
        assert _ev(pred) is True


class TestTheVocabularyIsTheContracts:
    def test_an_unknown_operator_fails_loud(self) -> None:
        # Answering False would run the read to exhaustion instead.
        with pytest.raises(UnknownPredicate, match="matches"):
            _ev({"matches": ["a", "b"]})

    def test_a_predicate_naming_two_operators_is_refused(self) -> None:
        # A validated predicate carries exactly one; reading the first would
        # silently apply half of what was written.
        with pytest.raises(UnknownPredicate, match="exactly one"):
            _ev({"eq": [1, 1], "neq": [1, 2]})

    def test_a_non_object_predicate_is_refused(self) -> None:
        with pytest.raises(UnknownPredicate, match="an object"):
            _ev(["eq", 1, 1])


class TestEveryContractOperatorIsCovered:
    """The check that de-risks reading operators off raw nodes.

    Selecting by ``isinstance`` over the contract's models made coverage
    self-evident: a missing branch was a class nobody handled. Reading a key
    instead means a contract release could add an operator this build would
    meet only at runtime, on a customer's stop condition. So the operator set
    is read back off the contract here -- the test imports it, the CDK module
    does not.
    """

    def _contract_operators(self) -> set[str]:
        import typing

        from analitiq.contracts.endpoints import Predicate

        members = typing.get_args(typing.get_args(Predicate)[0])
        aliases: set[str] = set()
        for member in members:
            model = typing.get_args(member)[0] if typing.get_args(member) else member
            for name, field in getattr(model, "model_fields", {}).items():
                aliases.add(field.alias or name)
        return aliases

    def test_the_dispatch_covers_the_whole_union(self) -> None:
        covered = set(_COMPARISONS) | set(_PRESENCE) | {"and", "or", "not"}
        missing = self._contract_operators() - covered
        assert not missing, (
            f"the contract declares stop_when operators this build cannot "
            f"evaluate: {sorted(missing)}"
        )

    def test_every_covered_operator_is_one_the_contract_declares(self) -> None:
        covered = set(_COMPARISONS) | set(_PRESENCE) | {"and", "or", "not"}
        invented = covered - self._contract_operators()
        assert not invented, f"operators the contract does not declare: {invented}"


class TestAParsedStopConditionReadsTheSame:
    """The stop condition arrives as a contract model, and answers the same.

    ``operations.read.pagination.stop_when`` is a parsed ``Predicate`` on
    the document the read path now navigates, so what reaches this
    evaluator is a model rather than the node its author wrote. It is read
    in its authored form -- alias names restored, the author's omissions
    still omitted -- which is what lets the operator table below stay the
    contract's vocabulary without importing the contract.
    """

    _AUTHORED = {
        "and": [
            {"eq": [{"value": 1}, {"value": 1}]},
            {"not": {"empty": {"value": ["a"]}}},
        ]
    }

    def test_the_model_and_the_json_it_was_parsed_from_agree(self) -> None:
        import typing

        from analitiq.contracts.endpoints import Predicate
        from pydantic import TypeAdapter

        parsed = TypeAdapter(typing.cast(Any, Predicate)).validate_python(
            self._AUTHORED
        )
        assert _ev(parsed) is _ev(self._AUTHORED) is True


class TestTheModuleStaysCdkClean:
    def test_it_imports_no_contract_models(self) -> None:
        # The whole point of reading the operator off the node: selecting a
        # branch by isinstance would drag in all seventeen predicate models.
        source = inspect.getsource(predicates_module)
        assert "analitiq.contracts" not in source
        assert "Predicate" not in source.replace("UnknownPredicate", "")
