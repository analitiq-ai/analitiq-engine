"""Evaluate a declared stop condition against the page it was written for.

The engine validates the endpoint document against the published contract
before anything reads it, so this layer navigates an already-validated
document raw: a predicate is a one-key mapping whose key names the operator,
exactly as the contract's own union serialises. That is what lets the CDK
evaluate one without importing the contract's seventeen predicate models --
selecting a branch by ``isinstance`` would have dragged all of them in, and
the CDK's dependency set stays SQLAlchemy + Pydantic.

The vocabulary is still the contract's. An operator this build does not know
fails loud naming what it read, rather than quietly answering False and
running a read to exhaustion.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

__all__ = ["UnknownPredicate", "evaluate_predicate"]

#: Resolves a declared value expression to a concrete value.
Resolve = Callable[[Any], Any]


class UnknownPredicate(ValueError):
    """A stop condition naming an operator this build cannot evaluate."""


def _is_empty(value: Any) -> bool:
    """Report the contract's emptiness: absent, or sized with no items.

    Scalars are never empty. ``empty`` asks "is there nothing here", not "is
    this falsy", so ``0`` and ``False`` are present values.
    """
    if value is None:
        return True
    if isinstance(value, (list, dict, str)):
        return len(value) == 0
    return False


def _normalized(left: Any, right: Any) -> tuple[Any, Any]:
    """Align mixed Decimal/float operands so they compare by value.

    Response numbers arrive as ``Decimal`` (the lossless JSON parse) while
    authored literals are floats. The float's shortest decimal rendering IS
    the authored value, so it converts through ``str``: ``Decimal("0.1") ==
    0.1`` is False, but the author meant 0.1 the decimal.
    """
    if isinstance(left, Decimal) and isinstance(right, float):
        return left, Decimal(str(right))
    if isinstance(right, Decimal) and isinstance(left, float):
        return Decimal(str(left)), right
    return left, right


#: The contract's comparison operators. Each takes a two-operand list.
_COMPARISONS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": lambda a, b: bool(a == b),
    "neq": lambda a, b: bool(a != b),
    "lt": lambda a, b: bool(a < b),
    "lte": lambda a, b: bool(a <= b),
    "gt": lambda a, b: bool(a > b),
    "gte": lambda a, b: bool(a >= b),
}

#: The contract's presence operators. Each takes one operand expression.
_PRESENCE: dict[str, Callable[[Any], bool]] = {
    "exists": lambda value: value is not None,
    "missing": lambda value: value is None,
    "empty": _is_empty,
    "not_empty": lambda value: not _is_empty(value),
}


def _operator(pred: Mapping[str, Any]) -> str:
    """Return the single key naming this predicate's operator.

    A validated predicate carries exactly one. More than one is a document
    that could not have validated, and reading the first would silently
    apply half of what was written.
    """
    keys = list(pred)
    if len(keys) != 1:
        raise UnknownPredicate(
            f"a stop_when predicate names exactly one operator; read {keys}"
        )
    return keys[0]


def evaluate_predicate(pred: Any, resolve: Resolve) -> bool:
    """Evaluate a declared stop condition, resolving operands through *resolve*.

    An incomparable pair -- ordering ``None`` against a number, say -- raises
    naming the operator rather than guessing a truth value, because a guess
    here either truncates a read or runs it forever.
    """
    if not isinstance(pred, Mapping):
        raise UnknownPredicate(
            f"a stop_when predicate is an object; read {type(pred).__name__}"
        )
    op = _operator(pred)
    operand = pred[op]

    if op == "and":
        return all(evaluate_predicate(p, resolve) for p in operand)
    if op == "or":
        return any(evaluate_predicate(p, resolve) for p in operand)
    if op == "not":
        return not evaluate_predicate(operand, resolve)
    if op in _PRESENCE:
        return _PRESENCE[op](resolve(operand))
    if op in _COMPARISONS:
        left, right = _normalized(*(resolve(item) for item in operand))
        try:
            return _COMPARISONS[op](left, right)
        except TypeError as err:
            raise ValueError(
                f"stop_when {op!r} cannot compare "
                f"{type(left).__name__} with {type(right).__name__}"
            ) from err
    raise UnknownPredicate(f"unknown stop_when operator {op!r}")
