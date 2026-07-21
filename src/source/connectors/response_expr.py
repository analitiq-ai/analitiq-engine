"""Response-scope evaluation of contract value expressions and predicates.

The api-endpoint contract declares per-page read behavior against the
provider's response: ``pagination.stop_when`` is a predicate over the parsed
body, and ``cursor.next_cursor`` / ``link.next_url`` are value expressions
resolved from it. Value resolution goes through the shared CDK
:class:`~cdk.resolver.Resolver` — the engine's one resolution vocabulary —
with the ``response`` scope holding the page body, so every expression form
(``ref`` / ``template`` / ``literal`` / ``function``) behaves here exactly
as it does in request binding. This module layers the contract's
stop-condition predicate grammar (spec: §Stop Conditions) over that shared
resolution.

Expression nodes arrive in two forms: parsed models where the contract
types the field with the expression union (``cursor.next_cursor``,
``link.next_url``), and raw nodes where it types the slot ``Any``
(predicate operands). Both resolve identically; a value that addresses
nothing resolves to ``None`` (the pagination loops' stop signal), while
authoring defects (a typo'd scope, an unknown function) raise through the
resolver's own error surface.
"""

from __future__ import annotations

from typing import Any

from analitiq.contracts.endpoints import (
    FunctionExpr,
    LiteralExpr,
    PredAnd,
    PredEmpty,
    PredEq,
    PredExists,
    PredGt,
    PredGte,
    PredLt,
    PredLte,
    PredMissing,
    PredNeq,
    PredNot,
    PredNotEmpty,
    PredOr,
    RefExpr,
    TemplateExpr,
)

from cdk.resolver import Resolver

_EXPRESSION_MODELS = (RefExpr, LiteralExpr, TemplateExpr, FunctionExpr)


def resolve_response_expr(expr: Any, resolver: Resolver) -> Any:
    """Resolve a contract value expression through a response-scoped resolver.

    ``resolver`` carries the page's parsed body in its ``response`` scope
    (``Resolver.with_response({"body": data})``). Parsed expression models
    are dumped back to their authored node shape; raw nodes and plain
    operand values go through as-is — ``resolve_for_request`` owns the
    grammar, including passing non-expression values through unchanged.
    """
    if isinstance(expr, _EXPRESSION_MODELS):
        expr = expr.model_dump(mode="json", by_alias=True, exclude_unset=True)
    return resolver.resolve_for_request(expr)


def _is_empty(value: Any) -> bool:
    """Apply the contract's emptiness: absent/null, or a sized value with no items.

    Scalars (numbers, booleans) are never empty — ``empty`` asks "is there
    nothing here", not "is this falsy", so ``0`` and ``False`` are present
    values.
    """
    if value is None:
        return True
    if isinstance(value, (list, dict, str)):
        return len(value) == 0
    return False


def evaluate_predicate(pred: Any, resolver: Resolver) -> bool:
    """Evaluate a contract stop-condition predicate against a page.

    Comparison operators resolve both operands through
    :func:`resolve_response_expr`; an incomparable pair (e.g. ordering
    ``None`` against a number) raises :class:`ValueError` naming the
    predicate rather than guessing a truth value.
    """
    if isinstance(pred, PredAnd):
        return all(evaluate_predicate(p, resolver) for p in pred.and_)
    if isinstance(pred, PredOr):
        return any(evaluate_predicate(p, resolver) for p in pred.or_)
    if isinstance(pred, PredNot):
        return not evaluate_predicate(pred.not_, resolver)
    if isinstance(pred, PredExists):
        return resolve_response_expr(pred.exists, resolver) is not None
    if isinstance(pred, PredMissing):
        return resolve_response_expr(pred.missing, resolver) is None
    if isinstance(pred, PredEmpty):
        return _is_empty(resolve_response_expr(pred.empty, resolver))
    if isinstance(pred, PredNotEmpty):
        return not _is_empty(resolve_response_expr(pred.not_empty, resolver))

    comparisons = (
        (PredEq, "eq", lambda a, b: a == b),
        (PredNeq, "neq", lambda a, b: a != b),
        (PredLt, "lt", lambda a, b: a < b),
        (PredLte, "lte", lambda a, b: a <= b),
        (PredGt, "gt", lambda a, b: a > b),
        (PredGte, "gte", lambda a, b: a >= b),
    )
    for cls, op, compare in comparisons:
        if isinstance(pred, cls):
            left, right = (
                resolve_response_expr(operand, resolver)
                for operand in getattr(pred, op)
            )
            try:
                return bool(compare(left, right))
            except TypeError as err:
                raise ValueError(
                    f"stop_when {op!r} cannot compare "
                    f"{type(left).__name__} with {type(right).__name__}"
                ) from err
    raise ValueError(f"unknown stop_when predicate {type(pred).__name__!r}")
