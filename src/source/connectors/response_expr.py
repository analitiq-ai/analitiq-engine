"""Response-scope evaluation of contract value expressions and predicates.

The api-endpoint contract declares per-page read behavior against the
provider's response: ``pagination.stop_when`` is a predicate over the parsed
body, and ``cursor.next_cursor`` / ``link.next_url`` are value expressions
resolved from it. This module owns that evaluation, mirroring the contract's
grammar (spec: §Stop Conditions):

* ``{"ref": "response.body[.path]"}`` walks the parsed body; any other ref
  scope is not resolvable from a response and fails loud.
* ``{"literal": value}`` and plain scalars pass through.
* ``template`` / ``function`` forms are declared by the contract's expression
  union but have no response-scope semantics here yet — they fail loud rather
  than being silently skipped or sent on as their raw structure.

Failures raise :class:`ValueError`; the connector translates them into its
deterministic read errors.
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

from ...shared.dict_path import walk_path

_BODY_PREFIX = "response.body"


def _resolve_ref(ref: str, data: Any) -> Any:
    if ref == _BODY_PREFIX:
        return data
    if ref.startswith(_BODY_PREFIX + "."):
        return walk_path(data, ref[len(_BODY_PREFIX) + 1 :].split("."))
    raise ValueError(
        f"ref {ref!r} is not response-scoped; only "
        f"'{_BODY_PREFIX}[.path]' refs can be resolved from a response"
    )


def resolve_response_expr(expr: Any, data: Any) -> Any:
    """Resolve a contract value expression against a parsed response body.

    ``data`` is the decoded JSON payload of the current page. Returns the
    resolved value, with ``None`` meaning the addressed value is absent
    (or stored as null — the two are indistinguishable per ``walk_path``).

    Expression nodes arrive in two forms: parsed models where the contract
    types the field with the expression union (``cursor.next_cursor``,
    ``link.next_url``), and raw single-operator dicts where it types the
    slot ``Any`` (predicate operands). Both follow the same grammar and
    resolve identically; anything that is neither form passes through as a
    literal operand value.
    """
    if isinstance(expr, RefExpr):
        return _resolve_ref(expr.ref, data)
    if isinstance(expr, LiteralExpr):
        return expr.literal
    if isinstance(expr, (TemplateExpr, FunctionExpr)):
        form = "template" if isinstance(expr, TemplateExpr) else "function"
        raise ValueError(f"{form!r} expressions are not supported in response scope")
    if isinstance(expr, dict):
        if "function" in expr:
            # Function nodes carry sibling keys (input/map/safe); detect on
            # the operator key alone.
            raise ValueError(
                "'function' expressions are not supported in response scope"
            )
        if len(expr) == 1:
            key, value = next(iter(expr.items()))
            if key == "ref" and isinstance(value, str):
                return _resolve_ref(value, data)
            if key == "literal":
                return value
            if key == "template":
                raise ValueError(
                    "'template' expressions are not supported in response scope"
                )
    # A plain operand value (e.g. the 0 in {"eq": [{"ref": ...}, 0]}).
    return expr


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


def evaluate_predicate(pred: Any, data: Any) -> bool:
    """Evaluate a contract stop-condition predicate against a response body.

    Comparison operators resolve both operands through
    :func:`resolve_response_expr`; an incomparable pair (e.g. ordering
    ``None`` against a number) raises :class:`ValueError` naming the
    predicate rather than guessing a truth value.
    """
    if isinstance(pred, PredAnd):
        return all(evaluate_predicate(p, data) for p in pred.and_)
    if isinstance(pred, PredOr):
        return any(evaluate_predicate(p, data) for p in pred.or_)
    if isinstance(pred, PredNot):
        return not evaluate_predicate(pred.not_, data)
    if isinstance(pred, PredExists):
        return resolve_response_expr(pred.exists, data) is not None
    if isinstance(pred, PredMissing):
        return resolve_response_expr(pred.missing, data) is None
    if isinstance(pred, PredEmpty):
        return _is_empty(resolve_response_expr(pred.empty, data))
    if isinstance(pred, PredNotEmpty):
        return not _is_empty(resolve_response_expr(pred.not_empty, data))

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
                resolve_response_expr(operand, data) for operand in getattr(pred, op)
            )
            try:
                return bool(compare(left, right))
            except TypeError as err:
                raise ValueError(
                    f"stop_when {op!r} cannot compare "
                    f"{type(left).__name__} with {type(right).__name__}"
                ) from err
    raise ValueError(f"unknown stop_when predicate {type(pred).__name__!r}")
