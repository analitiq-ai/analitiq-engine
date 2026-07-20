"""Predicate evaluation for the endpoint contract's stop/success conditions.

The api-endpoint contract expresses "when is this loop done" and "did this
write succeed" as a predicate object rather than as engine-side heuristics.
A predicate carries exactly one operator key; its operands are ordinary
value expressions resolved through :class:`cdk.resolver.Resolver`, so a
predicate can address any resolution scope the caller has populated
(typically ``response.*`` for a page that just came back).

Operators, per the contract's predicate grammar:

* ``eq`` / ``neq`` / ``lt`` / ``lte`` / ``gt`` / ``gte`` — two operands.
* ``exists`` / ``missing`` — does the operand resolve to a value at all.
* ``empty`` / ``not_empty`` — does the operand resolve to nothing or to an
  empty string/list/dict.
* ``and`` / ``or`` / ``not`` — nesting.

Two operand policies, and the split is the whole point of the module. The
absence-testing operators (``exists``/``missing``/``empty``/``not_empty``)
are the contract's designated way to ask about data that may not be there,
so an unresolved operand is an *answer* for them, not a failure. The
comparison operators are not: an author writing
``{"gte": [{"ref": "response.body.page"}, {"ref": "response.body.pages"}]}``
against a provider that sends neither field has a defect, and inventing a
truth value there would either truncate a read or spin forever. So
comparison operands that do not resolve raise
:class:`~cdk.exceptions.TransportSpecError`, naming the operator and the
operand.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from cdk.exceptions import TransportSpecError, UnresolvedValueError

logger = logging.getLogger(__name__)


class _Unresolved:
    """Sentinel for an operand whose reference is not present in the context.

    Distinct from a resolved ``None``: ``{"literal": null}`` and a provider
    field that is genuinely absent read the same through
    ``resolve_for_request``, and ``exists``/``missing`` have to tell them
    apart, so operand resolution here keeps the strict path and converts
    only the missing-data failure into this sentinel.
    """

    _INSTANCE: _Unresolved | None = None

    def __new__(cls) -> _Unresolved:
        if cls._INSTANCE is None:
            cls._INSTANCE = super().__new__(cls)
        return cls._INSTANCE

    def __repr__(self) -> str:
        return "<unresolved>"


UNRESOLVED = _Unresolved()

# Two-operand comparisons. Ordering comparisons additionally require mutually
# comparable operands, which Python enforces for us (a TypeError is caught in
# :func:`_compare`).
_COMPARISONS: frozenset[str] = frozenset({"eq", "neq", "lt", "lte", "gt", "gte"})
_ABSENCE: frozenset[str] = frozenset({"exists", "missing", "empty", "not_empty"})
_NESTING: frozenset[str] = frozenset({"and", "or", "not"})

PREDICATE_OPERATORS: frozenset[str] = _COMPARISONS | _ABSENCE | _NESTING


def evaluate_predicate(node: Any, resolver: Any) -> bool:
    """Evaluate one contract predicate against *resolver*'s context.

    *resolver* must expose ``resolve(expr)`` with the strict failure policy
    (:class:`cdk.resolver.Resolver`) — this walks the missing-data case
    itself rather than letting the per-request drop policy flatten it.

    Raises :class:`~cdk.exceptions.TransportSpecError` for an authoring
    defect: a non-object predicate, no operator key, more than one operator
    key, the wrong operand count, an unresolved comparison operand, or
    operands that cannot be ordered against each other.
    """
    operator, operand = _single_operator(node)

    if operator in _NESTING:
        return _nest(operator, operand, resolver)
    if operator in _ABSENCE:
        return _absence(operator, operand, resolver)
    return _compare(operator, operand, resolver)


def _single_operator(node: Any) -> tuple[str, Any]:
    """Extract the one operator key and its operand, or name what is wrong.

    ``x-*`` keys are the contract's extension escape hatch and are never
    operators; anything else unrecognized is a typo worth surfacing by name.
    """
    if not isinstance(node, Mapping):
        raise TransportSpecError(
            f"predicate must be an object, got {type(node).__name__}: {node!r}"
        )
    operators = [
        key for key in node if isinstance(key, str) and not key.startswith("x-")
    ]
    unknown = [key for key in operators if key not in PREDICATE_OPERATORS]
    if unknown:
        raise TransportSpecError(
            f"predicate has unknown operator(s) {sorted(unknown)}; "
            f"valid operators: {sorted(PREDICATE_OPERATORS)}"
        )
    if len(operators) != 1:
        raise TransportSpecError(
            f"predicate must contain exactly one operator key, got "
            f"{sorted(operators)}"
        )
    return operators[0], node[operators[0]]


def _nest(operator: str, operand: Any, resolver: Any) -> bool:
    """Evaluate ``and`` / ``or`` / ``not``."""
    if operator == "not":
        return not evaluate_predicate(operand, resolver)
    branches = _branches(operand, operator)
    # Short-circuiting, deliberately. `_compare` refuses an operand that does
    # not resolve and tells the author to guard it with an absence operator,
    # so `{"or": [{"missing": A}, {"gte": [A, B]}]}` is the grammar's own
    # idiom for "stop when the field is gone, otherwise compare it".
    # Evaluating every branch would raise on the guarded comparison exactly
    # when the guard says not to look, making the documented pattern
    # unusable. The cost is that a malformed later branch stays unseen while
    # an earlier one decides the result; `_absence` logs the unresolved
    # operands that would otherwise hide a typo.
    results = (evaluate_predicate(child, resolver) for child in branches)
    return all(results) if operator == "and" else any(results)


def _absence(operator: str, operand: Any, resolver: Any) -> bool:
    """Evaluate ``exists`` / ``missing`` / ``empty`` / ``not_empty``."""
    value = _resolve_operand(operand, resolver)
    if value is UNRESOLVED:
        # A genuine answer for these operators, so it is not an error -- but
        # a stop condition on a mistyped sub-path (`response.body.objcts`)
        # reads as "empty" on every page and ends the read after the first
        # one, reporting success. The scope token is all the contract can
        # validate, so this log is the only place the mistake is visible.
        logger.warning(
            "predicate %r: operand %r did not resolve; answering from its "
            "absence. If the read ends earlier than expected, check this "
            "path against the provider's response",
            operator,
            operand,
        )
    if operator == "exists":
        return value is not UNRESOLVED
    if operator == "missing":
        return value is UNRESOLVED
    # Only containers and absence count as empty. ``0`` and ``False`` are
    # values: a page-count of zero conflated with absence is how a caller ends
    # up stopping on a legitimate result.
    is_empty = (
        value is UNRESOLVED
        or value is None
        or (isinstance(value, (str, bytes, list, tuple, dict, set)) and len(value) == 0)
    )
    return is_empty if operator == "empty" else not is_empty


def _branches(operand: Any, operator: str) -> list[Any]:
    """Operand list for ``and`` / ``or``, which the contract requires non-empty.

    An empty list would make ``and`` vacuously true (stop immediately) and
    ``or`` vacuously false (never stop) — opposite silent failures from the
    same typo, so it is rejected rather than given a default.
    """
    if not isinstance(operand, list) or not operand:
        raise TransportSpecError(
            f"predicate {operator!r} requires a non-empty list of predicates, "
            f"got {operand!r}"
        )
    return operand


def _align_numbers(left: Any, right: Any) -> tuple[Any, Any]:
    """Put a Decimal and a float on one scale before they are compared.

    Response bodies are parsed with ``parse_float=Decimal`` so a provider's
    digits survive to Arrow exactly, while a literal in the endpoint document
    arrives from ordinary JSON parsing as a ``float``. Python then compares
    the two by widening the Decimal to the float's binary value, so operands
    that are identical in JSON are not equal here: ``Decimal("0.1") == 0.1``
    is False, and ``Decimal("0.1") >= 0.1`` is False as well. A stop predicate
    written against a fractional field would never fire, and the read would
    run past its real last page.

    Converting the float side rather than the Decimal side keeps the
    provider's value exact -- it is the one being tested -- and gives the
    literal the value its digits spell. Non-numbers, and int/Decimal pairs
    (already exact together), are returned untouched.
    """
    if isinstance(left, float) and isinstance(right, Decimal):
        return Decimal(str(left)), right
    if isinstance(left, Decimal) and isinstance(right, float):
        return left, Decimal(str(right))
    return left, right


def _compare(operator: str, operand: Any, resolver: Any) -> bool:
    """Evaluate one two-operand comparison."""
    if not isinstance(operand, list) or len(operand) != 2:
        raise TransportSpecError(
            f"predicate {operator!r} requires exactly two operands, got {operand!r}"
        )
    left, right = (_resolve_operand(item, resolver) for item in operand)
    for label, value, expr in (
        ("left", left, operand[0]),
        ("right", right, operand[1]),
    ):
        if value is UNRESOLVED:
            raise TransportSpecError(
                f"predicate {operator!r}: {label} operand {expr!r} did not "
                f"resolve. Comparison operators require a value; use "
                f"`exists`/`missing`/`empty`/`not_empty` to test for a field "
                f"the provider may omit"
            )

    left, right = _align_numbers(left, right)
    if operator == "eq":
        return bool(left == right)
    if operator == "neq":
        return bool(left != right)
    try:
        if operator == "lt":
            return bool(left < right)
        if operator == "lte":
            return bool(left <= right)
        if operator == "gt":
            return bool(left > right)
        return bool(left >= right)
    except (TypeError, InvalidOperation) as err:
        # InvalidOperation, not just TypeError: `json.loads` accepts a bare
        # `NaN` token, and ordering any Decimal against a NaN raises it. It
        # is an ArithmeticError, so the narrower guard let it escape as an
        # unclassified failure and the read was retried to exhaustion on a
        # response that will never parse differently.
        raise TransportSpecError(
            f"predicate {operator!r}: cannot order {type(left).__name__} "
            f"against {type(right).__name__} ({left!r} vs {right!r})"
        ) from err


def _resolve_operand(expr: Any, resolver: Any) -> Any:
    """Resolve one operand, mapping the missing-data case to :data:`UNRESOLVED`.

    Only :class:`~cdk.exceptions.UnresolvedValueError` — a well-formed
    reference to data that is not there — becomes the sentinel. Authoring
    defects (an unknown scope, conflicting expression markers, an unknown
    function) propagate, because the absence-testing operators must not
    swallow a typo'd ref into a confident "missing".
    """
    try:
        return resolver.resolve(expr)
    except UnresolvedValueError as err:
        logger.debug("predicate operand %r unresolved: %s", expr, err)
        return UNRESOLVED
