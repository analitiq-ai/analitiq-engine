"""Unit tests for :mod:`src.source.connectors.response_expr`.

The module evaluates contract value expressions and stop-condition
predicates against a page through the shared CDK resolver's ``response``
scope; these pin the grammar's semantics directly (the pagination tests
exercise them end-to-end).
"""

from __future__ import annotations

import pytest
from analitiq.contracts.endpoints import Cursor, Predicate
from pydantic import TypeAdapter

from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.resolver import ResolutionContext, Resolver
from src.source.connectors.response_expr import (
    evaluate_predicate,
    resolve_response_expr,
)

_PREDICATE = TypeAdapter(Predicate)


def _pred(raw: dict) -> object:
    """Parse a raw predicate dict through the contract's own union."""
    return _PREDICATE.validate_python(raw)


def _expr(raw: object) -> object:
    """Parse a raw value-expression node the way the contract models do."""
    return Cursor.model_validate({"param": "p", "next_cursor": raw}).next_cursor


def _resolver(data: object, *, parameters: dict | None = None) -> Resolver:
    """Response-scoped resolver over *data*, mirroring the connector's."""
    return Resolver(
        ResolutionContext(
            connection={"parameters": parameters or {}},
            response={"body": data},
        ),
        functions=DEFAULT_FUNCTIONS,
    )


class TestResolveResponseExpr:
    def test_body_ref_returns_whole_body(self):
        """``response.body`` addresses the payload itself."""
        assert resolve_response_expr(
            _expr({"ref": "response.body"}), _resolver([1, 2])
        ) == [1, 2]

    def test_dotted_ref_walks_the_body(self):
        """A dotted ref walks nested objects to the terminal value."""
        resolver = _resolver({"meta": {"next": "tok"}})
        assert (
            resolve_response_expr(_expr({"ref": "response.body.meta.next"}), resolver)
            == "tok"
        )

    def test_missing_path_resolves_to_none(self):
        """An absent path is ``None`` — the loop's stop signal, not an error."""
        assert (
            resolve_response_expr(
                _expr({"ref": "response.body.next"}), _resolver({"a": 1})
            )
            is None
        )

    def test_connection_scope_resolves_through_shared_vocabulary(self):
        """Every resolver scope is addressable — one resolution vocabulary."""
        resolver = _resolver({}, parameters={"token": "t-1"})
        assert (
            resolve_response_expr(
                _expr({"ref": "connection.parameters.token"}), resolver
            )
            == "t-1"
        )

    def test_unknown_scope_raises(self):
        """A typo'd scope is an authoring defect, never silently absorbed."""
        with pytest.raises(KeyError, match="scope"):
            resolve_response_expr(_expr({"ref": "bogus.path"}), _resolver({}))

    def test_literal_passes_through(self):
        """A literal expression yields its value."""
        assert resolve_response_expr(_expr({"literal": 5}), _resolver({})) == 5

    def test_plain_scalar_passes_through(self):
        """Predicate operands may be plain scalars, not expression nodes."""
        assert resolve_response_expr(10, _resolver({})) == 10

    def test_template_interpolates_response_values(self):
        """Template expressions interpolate response-scope placeholders."""
        resolver = _resolver({"next_page": 3})
        assert (
            resolve_response_expr(
                _expr({"template": "/items?page=${response.body.next_page}"}),
                resolver,
            )
            == "/items?page=3"
        )

    def test_template_interpolates_decimal_response_values(self):
        """Fractional response values arrive as Decimal (lossless parse)
        and must interpolate exactly, not raise as non-scalar."""
        from decimal import Decimal

        resolver = _resolver({"next_score": Decimal("1234567890.12345678")})
        assert (
            resolve_response_expr(
                _expr({"template": "after=${response.body.next_score}"}), resolver
            )
            == "after=1234567890.12345678"
        )

    def test_function_over_response_values(self):
        """Function expressions run the shared derived-function catalog."""
        import base64

        resolver = _resolver({"token": "abc"})
        assert resolve_response_expr(
            _expr(
                {
                    "function": "base64_encode",
                    "input": {"ref": "response.body.token"},
                }
            ),
            resolver,
        ) == base64.b64encode(b"abc").decode("ascii")


class TestEvaluatePredicate:
    def test_empty_on_absent_and_empty_list(self):
        """``empty`` holds for an absent path and for a zero-length list."""
        pred = _pred({"empty": {"ref": "response.body.records"}})
        assert evaluate_predicate(pred, _resolver({})) is True
        assert evaluate_predicate(pred, _resolver({"records": []})) is True
        assert evaluate_predicate(pred, _resolver({"records": [1]})) is False

    def test_scalars_are_never_empty(self):
        """``empty`` asks "is there nothing here" — 0 and False are values."""
        pred = _pred({"empty": {"ref": "response.body.count"}})
        assert evaluate_predicate(pred, _resolver({"count": 0})) is False
        assert evaluate_predicate(pred, _resolver({"count": False})) is False

    def test_exists_and_missing(self):
        """``exists``/``missing`` test presence of the addressed value."""
        resolver = _resolver({"next": "tok"})
        assert evaluate_predicate(
            _pred({"exists": {"ref": "response.body.next"}}), resolver
        )
        assert not evaluate_predicate(
            _pred({"missing": {"ref": "response.body.next"}}), resolver
        )
        assert evaluate_predicate(
            _pred({"missing": {"ref": "response.body.gone"}}), resolver
        )

    def test_comparisons_resolve_both_operands(self):
        """Comparison operands each resolve through the expression grammar."""
        resolver = _resolver({"page": 3, "total_pages": 3})
        assert evaluate_predicate(
            _pred(
                {
                    "gte": [
                        {"ref": "response.body.page"},
                        {"ref": "response.body.total_pages"},
                    ]
                }
            ),
            resolver,
        )
        assert not evaluate_predicate(
            _pred({"lt": [{"ref": "response.body.page"}, 3]}), resolver
        )
        assert evaluate_predicate(
            _pred({"eq": [{"ref": "response.body.page"}, 3]}), resolver
        )
        assert evaluate_predicate(
            _pred({"neq": [{"ref": "response.body.page"}, 4]}), resolver
        )

    def test_decimal_response_values_compare_with_float_literals(self):
        """Response numbers arrive as Decimal (lossless parse); an authored
        float literal must compare by value, not by binary representation —
        Decimal("0.1") == 0.1 is False without normalization."""
        from decimal import Decimal

        resolver = _resolver({"rate": Decimal("0.1")})
        assert evaluate_predicate(
            _pred({"eq": [{"ref": "response.body.rate"}, 0.1]}), resolver
        )
        assert evaluate_predicate(
            _pred({"lt": [{"ref": "response.body.rate"}, 0.2]}), resolver
        )
        assert not evaluate_predicate(
            _pred({"neq": [0.1, {"ref": "response.body.rate"}]}), resolver
        )

    def test_incomparable_operands_raise(self):
        """Ordering ``None`` against a number is unanswerable — fail loud."""
        with pytest.raises(ValueError, match="cannot compare"):
            evaluate_predicate(
                _pred({"lt": [{"ref": "response.body.absent"}, 10]}), _resolver({})
            )

    def test_boolean_composition(self):
        """``and``/``or``/``not`` recurse over their branches."""
        resolver = _resolver({"records": [], "has_more": False})
        pred = _pred(
            {
                "and": [
                    {"empty": {"ref": "response.body.records"}},
                    {"not": {"eq": [{"ref": "response.body.has_more"}, True]}},
                ]
            }
        )
        assert evaluate_predicate(pred, resolver) is True
        assert (
            evaluate_predicate(pred, _resolver({"records": [1], "has_more": False}))
            is False
        )
        assert evaluate_predicate(
            _pred(
                {
                    "or": [
                        {"not_empty": {"ref": "response.body.records"}},
                        {"exists": {"ref": "response.body.has_more"}},
                    ]
                }
            ),
            resolver,
        )
